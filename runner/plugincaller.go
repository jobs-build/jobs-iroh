//go:build linux

package runner

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"

	"github.com/amber-store/core/key"
	"github.com/fxamacker/cbor/v2"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/recipe"
	"github.com/jobs-build/jobs-iroh/sandbox"
	"github.com/jobs-build/jobs-iroh/tailbuf"
)

// exTempFail (EX_TEMPFAIL == 75) marks a retryable plugin failure (build.md §6,
// §11); it is declared in importjob.go and shared across the runner package.

// Paths inside the plugin sandbox (post-pivot). The shell and the plugin live in
// one content-addressed store mount at /jobs/store/<BOK> (build.md §6), like the
// build sandbox. A /jobs/shell compat symlink points at the shell's store entry
// so a plugin's fixed #!/jobs/shell/bin/bash shebang still resolves (the store
// path is content-dynamic). The plugin runs with its store dir as CWD and reads
// the read-only source tree at /jobs/source (passed in the CBOR request).
const (
	pluginStoreDir    = "/jobs/store"  // content-addressed store (shell + plugin), read-only
	pluginShellCompat = "/jobs/shell"  // compat symlink -> /jobs/store/<shellBOK>
	pluginSourceDir   = "/jobs/source" // materialized source tree, read-only
)

// respDec decodes a plugin's CBOR response with string map keys, so nested maps
// surface as map[string]any (matching recipe's toStarlark/asInputSpec). Mirrors
// recipe.respDec (unexported there); re-created here at the runner boundary.
var respDec = func() cbor.DecMode {
	m, err := cbor.DecOptions{DefaultMapType: reflect.TypeOf(map[string]any(nil))}.DecMode()
	if err != nil {
		panic(err)
	}
	return m
}()

// pluginRequest is the CBOR stdin payload (build.md §6): the call kwargs, the
// in-sandbox path of the read-only source tree, and — when the recipe declared
// resolution deps — their in-sandbox mount paths by name (resolution-deps
// design §3.3). Dir is the build dir within the source tree (sibling-sources
// design §9): for a widened build /jobs/source is the WHOLE context and Dir
// tells the plugin where the consumer package lives; legacy builds omit it
// (source root == build root). omitempty keeps old-shaped requests
// byte-identical.
type pluginRequest struct {
	Call   map[string]any    `cbor:"call"`
	Source string            `cbor:"source"`
	Deps   map[string]string `cbor:"deps,omitempty"`
	Dir    string            `cbor:"dir,omitempty"`
}

// SandboxedPluginCaller runs a plugin BINARY hermetically and exchanges CBOR over
// stdio (build.md §6). Unlike the build executor it is not build-script-shaped:
// the plugin's own executable (./plugin at the artifact root) is exec'd with the
// CBOR request on stdin and its CBOR response captured from stdout, net-free,
// with the source tree mounted read-only.
//
// The plugin artifact (PluginKey) and the shell (ShellKey) are assembled into one
// content-addressed store, materialized to disk, and bind-mounted read-only at
// /jobs/store (provisionStore — jobs-iroh is materialize-only, no FUSE); the
// plugin's own binary is exec'd from there. The already-materialized SourceDir
// is bind-mounted read-only, and the vendored shell is statically linked
// (build.md §8, §14) so no /nix bind is needed. The sandbox is
// User+Mount+PID+Net+UTS+IPC (Net ⇒ net=none).
type SandboxedPluginCaller struct {
	Cl        *amber.Store
	PluginKey key.Key
	SourceDir string          // host path of the materialized source tree
	ShellKey  key.Key         // vendored bash+coreutils artifact
	Ctx       context.Context // nil → context.Background()
	// StderrSink, when non-nil, additionally receives the plugin's full
	// stderr (build-events output capture). Stdout is NEVER captured as
	// output — it is the CBOR protocol response.
	StderrSink io.Writer
	// DepDirs are the materialized resolution deps (name → host dir), each
	// bind-mounted read-only at /jobs/deps/<name> and announced to the plugin
	// via the request's deps map (resolution-deps design §3.3).
	DepDirs map[string]string
	// Dir is the build dir within the mounted source tree (sibling-sources
	// design §9); "" for legacy narrow builds.
	Dir string
}

var _ recipe.PluginCaller = SandboxedPluginCaller{}

func (p SandboxedPluginCaller) Call(kwargs map[string]any) (any, error) {
	ctx := p.Ctx
	if ctx == nil {
		ctx = context.Background()
	}

	// Resolution deps: stable order for deterministic mount setup; each dep is
	// bound read-only at /jobs/deps/<name> and its path carried in the request.
	depNames := make([]string, 0, len(p.DepDirs))
	for name := range p.DepDirs {
		depNames = append(depNames, name)
	}
	sort.Strings(depNames)
	var reqDeps map[string]string
	depMounts := make([]sandbox.Mount, 0, len(depNames))
	if len(depNames) > 0 {
		reqDeps = make(map[string]string, len(depNames))
		for _, name := range depNames {
			tgt := pluginDepsDir + "/" + name
			reqDeps[name] = tgt
			depMounts = append(depMounts, sandbox.Mount{Source: p.DepDirs[name], Target: tgt, ReadOnly: true})
		}
	}

	reqBytes, err := cbor.Marshal(pluginRequest{Call: kwargs, Source: pluginSourceDir, Deps: reqDeps, Dir: p.Dir})
	if err != nil {
		return nil, fmt.Errorf("encode plugin request: %w", err)
	}

	work, err := os.MkdirTemp("", "jobs-plugin-")
	if err != nil {
		return nil, fmt.Errorf("create plugin work dir: %w", err)
	}
	newRoot := filepath.Join(work, "root")
	defer os.RemoveAll(work)

	// One content-addressed store holds both the shell and the plugin at
	// /jobs/store/<BOK> (build.md §6), mirroring the build sandbox: materialized
	// to disk under the work tree and bind-mounted read-only via provisionStore
	// (the SourceDir/tmp mounts are likewise expressed as sandbox.Mount so
	// childSetup binds them into newRoot).
	shellStoreDir := pluginStoreDir + "/" + p.ShellKey.String()
	pluginStorePath := pluginStoreDir + "/" + p.PluginKey.String()
	storeKey, err := p.Cl.BuildStoreTree(ctx, []key.Key{p.ShellKey, p.PluginKey})
	if err != nil {
		return nil, fmt.Errorf("assemble plugin store: %w", err)
	}
	storeBinds, err := provisionStore(ctx, p.Cl, storeKey, newRoot, work)
	if err != nil {
		return nil, err
	}
	// Compat symlink so a plugin's fixed #!/jobs/shell/bin/bash shebang resolves to
	// the content-addressed shell (whose store path is dynamic). provisionStore
	// already created newRoot/jobs (the parent of the /jobs/store mountpoint).
	if err := os.Symlink(shellStoreDir, filepath.Join(newRoot, "jobs", "shell")); err != nil {
		return nil, fmt.Errorf("symlink compat shell: %w", err)
	}

	// Minimal /etc/hosts + empty resolv.conf so resolver paths fail fast
	// instead of hanging forever under net=none (issue #99) — same guarantee
	// as the build sandbox.
	if err := writeHermeticEtc(newRoot); err != nil {
		return nil, err
	}

	var out bytes.Buffer
	tail := tailbuf.New(4 << 10)
	var stderr io.Writer = tail
	if p.StderrSink != nil {
		stderr = io.MultiWriter(tail, p.StderrSink)
	}

	cfg := sandbox.Config{
		Command: []string{pluginStorePath + "/plugin"},
		Dir:     pluginStorePath,
		Env: []string{
			"PATH=" + shellStoreDir + "/bin",
			"HOME=/tmp",
		},
		NewRoot: newRoot,
		Mounts: append([]sandbox.Mount{
			// The materialized source tree, read-only. It is already on disk on the
			// host; expressed as a sandbox.Mount so childSetup binds it into newRoot
			// (the host path is still visible to the child pre-pivot).
			{Source: p.SourceDir, Target: pluginSourceDir, ReadOnly: true},
			// The vendored shell is statically linked (build.md §8, §14), so no
			// /nix/store bind is needed. A plugin binary must likewise be relocatable
			// (static or scripted via the vendored shell) to run hermetically here.
			// A writable tmpfs /tmp for plugin scratch + $HOME (neither is on the
			// read-only root).
			{Source: "tmpfs", Target: "/tmp", FSType: "tmpfs"},
		}, append(depMounts, storeBinds...)...),
		Namespaces: sandbox.Namespaces{
			User: true, Mount: true, PID: true, Net: true, UTS: true, IPC: true,
		},
		Stdin:  bytes.NewReader(reqBytes),
		Stdout: &out,
		Stderr: stderr,
	}

	code, runErr := sandbox.Run(ctx, cfg)
	if runErr != nil {
		// Infra failure: the plugin did not run to completion.
		return nil, fmt.Errorf("run plugin sandbox: %w", runErr)
	}
	switch code {
	case 0:
		var v any
		if err := respDec.Unmarshal(out.Bytes(), &v); err != nil {
			return nil, fmt.Errorf("decode plugin response: %w", err)
		}
		return v, nil
	case exTempFail:
		return nil, &recipe.PluginError{ExitCode: exTempFail, Retryable: true, Stderr: tail.String()}
	default:
		return nil, &recipe.PluginError{ExitCode: code, Stderr: tail.String()}
	}
}
