//go:build linux

package runner

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/sandbox"
)

// RunIO wires the run child's standard streams. The CLI passes the host's
// os.Stdin/Stdout/Stderr; tests pass buffers.
type RunIO struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// RunFromSource builds the local source's target (exactly as `jobs develop`
// prepares it) and then EXECUTES the built artifact's JOBS.entrypoint instead of
// opening a shell. It publishes build-output:F + build-output-deps:F so an
// unchanged target reuses its cached output on the next run. Returns the
// entrypoint's exit code (a non-zero exit is NOT a Go error); a Go error means
// the build or sandbox setup failed. p receives the build phase's step
// events (nil for silence); a nil p from the CLI is replaced by the classic
// plain reporter on rio.Stderr so `run` never builds mutely by accident.
func RunFromSource(ctx context.Context, st *amber.Store, cfg DevelopConfig, extraArgs []string, rio RunIO, p *Progress) (int, error) {
	if p == nil {
		p = NewProgress(rio.Stderr) // progress to the run command's stderr
	}
	art, err := prepareSourceArtifact(ctx, st, cfg, p)
	if err != nil {
		return -1, err
	}
	// The run store mounts the artifact itself, the shell, and the dep closure.
	runStoreKey, err := st.BuildStoreTree(ctx, append(art.depBOKs, art.bokSelf, art.shellKey))
	if err != nil {
		return -1, fmt.Errorf("assemble run store: %w", err)
	}
	return runEntrypoint(ctx, st, runStoreKey, art.bokSelf, art.shellKey, art.ep, extraArgs, rio)
}

// BuildFromSource hermetically builds cfg's target (exactly as `jobs develop`
// prepares it) WITHOUT executing anything afterwards, returning the build's
// local identity F. It resolves the shell, computes F via localBuildFrom
// (IngestSourceDir → env subtree → BuildFromTree), and drives the F-keyed
// pipeline to completion (driveFStages with runFinal=true), tagging
// build-output:F + build-output-deps:F so an unchanged target is a near no-op
// on subsequent builds. The build-only exported entry point of the local path
// (jobs-client `build`); RunFromSource layers entrypoint execution on top.
func BuildFromSource(ctx context.Context, st *amber.Store, cfg DevelopConfig, p *Progress) (key.Key, error) {
	platform := cfg.Platform
	if platform == "" {
		platform = Platform()
	}
	shellRef := cfg.ShellRef
	if shellRef == "" {
		shellRef = "shell:" + platform
	}
	shellKey, ok, err := st.GetKey(ctx, shellRef)
	if err != nil {
		return key.Key{}, fmt.Errorf("resolve %s: %w", shellRef, err)
	}
	if !ok {
		return key.Key{}, fmt.Errorf("shell artifact %q not found (seed missing — restart to re-seed)", shellRef)
	}
	brc := BuildRunCfg{Platform: platform, ShellKey: shellKey, CacheDir: cfg.CacheDir}

	bf := p.Start("build-from")
	f, err := localBuildFrom(ctx, st, brc, cfg)
	if err != nil {
		bf(err)
		return key.Key{}, err
	}
	bf(nil)

	d := &developDriver{
		ctx: ctx, st: st, rw: NewLocalRefWriter(st), brc: brc, secrets: cfg.Secrets,
		visited: map[string]bool{}, inProgress: map[string]bool{},
	}
	if err := d.driveFStages(f, true, p); err != nil { // runFinal=true: write build-output:F (or join)
		return key.Key{}, err
	}
	return f, nil
}

// prepareSourceArtifact builds cfg's target hermetically (BuildFromSource) and
// returns everything `run` and `image` need: the artifact (c/) BOK, the shell,
// the runtime-closure dep BOKs, and the JOBS.entrypoint. Linux-only: it drives
// the namespace build sandbox via the F-keyed orchestrator (driveFStages),
// tagging build-output:F so an unchanged target is a near no-op on subsequent
// runs. Shared by RunFromSource and BuildImageFromSource.
func prepareSourceArtifact(ctx context.Context, st *amber.Store, cfg DevelopConfig, p *Progress) (resolvedArtifact, error) {
	platform := cfg.Platform
	if platform == "" {
		platform = Platform()
	}
	shellRef := cfg.ShellRef
	if shellRef == "" {
		shellRef = "shell:" + platform
	}
	f, err := BuildFromSource(ctx, st, cfg, p)
	if err != nil {
		return resolvedArtifact{}, err
	}
	// The build is tagged at build-output:F; resolve the runnable artifact.
	// resolveByKeyArtifact tries two-hop (build-from:F) first; that won't exist
	// for a local F so it falls back to direct build-output:F lookup.
	return resolveByKeyArtifact(ctx, st, f, platform, shellRef, true)
}

// RunByKey executes an already-built output (build-output:K) by its build key K:
// it resolves the artifact + its materialized runtime closure from the store (via
// the shared resolveByKeyArtifact, also used by `jobs image`) and executes the
// JOBS.entrypoint. No building is done.
func RunByKey(ctx context.Context, st *amber.Store, k key.Key, platform, shellRef string, extraArgs []string, rio RunIO) (int, error) {
	art, err := resolveByKeyArtifact(ctx, st, k, platform, shellRef, true)
	if err != nil {
		return -1, err
	}
	// The run store mounts the artifact itself, the shell, and the dep closure.
	boks := append([]key.Key{art.bokSelf, art.shellKey}, art.depBOKs...)
	runStoreKey, err := st.BuildStoreTree(ctx, boks)
	if err != nil {
		return -1, fmt.Errorf("assemble run store: %w", err)
	}
	return runEntrypoint(ctx, st, runStoreKey, art.bokSelf, art.shellKey, art.ep, extraArgs, rio)
}

// runEntrypoint assembles the run sandbox — the runtime-closure store at
// /jobs/store, host network, host /dev, a writable /tmp, the artifact as cwd —
// and executes the entrypoint command. It is deliberately NOT hermetic: it runs a
// built artifact, so it shares the host network (a server binds host localhost)
// and binds host /dev, exactly as `jobs develop` relaxes /dev for a shell.
//
// The store is materialized to real disk (extracted, not mounted lazily) —
// jobs-iroh is materialize-only. Extracting keeps the same /jobs/store/<BOK>
// layout the artifact's baked-in paths expect.
func runEntrypoint(ctx context.Context, st *amber.Store, runStoreKey, bokSelf, shellBOK key.Key, ep Entrypoint, extraArgs []string, rio RunIO) (int, error) {
	work, err := os.MkdirTemp("", "jobs-run-")
	if err != nil {
		return -1, fmt.Errorf("create run work dir: %w", err)
	}
	defer os.RemoveAll(work)
	newRoot := filepath.Join(work, "root")

	storeMP := filepath.Join(newRoot, "jobs", "store")
	if err := os.MkdirAll(storeMP, 0o755); err != nil {
		return -1, fmt.Errorf("mkdir store dir: %w", err)
	}
	rc, err := st.Tar(ctx, runStoreKey, "")
	if err != nil {
		return -1, fmt.Errorf("read run store: %w", err)
	}
	err = extractTar(rc, storeMP)
	rc.Close()
	if err != nil {
		return -1, fmt.Errorf("materialize run store: %w", err)
	}

	// Compat symlink /jobs/shell -> /jobs/store/<shellBOK> so a script entrypoint's
	// fixed #!/jobs/shell/bin/bash shebang resolves (mirrors the plugin sandbox).
	if err := os.Symlink(storePath(shellBOK), filepath.Join(newRoot, "jobs", "shell")); err != nil {
		return -1, fmt.Errorf("symlink compat shell: %w", err)
	}

	outRoot := storePath(bokSelf) // the artifact root: cwd + base for a relative command
	cmdPath := ep.Command
	if !strings.HasPrefix(cmdPath, "/") {
		cmdPath = outRoot + "/" + cmdPath
	}
	argv := append([]string{cmdPath}, ep.Args...)
	argv = append(argv, extraArgs...)

	cfg := sandbox.Config{
		Command: argv,
		Env:     runEnv(ep.Env, bokSelf, shellBOK),
		Dir:     outRoot,
		NewRoot: newRoot,
		Mounts: []sandbox.Mount{
			{Source: "tmpfs", Target: "/tmp", FSType: "tmpfs"},
			{Source: "/dev", Target: "/dev"}, // recursive bind (applyMount adds MS_REC)
		},
		// Net=false ⇒ share the host network namespace so a server binds host
		// localhost. The other namespaces still isolate fs/pids/uts/ipc.
		Namespaces: sandbox.Namespaces{User: true, Mount: true, PID: true, Net: false, UTS: true, IPC: true},
		Stdin:      rio.Stdin,
		Stdout:     rio.Stdout,
		Stderr:     rio.Stderr,
	}
	return sandbox.Run(ctx, cfg)
}

// runEnv builds the run child's environment: a small structural base (PATH with
// the artifact's bin + the shell's bin, HOME=/tmp) overlaid with the entrypoint's
// env (which wins). No JOBS_DEPS — the entrypoint is static and any baked-in
// /jobs/store/<BOK> references resolve through the mounted closure.
func runEnv(epEnv map[string]string, bokSelf, shellBOK key.Key) []string {
	merged := map[string]string{
		"PATH": storePath(bokSelf) + "/bin:" + storePath(shellBOK) + "/bin",
		"HOME": "/tmp",
	}
	for k, v := range epEnv {
		merged[k] = v
	}
	out := make([]string, 0, len(merged))
	for k, v := range merged {
		out = append(out, k+"="+v)
	}
	return out
}
