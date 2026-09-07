//go:build linux

package runner

// The local build orchestrator + the interactive develop shell (jobs'
// develop_linux.go): the developDriver (memoized, cycle-detected,
// skip-by-ref-existence ensure loops) the local build/run pipeline shares,
// plus prepareDevelop/developRun/RunDevelop — the PTY shell in the EXACT
// hermetic build sandbox the runner would use (script printed, not run).

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"syscall"

	"github.com/amber-store/core/key"
	"github.com/creack/pty"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/importdef"
	"github.com/jobs-build/jobs-iroh/sandbox"
	"golang.org/x/term"
)

// DevelopConfig configures a `jobs develop` / local `jobs run --source` run.
type DevelopConfig struct {
	SourceDir string // local dir to ingest as the build source (default ".")
	Dir       string // build root within the source (where BUILD.jobs lives)
	BuildFile string // optional recipe path relative to Dir (default BUILD.jobs)
	Platform  string // default Platform()
	Params    []byte // canonical (CBOR) build params
	ShellRef  string // default "shell:<platform>"
	CacheDir  string // fetcher artifact cache
	Secrets   map[string]TagSecret
}

// developDriver makes a dependency graph present in the local amber store,
// depth-first, reusing the runner's stage drivers. visited de-dups by node.
type developDriver struct {
	ctx        context.Context
	st         *amber.Store
	rw         RefWriter // the local ref publisher for the stage-driver calls (RefWriter seam)
	brc        BuildRunCfg
	secrets    map[string]TagSecret
	visited    map[string]bool
	inProgress map[string]bool
}

// ensureInput makes one builddef.Input's value present: ingest its definition
// (so it is readable by key), then build or import it by kind.
// ensureInput drives one input to done. name is the recipe-declared
// display name for progress labels; empty falls back to key forms.
func (d *developDriver) ensureInput(in builddef.Input, name string, p *Progress) error {
	if _, err := d.st.IngestFile(d.ctx, in.Definition); err != nil {
		return fmt.Errorf("ingest input definition: %w", err)
	}
	k, err := in.Key()
	if err != nil {
		return err
	}
	switch in.Kind {
	case builddef.KindImport:
		idef, err := importdef.Decode(in.Definition)
		if err != nil {
			return fmt.Errorf("decode import definition: %w", err)
		}
		return d.ensureImport(k, idef, p)
	case builddef.KindBuild:
		return d.ensureBuild(k, name, p)
	case builddef.KindTree:
		// A tree input is already-present content (a sub-build's source, a
		// subtree of the build root). There is nothing to build or import; the
		// build-from stage (RunBuildFrom) resolves it directly.
		return nil
	default:
		return fmt.Errorf("unknown input kind %q", in.Kind)
	}
}

func (d *developDriver) ensureImport(k key.Key, idef importdef.Definition, p *Progress) error {
	node := "import|" + k.String()
	if d.visited[node] {
		return nil
	}
	if d.inProgress[node] {
		return fmt.Errorf("dependency cycle detected at %s", node)
	}
	d.inProgress[node] = true
	defer delete(d.inProgress, node)
	label := importFetchLabel(idef)
	if _, ok, err := d.st.GetKey(d.ctx, "import-output:"+k.String()); err != nil {
		return err
	} else if ok {
		p.Cached(label)
		d.visited[node] = true
		return nil
	}
	// A recipe-declared fetcher is an ordinary build dependency: drive its
	// build first (joining the shared content-addressed cache — a fetcher built
	// elsewhere is found, not rebuilt), then RunImport resolves its artifact by
	// content (recipe-declared-fetchers design §9). Named fetchers resolve
	// against the self-seeded refs inside RunImport; a miss fails there with a
	// clear message.
	if len(idef.FetcherDef) > 0 {
		if err := d.ensureInput(builddef.Input{Kind: builddef.KindBuild, Definition: idef.FetcherDef}, "fetcher "+idef.Fetcher, p); err != nil {
			return fmt.Errorf("build fetcher for import: %w", err)
		}
	}
	done := p.Start(label)
	// The hermetic import root is part of the fetcher contract: a fetcher's
	// runtime closure is mounted at the /jobs/store paths its build baked
	// (e.g. gomod's env.sh toolchain), so the local pipeline must use the
	// same executor as the runner — Subprocess would leave those paths
	// dangling and leak the host userland. Falls back to Subprocess (with a
	// one-time notice) only where user namespaces are unavailable.
	out := RunImport(d.ctx, d.st, d.rw, defaultImportExecutor(0), d.brc.CacheDir, k, d.secrets, nil)
	if err := outcomeErr("import "+k.String(), out); err != nil {
		done(err)
		return err
	}
	done(nil)
	d.visited[node] = true
	return nil
}

// importFetchLabel renders an import's progress label as "fetch <fetcher> <k=v ...>",
// showing the fetcher name and its arguments (params, sorted by key) instead of
// the opaque content key. Params that aren't a flat JSON object (or are empty)
// render as just "fetch <fetcher>".
func importFetchLabel(idef importdef.Definition) string {
	s := "fetch " + idef.Fetcher
	pj, err := idef.ParamsJSON()
	if err != nil {
		return s
	}
	var m map[string]any
	if json.Unmarshal(pj, &m) != nil || len(m) == 0 {
		return s
	}
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		s += fmt.Sprintf(" %s=%v", name, m[name])
	}
	return s
}

func (d *developDriver) ensureBuild(k key.Key, name string, p *Progress) error {
	node := "build|" + k.String()
	if d.visited[node] {
		return nil
	}
	if d.inProgress[node] {
		return fmt.Errorf("dependency cycle detected at %s", node)
	}
	d.inProgress[node] = true
	defer delete(d.inProgress, node)
	// Check via two-hop: build-from:K -> F -> build-output:F.
	if _, ok, err := d.st.ResolveBuildOutput(d.ctx, k); err != nil {
		return err
	} else if ok {
		p.Cached("build " + buildLabel(k, name) + " (build)")
		d.visited[node] = true
		return nil
	}

	// build def (source Input) was ingested by ensureInput; key is k.
	defBytes, err := pullFileBytes(d.ctx, d.st, k)
	if err != nil {
		return fmt.Errorf("read build def %s: %w", k.String(), err)
	}
	def, err := builddef.DecodeDefinition(defBytes)
	if err != nil {
		return fmt.Errorf("decode build def %s: %w", k.String(), err)
	}
	if err := d.ensureInput(def.Source, "", p.Sub()); err != nil {
		return fmt.Errorf("build %s source: %w", k.String(), err)
	}
	done := p.Start("build-from " + buildLabel(k, name) + " (build)")
	bfOut := RunBuildFrom(d.ctx, d.st, d.rw, d.brc, k)
	if err := outcomeErr("build-from "+k.String(), bfOut); err != nil {
		done(err)
		return err
	}
	done(nil)
	if err := d.driveFStages(bfOut.OutputKey, true, p.Sub()); err != nil {
		return err
	}
	d.visited[node] = true
	return nil
}

// buildLabel renders a build step's display name: the recipe dep name
// when known, else the def key.
func buildLabel(k key.Key, name string) string {
	if name != "" {
		return name
	}
	return k.String()
}

// outcomeErr turns a non-success Outcome into a descriptive error.
func outcomeErr(what string, out Outcome) error {
	switch {
	case out.Cancelled:
		return fmt.Errorf("%s cancelled", what)
	case out.Decline:
		return fmt.Errorf("%s declined: %s", what, out.DeclineReason)
	case out.Failed:
		return fmt.Errorf("%s failed (%s, phase %s, exit %d): %s", what, out.Class, out.Phase, out.ExitCode, out.Stderr)
	default:
		return nil
	}
}

// prepareDevelop computes F for the local target, drives it through the F-keyed
// stages (tagging build-plugin-resolved:F + build-pinned:F, skipping cached
// stages, reporting progress), ensures inputs, and assembles the dev-shell
// sandbox spec from build-pinned:F. It does NOT run build-output (develop shells
// in). Returns the spec + the build script. jobs' prepareDevelop minus the
// signer: local refs are plain unsigned refstore records (LocalRefWriter).
func prepareDevelop(ctx context.Context, st *amber.Store, cfg DevelopConfig, p *Progress) (BuildSpec, string, error) {
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
		return BuildSpec{}, "", fmt.Errorf("resolve %s: %w", shellRef, err)
	}
	if !ok {
		return BuildSpec{}, "", fmt.Errorf("shell artifact %q not found (seed missing — restart to re-seed)", shellRef)
	}
	brc := BuildRunCfg{Platform: platform, ShellKey: shellKey, CacheDir: cfg.CacheDir}

	bf := p.Start("build-from")
	f, err := localBuildFrom(ctx, st, brc, cfg)
	if err != nil {
		bf(err)
		return BuildSpec{}, "", err
	}
	bf(nil)

	d := &developDriver{
		ctx: ctx, st: st, rw: NewLocalRefWriter(st), brc: brc, secrets: cfg.Secrets,
		visited: map[string]bool{}, inProgress: map[string]bool{},
	}
	if err := d.driveFStages(f, false, p); err != nil { // runFinal=false: stop at pin
		return BuildSpec{}, "", err
	}
	// The develop PTY sandboxes the COVERED tree, not the whole context —
	// driveFStages derived KP after pin; re-deriving here is idempotent (all
	// objects dedup) and hands us the spec key (sibling-sources design §9).
	kp, err := d.deriveKP(f)
	if err != nil {
		return BuildSpec{}, "", err
	}
	spec, _, out := assembleBuildSpec(ctx, st, brc, kp)
	if out != nil {
		return BuildSpec{}, "", outcomeErr("assemble build spec", *out)
	}
	return spec, spec.Script, nil
}

// developRcfile is sourced by the interactive shell: a recognizable prompt + a
// one-line reminder of how to run the real build script.
const developRcfile = `PS1='(jobs develop) \w \$ '
echo 'jobs develop: $SRC is the source copy, $out is the (empty) output dir.'
echo 'The runner build script is printed above and saved at /build/build.sh (run: bash -e /build/build.sh).'
`

// developRun assembles the target sandbox and runs command in it. It binds the
// host /dev recursively so an interactive shell has /dev/tty etc. (a develop-only
// relaxation; the hermetic build path gets only a minimal pseudo-device /dev).
// The build script is written to /build/build.sh and the rcfile to
// /build/.developrc inside the sandbox. When tty is non-nil it is the
// controlling terminal; otherwise stdout/stderr wire the command's output.
func developRun(ctx context.Context, st *amber.Store, spec BuildSpec, command []string, stdout, stderr io.Writer, tty *os.File) (int, error) {
	a, err := assembleSandbox(ctx, st, spec, command, []sandbox.Mount{
		{Source: "/dev", Target: "/dev"}, // recursive bind (applyMount adds MS_REC)
	})
	if err != nil {
		return -1, err
	}
	defer func() {
		if a.cg != nil {
			_ = a.cg.Close()
		}
		_ = os.RemoveAll(a.work)
	}()

	// Drop the script + rcfile into the sandbox's /build (== <newRoot>/build).
	buildDir := filepath.Join(a.cfg.NewRoot, "build")
	if err := os.WriteFile(filepath.Join(buildDir, "build.sh"), []byte(spec.Script), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "jobs develop: warning: write build.sh: %v\n", err)
	}
	if err := os.WriteFile(filepath.Join(buildDir, ".developrc"), []byte(developRcfile), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "jobs develop: warning: write .developrc: %v\n", err)
	}

	if tty != nil {
		a.cfg.Tty = tty
	} else {
		a.cfg.Stdout, a.cfg.Stderr = stdout, stderr
	}
	return sandbox.Run(ctx, a.cfg)
}

// printScript writes a human summary of the assembled build + the script the
// runner would execute, before the interactive shell starts.
func printScript(w io.Writer, spec BuildSpec, script string) {
	fmt.Fprintln(w, "=== jobs develop ===")
	fmt.Fprintf(w, "SRC=%s  out=%s\n", sandboxSrcDir, sandboxOutDir)
	if len(spec.JobsDeps) > 0 {
		fmt.Fprintln(w, "deps (JOBS_DEPS — name → /jobs/store/<BOK>, read-only):")
		names := make([]string, 0, len(spec.JobsDeps))
		for n := range spec.JobsDeps {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			fmt.Fprintf(w, "  %s=%s\n", n, spec.JobsDeps[n])
		}
	}
	if len(spec.Env) > 0 {
		fmt.Fprintln(w, "env:")
		envKeys := make([]string, 0, len(spec.Env))
		for k := range spec.Env {
			envKeys = append(envKeys, k)
		}
		sort.Strings(envKeys)
		for _, k := range envKeys {
			fmt.Fprintf(w, "  %s=%s\n", k, spec.Env[k])
		}
	}
	fmt.Fprintln(w, "--- build script (runner would execute; saved at /build/build.sh) ---")
	fmt.Fprintln(w, script)
	fmt.Fprintln(w, "----------------------------------------------------------------------")
}

// RunDevelop ensures the target's dependencies are built, prints the build
// script, and opens an interactive bash shell in the assembled hermetic sandbox
// over a PTY (real prompt, line editing, Ctrl-C job control).
func RunDevelop(ctx context.Context, st *amber.Store, cfg DevelopConfig) error {
	p := NewProgress(os.Stderr)
	spec, script, err := prepareDevelop(ctx, st, cfg, p)
	if err != nil {
		return err
	}
	// Develop mounts the declared caches warm but never uploads (build-cache
	// design §7): interactive state is not shared cache state. The host dirs
	// are discarded on exit.
	defer removeCacheDirs(spec.Caches)
	for _, cm := range spec.Caches {
		fmt.Fprintf(os.Stderr, "jobs develop: cache %q mounted rw at %s (not uploaded on exit)\n", cm.ID, cm.Path)
	}
	printScript(os.Stdout, spec, script)

	ptmx, tty, err := pty.Open()
	if err != nil {
		return fmt.Errorf("open pty: %w", err)
	}
	defer ptmx.Close()

	// Host terminal → raw mode for the duration (best-effort: if stdin is not a
	// terminal, e.g. piped, skip raw mode and continue).
	if oldState, err := term.MakeRaw(int(os.Stdin.Fd())); err == nil {
		defer term.Restore(int(os.Stdin.Fd()), oldState)
	}

	// Propagate window size now and on SIGWINCH.
	winch := make(chan os.Signal, 1)
	signal.Notify(winch, syscall.SIGWINCH)
	defer signal.Stop(winch)
	go func() {
		for range winch {
			_ = pty.InheritSize(os.Stdin, ptmx)
		}
	}()
	winch <- syscall.SIGWINCH

	// Forward host stdio ↔ PTY master.
	go func() { _, _ = io.Copy(ptmx, os.Stdin) }()
	go func() { _, _ = io.Copy(os.Stdout, ptmx) }()

	command := []string{storeShellDir(spec.ShellBOK) + "/bin/bash", "--rcfile", "/build/.developrc", "-i"}
	_, err = developRun(ctx, st, spec, command, nil, nil, tty)
	_ = tty.Close()
	return err
}
