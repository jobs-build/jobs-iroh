package clientcli

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/urfave/cli/v2"

	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/bootstrap"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/importdef"
	"github.com/jobs-build/jobs-iroh/runner"
)

// localConfig carries the flags shared by the local build commands (build,
// run): where the data dir lives, what to build, and for which platform.
// jobs' extra local flags (--signing-key, --user, --tags-file) died with the
// signing plumbing; --tags-file (fetcher tag secrets) returns in a later
// milestone when a credentialed fetcher needs it.
type localConfig struct {
	dataDir    string
	source     string
	dir        string
	buildFile  string
	platform   string
	shellRef   string
	sourceRoot string
	noRepoRoot bool
	// test selects the TEST pipeline (tests design §8): TESTS.jobs is the
	// recipe searched for and run, --test-file replaces --build-file.
	test bool
}

// what names the command's kind in messages: "build" or "test".
func (cfg *localConfig) what() string {
	if cfg.test {
		return "test"
	}
	return "build"
}

// recipeName is the effective recipe file (relative to dir): the explicit
// flag, else BUILD.jobs for builds and TESTS.jobs for tests — the same
// default runner's localBuildFrom applies.
func (cfg *localConfig) recipeName() string {
	if cfg.buildFile != "" {
		return cfg.buildFile
	}
	if cfg.test {
		return builddef.TestsFile
	}
	return builddef.BuildFile
}

// flags returns the shared flag set, destinations bound to cfg. The recipe
// flag follows the kind: --build-file (BUILD.jobs) or --test-file
// (TESTS.jobs), both landing in cfg.buildFile.
func (cfg *localConfig) flags() []cli.Flag {
	what, recipe := cfg.what(), cfg.recipeName()
	recipeFlag := &cli.StringFlag{Name: "build-file", Usage: "recipe path relative to dir (default BUILD.jobs)", Destination: &cfg.buildFile}
	if cfg.test {
		recipeFlag = &cli.StringFlag{Name: "test-file", Usage: "test recipe path relative to dir (default TESTS.jobs)", Destination: &cfg.buildFile}
	}
	return []cli.Flag{
		&cli.StringFlag{Name: "data-dir", EnvVars: []string{"JOBS_DATA_DIR"}, Value: defaultDataDir(), Usage: "client data directory (embedded store + cache)", Destination: &cfg.dataDir},
		&cli.StringFlag{Name: "source", Usage: "source directory to ingest as the " + what + " source (default: the nearest ancestor of the current directory holding " + recipe + ", searched up to the repo root)", Destination: &cfg.source},
		&cli.StringFlag{Name: "dir", Usage: what + " root within the source (where " + recipe + " lives)", Destination: &cfg.dir},
		recipeFlag,
		&cli.StringFlag{Name: "source-root", Usage: "explicit context root (--source must live under it); default: the git repo root above --source", Destination: &cfg.sourceRoot},
		&cli.BoolFlag{Name: "no-repo-root", Usage: "disable the git-root context default (ingest --source itself)", Destination: &cfg.noRepoRoot},
		&cli.StringFlag{Name: "platform", EnvVars: []string{"JOBS_PLATFORM"}, Value: runner.Platform(), Usage: "target platform, e.g. linux/amd64", Destination: &cfg.platform},
		&cli.StringFlag{Name: "shell-ref", EnvVars: []string{"JOBS_SHELL_REF"}, Usage: "amber ref for the vendored shell artifact (default shell:<platform>)", Destination: &cfg.shellRef},
		&cli.StringSliceFlag{Name: "param", Usage: "key=value " + what + " param (repeatable)"},
	}
}

// effectiveShellRef is the shell ref the bootstrap gate checks: the --shell-ref
// override, else shell:<platform> — the same default the runner applies inside
// DevelopConfig.
func (cfg *localConfig) effectiveShellRef() string {
	if cfg.shellRef != "" {
		return cfg.shellRef
	}
	return "shell:" + cfg.platform
}

// resolveContext infers an omitted --source from the cwd and applies the
// repo-root context default in place (sibling-sources design §11.1) — must
// run before any ingest, and the rule is identical on the remote path (the
// local↔remote F join depends on it). The resolution is reported on stderr:
// the ingest root can be an entire repository, so it is always shown.
func (cfg *localConfig) resolveContext(lv *liveView) error {
	root, dir, err := resolveSource(cfg.source, cfg.dir, cfg.sourceRoot, cfg.recipeName(), cfg.noRepoRoot)
	if err != nil {
		return err
	}
	cfg.source, cfg.dir = root, dir
	lv.Println(contextLine(cfg.source, cfg.dir, cfg.recipeName()))
	return nil
}

// developConfig assembles the runner config shared by build and run. Secrets
// (tag-credentialed fetchers) are not wired in this milestone.
func (cfg *localConfig) developConfig(params []byte, cacheDir string) runner.DevelopConfig {
	return runner.DevelopConfig{
		SourceDir: cfg.source,
		Dir:       cfg.dir,
		BuildFile: cfg.buildFile,
		Platform:  cfg.platform,
		Params:    params,
		ShellRef:  cfg.shellRef,
		CacheDir:  cacheDir,
		Test:      cfg.test,
	}
}

// parseParams folds repeated --param key=value flags into canonical CBOR —
// the exact bytes that become the params file of the build-from tree
// (identity: the same params must yield the same F everywhere).
func parseParams(kvs []string) ([]byte, error) {
	m := map[string]any{}
	for _, kv := range kvs {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return nil, fmt.Errorf("bad --param %q (want key=value)", kv)
		}
		m[k] = v
	}
	params, err := importdef.CanonicalParams(m)
	if err != nil {
		return nil, fmt.Errorf("encode params: %w", err)
	}
	return []byte(params), nil
}

// seedLocal publishes the embedded seed artifacts (shell:<p>,
// fetcher:tarball+https:<p>, fetcher:hostmusl:<p>, fetcher:github:<p>) into
// the local store, idempotently. A seed I/O failure is a non-fatal warning
// (jobs' localSeed contract) — a real gap surfaces later as a missing-ref
// error from whatever needed the artifact.
func seedLocal(ctx context.Context, st *amber.Store) {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := bootstrap.Seed(ctx, st, bootstrap.SeededPlatforms(), log); err != nil {
		fmt.Fprintln(os.Stderr, "seed warning:", err)
	}
}

// seedBootstrap seeds (seedLocal) and then verifies the bootstrap floor:
// without shellRef no hermetic build sandbox can be assembled, so the
// source-building commands hard-gate on it after seeding.
func seedBootstrap(ctx context.Context, st *amber.Store, shellRef string) error {
	seedLocal(ctx, st)
	if _, ok, err := st.GetKey(ctx, shellRef); err != nil {
		return fmt.Errorf("resolve %s: %w", shellRef, err)
	} else if !ok {
		return fmt.Errorf("shell artifact %q not found after seeding (no embedded seed for this platform?)", shellRef)
	}
	return nil
}

// --- build ---

func buildCmd() *cli.Command {
	cfg := &localConfig{}
	return &cli.Command{
		Name:   "build",
		Usage:  "hermetically build a local source tree into the embedded store",
		Flags:  cfg.flags(),
		Action: cfg.runBuild,
	}
}

// runBuild drives a full local hermetic build: open+EX-lock the embedded
// store, seed the bootstrap floor (hard error if the shell is still missing),
// then run the runner's local path — IngestSourceDir + BuildFromTree → F,
// driveFStages(F, runFinal=true) — to completion. Prints the build identity F
// and the output tree key; the refs land in the embedded store under
// <data-dir>/store.
func (cfg *localConfig) runBuild(c *cli.Context) error {
	lv := cliLiveView(c)
	if err := cfg.resolveContext(lv); err != nil {
		return err
	}
	ctx, stop := signalCtx(c.Context)
	defer stop()

	params, err := parseParams(c.StringSlice("param"))
	if err != nil {
		return err
	}
	cs, err := openClientStore(cfg.dataDir, lockExclusive)
	if err != nil {
		return err
	}
	defer cs.Close()

	if err := seedBootstrap(ctx, cs.Store, cfg.effectiveShellRef()); err != nil {
		return err
	}

	// runner.BuildFromSource is the build-only exported entry point of the
	// ported local path (jobs kept it unexported inside prepareSourceArtifact,
	// run_linux.go:53-90): resolve shell → localBuildFrom (IngestSourceDir →
	// env subtree → BuildFromTree) → driveFStages(f, runFinal=true, p). It
	// returns F; the output tree is at ref build-output:F. Step progress
	// renders through the live view: in-place →-block on a TTY stderr,
	// jobs' classic plain →/✓/✗ lines otherwise.
	lp := newLiveProgress(lv)
	defer lp.Close()
	f, err := runner.BuildFromSource(ctx, cs.Store, cfg.developConfig(params, cs.CacheDir), runner.NewProgressSink(lp))
	lp.Close()
	if err != nil {
		return err
	}
	outKey, ok, err := cs.Store.GetKey(ctx, "build-output:"+f.String())
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("build completed but ref build-output:%s is missing", f.String())
	}
	fmt.Fprintf(c.App.Writer, "build:  %s\n", f.String())
	fmt.Fprintf(c.App.Writer, "output: %s\n", outKey.String())
	fmt.Fprintf(c.App.Writer, "refs:   build-output:%s (+ build-output-deps:%s) in %s\n",
		f.String(), f.String(), filepath.Join(cfg.dataDir, "store"))
	cs.MaybeGC(ctx)
	return nil
}

// --- run ---

func runCmd() *cli.Command {
	cfg := &localConfig{}
	return &cli.Command{
		Name:      "run",
		Usage:     "hermetically build a local source tree, then execute its JOBS.entrypoint",
		ArgsUsage: "[-- args...]",
		Flags:     cfg.flags(),
		Action:    cfg.runRun,
	}
}

// runRun builds exactly like runBuild (same store, same seam, joining the
// same cached refs), then executes the output artifact's JOBS.entrypoint in
// the runner's non-hermetic run sandbox (host network, host /dev), passing
// the entrypoint's exit code through. Positional args are appended to the
// entrypoint's args.
func (cfg *localConfig) runRun(c *cli.Context) error {
	lv := cliLiveView(c)
	if err := cfg.resolveContext(lv); err != nil {
		return err
	}
	ctx, stop := signalCtx(c.Context)
	defer stop()

	params, err := parseParams(c.StringSlice("param"))
	if err != nil {
		return err
	}
	cs, err := openClientStore(cfg.dataDir, lockExclusive)
	if err != nil {
		return err
	}
	defer cs.Close()

	if err := seedBootstrap(ctx, cs.Store, cfg.effectiveShellRef()); err != nil {
		return err
	}

	// The build phase's step progress renders through the live view. By the
	// time the entrypoint executes every step has finished, so the block is
	// empty and the refresh ticker writes nothing — the child owns the
	// terminal untouched. Close (idempotent) stops the ticker afterwards.
	lp := newLiveProgress(lv)
	defer lp.Close()
	code, err := runner.RunFromSource(ctx, cs.Store, cfg.developConfig(params, cs.CacheDir), c.Args().Slice(),
		runner.RunIO{Stdin: os.Stdin, Stdout: os.Stdout, Stderr: os.Stderr}, runner.NewProgressSink(lp))
	lp.Close()
	if err != nil {
		return err
	}
	// The build ran regardless of the entrypoint's own exit code, so sweep
	// here — before the exit-code translation below, which may turn this
	// return into a non-zero os.Exit via cli.Exit.
	cs.MaybeGC(ctx)
	if code != 0 {
		// cli.Exit makes urfave/cli os.Exit(code) AFTER this action's defers run
		// (store close + flock release), so the entrypoint's exit code is
		// faithfully propagated.
		return cli.Exit("", code)
	}
	return nil
}

// --- develop ---

func developCmd() *cli.Command {
	cfg := &localConfig{}
	return &cli.Command{
		Name:   "develop",
		Usage:  "build the local source's dependencies and open an interactive shell in its build sandbox",
		Flags:  cfg.flags(),
		Action: cfg.runDevelop,
	}
}

// runDevelop prepares the target exactly like runBuild (same store, same seam,
// joining the same cached refs) but stops at build-pinned:F and shells in:
// runner.RunDevelop prints the pinned build script (saved at /build/build.sh,
// NOT executed) and opens an interactive bash over a PTY inside the exact
// hermetic build sandbox — host /dev bound (develop-only relaxation), declared
// caches mounted warm rw but never uploaded. The store flock (EX) is held for
// the WHOLE interactive session — jobs' `jobs develop` contract: cs.Close
// (flock release) runs only after RunDevelop returns.
func (cfg *localConfig) runDevelop(c *cli.Context) error {
	// develop drives no progress block — the live view exists purely to place
	// the context line on stderr before the PTY takes the terminal.
	if err := cfg.resolveContext(cliLiveView(c)); err != nil {
		return err
	}
	ctx, stop := signalCtx(c.Context)
	defer stop()

	params, err := parseParams(c.StringSlice("param"))
	if err != nil {
		return err
	}
	cs, err := openClientStore(cfg.dataDir, lockExclusive)
	if err != nil {
		return err
	}
	defer cs.Close()

	if err := seedBootstrap(ctx, cs.Store, cfg.effectiveShellRef()); err != nil {
		return err
	}

	derr := runner.RunDevelop(ctx, cs.Store, cfg.developConfig(params, cs.CacheDir))
	cs.MaybeGC(ctx) // after the PTY session ends, regardless of its outcome
	return derr
}
