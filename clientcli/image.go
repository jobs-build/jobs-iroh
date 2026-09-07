package clientcli

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/urfave/cli/v2"
	"golang.org/x/term"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/runner"
)

// imageConfig carries the `image` command's flags: the shared local-build
// surface (data dir, source/dir/build-file/platform/shell-ref) plus the image
// specifics (tag, output, no-shell). Unlike build/run, `image` has two modes,
// discriminated by the POSITIONAL build key: given one, it images an
// already-built output (a K pulled home by remote-build, or a local F)
// without building anything; given none, it builds --source — which, like
// build/run/develop, defaults to the cwd-resolved context.
type imageConfig struct {
	dataDir    string
	source     string
	dir        string
	buildFile  string
	platform   string
	shellRef   string
	tag        string
	output     string
	noShell    bool
	sourceRoot string
	noRepoRoot bool
}

func imageCmd() *cli.Command {
	cfg := &imageConfig{}
	return &cli.Command{
		Name:      "image",
		Usage:     "build a docker-loadable OCI image from a build output (build from --source, or an existing build key)",
		ArgsUsage: "[build-K]",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "data-dir", EnvVars: []string{"JOBS_DATA_DIR"}, Value: defaultDataDir(), Usage: "client data directory (embedded store + cache)", Destination: &cfg.dataDir},
			&cli.StringFlag{Name: "output", Aliases: []string{"o"}, Required: true, Usage: "write the image tarball here (- for stdout; load with `docker load -i <file>`)", Destination: &cfg.output},
			&cli.StringFlag{Name: "tag", Usage: "image reference for the tarball manifest, e.g. myapp:1.0 (default derived)", Destination: &cfg.tag},
			&cli.BoolFlag{Name: "no-shell", Usage: "do not bake the shell artifact (/bin/sh, /jobs/shell) into the image", Destination: &cfg.noShell},
			&cli.StringFlag{Name: "source", Usage: "build this local source tree, then image it (default with no build key: the nearest ancestor of the current directory holding BUILD.jobs, searched up to the repo root)", Destination: &cfg.source},
			&cli.StringFlag{Name: "dir", Usage: "build root within the source (where BUILD.jobs lives)", Destination: &cfg.dir},
			&cli.StringFlag{Name: "source-root", Usage: "explicit context root (--source must live under it); default: the git repo root above --source", Destination: &cfg.sourceRoot},
			&cli.BoolFlag{Name: "no-repo-root", Usage: "disable the git-root context default (ingest --source itself)", Destination: &cfg.noRepoRoot},
			&cli.StringFlag{Name: "build-file", Usage: "recipe path relative to dir (default BUILD.jobs)", Destination: &cfg.buildFile},
			&cli.StringFlag{Name: "platform", EnvVars: []string{"JOBS_PLATFORM"}, Value: runner.Platform(), Usage: "target platform, e.g. linux/amd64 (also the image OS/arch)", Destination: &cfg.platform},
			&cli.StringFlag{Name: "shell-ref", EnvVars: []string{"JOBS_SHELL_REF"}, Usage: "amber ref for the vendored shell artifact (default shell:<platform>)", Destination: &cfg.shellRef},
			&cli.StringSliceFlag{Name: "param", Usage: "key=value build param (repeatable)"},
		},
		Action: cfg.run,
	}
}

func (cfg *imageConfig) run(c *cli.Context) (err error) {
	ctx, stop := signalCtx(c.Context)
	defer stop()

	// The positional build key decides the mode, so it must be checked before
	// anything with a side effect (the output file, the store lock): naming
	// both a key and a source is a contradiction, not a precedence question.
	if c.Args().Len() > 0 && cfg.source != "" {
		return fmt.Errorf("--source and a build key are mutually exclusive: drop one (see `jobs-client image --help`)")
	}

	cs, err := openClientStore(cfg.dataDir, lockExclusive)
	if err != nil {
		return err
	}
	defer cs.Close()
	args := c.Args().Slice()

	// Open the tarball destination (file, or stdout via "-").
	var w io.Writer
	if cfg.output == "-" {
		if term.IsTerminal(int(os.Stdout.Fd())) {
			return fmt.Errorf("refusing to write an image tarball to the terminal; redirect stdout or pass -o <file>")
		}
		w = os.Stdout
	} else {
		f, e := os.Create(cfg.output)
		if e != nil {
			return fmt.Errorf("create output %q: %w", cfg.output, e)
		}
		// Capture the close error only when the build itself succeeded.
		defer func() {
			if cerr := f.Close(); err == nil {
				err = cerr
			}
		}()
		w = f
	}

	opts := runner.ImageOptions{Tag: cfg.tag, IncludeShell: !cfg.noShell, Output: w}

	// Source mode: no build key, so build the local tree and image it. Same
	// two resolution calls, in the same order, as build/run/develop.
	if len(args) == 0 {
		root, rdir, rerr := resolveSource(cfg.source, cfg.dir, cfg.sourceRoot, cfg.buildFile, cfg.noRepoRoot)
		if rerr != nil {
			return rerr
		}
		cfg.source, cfg.dir = root, rdir
		cliLiveView(c).Println(contextLine(cfg.source, cfg.dir, cfg.buildFile))
		params, perr := parseParams(c.StringSlice("param"))
		if perr != nil {
			return perr
		}
		// The hermetic build needs the shell regardless of --no-shell, so the
		// bootstrap hard gate applies (same as build/run).
		shellRef := cfg.shellRef
		if shellRef == "" {
			shellRef = "shell:" + cfg.platform
		}
		if err := seedBootstrap(ctx, cs.Store, shellRef); err != nil {
			return err
		}
		err = runner.BuildImageFromSource(ctx, cs.Store, runner.DevelopConfig{
			SourceDir: cfg.source,
			Dir:       cfg.dir,
			BuildFile: cfg.buildFile,
			Platform:  cfg.platform,
			Params:    params,
			ShellRef:  cfg.shellRef,
			CacheDir:  cs.CacheDir,
		}, cfg.platform, opts)
		if err == nil {
			cs.MaybeGC(ctx)
		}
		return err
	}

	// By-key mode: image an already-built output. The first positional is the
	// build key (a submission K, or a local build identity F —
	// resolveByKeyArtifact falls back from two-hop to the direct build-output
	// ref).
	raw, derr := hex.DecodeString(args[0])
	if derr != nil {
		return fmt.Errorf("bad build key %q: %w", args[0], derr)
	}
	k, kerr := key.Parse(raw)
	if kerr != nil {
		return fmt.Errorf("bad build key %q: %w", args[0], kerr)
	}
	// Seed first, warn-only (jobs' by-key contract): a store populated only by
	// a remote-build pull holds the output but no shell:<platform>, which image
	// assembly needs unless --no-shell. No hard gate — a --no-shell image never
	// resolves the shell ref at all, so it must work against a shell-less store.
	seedLocal(ctx, cs.Store)
	err = runner.BuildImageByKey(ctx, cs.Store, k, cfg.platform, cfg.shellRef, opts)
	if err == nil {
		cs.MaybeGC(ctx)
	}
	return err
}
