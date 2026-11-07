package clientcli

import (
	"fmt"
	"path/filepath"

	"github.com/urfave/cli/v2"

	"github.com/jobs-build/jobs-iroh/cover"
	"github.com/jobs-build/jobs-iroh/runner"
)

// --- test ---

func testCmd() *cli.Command {
	cfg := &localConfig{test: true}
	return &cli.Command{
		Name:   "test",
		Usage:  "hermetically run a local source tree's TESTS.jobs (recording a pass so unchanged tests never re-run)",
		Flags:  cfg.flags(),
		Action: cfg.runTest,
	}
}

// runTest drives the local TEST pipeline (tests design §6, §8) exactly like
// runBuild drives a build — same store lock, bootstrap gate and live
// progress — through runner.TestFromSource: the test F-tree, the shared
// resolve/pin stages, every input the recipe names (the directory's own
// build when listed), KT, and either the test-passed memo hit or the run.
// Prints the test identity F and the run identity KT; a failing test is a
// non-zero exit with the sandbox's stderr tail.
func (cfg *localConfig) runTest(c *cli.Context) error {
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

	lp := newLiveProgress(lv)
	defer lp.Close()
	out, err := runner.TestFromSource(ctx, cs.Store, cfg.developConfig(params, cs.CacheDir), runner.NewProgressSink(lp))
	lp.Close()
	if err != nil {
		return err
	}
	fmt.Fprintf(c.App.Writer, "test:   %s\n", out.F.String())
	fmt.Fprintf(c.App.Writer, "passed: %s\n", out.KT.String())
	fmt.Fprintf(c.App.Writer, "refs:   %s (+ %s) in %s\n",
		cover.TestPassedRef(out.KT), cover.TestPassedRef(out.F), filepath.Join(cfg.dataDir, "store"))
	cs.MaybeGC(ctx)
	return nil
}
