package runner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/cover"
)

// DefaultTestTimeout bounds a test run whose recipe declares no `timeout`
// (tests design §6.1). Builds have no limit; a test is expected to finish,
// and a hung one would otherwise hold its runner slot forever (the queue
// lease is heartbeat-extended for as long as the process lives).
const DefaultTestTimeout = 5 * time.Minute

// testTimeout is the pinned test's wall-clock limit: the recipe's, else the
// default.
func testTimeout(pinned builddef.Pinned) time.Duration {
	if pinned.TimeoutSec > 0 {
		return time.Duration(pinned.TimeoutSec) * time.Second
	}
	return DefaultTestTimeout
}

// RunTest executes one test run job, keyed by KT (tests design §6): pull
// build-pinned:<KT>, assemble the very same hermetic sandbox a build gets
// (assembleBuildSpec — the KT tree is KP-shaped, so src/ and the pinned Dir
// land exactly as for a buildrun), run the script under the pinned timeout,
// and on exit 0 publish [caches…, test-passed:<KT> → KT]. There is no
// output: $out exists and is discarded; a non-zero exit or the timeout is a
// hard failure that leaves no ref, so the test re-runs on the next submit.
func RunTest(ctx context.Context, st *amber.Store, rw RefWriter, brc BuildRunCfg, ex BuildExecutor, kt key.Key) Outcome {
	ev := brc.Events
	ev.Phase("assembling")
	spec, pinned, out := assembleBuildSpec(ctx, st, brc, kt)
	if out != nil {
		return *out
	}
	defer removeCacheDirs(spec.Caches)

	spec.Events = ev
	spec.Node = "test|" + kt.String()
	if ow := ev.Output("stdout", "testing"); ow != nil {
		spec.StdoutSink = ow
		defer ow.Close()
	}
	if ow := ev.Output("stderr", "testing"); ow != nil {
		spec.StderrSink = ow
		defer ow.Close()
	}

	// The limit is a context deadline on the executor: past it the sandbox
	// child — PID 1 of its own pid namespace — is killed and the whole test
	// process tree dies with it.
	limit := testTimeout(pinned)
	tctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	start := time.Now()
	res, err := ex.RunBuild(tctx, st, spec)
	// The executor leaves the populated work tree to the caller (the
	// BuildResult.OutDir lifecycle); a test keeps nothing from it, whatever
	// the verdict.
	removeWorkTree(res.OutDir)
	if ctx.Err() != nil {
		// A cancel is a cancel, even one that lands after the deadline.
		return Outcome{Cancelled: true}
	}
	if err != nil || res.ExitCode != 0 {
		// An exit 0 that raced the deadline is still a pass; only a script
		// that did not finish on its own gets the timeout verdict.
		if errors.Is(tctx.Err(), context.DeadlineExceeded) {
			msg := fmt.Sprintf("timed out after %s", limit)
			if res.StderrTail != "" {
				msg += "\n" + res.StderrTail
			}
			return hard("testing", msg, res.ExitCode)
		}
		if err != nil {
			return retryable("testing", err)
		}
		return Outcome{Failed: true, Class: "hard", ExitCode: res.ExitCode, Phase: "testing", Stderr: res.StderrTail}
	}

	// Success only: caches are pruned to what the test accessed and
	// published first (they gate nothing); the memo ref lands last.
	ev.Phase("finalizing")
	cacheRefs, out := finalizeCaches(ctx, st, spec.Caches, brc.Platform, start, ev)
	if out != nil {
		return *out
	}
	refs := append(cacheRefs, Ref{Name: cover.TestPassedRef(kt), Key: kt})
	ev.Phase("pushing")
	if _, err := rw.WriteRefs(ctx, refs); err != nil {
		return pushOutcome("pushing", err)
	}
	return Outcome{OutputKey: kt}
}

// removeWorkTree deletes the executor's whole work tree given its $out host
// path (<work>/root/build/out — the layout finalizeOutput relies on too).
func removeWorkTree(outDir string) {
	if outDir == "" {
		return
	}
	_ = os.RemoveAll(filepath.Dir(filepath.Dir(filepath.Dir(outDir))))
}
