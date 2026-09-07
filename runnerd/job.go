package runnerd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/jobs-build/jobs-iroh/events"
	"github.com/jobs-build/jobs-iroh/runner"
	"github.com/jobs-build/jobs-iroh/wire"
)

// syncClient is the daemon's seam over amberclient: pull an input ref's
// closure from the server, push an output closure under a scratch ref.
type syncClient interface {
	Pull(ctx context.Context, name string) (key.Key, error)
	Push(ctx context.Context, name string, root key.Key) error
}

// errPullIncomplete marks a ref whose closure stayed incomplete after a
// re-pull: the server itself is missing objects below the ref, which no
// retry from this side fixes (spec §3: miss → re-pull → hard).
var errPullIncomplete = errors.New("pulled ref closure incomplete after re-pull")

// handleJob runs one fetched work-queue message end to end: ack-window
// heartbeats, the attempt itself, the result publish, and the ack/nak. The
// admission reservation is released by the caller.
func (d *daemon) handleJob(ctx context.Context, msg jetstream.Msg, job wire.Job) {
	log := d.log.With("node", job.Node, "gen", job.Gen, "kind", job.Kind)
	log.Info("job start")
	start := time.Now()

	stopHB := startMsgHeartbeat(msg, d.msgHBInterval, log)
	defer stopHB()

	sink := newJobSink(d.nc, job.Node, job.Gen)
	ev := events.NewJob(sink, job.Node, d.id, nil)
	ev.Started(job.Kind)

	out, refs, scratch, readRefs := d.attempt(ctx, job, ev)

	ru := wire.Rusage{WallNs: time.Since(start).Nanoseconds()}
	if cpuMs, memPeak, ok := sink.Usage(); ok {
		if cpuMs >= 0 {
			ru.CPUNs = cpuMs * int64(time.Millisecond)
		}
		if memPeak >= 0 {
			ru.MaxRSS = memPeak
		}
	}
	res := buildResult(job, d.id, out, refs, scratch, ru, readRefs)
	ev.Finished(finishOutcome(res.Class), res.Exit)

	// The terminal exchange must survive the run context's cancellation —
	// a cancelled result is exactly what a shutdown needs to report.
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), d.publishTimeout)
	defer cancel()
	if err := d.publishResult(pctx, res); err != nil {
		// Could not report: leave the message unacked so it redelivers and
		// another attempt reports (running twice is wasteful, never wrong).
		log.Error("publish result failed; nak for redelivery", "error", err)
		_ = msg.Nak()
		return
	}
	if res.Class == wire.ClassCancelled {
		// Shutdown mid-job: the result is informational; the job itself goes
		// back to the queue for another runner.
		log.Info("job cancelled; nak for redelivery")
		_ = msg.Nak()
		return
	}
	if err := msg.Ack(); err != nil {
		log.Warn("ack failed (result already published; dedup covers the redelivery)", "error", err)
	}
	log.Info("job done", "class", res.Class, "wallMs", ru.WallNs/int64(time.Millisecond))
}

// attempt executes the runner flow of spec §3: pull pullRefs → in-band def →
// stage driver → scratch push. It returns the driver outcome (or a synthetic
// failure outcome), the ordered ref proposals, the scratch ref name (""
// when nothing was pushed), and the pull-ref names successfully ensured on
// this attempt (populated on every path, including failures, so the server's
// GC learns which cached refs were actually used).
func (d *daemon) attempt(ctx context.Context, job wire.Job, ev *events.Job) (runner.Outcome, []wire.RefProposal, string, []string) {
	// 1. Pull every input ref's closure from the server store.
	ev.Phase("pulling")
	var readRefs []string
	for _, name := range job.PullRefs {
		if _, err := d.ensureRef(ctx, name); err != nil {
			if ctx.Err() != nil {
				return runner.Outcome{Cancelled: true}, nil, "", readRefs
			}
			class := "retryable"
			if errors.Is(err, errPullIncomplete) {
				class = "hard"
			}
			return runner.Outcome{Failed: true, Class: class, Phase: "pulling", Stderr: err.Error()}, nil, "", readRefs
		}
		readRefs = append(readRefs, name)
	}

	// 2. In-band def: ingest, verify identity, write the local bookkeeping
	// ref so the driver's ensureImportDef/ensureBuildDef resolve without a
	// remote trip (execcore's exact move).
	k, err := wire.ParseKey(job.Key)
	if err != nil {
		return runner.Outcome{Failed: true, Class: "hard", Phase: "pulling", Stderr: "bad job key: " + err.Error()}, nil, "", readRefs
	}
	if len(job.Def) > 0 {
		ik, err := d.st.IngestFile(ctx, job.Def)
		if err != nil {
			return runner.Outcome{Failed: true, Class: "retryable", Phase: "pulling", Stderr: "ingest def: " + err.Error()}, nil, "", readRefs
		}
		if ik != k {
			return runner.Outcome{Failed: true, Class: "hard", Phase: "pulling",
				Stderr: fmt.Sprintf("in-band def hashes to %s, job key is %s", ik, k)}, nil, "", readRefs
		}
		var refName string
		switch job.Kind {
		case wire.KindImport:
			refName = "import:" + k.String()
		case wire.KindBuildFrom:
			refName = "build:" + k.String()
		}
		if refName != "" {
			if err := d.st.PutRef(ctx, refName, k); err != nil {
				return runner.Outcome{Failed: true, Class: "retryable", Phase: "pulling", Stderr: "local def ref: " + err.Error()}, nil, "", readRefs
			}
		}
	}

	// 3. Run the stage driver with the collecting ref writer.
	rw := newCollectRefWriter(d.st)
	out := d.execute(ctx, job, rw, ev)
	if ctx.Err() != nil && (out.Failed || out.Decline) {
		// The shutdown that cancelled ctx reaches the sandboxed children too
		// (same process group as the runner): the driver then sees its child
		// die and reports an ordinary failure. Under a dying context that
		// verdict is untrustworthy — report cancelled so the message naks
		// back to the queue (running twice is wasteful, never wrong; a hard
		// failure here would fail the whole request on a mere restart).
		return runner.Outcome{Cancelled: true}, nil, "", readRefs
	}
	if out.Cancelled || out.Decline || out.Failed {
		return out, nil, "", readRefs
	}

	// 4. Push the attempt's outputs: ONE scratch ref over a BuildStoreTree
	// union of every proposed root, so a single push covers all proposed
	// keys' closures (decision documented in runnerd.go).
	refs := rw.proposals()
	if len(refs) == 0 {
		return out, refs, "", readRefs
	}
	ev.Phase("pushing")
	roots := make([]key.Key, 0, len(refs))
	for _, r := range refs {
		rk, err := wire.ParseKey(r.Key)
		if err != nil {
			return runner.Outcome{Failed: true, Class: "hard", Phase: "pushing", Stderr: "proposed ref key: " + err.Error()}, nil, "", readRefs
		}
		roots = append(roots, rk)
	}
	union, err := d.st.BuildStoreTree(ctx, roots)
	if err != nil {
		return runner.Outcome{Failed: true, Class: "retryable", Phase: "pushing", Stderr: "assemble scratch tree: " + err.Error()}, nil, "", readRefs
	}
	scratch := scratchRefName(d.id, job.Node, job.Gen)
	if err := d.sync.Push(ctx, scratch, union); err != nil {
		if ctx.Err() != nil {
			return runner.Outcome{Cancelled: true}, nil, "", readRefs
		}
		return runner.Outcome{Failed: true, Class: "retryable", Phase: "pushing", Stderr: "push outputs: " + err.Error()}, nil, "", readRefs
	}
	return out, refs, scratch, readRefs
}

// scratchRefName is the runner-push scratch ref for one attempt. The server
// deletes it after committing the gated refs.
func scratchRefName(runnerID, node string, gen uint64) string {
	return fmt.Sprintf("runner-push/%s/%s-%d", runnerID, node, gen)
}

// ensureRef makes ref name resolvable-and-complete in the local store:
// already-present refs short-circuit (GetKey + CheckComplete), otherwise the
// closure is pulled from the server; a post-pull incompleteness re-pulls
// once, then fails with errPullIncomplete (hard).
func (d *daemon) ensureRef(ctx context.Context, name string) (key.Key, error) {
	if k, ok, err := d.st.GetKey(ctx, name); err == nil && ok {
		if d.checkComplete(k) == nil {
			return k, nil
		}
	}
	root, err := d.sync.Pull(ctx, name)
	if err != nil {
		return key.Key{}, fmt.Errorf("pull %s: %w", name, err)
	}
	if d.checkComplete(root) == nil {
		return root, nil
	}
	root, err = d.sync.Pull(ctx, name)
	if err != nil {
		return key.Key{}, fmt.Errorf("re-pull %s: %w", name, err)
	}
	if err := d.checkComplete(root); err != nil {
		return key.Key{}, fmt.Errorf("%s: %w: %v", name, errPullIncomplete, err)
	}
	return root, nil
}

// checkComplete verifies a root's whole closure is present locally
// (objects-before-anything: presence of a parent proves nothing).
func (d *daemon) checkComplete(root key.Key) error {
	obj := d.st.Objects()
	_, err := fstree.CheckComplete(root, obj.Get, obj.Has, 0)
	return err
}

// driveStage is the real driver dispatch (execcore's switch, keyed by the
// wire kind). Tests replace d.execute with a stub.
func (d *daemon) driveStage(ctx context.Context, job wire.Job, rw runner.RefWriter, ev *events.Job) runner.Outcome {
	k, err := wire.ParseKey(job.Key)
	if err != nil {
		return runner.Outcome{Failed: true, Class: "hard", Stderr: "bad job key: " + err.Error()}
	}
	switch job.Kind {
	case wire.KindImport:
		ex := runner.CgroupExecutor{MemoryMaxBytes: job.MemBytes}
		return runner.RunImport(ctx, d.st, rw, ex, d.cacheDir, k, nil, ev)
	case wire.KindBuildFrom, wire.KindPluginResolve, wire.KindPin, wire.KindBuildRun:
		brc, oc := d.buildRunCfg(ctx, job, ev)
		if oc != nil {
			return *oc
		}
		switch job.Kind {
		case wire.KindBuildFrom:
			return runner.RunBuildFrom(ctx, d.st, rw, brc, k)
		case wire.KindPluginResolve:
			return runner.RunPluginResolve(ctx, d.st, rw, brc, k)
		case wire.KindPin:
			return runner.RunPin(ctx, d.st, rw, brc, k)
		default:
			return runner.RunBuild(ctx, d.st, rw, brc, runner.NamespaceBuildExecutor{}, k)
		}
	default:
		return runner.Outcome{Failed: true, Class: "hard", Stderr: fmt.Sprintf("kind %q is not executable", job.Kind)}
	}
}

// buildRunCfg assembles the F-stage driver config, resolving the vendored
// shell artifact with pull-on-miss (the runner never self-seeds — the server
// bootstrap-seeds shell:<platform>, spec: "everything pulls from server").
func (d *daemon) buildRunCfg(ctx context.Context, job wire.Job, ev *events.Job) (runner.BuildRunCfg, *runner.Outcome) {
	platform := job.Platform
	if platform == "" {
		platform = d.platform
	}
	shellKey, err := d.ensureRef(ctx, "shell:"+platform)
	if err != nil {
		o := runner.Outcome{Failed: true, Class: "retryable", Phase: "pulling", Stderr: "resolve shell: " + err.Error()}
		if ctx.Err() != nil {
			o = runner.Outcome{Cancelled: true}
		}
		return runner.BuildRunCfg{}, &o
	}
	return runner.BuildRunCfg{
		Platform:       platform,
		ShellKey:       shellKey,
		CacheDir:       d.cacheDir,
		MemoryMaxBytes: job.MemBytes,
		Events:         ev,
	}, nil
}

// buildResult maps a driver runner.Outcome onto the wire.Result envelope.
// Classes follow execcore's mapping: Cancelled → cancelled (here only ctx
// cancellation produces it — the daemon's ref writer never loses assignment
// races, the server-side gate arbitrates those); Decline → retryable (a
// declined attempt says nothing hard about the job); Failed → the driver's
// own class, defaulting hard when unset (fail closed); success → ok with the
// ordered proposals and the scratch ref covering their closures. readRefs
// rides every class unconditionally — the server's GC needs to know which
// cached refs were used even when the attempt didn't succeed.
func buildResult(job wire.Job, runnerID string, out runner.Outcome, refs []wire.RefProposal, scratch string, ru wire.Rusage, readRefs []string) wire.Result {
	res := wire.Result{
		Node:     job.Node,
		Gen:      job.Gen,
		Runner:   runnerID,
		Rusage:   ru,
		ReadRefs: readRefs,
	}
	switch {
	case out.Cancelled:
		res.Class = wire.ClassCancelled
	case out.Decline:
		res.Class = wire.ClassRetryable
		res.ErrSummary = "declined: " + out.DeclineReason
	case out.Failed:
		res.Class = out.Class
		if res.Class == "" {
			res.Class = wire.ClassHard
		}
		res.Exit = out.ExitCode
		res.ErrSummary = out.Stderr
		if out.Phase != "" && res.ErrSummary != "" {
			res.ErrSummary = out.Phase + ": " + res.ErrSummary
		} else if out.Phase != "" {
			res.ErrSummary = out.Phase + ": failed"
		}
	default:
		res.Class = wire.ClassOK
		res.Refs = refs
		res.ScratchRef = scratch
	}
	return res
}

// finishOutcome maps a result class onto the exec.finished outcome
// vocabulary (completed|failed|cancelled).
func finishOutcome(class string) string {
	switch class {
	case wire.ClassOK:
		return "completed"
	case wire.ClassCancelled:
		return "cancelled"
	default:
		return "failed"
	}
}

// startMsgHeartbeat extends the JetStream ack window (msg.InProgress) every
// interval while the attempt runs, so a healthy long job never redelivers
// (spec §2: AckWait 90s, heartbeats every 5s).
func startMsgHeartbeat(msg jetstream.Msg, interval time.Duration, log interface {
	Warn(msg string, args ...any)
}) (stop func()) {
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if err := msg.InProgress(); err != nil {
					log.Warn("job heartbeat (InProgress) failed", "error", err)
				}
			}
		}
	}()
	var once bool
	return func() {
		if once {
			return
		}
		once = true
		close(done)
		<-finished
	}
}
