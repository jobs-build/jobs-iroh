package sched

import (
	"bytes"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/jobs-build/jobs-iroh/api"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/wire"
)

// failFirstBuildFrom is the standard pipeline except that the first
// buildfrom attempt hard-fails — a failure that sticks until something
// re-activates the node.
func (e *env) failFirstBuildFrom(failures *atomic.Int32) func(wire.Job) *wire.Result {
	std := e.standardHandler(builddef.Pinned{})
	return func(job wire.Job) *wire.Result {
		if job.Kind == wire.KindBuildFrom && failures.Add(-1) >= 0 {
			return &wire.Result{Class: wire.ClassHard, Exit: 1, ErrSummary: "import race"}
		}
		return std(job)
	}
}

// TestResubmitRetriesStickyFailure is issue #22's sticky node: a failed
// request still holds its failed node, and a resubmit of the same K used to
// join it and fail at once. Resubmitting now re-runs the failed node.
func TestResubmitRetriesStickyFailure(t *testing.T) {
	e := newEnv(t)
	var failures atomic.Int32
	failures.Store(1)
	fr := e.startRunner([]wire.Class{"c0.2-m1", "c1-m1"}, e.failFirstBuildFrom(&failures))

	def := e.treeDef("sticky")
	first, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: def})
	if err != nil {
		t.Fatal(err)
	}
	if snap := e.watchTerminal(first.RequestID); snap.Phase != "failed" {
		t.Fatalf("first submit phase = %s, want failed", snap.Phase)
	}

	// The failed request is NOT cancelled or deleted: it keeps its interest.
	second, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: def})
	if err != nil {
		t.Fatal(err)
	}
	if snap := e.watchTerminal(second.RequestID); snap.Phase != "done" {
		t.Fatalf("resubmit phase = %s, want done (%+v)", snap.Phase, snap)
	}
	if n := len(fr.byKind(wire.KindBuildFrom)); n != 2 {
		t.Fatalf("buildfrom attempts = %d, want 2 (the failure, then the resubmit's re-run)", n)
	}
	// The shared node succeeded for the earlier request too.
	e.s.mu.Lock()
	phase := e.s.assembleLocked(e.s.requests[first.RequestID]).Phase
	e.s.mu.Unlock()
	if phase != "done" {
		t.Fatalf("first request after the re-run = %s, want done", phase)
	}
}

// TestRetryRequest re-runs a failed request in place by ID, and refuses
// requests that are not failed.
func TestRetryRequest(t *testing.T) {
	e := newEnv(t)
	var failures atomic.Int32
	failures.Store(1)
	e.startRunner([]wire.Class{"c0.2-m1", "c1-m1"}, e.failFirstBuildFrom(&failures))

	sub, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: e.treeDef("retry-in-place")})
	if err != nil {
		t.Fatal(err)
	}
	if snap := e.watchTerminal(sub.RequestID); snap.Phase != "failed" {
		t.Fatalf("phase = %s, want failed", snap.Phase)
	}
	n, err := e.s.Retry(e.ctx, sub.RequestID)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if n != 1 {
		t.Fatalf("re-activated %d nodes, want 1 (the failed buildfrom)", n)
	}
	if snap := e.watchTerminal(sub.RequestID); snap.Phase != "done" {
		t.Fatalf("retried request phase = %s, want done (%+v)", snap.Phase, snap)
	}

	var se *Error
	if _, err := e.s.Retry(e.ctx, sub.RequestID); !errors.As(err, &se) || se.Code != api.CodeBadRequest {
		t.Fatalf("retry of a done request: %v, want bad-request", err)
	}
	if _, err := e.s.Retry(e.ctx, "rnope"); !errors.As(err, &se) || se.Code != api.CodeNotFound {
		t.Fatalf("retry of an unknown request: %v, want not-found", err)
	}
	if err := e.s.Cancel(e.ctx, sub.RequestID); err != nil {
		t.Fatal(err)
	}
	if _, err := e.s.Retry(e.ctx, sub.RequestID); !errors.As(err, &se) || se.Code != api.CodeBadRequest {
		t.Fatalf("retry of a cancelled request: %v, want bad-request", err)
	}
}

// TestRetryNoRunnersFailure re-joins a request the no-runners watchdog
// failed (its interest was dropped like a cancel).
func TestRetryNoRunnersFailure(t *testing.T) {
	e := newEnv(t)
	e.startRunner([]wire.Class{"c0.2-m1", "c1-m1"}, e.standardHandler(builddef.Pinned{}))
	sub, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: e.treeDef("watchdog")})
	if err != nil {
		t.Fatal(err)
	}
	e.s.mu.Lock()
	r := e.s.requests[sub.RequestID]
	r.failReason = "no live runners (test)"
	e.s.dropInterestLocked(r.id, r.target)
	e.s.mu.Unlock()

	if _, err := e.s.Retry(e.ctx, sub.RequestID); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if snap := e.watchTerminal(sub.RequestID); snap.Phase != "done" || snap.Error != "" {
		t.Fatalf("retried request = %s %q, want done", snap.Phase, snap.Error)
	}
}

// TestJobsCarryPullKeys: every queued job names, next to each PullRefs
// entry, the key the server bound it to — what lets a runner refuse a stale
// local copy (issue #22's first-build import failure).
func TestJobsCarryPullKeys(t *testing.T) {
	e := newEnv(t)
	fr := e.startRunner([]wire.Class{"c0.2-m1", "c1-m1"}, e.standardHandler(builddef.Pinned{}))
	sub, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: e.treeDef("pullkeys")})
	if err != nil {
		t.Fatal(err)
	}
	if snap := e.watchTerminal(sub.RequestID); snap.Phase != "done" {
		t.Fatalf("phase = %s", snap.Phase)
	}
	checked := 0
	for _, kind := range []string{wire.KindBuildFrom, wire.KindPluginResolve, wire.KindPin, wire.KindBuildRun} {
		for _, job := range fr.byKind(kind) {
			if len(job.PullKeys) != len(job.PullRefs) {
				t.Fatalf("%s: %d pull keys for %d pull refs", job.Node, len(job.PullKeys), len(job.PullRefs))
			}
			for i, name := range job.PullRefs {
				k, ok := e.getRef(name)
				if !ok || !bytes.Equal(job.PullKeys[i], k[:]) {
					t.Fatalf("%s: pull key for %s = %x, server has %s (present %v)", job.Node, name, job.PullKeys[i], k, ok)
				}
				checked++
			}
		}
	}
	if checked == 0 {
		t.Fatal("no pull refs checked")
	}
}
