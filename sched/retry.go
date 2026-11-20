package sched

import (
	"context"
	"time"

	"github.com/jobs-build/jobs-iroh/api"
	"github.com/jobs-build/jobs-iroh/wire"
)

// retryFailedLocked re-activates every node in target's closure that failed
// before the cutoff, deps before dependents, and returns how many it
// re-activated. It is what makes "retry is by resubmit" (failNodeLocked)
// true while other requests still hold interest in a failed node: before
// it, a failed node stayed failed for every resubmit until each request
// holding it was cancelled or deleted (issue #22). A re-activated node
// re-derives from the CAS like a fresh one — its output ref may exist by now
// (another path built it), else it re-unfolds and re-queues with a fresh
// retry budget. The cutoff spares failures the caller's own work just
// produced (a submit's join failing synchronously, e.g. a sub-build cycle):
// re-running those at once would only fail again and record it twice.
func (s *Sched) retryFailedLocked(target *node, failedBefore time.Time) int {
	if target == nil {
		return 0
	}
	seen := map[*node]bool{}
	var failed []*node
	var walk func(n *node)
	walk = func(n *node) {
		if seen[n] {
			return
		}
		seen[n] = true
		for d := range n.deps {
			walk(d)
		}
		if n.phase == wire.PhaseFailed && n.doneAt.Before(failedBefore) {
			failed = append(failed, n) // post-order: every dep is listed first
		}
	}
	walk(target)
	for _, n := range failed {
		s.reactivateLocked(n)
	}
	return len(failed)
}

// reactivateLocked turns one failed node back into a waiting one and drives
// it exactly as require drives a fresh node: doneness fast-path, else unfold
// (idempotent joins) and enqueue when its deps are done; orchestrators
// re-advance. Its failure records stay in FAILURES — diagnose keeps the
// history across the retry.
func (s *Sched) reactivateLocked(n *node) {
	if n.phase != wire.PhaseFailed || s.nodes[n.id] != n {
		return
	}
	n.phase = wire.PhaseWaiting
	n.errSummary = ""
	n.consecRetry, n.consecControl = 0, 0
	n.doneAt = time.Time{}
	s.putNodeStatusLocked(n)
	s.bumpLocked()
	s.log.Info("failed node re-activated", "node", n.name)
	switch n.id.kind {
	case wire.KindBuildValue:
		s.advanceBuildValueLocked(n)
		return
	case wire.KindTestValue:
		// The input loop may have failed part-way after marking itself done;
		// requiring the inputs again is an idempotent join.
		n.inputsRequired = false
		s.advanceTestValueLocked(n)
		return
	}
	done, err := s.outputExistsLocked(n)
	switch {
	case err != nil:
		s.failNodeLocked(n, "doneness check: "+err.Error())
	case done:
		n.cached = true // built elsewhere meanwhile: this incarnation never ran
		s.nodeDoneLocked(n)
	default:
		s.unfoldLocked(n)
		s.maybeEnqueueLocked(n)
	}
}

// Retry re-runs a failed request in place: its failed nodes are re-activated
// (shared ones for every request holding them), and a request the
// no-runners watchdog failed re-joins its target. The request ID stays the
// same, so `watch` re-attaches to it. A request that is running again —
// because retrying another request re-activated the failed nodes they share
// — is answered with 0 re-activated, not an error, so retrying every failed
// request in turn works. A done request, or a cancelled one (that was a
// decision — resubmit to undo it), is a bad request; a platform without a
// live runner is unavailable, as at submit.
func (s *Sched) Retry(ctx context.Context, requestID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.requests[requestID]
	if !ok {
		return 0, notFound("request %s not found", requestID)
	}
	if r.cancelled {
		return 0, badRequest("request %s was cancelled — resubmit it instead", requestID)
	}
	switch phase := s.assembleLocked(r).Phase; phase {
	case "failed":
	case "running":
		return 0, nil
	default:
		return 0, badRequest("request %s is %s, not failed", requestID, phase)
	}
	if !s.livePlatformLocked(r.platform, time.Now()) {
		return 0, &Error{Code: api.CodeUnavailable, Text: "no live runners for platform " + r.platform + " — attach a runner and retry"}
	}
	if r.failReason != "" {
		// The watchdog dropped the request's interest like a cancel: re-join
		// the target (re-created from its def if it was evicted meanwhile).
		r.failReason, r.noRunnersSince = "", time.Time{}
		t := r.target
		r.target = s.require(t.id.kind, t.id.key, t.def, r.platform, nil, map[string]struct{}{r.id: {}}, t.label)
	}
	n := s.retryFailedLocked(r.target, time.Now())
	s.putRequestStatusLocked(r, s.assembleLocked(r))
	s.bumpLocked()
	s.log.Info("request retried", "request", requestID, "reactivated", n)
	return n, nil
}
