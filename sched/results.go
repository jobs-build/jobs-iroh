package sched

import (
	"errors"
	"fmt"
	"strings"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/wire"
)

// runnerScratchPrefix is the only ref namespace the scheduler will delete on
// a runner's behalf (the runner's push ref for one attempt's output
// closure). Anything else in Result.ScratchRef is refused — a buggy or
// hostile result must not delete real refs.
const runnerScratchPrefix = "runner-push/"

// clientScratchPrefix is the analogue for SubmitRequest.ScratchRef (the
// client's source push ref) — the only namespace Submit will delete.
const clientScratchPrefix = "client-push/"

// runResults is the results consumer goroutine: one strictly-ordered,
// explicit-ack loop over the durable RESULTS consumer. Every message is
// acked exactly once, after handling — handling is idempotent against the
// node table ((node, gen) monotonicity), so redelivery after a crash between
// handle and ack merely re-runs a no-op.
func (s *Sched) runResults(it jetstream.MessagesContext) {
	defer s.wg.Done()
	for {
		msg, err := it.Next()
		if err != nil {
			if s.ctx.Err() == nil && !errors.Is(err, jetstream.ErrMsgIteratorClosed) {
				s.log.Warn("results iterator stopped", "error", err)
			}
			return
		}
		s.handleResult(msg.Data())
		if err := msg.Ack(); err != nil && s.ctx.Err() == nil {
			s.log.Warn("result ack failed", "error", err)
		}
	}
}

// handleResult applies one wire.Result to the graph. Dedup: a result is
// live only when its (node, gen) matches the node's current attempt and no
// result for that gen was handled yet; everything else — unknown node
// (cancelled/donated interest), stale gen, duplicate delivery — is dropped
// after scratch-ref cleanup.
func (s *Sched) handleResult(data []byte) {
	var res wire.Result
	if err := wire.Decode(data, &res); err != nil {
		s.log.Warn("undecodable result dropped", "error", err)
		return
	}
	if s.touch != nil && len(res.ReadRefs) > 0 {
		// Reads happened regardless of how the result is disposed of below
		// (stale, duplicate, unknown node) — touch before the dedup gate.
		s.touch(res.ReadRefs)
	}
	defer s.deleteScratch(res.ScratchRef)

	kind, k, err := wire.ParseNodeName(res.Node)
	if err != nil {
		s.log.Warn("result for unparsable node dropped", "node", res.Node, "error", err)
		return
	}

	s.mu.Lock()
	n := s.nodes[nodeID{kind: kind, key: k}]
	if n == nil || res.Gen != n.gen || res.Gen <= n.lastResultGen ||
		(n.phase != wire.PhaseQueued && n.phase != wire.PhaseRunning) {
		s.mu.Unlock()
		s.log.Debug("stale or unclaimed result dropped", "node", res.Node, "gen", res.Gen, "class", res.Class)
		return
	}
	n.lastResultGen = res.Gen
	n.runner = res.Runner
	if n.startedAt.IsZero() {
		n.startedAt = n.enqueuedAt
	}

	switch res.Class {
	case wire.ClassOK:
		n.phase = wire.PhasePublishing
		s.putNodeStatusLocked(n)
		s.bumpLocked()
		// Snapshot everything commit needs, then run the store-heavy part
		// (gate + CheckComplete + ref writes) without the lock.
		spec := commitSpec{
			kind:     kind,
			key:      k,
			platform: n.platform,
			caches:   n.caches,
			refs:     res.Refs,
		}
		s.mu.Unlock()
		commitErr := s.commit(spec)
		s.mu.Lock()
		defer s.mu.Unlock()
		if cur := s.nodes[n.id]; cur != n || n.gen != res.Gen {
			// Cancelled mid-commit. Any refs already written are CAS truth —
			// harmless, a later activation fast-paths on them.
			return
		}
		switch {
		case commitErr == nil:
			n.consecRetry, n.consecControl = 0, 0
			s.applyRecipeLabelLocked(n, res.Refs)
			s.nodeDoneLocked(n)
		case isGateError(commitErr):
			// A gate rejection means a broken or hostile runner — hard, one
			// strike (fail closed; nothing was written).
			s.failNodeAttrLocked(n, &res, wire.FailOriginGate, commitErr.Error())
		default:
			// Incomplete closure or a store hiccup: the objects may still be
			// syncing or the runner crashed mid-push. Retryable — re-run,
			// never trust an unverified batch.
			s.retryLocked(n, &res, wire.FailOriginCommit, "commit: "+commitErr.Error(), false)
		}
		return

	case wire.ClassHard:
		summary := res.ErrSummary
		if summary == "" {
			summary = fmt.Sprintf("hard failure (exit %d)", res.Exit)
		}
		s.failNodeAttrLocked(n, &res, wire.FailOriginResult, summary)

	case wire.ClassRetryable:
		s.retryLocked(n, &res, wire.FailOriginResult, res.ErrSummary, false)

	case wire.ClassControl:
		// Control-plane verdicts must not burn the job's retryable budget —
		// they say nothing about the job (issue #153); own generous cap.
		s.retryLocked(n, &res, wire.FailOriginResult, res.ErrSummary, true)

	case wire.ClassCancelled:
		// Runner-side cancel (shutdown, lost duplicate race). Re-enqueue as
		// long as somebody still wants the node; no budget burned, but the
		// control cap bounds pathological loops.
		if len(n.interest) > 0 {
			s.retryLocked(n, &res, wire.FailOriginResult, res.ErrSummary, true)
		}

	default:
		// Unknown class fails closed.
		s.failNodeAttrLocked(n, &res, wire.FailOriginResult, "unknown result class "+res.Class)
	}
	s.mu.Unlock()
}

// retryLocked implements the ported retry budget: retryable results burn a
// consecutive budget of maxConsecRetryable (3) with exponential backoff
// 1s·2^(n−1) capped at 30s, then escalate to hard ("retry budget
// exhausted"). Control-class re-runs skip the budget and the backoff but are
// capped at maxConsecControl (20) consecutive occurrences. Every decision
// folds a durable FailureRecord (before the re-enqueue that would bump the
// gen and reset the log ring); res attributes the attempt for that record.
func (s *Sched) retryLocked(n *node, res *wire.Result, origin, summary string, control bool) {
	if control {
		n.consecControl++
		if n.consecControl > maxConsecControl {
			s.failNodeAttrLocked(n, res, origin, "control budget exhausted: "+summary)
			return
		}
		n.phase = wire.PhaseWaiting
		n.errSummary = summary
		s.recordFailureLocked(n, res, origin, wire.FailDispositionRetry, summary, 0)
		s.enqueueLocked(n)
		return
	}
	n.consecRetry++
	n.consecControl = 0
	if n.consecRetry > maxConsecRetryable {
		s.failNodeAttrLocked(n, res, origin, "retry budget exhausted: "+summary)
		return
	}
	backoff := s.retryBase << (n.consecRetry - 1)
	if limit := 30 * s.retryBase; backoff > limit {
		backoff = limit
	}
	n.phase = wire.PhaseWaiting
	n.errSummary = summary
	s.recordFailureLocked(n, res, origin, wire.FailDispositionRetry, summary, backoff)
	s.putNodeStatusLocked(n)
	s.bumpLocked()
	s.log.Info("retryable failure; backing off", "node", n.name, "gen", n.gen,
		"consecutive", n.consecRetry, "backoff", backoff, "error", summary)
	s.armReenqueueLocked(n, backoff)
}

// commitSpec is the immutable snapshot commit works from once the lock is
// released.
type commitSpec struct {
	kind     string
	key      key.Key
	platform string
	caches   []builddef.PinnedCache
	refs     []wire.RefProposal
}

// commit runs the server-side publish step for one ok result: gate-check
// every proposed name (fail closed, nothing written on rejection), verify
// object completeness of every proposed key against the server store
// (objects-before-ref), then write the refs in payload order — the runner
// ordered the batch [caches…, build-output-deps:F, build-output:F], so a
// crash mid-write never yields a done-looking build with a missing closure.
// A buildfrom commit additionally writes the server-side f-tree/<F> closure
// carrier (see FTreeRef).
func (s *Sched) commit(sp commitSpec) error {
	if err := gateAllowed(sp.kind, sp.key, sp.platform, sp.refs, sp.caches); err != nil {
		return err
	}
	keys := make([]key.Key, len(sp.refs))
	for i, r := range sp.refs {
		k, err := wire.ParseKey(r.Key)
		if err != nil {
			return &gateError{fmt.Errorf("ref %q: bad key: %w", r.Name, err)}
		}
		keys[i] = k
	}
	get := func(k key.Key) ([]byte, error) { return s.store.Get(k) }
	has := func(k key.Key) (bool, error) { return s.store.Has(k) }
	for i, r := range sp.refs {
		if _, err := fstree.CheckComplete(keys[i], get, has, 0); err != nil {
			return fmt.Errorf("ref %q: incomplete closure: %w", r.Name, err)
		}
	}
	for i, r := range sp.refs {
		if err := s.store.PutRef(s.ctx, r.Name, keys[i]); err != nil {
			return fmt.Errorf("put ref %q: %w", r.Name, err)
		}
	}
	if sp.kind == wire.KindBuildFrom {
		for i, r := range sp.refs {
			if r.Name == "build-from:"+sp.key.String() {
				if err := s.store.PutRef(s.ctx, FTreeRef(keys[i]), keys[i]); err != nil {
					return fmt.Errorf("put ref %q: %w", FTreeRef(keys[i]), err)
				}
			}
		}
	}
	// Pin commit derives the KP binding server-side (sibling-sources design
	// §6.3): build-pinned:<KP> + kp-tree/<KP> + pin-cover/<v>:F, from the
	// just-gated pinned blob — "re-derived from the CAS, never trusted from
	// the runner". A crash anywhere in the sequence is healed by
	// resolveKPLocked's on-demand re-derivation, so this is an optimization
	// of the common path, not a correctness point. An error here is a commit
	// error (retryable — the pin re-runs, wasteful but never wrong).
	if sp.kind == wire.KindPin {
		if _, err := s.deriveKP(sp.key); err != nil {
			return fmt.Errorf("derive KP for %s: %w", sp.key, err)
		}
	}
	return nil
}

func isGateError(err error) bool {
	var ge *gateError
	return errors.As(err, &ge)
}

// deleteScratch removes the runner's per-attempt push ref after commit (or
// after a dropped result). Only the runner-push/ namespace is deletable.
func (s *Sched) deleteScratch(ref string) {
	if ref == "" {
		return
	}
	if !strings.HasPrefix(ref, runnerScratchPrefix) {
		s.log.Warn("refusing to delete non-scratch ref", "ref", ref)
		return
	}
	if err := s.store.DeleteRef(s.ctx, ref); err != nil && !errors.Is(err, amber.ErrRefNotFound) {
		s.log.Warn("scratch ref delete failed", "ref", ref, "error", err)
	}
}
