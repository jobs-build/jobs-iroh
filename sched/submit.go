package sched

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"

	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/api"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/resources"
	"github.com/jobs-build/jobs-iroh/wire"
)

// request is one submitted build: in-memory truth (plus a status-KV mirror);
// requests do NOT survive a server restart — recovery is by resubmit, and
// the doneness fast-path makes a resubmitted finished closure free.
type request struct {
	id        string
	k         key.Key
	platform  string
	res       resources.Resources
	createdNs int64
	cancelled bool
	target    *node // the buildvalue/K value node
	// failReason marks a request-level failure that belongs to no single
	// node (the no-runners watchdog, issue #8); non-empty ⇒ phase "failed".
	failReason string
	// noRunnersSince is the watchdog's bookkeeping: when the request's
	// platform last lost its final live runner (zero while one is live).
	noRunnersSince time.Time
}

// Submit validates and registers one build request: decode + canonicality-
// check the client Definition (K = its file key), completeness-check and
// publish a tree source, ingest the def bytes, and join the target
// buildvalue/K node. The def's referenced objects must already be in the
// server store (pushed over jobs-amber-admin) — a missing tree closure is
// api.CodeIncomplete.
func (s *Sched) Submit(ctx context.Context, req api.SubmitRequest) (api.Submitted, error) {
	if len(req.Def) == 0 {
		return api.Submitted{}, badRequest("submit: empty definition")
	}
	def, err := builddef.DecodeDefinition(req.Def)
	if err != nil {
		return api.Submitted{}, badRequest("submit: decode definition: %v", err)
	}
	canon, err := def.Canonical()
	if err != nil {
		return api.Submitted{}, badRequest("submit: canonicalize definition: %v", err)
	}
	if !bytes.Equal(canon, req.Def) {
		// Identity is the canonical bytes; accepting a non-canonical encoding
		// would fork K between client and server.
		return api.Submitted{}, badRequest("submit: definition is not canonical CBOR")
	}
	if def.Platform == "" {
		return api.Submitted{}, badRequest("submit: definition has no platform")
	}
	switch def.Source.Kind {
	case builddef.KindImport, builddef.KindBuild, builddef.KindTree:
	default:
		return api.Submitted{}, badRequest("submit: unknown source kind %q", def.Source.Kind)
	}

	var reqRes resources.Resources
	if req.Resources != nil {
		if req.Resources.CPU != "" {
			if reqRes.CPUMilli, err = resources.ParseCPU(req.Resources.CPU); err != nil {
				return api.Submitted{}, badRequest("submit: %v", err)
			}
		}
		if req.Resources.Memory != "" {
			if reqRes.MemBytes, err = resources.ParseMem(req.Resources.Memory); err != nil {
				return api.Submitted{}, badRequest("submit: %v", err)
			}
		}
	}

	// Tree source: the client pushed the tree objects beforehand; verify the
	// closure is complete in the server store, then publish the
	// build-from-tree:<T> carrier the buildfrom stage pulls by name.
	if def.Source.Kind == builddef.KindTree {
		t, err := builddef.DecodeTreeKey(def.Source.Definition)
		if err != nil {
			return api.Submitted{}, badRequest("submit: decode tree source: %v", err)
		}
		get := func(k key.Key) ([]byte, error) { return s.store.Get(k) }
		has := func(k key.Key) (bool, error) { return s.store.Has(k) }
		if _, err := fstree.CheckComplete(t, get, has, 0); err != nil {
			return api.Submitted{}, incomplete("submit: source tree %s incomplete: %v", t, err)
		}
		if err := s.store.PutRef(ctx, "build-from-tree:"+t.String(), t); err != nil {
			return api.Submitted{}, internal("submit: publish build-from-tree: %v", err)
		}
	}

	// Ingest the definition bytes — K is their file key by construction; the
	// stored blob is what runners and later unfolds read back.
	k, err := s.store.IngestFile(ctx, req.Def)
	if err != nil {
		return api.Submitted{}, internal("submit: ingest definition: %v", err)
	}
	want, err := (builddef.Input{Kind: builddef.KindBuild, Definition: req.Def}).Key()
	if err != nil || k != want {
		return api.Submitted{}, internal("submit: ingested key %s does not match definition key", k)
	}
	// Bookkeeping ref for submitted roots (def durability + by-key tooling);
	// unfold-discovered defs live inside their parents' pinned trees.
	if err := s.store.PutRef(ctx, "build:"+k.String(), k); err != nil {
		return api.Submitted{}, internal("submit: bookkeeping ref: %v", err)
	}

	id, err := newRequestID()
	if err != nil {
		return api.Submitted{}, internal("submit: %v", err)
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return api.Submitted{}, &Error{Code: api.CodeUnavailable, Text: "scheduler closed"}
	}
	r := &request{
		id:        id,
		k:         k,
		platform:  def.Platform,
		res:       reqRes,
		createdNs: time.Now().UnixNano(),
	}
	// Register the request BEFORE the graph unfolds: requirementLocked
	// consults s.requests, so the very first enqueue already carries the
	// request's resources.
	s.requests[id] = r
	r.target = s.require(wire.KindBuildValue, k, req.Def, def.Platform, nil, map[string]struct{}{id: {}}, req.Label)
	snap := s.assembleLocked(r)
	// No-runners rejection (issue #8): a build that still needs runner work
	// on a platform with no live runner would stall forever — reject it now.
	// Checked AFTER the join so a fully cached closure (terminal right away
	// via the doneness fast-path) still completes without any fleet. The
	// client's scratch ref is deliberately left alone: the client may attach
	// a runner and resubmit, and the successful submit cleans it up.
	if !snap.Terminal && !s.livePlatformLocked(r.platform, time.Now()) {
		s.dropInterestLocked(id, r.target)
		delete(s.requests, id)
		s.mu.Unlock()
		return api.Submitted{}, &Error{Code: api.CodeUnavailable,
			Text: fmt.Sprintf("no live runners for platform %s — build rejected (attach a runner and resubmit)", def.Platform)}
	}
	s.putRequestStatusLocked(r, snap)
	s.bumpLocked()
	s.mu.Unlock()

	// The client's push scratch ref is no longer needed once the submit
	// committed (objects are named by build:K / build-from-tree:T now). Only
	// the client-push/ namespace is deletable — a hostile or buggy client
	// must not be able to name a real ref (build-output:F, shell:<p>, …)
	// here and have the server delete it (mirrors deleteScratch's
	// runner-push/ guard).
	if req.ScratchRef != "" {
		if !strings.HasPrefix(req.ScratchRef, clientScratchPrefix) {
			s.log.Warn("refusing to delete non-scratch submit ref", "ref", req.ScratchRef)
		} else if err := s.store.DeleteRef(ctx, req.ScratchRef); err != nil && !errors.Is(err, amber.ErrRefNotFound) {
			s.log.Warn("client scratch ref delete failed", "ref", req.ScratchRef, "error", err)
		}
	}

	s.log.Info("build submitted", "request", id, "k", k.String(), "platform", def.Platform)
	return api.Submitted{RequestID: id, K: k[:]}, nil
}

func newRequestID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "r" + hex.EncodeToString(b[:]), nil
}

// Cancel drops the request's interest: nodes needed by nobody else leave
// the table and their queued job messages are best-effort purged from the
// work queue. In-flight attempts are not chased — their results are
// dropped on arrival (running once too often is wasteful, never wrong).
// The request stays inspectable until Delete.
func (s *Sched) Cancel(ctx context.Context, requestID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.requests[requestID]
	if !ok {
		return notFound("request %s not found", requestID)
	}
	if r.cancelled {
		return nil
	}
	r.cancelled = true
	s.dropInterestLocked(requestID, r.target)
	snap := s.assembleLocked(r)
	s.putRequestStatusLocked(r, snap)
	s.bumpLocked()
	s.log.Info("request cancelled", "request", requestID)
	return nil
}

// Delete cancels (if needed) and forgets the request: memory, watchers and
// the status-KV mirror all drop. History is gone — jobs-iroh keeps no
// durable request record.
func (s *Sched) Delete(ctx context.Context, requestID string) error {
	s.mu.Lock()
	r, ok := s.requests[requestID]
	if !ok {
		s.mu.Unlock()
		return notFound("request %s not found", requestID)
	}
	r.cancelled = true
	s.dropInterestLocked(requestID, r.target)
	final := s.assembleLocked(r)
	final.Terminal = true
	delete(s.requests, requestID)
	for w := range s.watchers {
		if w.reqID == requestID {
			w.final = &final
		}
	}
	s.bumpLocked()
	s.mu.Unlock()

	if err := s.kv.Purge(s.ctx, requestKVKey(requestID)); err != nil && s.ctx.Err() == nil {
		s.log.Warn("request status purge failed", "request", requestID, "error", err)
	}
	s.log.Info("request deleted", "request", requestID)
	return nil
}

// watcher is one Watch subscription: snapshots are pushed coalesced (at
// most one per snapshot tick, ≤4/s), and the terminal snapshot is
// guaranteed — it retries every tick until delivered, then the channel
// closes.
type watcher struct {
	reqID   string
	ch      chan api.Snapshot
	lastVer uint64
	closed  bool
	final   *api.Snapshot // set when the request was deleted under the watcher
}

// Watch streams coalesced snapshots of one request until terminal. The
// returned stop function detaches the watcher (also wired to ctx); the
// channel closes after the terminal snapshot (or on stop).
func (s *Sched) Watch(ctx context.Context, requestID string) (<-chan api.Snapshot, func(), error) {
	s.mu.Lock()
	r, ok := s.requests[requestID]
	if !ok {
		s.mu.Unlock()
		return nil, nil, notFound("request %s not found", requestID)
	}
	w := &watcher{reqID: requestID, ch: make(chan api.Snapshot, 8), lastVer: s.version}
	snap := s.assembleLocked(r)
	w.ch <- snap // buffered, never blocks here
	if snap.Terminal {
		close(w.ch)
		s.mu.Unlock()
		return w.ch, func() {}, nil
	}
	s.watchers[w] = struct{}{}
	s.mu.Unlock()

	stop := func() {
		s.mu.Lock()
		if !w.closed {
			w.closed = true
			delete(s.watchers, w)
			close(w.ch)
		}
		s.mu.Unlock()
	}
	context.AfterFunc(ctx, stop)
	return w.ch, stop, nil
}

// Requests lists all live requests (admin), newest first.
func (s *Sched) Requests() []api.RequestInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]api.RequestInfo, 0, len(s.requests))
	for _, r := range s.requests {
		snap := s.assembleLocked(r)
		out = append(out, api.RequestInfo{
			RequestID: r.id,
			K:         r.k[:],
			Platform:  r.platform,
			Phase:     snap.Phase,
			Counts:    snap.Counts,
			CreatedNs: r.createdNs,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedNs != out[j].CreatedNs {
			return out[i].CreatedNs > out[j].CreatedNs
		}
		return out[i].RequestID < out[j].RequestID
	})
	return out
}

// Stats reports scheduler-level counters (admin). StoreBytes stays zero
// here — disk usage belongs to the layer that owns the data directory; the
// scheduler only sees the opened store.
func (s *Sched) Stats() api.StatsReply {
	refs, err := s.store.ListRefs(s.ctx)
	if err != nil {
		s.log.Warn("ref listing failed", "error", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return api.StatsReply{
		RefCount:     len(refs),
		UptimeNs:     time.Since(s.startedAt).Nanoseconds(),
		Requests:     len(s.requests),
		NodesTracked: len(s.nodes),
	}
}

// assembleLocked builds the request's snapshot: closure walk from the
// target, per-node phase with failed-upstream derived (never stored), counts
// and server-side elapsed times.
func (s *Sched) assembleLocked(r *request) api.Snapshot {
	var closure []*node
	seen := map[*node]bool{}
	var walk func(n *node)
	walk = func(n *node) {
		if n == nil || seen[n] {
			return
		}
		seen[n] = true
		closure = append(closure, n)
		for d := range n.deps {
			walk(d)
		}
	}
	walk(r.target)

	// failed-upstream: any transitive dep hard-failed (per-request
	// derivation over this closure).
	memo := map[*node]bool{}
	var upstreamFailed func(n *node) bool
	upstreamFailed = func(n *node) bool {
		if v, ok := memo[n]; ok {
			return v
		}
		memo[n] = false
		v := false
		for d := range n.deps {
			if d.phase == wire.PhaseFailed || upstreamFailed(d) {
				v = true
				break
			}
		}
		memo[n] = v
		return v
	}

	now := time.Now()
	var counts wire.Counts
	anyFailed := false
	nodes := make([]api.NodeSnap, 0, len(closure))
	for _, n := range closure {
		phase := n.phase
		if phase != wire.PhaseDone && phase != wire.PhaseFailed && upstreamFailed(n) {
			phase = wire.PhaseUpstream
		}
		counts.Total++
		switch phase {
		case wire.PhaseDone:
			counts.Done++
		case wire.PhaseFailed, wire.PhaseUpstream:
			counts.Failed++
			anyFailed = true
		case wire.PhaseQueued, wire.PhaseRunning, wire.PhasePublishing:
			counts.Running++
		default:
			counts.Waiting++
		}
		var deps []string
		for d := range n.deps {
			deps = append(deps, d.name)
		}
		sort.Strings(deps)
		nodes = append(nodes, api.NodeSnap{
			Node:       n.name,
			Label:      n.label,
			Phase:      phase,
			Gen:        n.gen,
			Runner:     n.runner,
			ElapsedMs:  elapsedMs(n, now),
			ErrSummary: n.errSummary,
			Deps:       deps,
			Cached:     n.cached,
		})
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Node < nodes[j].Node })

	phase := "running"
	switch {
	case r.cancelled:
		phase = "cancelled"
	case r.failReason != "":
		// Request-level failure (no-runners watchdog): no node carries it.
		phase = "failed"
	case r.target != nil && r.target.phase == wire.PhaseDone:
		phase = "done"
	case anyFailed:
		phase = "failed"
	}
	return api.Snapshot{
		RequestID: r.id,
		Phase:     phase,
		Counts:    counts,
		Nodes:     nodes,
		Terminal:  phase != "running",
		Error:     r.failReason,
	}
}

// elapsedMs computes a node's attempt duration server-side (clock-skew
// safety — clients never subtract their own clocks).
func elapsedMs(n *node, now time.Time) int64 {
	start := n.startedAt
	if start.IsZero() {
		start = n.enqueuedAt
	}
	if start.IsZero() {
		return 0
	}
	end := now
	if !n.doneAt.IsZero() {
		end = n.doneAt
	}
	if end.Before(start) {
		return 0
	}
	return end.Sub(start).Milliseconds()
}

// runTicker is the snapshot ticker: every snapshotTick it pushes coalesced
// snapshots to dirty watchers (retrying terminal delivery until it lands,
// then closing) and mirrors changed request statuses into the status KV.
func (s *Sched) runTicker() {
	defer s.wg.Done()
	tick := time.NewTicker(snapshotTick)
	defer tick.Stop()
	var lastNoRunnerCheck time.Time
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-tick.C:
		}
		type kvWrite struct {
			key string
			val []byte
		}
		var kvWrites []kvWrite

		s.mu.Lock()
		// No-runners watchdog (issue #8), at its own coarser cadence and
		// BEFORE the snapshot fold so a failure it declares reaches watchers
		// and the KV mirror in this same tick.
		if now := time.Now(); now.Sub(lastNoRunnerCheck) >= s.noRunnerCheck {
			lastNoRunnerCheck = now
			s.checkNoRunnersLocked(now)
		}
		version := s.version
		cache := map[string]api.Snapshot{}
		snapFor := func(id string) (api.Snapshot, bool) {
			if snap, ok := cache[id]; ok {
				return snap, true
			}
			r, ok := s.requests[id]
			if !ok {
				return api.Snapshot{}, false
			}
			snap := s.assembleLocked(r)
			cache[id] = snap
			return snap, true
		}

		for w := range s.watchers {
			var snap api.Snapshot
			switch {
			case w.final != nil:
				snap = *w.final
			default:
				var ok bool
				snap, ok = snapFor(w.reqID)
				if !ok {
					snap = api.Snapshot{RequestID: w.reqID, Phase: "cancelled", Terminal: true}
				}
				if w.lastVer == version && !snap.Terminal {
					continue
				}
			}
			select {
			case w.ch <- snap:
				w.lastVer = version
				if snap.Terminal {
					w.closed = true
					delete(s.watchers, w)
					close(w.ch)
				}
			default:
				// Receiver lagging: coalesce — retry next tick (terminal
				// snapshots keep retrying until delivered).
			}
		}

		if version != s.lastKVVersion {
			s.lastKVVersion = version
			for id, r := range s.requests {
				snap, _ := snapFor(id)
				fp := fmt.Sprintf("%s|%+v", snap.Phase, snap.Counts)
				if s.lastReqKV[id] == fp {
					continue
				}
				s.lastReqKV[id] = fp
				kvWrites = append(kvWrites, kvWrite{
					key: requestKVKey(id),
					val: wire.MustEncode(requestStatus(r, snap)),
				})
			}
			for id := range s.lastReqKV {
				if _, ok := s.requests[id]; !ok {
					delete(s.lastReqKV, id)
				}
			}
		}
		s.mu.Unlock()

		for _, wr := range kvWrites {
			if _, err := s.kv.Put(s.ctx, wr.key, wr.val); err != nil && s.ctx.Err() == nil {
				s.log.Debug("request status write failed", "key", wr.key, "error", err)
			}
		}
	}
}

// checkNoRunnersLocked fails every non-terminal request whose platform has
// had no live runner for a continuous noRunnerAfter (issue #8): interest is
// dropped exactly like Cancel (queued job messages purged, in-flight results
// dropped on arrival), the request stays inspectable until Delete, and the
// reason rides Snapshot.Error/RequestStatus.Error. A runner reappearing
// resets the clock.
func (s *Sched) checkNoRunnersLocked(now time.Time) {
	for _, r := range s.requests {
		if r.cancelled || r.failReason != "" {
			r.noRunnersSince = time.Time{}
			continue
		}
		if snap := s.assembleLocked(r); snap.Terminal {
			r.noRunnersSince = time.Time{}
			continue
		}
		if s.livePlatformLocked(r.platform, now) {
			r.noRunnersSince = time.Time{}
			continue
		}
		if r.noRunnersSince.IsZero() {
			r.noRunnersSince = now
			continue
		}
		if now.Sub(r.noRunnersSince) < s.noRunnerAfter {
			continue
		}
		r.failReason = fmt.Sprintf("no live runners for platform %s for %s — build failed (attach a runner and resubmit)",
			r.platform, now.Sub(r.noRunnersSince).Round(time.Second))
		s.dropInterestLocked(r.id, r.target)
		s.bumpLocked()
		s.log.Warn("request failed: no runners for platform",
			"request", r.id, "platform", r.platform, "stalled", now.Sub(r.noRunnersSince))
	}
}

func requestKVKey(id string) string { return "req." + id }

func requestStatus(r *request, snap api.Snapshot) wire.RequestStatus {
	return wire.RequestStatus{
		Phase:     snap.Phase,
		TargetK:   r.k[:],
		Platform:  r.platform,
		Counts:    snap.Counts,
		CreatedNs: r.createdNs,
		Error:     snap.Error,
	}
}

// putRequestStatusLocked writes the request's status KV entry immediately
// (submit/cancel transitions; steady-state coalescing is the ticker's job).
func (s *Sched) putRequestStatusLocked(r *request, snap api.Snapshot) {
	s.lastReqKV[r.id] = fmt.Sprintf("%s|%+v", snap.Phase, snap.Counts)
	if _, err := s.kv.Put(s.ctx, requestKVKey(r.id), wire.MustEncode(requestStatus(r, snap))); err != nil && s.ctx.Err() == nil {
		s.log.Debug("request status write failed", "request", r.id, "error", err)
	}
}
