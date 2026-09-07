package sched

import (
	"sort"
	"strings"
	"time"

	"github.com/amber-store/core/key"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/importdef"
	"github.com/jobs-build/jobs-iroh/resources"
	"github.com/jobs-build/jobs-iroh/wire"
)

// nodeID keys the in-memory node table — (kind, key) get-or-create IS the
// join (jobs-scheduler-semantics §1).
type nodeID struct {
	kind string
	key  key.Key
}

// node is one entry of the in-memory graph. All fields are guarded by
// Sched.mu. States: waiting → queued → running → publishing → done | failed
// ("ready" is instantaneous in a single process — a node whose deps are all
// done enqueues in the same critical section).
type node struct {
	id   nodeID
	name string // <kind>_<hexkey>

	phase    string
	gen      uint64
	def      []byte // canonical def CBOR (import: importdef; buildfrom/buildvalue: builddef)
	platform string

	deps       map[*node]struct{}
	dependents map[*node]struct{}
	interest   map[string]struct{} // live request IDs

	// buildvalue pipeline state.
	f          key.Key
	fResolved  bool
	kp         key.Key // resolved from pin-cover/<v>:F after pin (sibling-sources §10.1)
	kpResolved bool

	// Unfold-derived placement inputs.
	pinnedRes resources.Resources    // buildrun: Pinned.Resources
	caches    []builddef.PinnedCache // buildrun: Pinned.Caches (gate ground truth)
	pinInputs []builddef.Input       // pin: plugins ∪ resolution deps (stable order)
	// label is the node's display-only name: the recipe dep name (or dir /
	// fetcher) it was first required under. Never part of identity.
	label     string
	runInputs []builddef.Input // buildrun: Pinned.Inputs

	// Failure/retry accounting.
	consecRetry   int
	consecControl int
	lastResultGen uint64

	runner     string
	queueSeq   uint64 // JOBS stream seq of the current queued job msg (0 = none)
	errSummary string
	enqueuedAt time.Time
	startedAt  time.Time
	doneAt     time.Time
	// cached: fast-pathed done at creation (output ref existed) — the node
	// never ran for this incarnation. Explicit rather than inferred from
	// zero timestamps: buildvalue never gets startedAt either way.
	cached bool
}

func placeable(kind string) bool { return kind != wire.KindBuildValue }

// genSeed is a fresh node's attempt-generation base. Node state is
// memory-only, but the JOBS/RESULTS msg-id dedup windows (job-<node>-<gen> /
// result-<node>-<gen>) are JetStream-durable: if a re-created node (server
// restart, cancel+resubmit) restarted gens at 1, its publishes would collide
// with the previous incarnation's msg ids inside the duplicates window and be
// silently dropped. A wall-clock base makes (node, gen) unique across
// incarnations while staying monotonic per node (enqueue does gen++), so
// same-gen redelivery dedup — the intended use — still works.
func genSeed() uint64 { return uint64(time.Now().UnixNano()) }

// resKind maps a wire node kind onto the resources package's default table.
func resKind(kind string) string {
	switch kind {
	case wire.KindImport:
		return resources.KindImport
	case wire.KindBuildFrom:
		return resources.KindBuildFrom
	case wire.KindPluginResolve:
		return resources.KindBuildPluginResolve
	case wire.KindPin:
		return resources.KindBuildPin
	case wire.KindBuildRun:
		return resources.KindBuild
	default:
		return kind
	}
}

// require is the join: get-or-create the node (kind, k), link it as a dep of
// parent (if any), propagate interest, and — for a fresh node — run the
// doneness fast-path, unfold its deps, and enqueue it if ready. def and
// platform fill missing node fields, never overwrite. extra is additional
// request-id interest (submit targets). Callers hold s.mu.
func (s *Sched) require(kind string, k key.Key, def []byte, platform string, parent *node, extra map[string]struct{}, label string) *node {
	id := nodeID{kind: kind, key: k}
	n, existed := s.nodes[id]
	if !existed {
		n = &node{
			id:         id,
			name:       wire.NodeName(kind, k),
			phase:      wire.PhaseWaiting,
			gen:        genSeed(),
			deps:       map[*node]struct{}{},
			dependents: map[*node]struct{}{},
			interest:   map[string]struct{}{},
		}
		s.nodes[id] = n
	}
	if len(n.def) == 0 && len(def) > 0 {
		n.def = def
	}
	if n.platform == "" {
		n.platform = platform
	}
	if n.label == "" {
		n.label = label
	}
	if parent != nil {
		if _, dup := parent.deps[n]; !dup {
			parent.deps[n] = struct{}{}
			n.dependents[parent] = struct{}{}
		}
		s.propagateInterest(n, parent.interest)
	}
	if extra != nil {
		s.propagateInterest(n, extra)
	}
	if !existed {
		// Doneness = output-ref existence, fast-pathed at creation — this is
		// also the crash-recovery story: resubmitted requests short-circuit
		// finished subtrees.
		done, err := s.outputExistsLocked(n)
		switch {
		case err != nil:
			s.failNodeLocked(n, "doneness check: "+err.Error())
		case done:
			n.phase = wire.PhaseDone
			n.cached = true
			s.putNodeStatusLocked(n)
		default:
			s.unfoldLocked(n)
			s.maybeEnqueueLocked(n)
		}
		s.bumpLocked()
	}
	return n
}

// propagateInterest adds ids to n and, when anything was new for n, to n's
// whole dep subtree (the join case: an existing subtree gains a watcher).
func (s *Sched) propagateInterest(n *node, ids map[string]struct{}) {
	added := false
	for id := range ids {
		if _, ok := n.interest[id]; !ok {
			n.interest[id] = struct{}{}
			added = true
		}
	}
	if !added {
		return
	}
	for d := range n.deps {
		s.propagateInterest(d, ids)
	}
}

// outputExistsLocked is the per-kind doneness derivation: the node's output
// ref exists in the server store (buildvalue: the two-hop
// build-from:K → F → build-output:F). As a side effect a buildvalue whose
// build-from:K resolves records F, so the pipeline skips the re-resolve.
func (s *Sched) outputExistsLocked(n *node) (bool, error) {
	k := n.id.key.String()
	switch n.id.kind {
	case wire.KindImport:
		_, ok, err := s.store.GetKey(s.ctx, "import-output:"+k)
		return ok, err
	case wire.KindBuildFrom:
		_, ok, err := s.store.GetKey(s.ctx, "build-from:"+k)
		return ok, err
	case wire.KindPluginResolve:
		_, ok, err := s.store.GetKey(s.ctx, "build-plugin-resolved:"+k)
		return ok, err
	case wire.KindPin:
		_, ok, err := s.store.GetKey(s.ctx, "build-pinned:"+k)
		return ok, err
	case wire.KindBuildRun:
		_, ok, err := s.store.GetKey(s.ctx, "build-output:"+k)
		return ok, err
	case wire.KindBuildValue:
		f, ok, err := s.store.GetKey(s.ctx, "build-from:"+k)
		if err != nil || !ok {
			return false, err
		}
		n.f, n.fResolved = f, true
		_, ok, err = s.store.GetKey(s.ctx, "build-output:"+f.String())
		return ok, err
	default:
		return false, nil
	}
}

// unfoldLocked derives the node's static deps (jobs-scheduler-semantics §1's
// per-kind table) and creates/joins them. Amber reads are local and fast
// (single-process store), so they run under the lock.
func (s *Sched) unfoldLocked(n *node) {
	switch n.id.kind {
	case wire.KindImport:
		if len(n.def) == 0 {
			s.failNodeLocked(n, "import definition unavailable (defs travel in-band)")
			return
		}
		def, err := importdef.Decode(n.def)
		if err != nil {
			s.failNodeLocked(n, "decode import def: "+err.Error())
			return
		}
		if def.Platform != "" {
			n.platform = def.Platform
		}
		if n.label == "" {
			n.label = "fetch " + def.Fetcher
		}
		if len(def.FetcherDef) > 0 {
			fk, err := (builddef.Input{Kind: builddef.KindBuild, Definition: def.FetcherDef}).Key()
			if err != nil {
				s.failNodeLocked(n, "fetcher build def key: "+err.Error())
				return
			}
			s.require(wire.KindBuildValue, fk, def.FetcherDef, n.platform, n, nil, "fetcher "+def.Fetcher)
		}

	case wire.KindBuildFrom:
		if len(n.def) == 0 {
			s.failNodeLocked(n, "build definition unavailable (defs travel in-band)")
			return
		}
		def, err := builddef.DecodeDefinition(n.def)
		if err != nil {
			s.failNodeLocked(n, "decode build def: "+err.Error())
			return
		}
		if def.Platform != "" {
			n.platform = def.Platform
		}
		if n.label == "" && def.Dir != "" {
			n.label = def.Dir
		}
		s.requireInputLocked(n, def.Source, "")

	case wire.KindPluginResolve:
		// The F tree is self-contained — no deps.

	case wire.KindPin:
		// Deps come from the build-plugin-resolved:F blob, which provably
		// exists: pin is only created after pluginresolve reported done.
		pr, err := s.readPluginResolvedLocked(n.id.key)
		if err != nil {
			s.failNodeLocked(n, "read build-plugin-resolved: "+err.Error())
			return
		}
		named := sortedNamedInputs(pr.Plugins, pr.Deps)
		n.pinInputs = n.pinInputs[:0]
		for _, ni := range named {
			n.pinInputs = append(n.pinInputs, ni.In)
			s.requireInputLocked(n, ni.In, ni.Name)
		}

	case wire.KindBuildRun:
		pinned, err := s.readPinnedLocked(n.id.key)
		if err != nil {
			s.failNodeLocked(n, "read build-pinned: "+err.Error())
			return
		}
		if pinned.Resources != nil {
			n.pinnedRes = resources.Resources{CPUMilli: pinned.Resources.CPUMilli, MemBytes: pinned.Resources.MemBytes}
		}
		n.caches = pinned.Caches
		n.runInputs = make([]builddef.Input, 0, len(pinned.Inputs))
		for _, pi := range pinned.Inputs {
			in := builddef.Input{Kind: pi.Kind, Definition: pi.Definition}
			n.runInputs = append(n.runInputs, in)
			s.requireInputLocked(n, in, pi.Name)
		}

	case wire.KindBuildValue:
		s.advanceBuildValueLocked(n)
	}
}

// requireInputLocked maps one builddef.Input onto its dep node: import →
// import/K_in, build → buildvalue/K_in (the value node — the consumer gets
// the whole K→F pipeline and its join), tree → no node (already-present
// content, resolved by ref).
func (s *Sched) requireInputLocked(parent *node, in builddef.Input, label string) {
	switch in.Kind {
	case builddef.KindImport:
		k, err := in.Key()
		if err != nil {
			s.failNodeLocked(parent, "input key: "+err.Error())
			return
		}
		s.require(wire.KindImport, k, in.Definition, parent.platform, parent, nil, label)
	case builddef.KindBuild:
		k, err := in.Key()
		if err != nil {
			s.failNodeLocked(parent, "input key: "+err.Error())
			return
		}
		// Root-relative subbuilds make cycles expressible (A subbuilds B,
		// B subbuilds A — sibling-sources design §8); undetected they become a
		// silent mutual wait. A cycle exists iff the required buildvalue is
		// already an ancestor of the requiring node — a bounded DFS over the
		// in-memory dependent edges at creation time only.
		if chain := s.cycleChainLocked(parent, nodeID{kind: wire.KindBuildValue, key: k}); chain != nil {
			s.failNodeLocked(parent, "sub-build cycle: "+strings.Join(chain, " -> "))
			return
		}
		platform := parent.platform
		if d, err := builddef.DecodeDefinition(in.Definition); err == nil && d.Platform != "" {
			platform = d.Platform
		}
		s.require(wire.KindBuildValue, k, in.Definition, platform, parent, nil, label)
	case builddef.KindTree:
		// Fixed, already-present content — no work node.
	default:
		s.failNodeLocked(parent, "unknown input kind: "+in.Kind)
	}
}

// advanceBuildValueLocked walks the buildvalue orchestrator one step: it
// registers exactly the stages reached so far (buildfrom → pluginresolve →
// pin → buildrun, each fast-pathing to done if its ref exists) and marks the
// buildvalue done after buildrun. Re-entrant and idempotent — called at
// creation and on every stage completion.
func (s *Sched) advanceBuildValueLocked(bv *node) {
	if bv.phase == wire.PhaseDone || bv.phase == wire.PhaseFailed {
		return
	}
	k := bv.id.key
	bf := s.require(wire.KindBuildFrom, k, bv.def, bv.platform, bv, nil, bv.label)
	if bf.phase != wire.PhaseDone {
		return
	}
	if !bv.fResolved {
		f, ok, err := s.store.GetKey(s.ctx, "build-from:"+k.String())
		if err != nil {
			s.failNodeLocked(bv, "resolve F: "+err.Error())
			return
		}
		if !ok {
			s.failNodeLocked(bv, "build-from:"+k.String()+" missing after buildfrom done")
			return
		}
		bv.f, bv.fResolved = f, true
	}
	pr := s.require(wire.KindPluginResolve, bv.f, nil, bv.platform, bv, nil, bv.label)
	if pr.phase != wire.PhaseDone {
		return
	}
	pin := s.require(wire.KindPin, bv.f, nil, bv.platform, bv, nil, bv.label)
	if pin.phase != wire.PhaseDone {
		return
	}
	// The buildrun stage is keyed by KP, not F (sibling-sources design §10.1):
	// the KP binding is resolved from pin-cover/<v>:F (re-derived on demand —
	// a missing derived ref is a crash window, never a failure), and require's
	// doneness fast-path on build-output:<KP> IS the cross-context memo — a
	// second F whose monorepo changed outside the covered subset joins the
	// same node in flight or finds it done here, with no extra machinery.
	if !bv.kpResolved {
		kp, err := s.resolveKPLocked(bv.f)
		if err != nil {
			s.failNodeLocked(bv, "resolve KP: "+err.Error())
			return
		}
		bv.kp, bv.kpResolved = kp, true
	}
	run := s.require(wire.KindBuildRun, bv.kp, nil, bv.platform, bv, nil, bv.label)
	if run.phase != wire.PhaseDone {
		return
	}
	// F-aliases (design §10.2): deps-before-output, BEFORE the buildvalue
	// transitions to done — a terminal watch snapshot must imply the aliases
	// are durable (pullHome hard-fails on a missing build-output:F). Running
	// here — on every advance-to-done — uniformly covers the fresh-build,
	// memo-hit, late-join, and crash-heal paths. Store writes under s.mu
	// follow the existing precedent (putNodeStatusLocked, unfold reads).
	if err := s.ensureFAliasesLocked(bv.f, bv.kp); err != nil {
		s.failNodeLocked(bv, "write build-output aliases: "+err.Error())
		return
	}
	s.nodeDoneLocked(bv)
}

// cycleChainLocked reports whether target is an ancestor of from (walking
// dependent edges), returning the node-name chain target…from when it is —
// the sub-build cycle detector (sibling-sources design §8). Bounded: each
// node visited once, under s.mu, at input-creation time only.
func (s *Sched) cycleChainLocked(from *node, target nodeID) []string {
	seen := map[*node]bool{}
	var chain []string
	var dfs func(n *node) bool
	dfs = func(n *node) bool {
		if seen[n] {
			return false
		}
		seen[n] = true
		if n.id == target {
			chain = append(chain, n.name)
			return true
		}
		for d := range n.dependents {
			if dfs(d) {
				chain = append(chain, n.name)
				return true
			}
		}
		return false
	}
	if dfs(from) {
		return chain
	}
	return nil
}

// nodeDoneLocked transitions a node to done and advances its dependents.
func (s *Sched) nodeDoneLocked(n *node) {
	if n.phase == wire.PhaseDone {
		return
	}
	n.phase = wire.PhaseDone
	n.doneAt = time.Now()
	n.errSummary = ""
	s.putNodeStatusLocked(n)
	s.bumpLocked()
	s.log.Info("node done", "node", n.name, "gen", n.gen, "runner", n.runner)
	for d := range n.dependents {
		s.onDepDoneLocked(d)
	}
}

// onDepDoneLocked re-evaluates a dependent after one of its deps completed.
func (s *Sched) onDepDoneLocked(d *node) {
	if d.id.kind == wire.KindBuildValue {
		s.advanceBuildValueLocked(d)
		return
	}
	s.maybeEnqueueLocked(d)
}

// failNodeLocked marks a node FAILED — memory-only and sticky while watched
// (retry is by resubmit; a fresh activation re-derives from the CAS).
// failed-upstream is derived per request at snapshot time, never stored.
// Server-side failures (unfold, pull-ref computation, …) have no runner
// attempt to attribute; result-driven callers use failNodeAttrLocked.
func (s *Sched) failNodeLocked(n *node, summary string) {
	s.failNodeAttrLocked(n, nil, wire.FailOriginServer, summary)
}

// failNodeAttrLocked is failNodeLocked with the failing attempt's Result and
// origin attached to the durable FailureRecord.
func (s *Sched) failNodeAttrLocked(n *node, res *wire.Result, origin, summary string) {
	if n.phase == wire.PhaseFailed || n.phase == wire.PhaseDone {
		return
	}
	n.phase = wire.PhaseFailed
	n.errSummary = summary
	n.doneAt = time.Now()
	s.recordFailureLocked(n, res, origin, wire.FailDispositionFailed, summary, 0)
	s.putNodeStatusLocked(n)
	s.bumpLocked()
	s.log.Warn("node failed", "node", n.name, "error", summary)
}

// maybeEnqueueLocked publishes the node's job when it is placeable, waiting,
// watched, and all its deps are done.
func (s *Sched) maybeEnqueueLocked(n *node) {
	if !placeable(n.id.kind) || n.phase != wire.PhaseWaiting || len(n.interest) == 0 {
		return
	}
	for d := range n.deps {
		if d.phase != wire.PhaseDone {
			return
		}
	}
	s.enqueueLocked(n)
}

// requirementLocked resolves the node's placement requirement:
// max(per-kind default, Pinned.Resources, live submit-request resources).
// Request resources apply to the TARGET buildvalue's pipeline stages (its
// direct placeable deps) and expire with the request — jobs' watcher-carried
// requests, refcounted instead of leased.
func (s *Sched) requirementLocked(n *node) resources.Resources {
	req := resources.DefaultFor(resKind(n.id.kind)).Max(n.pinnedRes)
	for d := range n.dependents {
		if d.id.kind != wire.KindBuildValue {
			continue
		}
		for rid := range d.interest {
			if r, ok := s.requests[rid]; ok && !r.cancelled && r.k == d.id.key {
				req = req.Max(r.res)
			}
		}
	}
	return req
}

// enqueueLocked bumps the attempt generation and publishes one Job to the
// work queue lane jobs.<platform>.<class>, deduped by JobMsgID. A publish
// failure re-arms a retry timer rather than failing the node (NATS is
// in-process; transient failure means shutdown or a wedged server).
func (s *Sched) enqueueLocked(n *node) {
	if s.closed {
		return
	}
	n.gen++
	n.runner = ""
	n.startedAt = time.Time{}
	n.enqueuedAt = time.Now()
	n.queueSeq = 0

	req := s.requirementLocked(n)
	class, fits := wire.ClassFor(req)
	if !fits {
		s.log.Warn("requirement beyond ladder top; capping", "node", n.name, "req", req.String(), "class", class)
	}
	pull, err := s.computePullRefsLocked(n)
	if err != nil {
		s.failNodeLocked(n, "compute pull refs: "+err.Error())
		return
	}
	job := wire.Job{
		Node:     n.name,
		Kind:     n.id.kind,
		Key:      n.id.key[:],
		Gen:      n.gen,
		Platform: n.platform,
		Class:    string(class),
		PullRefs: pull,
		CPUMilli: req.CPUMilli,
		MemBytes: req.MemBytes,
	}
	if n.id.kind == wire.KindImport || n.id.kind == wire.KindBuildFrom {
		job.Def = n.def // defs travel in-band for K-kinds
	}
	subject := wire.JobsSubject(n.platform, class)
	s.ensureLaneLocked(n.platform, class)
	ack, err := s.js.PublishMsg(s.ctx, &nats.Msg{Subject: subject, Data: wire.MustEncode(job)},
		jetstream.WithMsgID(wire.JobMsgID(n.name, n.gen)))
	if err != nil {
		if s.closed || s.ctx.Err() != nil {
			return
		}
		s.log.Warn("job publish failed; retrying", "node", n.name, "gen", n.gen, "error", err)
		n.phase = wire.PhaseWaiting
		s.armReenqueueLocked(n, s.retryBase)
		return
	}
	n.queueSeq = ack.Sequence
	n.phase = wire.PhaseQueued
	s.putNodeStatusLocked(n)
	s.bumpLocked()
	s.log.Info("job queued", "node", n.name, "gen", n.gen, "subject", subject)
}

// armReenqueueLocked schedules a re-enqueue after d, fenced by the node's
// identity, generation, phase and interest (the stale-result guard).
func (s *Sched) armReenqueueLocked(n *node, d time.Duration) {
	gen := n.gen
	id := n.id
	time.AfterFunc(d, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		m := s.nodes[id]
		if m != n || s.closed || m.gen != gen || m.phase != wire.PhaseWaiting || len(m.interest) == 0 {
			return
		}
		s.enqueueLocked(m)
	})
}

// ensureLaneLocked creates/updates the shared durable work-queue consumer
// for one (platform, class) lane so published jobs are servable the moment a
// fitting runner binds it.
func (s *Sched) ensureLaneLocked(platform string, class wire.Class) {
	name := wire.ConsumerName(platform, class)
	if s.lanes[name] {
		return
	}
	_, err := s.js.CreateOrUpdateConsumer(s.ctx, wire.StreamJobs, LaneConsumerConfig(platform, class))
	if err != nil {
		if s.ctx.Err() == nil {
			s.log.Warn("lane consumer create failed", "lane", name, "error", err)
		}
		return
	}
	s.lanes[name] = true
}

// LaneConsumerConfig is the shared work-queue consumer configuration for one
// (platform, class) lane — exported so runners create/bind the identical
// consumer (CreateOrUpdateConsumer fails on config drift).
func LaneConsumerConfig(platform string, class wire.Class) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:       wire.ConsumerName(platform, class),
		FilterSubject: wire.JobsSubject(platform, class),
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       jobsAckWait,
		MaxDeliver:    jobsMaxDeliver,
		// Slots gate concurrency runner-side, not the consumer.
		MaxAckPending: jobsMaxAckPending,
	}
}

// dropInterestLocked removes reqID's interest from the target's closure and
// evicts every node left with zero interest: cancellation and watcher-loss
// cleanup are the same path. An evicted node's queued job message is
// best-effort deleted from the work queue; an attempt already in a runner
// is not chased — it completes and its result is dropped on arrival
// ("wasteful but never wrong").
func (s *Sched) dropInterestLocked(reqID string, target *node) {
	if target == nil {
		return
	}
	seen := map[*node]bool{}
	var walk func(n *node)
	walk = func(n *node) {
		if seen[n] {
			return
		}
		seen[n] = true
		delete(n.interest, reqID)
		for d := range n.deps {
			walk(d)
		}
	}
	walk(target)
	for n := range seen {
		if len(n.interest) > 0 {
			continue
		}
		// Guard against double-drop: only evict the node if it still IS the
		// table occupant (a resubmit may have re-created the id).
		if cur := s.nodes[n.id]; cur != n {
			continue
		}
		// Best-effort queue purge: an evicted node's undelivered job message
		// need never run. Deleting a delivered-but-unacked message (running,
		// or queued but already in a runner) stops AckWait redelivery, not
		// the in-flight attempt — its result is dropped on arrival as before.
		if n.queueSeq != 0 && (n.phase == wire.PhaseQueued || n.phase == wire.PhaseRunning) {
			if err := s.jobs.DeleteMsg(s.ctx, n.queueSeq); err != nil {
				s.log.Debug("queued job purge failed", "node", n.name, "seq", n.queueSeq, "error", err)
			}
		}
		delete(s.nodes, n.id)
		// Unlink from surviving deps so they never advance a ghost. The
		// node's own dep map is kept — terminal request snapshots still walk
		// the detached subgraph.
		for d := range n.deps {
			delete(d.dependents, n)
		}
		s.dropNodeLogLocked(n.name)
		s.purgeNodeStatusLocked(n.name)
	}
	s.bumpLocked()
}

// applyRecipeLabelLocked overrides the display labels of n and its
// buildvalue dependents with the recipe's own `name =`, carried as the
// Label on a build-pinned ref proposal (runner/buildeval.go keeps it out
// of the Pinned bytes — never identity). Recipe names beat the dep-name /
// dir defaults; nodes created later (the buildrun) inherit the overridden
// buildvalue label. Callers hold s.mu.
func (s *Sched) applyRecipeLabelLocked(n *node, refs []wire.RefProposal) {
	label := ""
	for _, r := range refs {
		if r.Label != "" && strings.HasPrefix(r.Name, "build-pinned:") {
			label = r.Label
			break
		}
	}
	if label == "" {
		return
	}
	n.label = label
	relabelled := 0
	for d := range n.dependents {
		if d.id.kind == wire.KindBuildValue {
			d.label = label
			relabelled++
		}
	}
	s.log.Debug("recipe label applied", "node", n.name, "label", label, "buildvalues", relabelled)
}

// namedInput pairs a recipe-declared name with its input, for unfold-time
// node labeling.
type namedInput struct {
	Name string
	In   builddef.Input
}

// sortedNamedInputs flattens the plugin and resolution-dep maps in a
// stable (name-sorted, plugins first) order so unfold and PullRefs are
// deterministic, keeping the names as display labels.
func sortedNamedInputs(plugins, deps map[string]builddef.Input) []namedInput {
	out := make([]namedInput, 0, len(plugins)+len(deps))
	for _, m := range []map[string]builddef.Input{plugins, deps} {
		names := make([]string, 0, len(m))
		for name := range m {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			out = append(out, namedInput{Name: name, In: m[name]})
		}
	}
	return out
}

// readPluginResolvedLocked reads and decodes build-plugin-resolved:F.
func (s *Sched) readPluginResolvedLocked(f key.Key) (builddef.PluginResolved, error) {
	b, err := s.readRefFileLocked("build-plugin-resolved:" + f.String())
	if err != nil {
		return builddef.PluginResolved{}, err
	}
	return builddef.DecodePluginResolved(b)
}

// readPinnedLocked reads and decodes build-pinned:F.
func (s *Sched) readPinnedLocked(f key.Key) (builddef.Pinned, error) {
	b, err := s.readRefFileLocked("build-pinned:" + f.String())
	if err != nil {
		return builddef.Pinned{}, err
	}
	return builddef.DecodePinned(b)
}

func (s *Sched) readRefFileLocked(name string) ([]byte, error) {
	k, ok, err := s.store.GetKey(s.ctx, name)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, notFound("%s not found", name)
	}
	return s.store.ReadFile(s.ctx, k)
}
