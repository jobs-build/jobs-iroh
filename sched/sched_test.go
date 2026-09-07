package sched

// Scheduler tests: embedded NATS in-process, a real amber store, NO iroh.
// Runners are scripted fakes: goroutines that consume the JOBS work queue
// through the real lane consumers and publish canned Results whose objects
// were written straight into the server store (simulating a completed
// runner push) — the server's gate/CheckComplete/commit path is exercised
// for real.

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amber-store/core/key"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/api"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/wire"
)

const testPlatform = "linux/amd64"

type env struct {
	t   *testing.T
	ctx context.Context
	st  *amber.Store
	nc  *nats.Conn
	js  jetstream.JetStream
	s   *Sched
}

func newEnv(t *testing.T) *env {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)

	ns, err := natsserver.NewServer(&natsserver.Options{
		ServerName: "sched-test",
		DontListen: true,
		JetStream:  true,
		StoreDir:   t.TempDir(),
	})
	if err != nil {
		t.Fatalf("new nats server: %v", err)
	}
	go ns.Start()
	t.Cleanup(ns.Shutdown)
	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("embedded nats not ready")
	}

	nc, err := nats.Connect("", nats.InProcessServer(ns))
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	t.Cleanup(nc.Close)

	st, err := amber.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	s, err := New(ctx, Options{Store: st, NC: nc, Log: log})
	if err != nil {
		t.Fatalf("sched new: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	// Shrink the retry backoff so budget tests run in milliseconds.
	s.mu.Lock()
	s.retryBase = time.Millisecond
	s.mu.Unlock()

	return &env{t: t, ctx: ctx, st: st, nc: nc, js: js, s: s}
}

// ingestFile writes bytes into the SERVER store, as a runner push would.
func (e *env) ingestFile(data []byte) key.Key {
	e.t.Helper()
	k, err := e.st.IngestFile(e.ctx, data)
	if err != nil {
		e.t.Fatalf("ingest file: %v", err)
	}
	return k
}

// ingestTree writes a small unique directory tree into the server store.
func (e *env) ingestTree(marker string) key.Key {
	e.t.Helper()
	dir := e.t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "file.txt"), []byte(marker), 0o644); err != nil {
		e.t.Fatal(err)
	}
	k, err := e.st.IngestDir(e.ctx, dir)
	if err != nil {
		e.t.Fatalf("ingest dir: %v", err)
	}
	return k
}

// treeDef builds a canonical tree-source build definition over a fresh
// source tree, returning its canonical bytes.
func (e *env) treeDef(marker string) []byte {
	e.t.Helper()
	src, err := builddef.TreeInput(e.ingestTree(marker))
	if err != nil {
		e.t.Fatal(err)
	}
	def := builddef.Definition{
		Source:   src,
		Platform: testPlatform,
		Params:   []byte{0xf6}, // canonical CBOR null
	}
	b, err := def.Canonical()
	if err != nil {
		e.t.Fatal(err)
	}
	return b
}

func (e *env) getRef(name string) (key.Key, bool) {
	e.t.Helper()
	k, ok, err := e.st.GetKey(e.ctx, name)
	if err != nil {
		e.t.Fatalf("get ref %s: %v", name, err)
	}
	return k, ok
}

// jobsMsgCount returns the JOBS work-queue stream's message count.
func (e *env) jobsMsgCount() uint64 {
	e.t.Helper()
	st, err := e.js.Stream(e.ctx, wire.StreamJobs)
	if err != nil {
		e.t.Fatalf("jobs stream: %v", err)
	}
	info, err := st.Info(e.ctx)
	if err != nil {
		e.t.Fatalf("jobs stream info: %v", err)
	}
	return info.State.Msgs
}

// fakeRunner consumes lanes of the real work queue and records every job.
type fakeRunner struct {
	mu   sync.Mutex
	jobs map[string][]wire.Job // by kind
}

func (fr *fakeRunner) record(job wire.Job) {
	fr.mu.Lock()
	fr.jobs[job.Kind] = append(fr.jobs[job.Kind], job)
	fr.mu.Unlock()
}

// byKind returns the recorded jobs of one kind.
func (fr *fakeRunner) byKind(kind string) []wire.Job {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	return append([]wire.Job(nil), fr.jobs[kind]...)
}

// startRunner scripts one fake runner: it consumes the given lanes, calls
// handle per job, publishes the returned Result (nil = silence), then acks —
// result-before-ack, like the real runner.
func (e *env) startRunner(classes []wire.Class, handle func(wire.Job) *wire.Result) *fakeRunner {
	e.t.Helper()
	return e.startRunnerOn(testPlatform, classes, handle)
}

// hello announces one fake runner for platform on runners.hello and waits
// for the fleet fold to absorb it — the no-runners submit gate (issue #8)
// consults the fleet, so every test runner must announce itself like the
// real runnerd does.
func (e *env) hello(platform string) {
	e.t.Helper()
	id := "fake-" + platform
	b := wire.MustEncode(wire.Hello{ID: id, Name: "fake", Platform: platform,
		Size: "c4-m16", CPUMilli: 4000, MemBytes: 16 << 30})
	if err := e.nc.Publish(wire.SubjectRunnerHello, b); err != nil {
		e.t.Fatalf("publish hello: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		e.s.mu.Lock()
		_, ok := e.s.fleet[id]
		e.s.mu.Unlock()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			e.t.Fatal("runner hello not folded into the fleet")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// staleFleet backdates every fleet entry past the liveness window, as if all
// runners stopped heartbeating.
func (e *env) staleFleet() {
	e.t.Helper()
	e.s.mu.Lock()
	for id, info := range e.s.fleet {
		info.SeenNs = time.Now().Add(-2 * runnerLiveWindow).UnixNano()
		e.s.fleet[id] = info
	}
	e.s.mu.Unlock()
}

// startRunnerOn is startRunner bound to an explicit platform (multi-platform
// tests need one fake runner per platform lane set).
func (e *env) startRunnerOn(platform string, classes []wire.Class, handle func(wire.Job) *wire.Result) *fakeRunner {
	e.t.Helper()
	e.hello(platform)
	fr := &fakeRunner{jobs: map[string][]wire.Job{}}
	for _, class := range classes {
		cons, err := e.js.CreateOrUpdateConsumer(e.ctx, wire.StreamJobs, LaneConsumerConfig(platform, class))
		if err != nil {
			e.t.Fatalf("lane consumer %s: %v", class, err)
		}
		it, err := cons.Messages(jetstream.PullMaxMessages(1))
		if err != nil {
			e.t.Fatalf("lane messages %s: %v", class, err)
		}
		e.t.Cleanup(it.Stop)
		go func() {
			for {
				msg, err := it.Next()
				if err != nil {
					return
				}
				var job wire.Job
				if err := wire.Decode(msg.Data(), &job); err != nil {
					e.t.Errorf("decode job: %v", err)
					_ = msg.Ack()
					continue
				}
				fr.record(job)
				if res := handle(job); res != nil {
					res.Node, res.Gen = job.Node, job.Gen
					if res.Runner == "" {
						res.Runner = "fake-runner"
					}
					_, err := e.js.PublishMsg(e.ctx,
						&nats.Msg{Subject: wire.ResultsSubject(job.Node), Data: wire.MustEncode(res)},
						jetstream.WithMsgID(wire.ResultMsgID(job.Node, job.Gen)))
					if err != nil && e.ctx.Err() == nil {
						e.t.Errorf("publish result: %v", err)
					}
				}
				_ = msg.Ack()
			}
		}()
	}
	return fr
}

// standardHandler completes every stage the canonical way, writing output
// objects into the server store and proposing the driver's exact ref batch.
// pinned parametrizes what the pin stage publishes.
func (e *env) standardHandler(pinned builddef.Pinned) func(wire.Job) *wire.Result {
	return func(job wire.Job) *wire.Result {
		fh, err := wire.ParseKey(job.Key)
		if err != nil {
			e.t.Errorf("job %s: bad key: %v", job.Node, err)
			return nil
		}
		f := fh.String()
		switch job.Kind {
		case wire.KindImport:
			out := e.ingestFile([]byte("imported " + job.Node))
			return okResult(job, ref("import-output:"+f, out))
		case wire.KindBuildFrom:
			// A REAL F-tree ({env/, params, platform}) — pin commit derives KP
			// from it (env subtree + platform file), so a marker blob no longer
			// suffices (sibling-sources design §6.3).
			envT := e.ingestTree("env for " + job.Node)
			ftree, err := e.st.BuildFromTree(e.ctx, envT, "", []byte("params"), testPlatform, nil)
			if err != nil {
				e.t.Errorf("fake BuildFromTree: %v", err)
				return nil
			}
			return okResult(job,
				ref("build-from:"+f, ftree),
				ref("build-from-tree:"+ftree.String(), ftree))
		case wire.KindPluginResolve:
			body, err := builddef.EncodePluginResolved(builddef.PluginResolved{})
			if err != nil {
				e.t.Error(err)
				return nil
			}
			return okResult(job, ref("build-plugin-resolved:"+f, e.ingestFile(body)))
		case wire.KindPin:
			body, err := builddef.EncodePinned(pinned)
			if err != nil {
				e.t.Error(err)
				return nil
			}
			return okResult(job, ref("build-pinned:"+f, e.ingestFile(body)))
		case wire.KindBuildRun:
			deps := e.ingestFile([]byte("deps " + job.Node))
			out := e.ingestFile([]byte("out " + job.Node))
			return okResult(job,
				ref("build-output-deps:"+f, deps),
				ref("build-output:"+f, out))
		default:
			e.t.Errorf("unexpected job kind %s", job.Kind)
			return nil
		}
	}
}

func okResult(job wire.Job, refs ...wire.RefProposal) *wire.Result {
	return &wire.Result{Node: job.Node, Gen: job.Gen, Class: wire.ClassOK, Refs: refs}
}

func ref(name string, k key.Key) wire.RefProposal {
	return wire.RefProposal{Name: name, Key: k[:]}
}

// watchTerminal drains a watch until its terminal snapshot.
func (e *env) watchTerminal(requestID string) api.Snapshot {
	e.t.Helper()
	ch, stop, err := e.s.Watch(e.ctx, requestID)
	if err != nil {
		e.t.Fatalf("watch: %v", err)
	}
	defer stop()
	deadline := time.After(60 * time.Second)
	for {
		select {
		case snap, ok := <-ch:
			if !ok {
				e.t.Fatal("watch channel closed before terminal snapshot")
			}
			if snap.Terminal {
				return snap
			}
		case <-deadline:
			e.t.Fatal("timeout waiting for terminal snapshot")
		}
	}
}

func nodePhase(t *testing.T, snap api.Snapshot, name string) api.NodeSnap {
	t.Helper()
	for _, n := range snap.Nodes {
		if n.Node == name {
			return n
		}
	}
	t.Fatalf("node %s not in snapshot (%d nodes)", name, len(snap.Nodes))
	return api.NodeSnap{}
}

func assertRefs(e *env, want map[string]bool) {
	e.t.Helper()
	for name, present := range want {
		if _, ok := e.getRef(name); ok != present {
			e.t.Errorf("ref %s: present=%v, want %v", name, ok, present)
		}
	}
}

func assertPullRefs(t *testing.T, job wire.Job, want []string) {
	t.Helper()
	if len(job.PullRefs) != len(want) {
		t.Fatalf("%s pullRefs = %v, want %v", job.Node, job.PullRefs, want)
	}
	for i := range want {
		if job.PullRefs[i] != want[i] {
			t.Fatalf("%s pullRefs = %v, want %v", job.Node, job.PullRefs, want)
		}
	}
}

// --- tests ---

// TestLinearChainDonePropagation drives one tree-source build through the
// whole buildfrom → pluginresolve → pin → buildrun pipeline and checks
// done-propagation, the committed refs (incl. the server-side f-tree/<F>
// carrier), the per-stage PullRefs, and the terminal snapshot.
func TestLinearChainDonePropagation(t *testing.T) {
	e := newEnv(t)
	fr := e.startRunner([]wire.Class{"c0.2-m1", "c1-m1"}, e.standardHandler(builddef.Pinned{}))

	defBytes := e.treeDef("chain")
	def, err := builddef.DecodeDefinition(defBytes)
	if err != nil {
		t.Fatal(err)
	}
	treeKey, err := builddef.DecodeTreeKey(def.Source.Definition)
	if err != nil {
		t.Fatal(err)
	}

	sub, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: defBytes})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	k, err := wire.ParseKey(sub.K)
	if err != nil {
		t.Fatal(err)
	}

	snap := e.watchTerminal(sub.RequestID)
	if snap.Phase != "done" {
		t.Fatalf("terminal phase = %s, want done (snapshot %+v)", snap.Phase, snap)
	}
	if snap.Counts.Done != snap.Counts.Total || snap.Counts.Total != 5 {
		t.Fatalf("counts = %+v, want 5/5 done", snap.Counts)
	}

	// Submit published the tree carrier and the bookkeeping ref.
	assertRefs(e, map[string]bool{
		"build-from-tree:" + treeKey.String(): true,
		"build:" + k.String():                 true,
	})

	f, ok := e.getRef("build-from:" + k.String())
	if !ok {
		t.Fatal("build-from:K not written")
	}
	// The pin commit derived the KP binding (sibling-sources design §6.3);
	// buildrun is KP-keyed, and the F-level output refs are server aliases.
	kp, ok := e.getRef(PinCoverRef(f))
	if !ok {
		t.Fatal("pin-cover/<v>:F not written at pin commit")
	}
	assertRefs(e, map[string]bool{
		"build-from-tree:" + f.String():       true,
		FTreeRef(f):                           true,
		"build-plugin-resolved:" + f.String(): true,
		"build-pinned:" + f.String():          true,
		"build-pinned:" + kp.String():         true,
		KPTreeRef(kp):                         true,
		"build-output-deps:" + kp.String():    true,
		"build-output:" + kp.String():         true,
		"build-output-deps:" + f.String():     true,
		"build-output:" + f.String():          true,
	})
	if ftv, _ := e.getRef(FTreeRef(f)); ftv != f {
		t.Fatalf("f-tree/%s -> %s, want %s (name==value)", f, ftv, f)
	}
	if ktv, _ := e.getRef(KPTreeRef(kp)); ktv != kp {
		t.Fatalf("kp-tree/%s -> %s, want %s (name==value)", kp, ktv, kp)
	}
	// The F-aliases point at the same keys as the KP refs (design §10.2).
	if av, _ := e.getRef("build-output:" + f.String()); func() key.Key { v, _ := e.getRef("build-output:" + kp.String()); return v }() != av {
		t.Fatalf("build-output:F is not an alias of build-output:KP")
	}

	// The snapshot carries the graph edges: the buildvalue's deps are its
	// whole stage chain, sorted; the chain nodes point at their own deps.
	// None of these nodes were cached — everything ran.
	bv := nodePhase(t, snap, wire.NodeName(wire.KindBuildValue, k))
	wantDeps := []string{
		wire.NodeName(wire.KindBuildFrom, k),
		wire.NodeName(wire.KindPluginResolve, f),
		wire.NodeName(wire.KindPin, f),
		wire.NodeName(wire.KindBuildRun, kp),
	}
	sort.Strings(wantDeps)
	if !reflect.DeepEqual(bv.Deps, wantDeps) {
		t.Fatalf("buildvalue deps = %v, want %v", bv.Deps, wantDeps)
	}
	for _, n := range snap.Nodes {
		if n.Cached {
			t.Fatalf("node %s marked cached in a run-everything build", n.Node)
		}
	}

	// PullRefs per stage, exact lists.
	for kind, want := range map[string][]string{
		wire.KindBuildFrom:     {"build-from-tree:" + treeKey.String()},
		wire.KindPluginResolve: {FTreeRef(f)},
		wire.KindPin:           {FTreeRef(f), "build-plugin-resolved:" + f.String()},
		wire.KindBuildRun:      {KPTreeRef(kp), "build-pinned:" + kp.String()},
	} {
		jobs := fr.byKind(kind)
		if len(jobs) != 1 {
			t.Fatalf("%s: %d jobs, want 1", kind, len(jobs))
		}
		assertPullRefs(t, jobs[0], want)
	}
}

// TestJoinTwoRequestsOneK submits the same definition twice: both requests
// converge on one node table entry per stage — one execution serves both.
func TestJoinTwoRequestsOneK(t *testing.T) {
	e := newEnv(t)
	defBytes := e.treeDef("join")

	// The no-runners gate rejects submits with an empty fleet; announce the
	// runner before submitting, but hold its lane consumption behind a gate
	// so both submits land before any job runs — the join stays exercised.
	release := make(chan struct{})
	handler := e.standardHandler(builddef.Pinned{})
	fr := e.startRunner([]wire.Class{"c0.2-m1", "c1-m1"}, func(job wire.Job) *wire.Result {
		<-release
		return handler(job)
	})

	sub1, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: defBytes})
	if err != nil {
		t.Fatal(err)
	}
	sub2, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: defBytes})
	if err != nil {
		t.Fatal(err)
	}
	if string(sub1.K) != string(sub2.K) {
		t.Fatal("same definition produced different K")
	}
	if sub1.RequestID == sub2.RequestID {
		t.Fatal("distinct submits share a request id")
	}
	close(release)
	for _, id := range []string{sub1.RequestID, sub2.RequestID} {
		if snap := e.watchTerminal(id); snap.Phase != "done" {
			t.Fatalf("request %s phase = %s, want done", id, snap.Phase)
		}
	}
	for _, kind := range []string{wire.KindBuildFrom, wire.KindPluginResolve, wire.KindPin, wire.KindBuildRun} {
		if jobs := fr.byKind(kind); len(jobs) != 1 {
			t.Fatalf("%s ran %d times, want 1 (the join)", kind, len(jobs))
		}
	}
}

// TestDonenessFastPath pre-writes the two-hop output refs: a submitted
// request completes without a single job being queued.
func TestDonenessFastPath(t *testing.T) {
	e := newEnv(t)
	defBytes := e.treeDef("fastpath")
	k, err := (builddef.Input{Kind: builddef.KindBuild, Definition: defBytes}).Key()
	if err != nil {
		t.Fatal(err)
	}
	f := e.ingestTree("prebuilt f")
	out := e.ingestFile([]byte("prebuilt output"))
	for name, kk := range map[string]key.Key{
		"build-from:" + k.String():   f,
		"build-output:" + f.String(): out,
	} {
		if err := e.st.PutRef(e.ctx, name, kk); err != nil {
			t.Fatal(err)
		}
	}

	sub, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: defBytes})
	if err != nil {
		t.Fatal(err)
	}
	snap := e.watchTerminal(sub.RequestID)
	if snap.Phase != "done" {
		t.Fatalf("phase = %s, want done", snap.Phase)
	}
	if snap.Counts.Total != 1 || snap.Counts.Done != 1 {
		t.Fatalf("counts = %+v, want the lone buildvalue done", snap.Counts)
	}
	// Fast-pathed done at creation ⇒ the snapshot marks the node cached.
	if bv := nodePhase(t, snap, wire.NodeName(wire.KindBuildValue, k)); !bv.Cached {
		t.Fatalf("fast-pathed buildvalue not marked cached: %+v", bv)
	}
	// Nothing was ever enqueued: the JOBS stream is empty.
	stream, err := e.js.Stream(e.ctx, wire.StreamJobs)
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 0 {
		t.Fatalf("JOBS stream holds %d msgs, want 0", info.State.Msgs)
	}
}

// TestRetryableBudgetExhaustion: 3 consecutive retryables re-enqueue with
// gen+1; the 4th escalates to hard ("retry budget exhausted").
func TestRetryableBudgetExhaustion(t *testing.T) {
	e := newEnv(t)
	fr := e.startRunner([]wire.Class{"c0.2-m1"}, func(job wire.Job) *wire.Result {
		return &wire.Result{Class: wire.ClassRetryable, ErrSummary: "flaky infra"}
	})

	defBytes := e.treeDef("retry")
	sub, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: defBytes})
	if err != nil {
		t.Fatal(err)
	}
	snap := e.watchTerminal(sub.RequestID)
	if snap.Phase != "failed" {
		t.Fatalf("phase = %s, want failed", snap.Phase)
	}

	k, _ := wire.ParseKey(sub.K)
	bf := nodePhase(t, snap, wire.NodeName(wire.KindBuildFrom, k))
	if bf.Phase != wire.PhaseFailed {
		t.Fatalf("buildfrom phase = %s, want failed", bf.Phase)
	}
	if want := "retry budget exhausted"; !contains(bf.ErrSummary, want) {
		t.Fatalf("errSummary = %q, want it to contain %q", bf.ErrSummary, want)
	}
	jobs := fr.byKind(wire.KindBuildFrom)
	if len(jobs) != 4 { // initial + 3 budgeted retries
		t.Fatalf("buildfrom attempts = %d, want 4", len(jobs))
	}
	// Gens are seeded from the wall clock per node incarnation (msg-id dedup
	// across incarnations) — assert consecutive monotonic bumps, not absolutes.
	for i := 1; i < len(jobs); i++ {
		if jobs[i].Gen != jobs[i-1].Gen+1 {
			t.Fatalf("attempt %d has gen %d, want %d (gen+1 per retry)", i, jobs[i].Gen, jobs[i-1].Gen+1)
		}
	}
}

// TestHardFailureFailedUpstream: one hard strike fails the node; the
// buildvalue above it derives failed-upstream in the request snapshot.
func TestHardFailureFailedUpstream(t *testing.T) {
	e := newEnv(t)
	e.startRunner([]wire.Class{"c0.2-m1"}, func(job wire.Job) *wire.Result {
		return &wire.Result{Class: wire.ClassHard, Exit: 3, ErrSummary: "recipe exploded"}
	})

	defBytes := e.treeDef("hard")
	sub, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: defBytes})
	if err != nil {
		t.Fatal(err)
	}
	snap := e.watchTerminal(sub.RequestID)
	if snap.Phase != "failed" {
		t.Fatalf("phase = %s, want failed", snap.Phase)
	}
	k, _ := wire.ParseKey(sub.K)
	bf := nodePhase(t, snap, wire.NodeName(wire.KindBuildFrom, k))
	if bf.Phase != wire.PhaseFailed || bf.ErrSummary != "recipe exploded" {
		t.Fatalf("buildfrom = %+v, want failed with the runner's summary", bf)
	}
	bv := nodePhase(t, snap, wire.NodeName(wire.KindBuildValue, k))
	if bv.Phase != wire.PhaseUpstream {
		t.Fatalf("buildvalue phase = %s, want %s", bv.Phase, wire.PhaseUpstream)
	}
	if snap.Counts.Failed != 2 {
		t.Fatalf("counts.Failed = %d, want 2 (failed + failed-upstream)", snap.Counts.Failed)
	}
}

// TestGateRejectionFailsClosed: a batch with a name outside the node's
// allow-table (or an inconsistent same-batch cross-check) hard-fails the
// node and writes NOTHING — not even the batch's valid entries.
func TestGateRejectionFailsClosed(t *testing.T) {
	t.Run("bad name", func(t *testing.T) {
		e := newEnv(t)
		var k key.Key
		e.startRunner([]wire.Class{"c0.2-m1"}, func(job wire.Job) *wire.Result {
			out := e.ingestFile([]byte("evil"))
			return okResult(job, ref("import-output:"+mustKeyString(t, job.Key), out))
		})
		defBytes := e.treeDef("gate-bad-name")
		sub, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: defBytes})
		if err != nil {
			t.Fatal(err)
		}
		snap := e.watchTerminal(sub.RequestID)
		if snap.Phase != "failed" {
			t.Fatalf("phase = %s, want failed", snap.Phase)
		}
		k, _ = wire.ParseKey(sub.K)
		bf := nodePhase(t, snap, wire.NodeName(wire.KindBuildFrom, k))
		if !contains(bf.ErrSummary, "gate") {
			t.Fatalf("errSummary = %q, want a gate rejection", bf.ErrSummary)
		}
		assertRefs(e, map[string]bool{
			"import-output:" + k.String(): false,
			"build-from:" + k.String():    false,
		})
	})

	t.Run("mismatched build-from-tree batch", func(t *testing.T) {
		e := newEnv(t)
		var f1, f2 key.Key
		e.startRunner([]wire.Class{"c0.2-m1"}, func(job wire.Job) *wire.Result {
			f1 = e.ingestTree("real F")
			f2 = e.ingestTree("smuggled F")
			return okResult(job,
				ref("build-from:"+mustKeyString(t, job.Key), f1),
				ref("build-from-tree:"+f2.String(), f2))
		})
		defBytes := e.treeDef("gate-mismatch")
		sub, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: defBytes})
		if err != nil {
			t.Fatal(err)
		}
		snap := e.watchTerminal(sub.RequestID)
		if snap.Phase != "failed" {
			t.Fatalf("phase = %s, want failed", snap.Phase)
		}
		k, _ := wire.ParseKey(sub.K)
		assertRefs(e, map[string]bool{
			"build-from:" + k.String():       false, // fail closed: whole batch refused
			"build-from-tree:" + f2.String(): false,
			FTreeRef(f1):                     false,
		})
	})
}

// TestCancel drops the request's interest: nodes leave the table, the
// queued job's late result is dropped at claim time, no refs get written.
func TestCancel(t *testing.T) {
	e := newEnv(t)
	e.hello(testPlatform) // pass the no-runners submit gate; nothing consumes
	defBytes := e.treeDef("cancel")
	sub, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: defBytes})
	if err != nil {
		t.Fatal(err)
	}
	ch, stop, err := e.s.Watch(e.ctx, sub.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	if n := e.jobsMsgCount(); n != 1 {
		t.Fatalf("JOBS msgs after submit = %d, want 1 (queued buildfrom)", n)
	}

	if err := e.s.Cancel(e.ctx, sub.RequestID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	deadline := time.After(30 * time.Second)
	for {
		var snap api.Snapshot
		var ok bool
		select {
		case snap, ok = <-ch:
		case <-deadline:
			t.Fatal("no terminal snapshot after cancel")
		}
		if !ok {
			t.Fatal("watch closed before terminal snapshot")
		}
		if snap.Terminal {
			if snap.Phase != "cancelled" {
				t.Fatalf("phase = %s, want cancelled", snap.Phase)
			}
			break
		}
	}
	if stats := e.s.Stats(); stats.NodesTracked != 0 {
		t.Fatalf("NodesTracked = %d, want 0 (zero-interest nodes leave the table)", stats.NodesTracked)
	}

	// The queued buildfrom job was purged from the work queue at cancel.
	if n := e.jobsMsgCount(); n != 0 {
		t.Fatalf("JOBS msgs after cancel = %d, want 0 (purged)", n)
	}

	// Backstop unchanged: a late result for an evicted node (e.g. from an
	// attempt that was already in a runner) is dropped and its refs never
	// committed — wasteful, never wrong.
	k, _ := wire.ParseKey(sub.K)
	nodeName := wire.NodeName(wire.KindBuildFrom, k)
	orphanOut := e.ingestFile([]byte("orphan output"))
	res := wire.Result{
		Node:   nodeName,
		Gen:    1,
		Runner: "fake-runner",
		Class:  wire.ClassOK,
		Refs:   []wire.RefProposal{{Name: "build-from:" + k.String(), Key: orphanOut[:]}},
	}
	if _, err := e.js.PublishMsg(e.ctx,
		&nats.Msg{Subject: wire.ResultsSubject(nodeName), Data: wire.MustEncode(res)},
		jetstream.WithMsgID(wire.ResultMsgID(nodeName, res.Gen))); err != nil {
		t.Fatalf("publish orphan result: %v", err)
	}
	time.Sleep(500 * time.Millisecond) // let the (dropped) result flow through
	assertRefs(e, map[string]bool{"build-from:" + k.String(): false})

	// Delete removes the request entirely.
	if err := e.s.Delete(e.ctx, sub.RequestID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if reqs := e.s.Requests(); len(reqs) != 0 {
		t.Fatalf("requests after delete = %d, want 0", len(reqs))
	}
	if err := e.s.Delete(e.ctx, sub.RequestID); ErrorCode(err) != api.CodeNotFound {
		t.Fatalf("second delete error = %v, want not-found", err)
	}
}

// TestCancelSharedInterestKeepsJob: eviction only purges nodes nobody else
// needs — a job message backing a node shared with a live request stays.
func TestCancelSharedInterestKeepsJob(t *testing.T) {
	e := newEnv(t)
	e.hello(testPlatform) // pass the no-runners submit gate; nothing consumes
	defBytes := e.treeDef("shared-cancel")

	subA, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: defBytes})
	if err != nil {
		t.Fatal(err)
	}
	subB, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: defBytes})
	if err != nil {
		t.Fatal(err)
	}
	if n := e.jobsMsgCount(); n != 1 {
		t.Fatalf("JOBS msgs after two joined submits = %d, want 1", n)
	}

	if err := e.s.Cancel(e.ctx, subA.RequestID); err != nil {
		t.Fatalf("cancel A: %v", err)
	}
	if n := e.jobsMsgCount(); n != 1 {
		t.Fatalf("JOBS msgs after cancelling one of two = %d, want 1 (B still interested)", n)
	}

	if err := e.s.Cancel(e.ctx, subB.RequestID); err != nil {
		t.Fatalf("cancel B: %v", err)
	}
	if n := e.jobsMsgCount(); n != 0 {
		t.Fatalf("JOBS msgs after cancelling both = %d, want 0", n)
	}
}

// TestCancelAfterDone: cancelling a finished request must not error — the
// evicted nodes are done, their queue messages long since acked, and the
// purge must skip them (phase guard).
func TestCancelAfterDone(t *testing.T) {
	e := newEnv(t)
	e.startRunner([]wire.Class{"c0.2-m1", "c1-m1"}, e.standardHandler(builddef.Pinned{}))

	sub, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: e.treeDef("cancel-after-done")})
	if err != nil {
		t.Fatal(err)
	}
	if snap := e.watchTerminal(sub.RequestID); snap.Phase != "done" {
		t.Fatalf("terminal phase = %s, want done", snap.Phase)
	}
	if err := e.s.Cancel(e.ctx, sub.RequestID); err != nil {
		t.Fatalf("cancel after done: %v", err)
	}
	if n := e.jobsMsgCount(); n != 0 {
		t.Fatalf("JOBS msgs = %d, want 0", n)
	}
}

// TestClassRounding: the requirement max(kind default, Pinned.Resources,
// submit request resources) rounds UP onto the ladder — the light stages
// carry the submit request (1500m/3Gi → c2-m4), buildrun additionally the
// pinned resources (2100m/3Gi → c4-m8).
func TestClassRounding(t *testing.T) {
	e := newEnv(t)
	pinned := builddef.Pinned{Resources: &builddef.PinnedResources{CPUMilli: 2100, MemBytes: 3 << 30}}
	fr := e.startRunner([]wire.Class{"c2-m4", "c4-m8"}, e.standardHandler(pinned))

	defBytes := e.treeDef("classes")
	sub, err := e.s.Submit(e.ctx, api.SubmitRequest{
		Def:       defBytes,
		Resources: &api.ResourceSpec{CPU: "1500m", Memory: "3Gi"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if snap := e.watchTerminal(sub.RequestID); snap.Phase != "done" {
		t.Fatalf("phase = %s, want done", snap.Phase)
	}
	for kind, wantClass := range map[string]string{
		wire.KindBuildFrom:     "c2-m4",
		wire.KindPluginResolve: "c2-m4",
		wire.KindPin:           "c2-m4",
		wire.KindBuildRun:      "c4-m8",
	} {
		jobs := fr.byKind(kind)
		if len(jobs) != 1 {
			t.Fatalf("%s: %d jobs, want 1", kind, len(jobs))
		}
		if jobs[0].Class != wantClass {
			t.Fatalf("%s class = %s, want %s", kind, jobs[0].Class, wantClass)
		}
	}
	// The buildrun job carries the resolved requirement, not the rung size.
	run := fr.byKind(wire.KindBuildRun)[0]
	if run.CPUMilli != 2100 || run.MemBytes != 3<<30 {
		t.Fatalf("buildrun requirement = %d/%d, want 2100/3Gi", run.CPUMilli, run.MemBytes)
	}
}

// TestLogsFold: chunks published on logs.<node> land in the per-(node,gen)
// head+tail buffer and fan out to followers.
func TestLogsFold(t *testing.T) {
	e := newEnv(t)
	nodeName := wire.NodeName(wire.KindImport, e.ingestFile([]byte("some key")))

	publish := func(gen, seq uint64, data string) {
		t.Helper()
		chunk := wire.LogChunk{Gen: gen, Stream: "stdout", Seq: seq, Data: []byte(data)}
		if err := e.nc.Publish(wire.LogsSubject(nodeName), wire.MustEncode(chunk)); err != nil {
			t.Fatal(err)
		}
	}
	publish(1, 0, "hello ")
	publish(1, 1, "world")
	publish(1, 1, "world") // duplicate seq: idempotent

	waitFor(t, func() bool {
		view, _, _, err := e.s.Logs(e.ctx, nodeName, false)
		if err != nil {
			t.Fatal(err)
		}
		return string(view.Head) == "hello world" && view.Gen == 1
	})

	view, ch, stopFollow, err := e.s.Logs(e.ctx, nodeName, true)
	if err != nil {
		t.Fatal(err)
	}
	defer stopFollow()
	if string(view.Head) != "hello world" {
		t.Fatalf("head = %q, want %q", view.Head, "hello world")
	}
	publish(2, 0, "next attempt") // newer gen resets the buffer
	select {
	case chunk := <-ch:
		if chunk.Gen != 2 || string(chunk.Data) != "next attempt" {
			t.Fatalf("follow chunk = %+v", chunk)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("follow chunk never arrived")
	}
	waitFor(t, func() bool {
		view, _, _, err := e.s.Logs(e.ctx, nodeName, false)
		if err != nil {
			t.Fatal(err)
		}
		return view.Gen == 2 && string(view.Head) == "next attempt"
	})
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition never became true")
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && strings.Contains(s, sub))
}

func mustKeyString(t *testing.T, b []byte) string {
	t.Helper()
	k, err := wire.ParseKey(b)
	if err != nil {
		t.Fatal(err)
	}
	return k.String()
}

// TestSubmitScratchRefGuard: Submit deletes only client-push/ scratch refs —
// a submit naming a real ref (a hostile or buggy client) must leave it alone.
func TestSubmitScratchRefGuard(t *testing.T) {
	e := newEnv(t)
	e.hello(testPlatform) // pass the no-runners submit gate; nothing consumes

	protected := e.ingestFile([]byte("precious shell artifact"))
	if err := e.st.PutRef(e.ctx, "shell:"+testPlatform, protected); err != nil {
		t.Fatal(err)
	}
	scratch := e.ingestFile([]byte("client push payload"))
	if err := e.st.PutRef(e.ctx, "client-push/deadbeef", scratch); err != nil {
		t.Fatal(err)
	}

	// A hostile scratch name survives the submit untouched.
	if _, err := e.s.Submit(e.ctx, api.SubmitRequest{
		Def:        e.treeDef("scratch-guard-hostile"),
		ScratchRef: "shell:" + testPlatform,
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	assertRefs(e, map[string]bool{"shell:" + testPlatform: true})

	// The legitimate namespace is cleaned up.
	if _, err := e.s.Submit(e.ctx, api.SubmitRequest{
		Def:        e.treeDef("scratch-guard-legit"),
		ScratchRef: "client-push/deadbeef",
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	assertRefs(e, map[string]bool{"client-push/deadbeef": false})
}

// TestLogFollowerStopAfterClose: a log follower whose stop fires after
// Sched.Close must not double-close the follower channel (Close already
// closed and unregistered every follower).
func TestLogFollowerStopAfterClose(t *testing.T) {
	e := newEnv(t)
	node := wire.NodeName(wire.KindImport, e.ingestFile([]byte("log follower node")))

	_, ch, stop, err := e.s.Logs(e.ctx, node, true)
	if err != nil {
		t.Fatalf("logs: %v", err)
	}
	if err := e.s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Close closed the channel; stop must be a no-op, not a second close.
	stop()
	if _, ok := <-ch; ok {
		t.Fatal("follower channel should be closed and empty")
	}
}
