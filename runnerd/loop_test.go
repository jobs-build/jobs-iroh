package runnerd

// In-process loop test: embedded NATS (JetStream, DontListen, in-process
// conns — no iroh anywhere), a real local amber store, a fake sync client
// and a stubbed stage driver. Covers the daemon's whole scheduling surface:
// consumer bind, admission-gated fetch, in-band def landing, ref collection,
// scratch push, result publish + ack, log-chunk mapping, and the cancelled
// path. The real drivers are covered by runner/'s own suite and the E2E
// phase.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amber-store/core/key"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/events"
	"github.com/jobs-build/jobs-iroh/runner"
	"github.com/jobs-build/jobs-iroh/wire"
)

const testPlatform = "linux/amd64"

// fakeSync is the daemon's sync seam for tests: pulls resolve from a preset
// map (writing the local ref like the real client), pushes are recorded.
type fakeSync struct {
	mu      sync.Mutex
	st      *amber.Store
	pullErr error
	pulls   map[string]key.Key
	pushed  map[string]key.Key
}

func newFakeSync(st *amber.Store) *fakeSync {
	return &fakeSync{st: st, pulls: map[string]key.Key{}, pushed: map[string]key.Key{}}
}

func (f *fakeSync) Pull(ctx context.Context, name string) (key.Key, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pullErr != nil {
		return key.Key{}, f.pullErr
	}
	k, ok := f.pulls[name]
	if !ok {
		return key.Key{}, fmt.Errorf("fake: no remote ref %q", name)
	}
	if err := f.st.PutRef(ctx, name, k); err != nil {
		return key.Key{}, err
	}
	return k, nil
}

func (f *fakeSync) Push(ctx context.Context, name string, root key.Key) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pushed[name] = root
	return nil
}

func (f *fakeSync) pushedRef(name string) (key.Key, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k, ok := f.pushed[name]
	return k, ok
}

// testHarness wires one daemon over embedded NATS.
type testHarness struct {
	t    *testing.T
	st   *amber.Store
	nc   *nats.Conn
	js   jetstream.JetStream
	sync *fakeSync
	d    *daemon

	cancel context.CancelFunc
	done   chan error
}

func startHarness(t *testing.T, execute func(ctx context.Context, job wire.Job, rw runner.RefWriter, ev *events.Job) runner.Outcome) *testHarness {
	t.Helper()

	ns, err := natsserver.NewServer(&natsserver.Options{
		ServerName: "runnerd-test",
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
		t.Fatal("nats server not ready")
	}

	nc, err := nats.Connect("", nats.InProcessServer(ns))
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	t.Cleanup(nc.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:       wire.StreamJobs,
		Subjects:   []string{wire.SubjectJobsRoot + ".>"},
		Retention:  jetstream.WorkQueuePolicy,
		Duplicates: 10 * time.Minute,
	}); err != nil {
		t.Fatalf("create JOBS stream: %v", err)
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:       wire.StreamResults,
		Subjects:   []string{wire.SubjectResultsRoot + ".>"},
		Duplicates: 15 * time.Minute,
	}); err != nil {
		t.Fatalf("create RESULTS stream: %v", err)
	}

	st, err := amber.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	fake := newFakeSync(st)
	d, err := newDaemon(daemonConfig{
		Log:      slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Store:    st,
		NC:       nc,
		Sync:     fake,
		ID:       "test-runner",
		Name:     "test",
		Platform: testPlatform,
		Capacity: wire.Class("c1-m2").Resources(),
		CacheDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}
	if execute != nil {
		d.execute = execute
	}
	d.pollInterval = 100 * time.Millisecond
	d.msgHBInterval = time.Second
	d.hbInterval = 250 * time.Millisecond
	d.publishTimeout = 10 * time.Second

	runCtx, runCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- d.run(runCtx) }()
	t.Cleanup(func() {
		runCancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("daemon run: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("daemon did not stop")
		}
	})

	return &testHarness{t: t, st: st, nc: nc, js: js, sync: fake, d: d, cancel: runCancel, done: done}
}

// publishJob enqueues one job on its class lane.
func (h *testHarness) publishJob(ctx context.Context, job wire.Job) {
	h.t.Helper()
	_, err := h.js.PublishMsg(ctx, &nats.Msg{
		Subject: wire.JobsSubject(job.Platform, wire.Class(job.Class)),
		Data:    wire.MustEncode(job),
	}, jetstream.WithMsgID(wire.JobMsgID(job.Node, job.Gen)))
	if err != nil {
		h.t.Fatalf("publish job: %v", err)
	}
}

// awaitResult consumes results.<node> until a result for gen arrives.
func (h *testHarness) awaitResult(ctx context.Context, node string, gen uint64) wire.Result {
	h.t.Helper()
	cons, err := h.js.CreateOrUpdateConsumer(ctx, wire.StreamResults, jetstream.ConsumerConfig{
		FilterSubject: wire.ResultsSubject(node),
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		h.t.Fatalf("results consumer: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		msgs, err := cons.Fetch(1, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			continue
		}
		for msg := range msgs.Messages() {
			_ = msg.Ack()
			var res wire.Result
			if err := wire.Decode(msg.Data(), &res); err != nil {
				h.t.Fatalf("decode result: %v", err)
			}
			if res.Gen == gen {
				return res
			}
		}
	}
	h.t.Fatalf("no result for %s gen %d", node, gen)
	return wire.Result{}
}

func TestLoopSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	execute := func(ctx context.Context, job wire.Job, rw runner.RefWriter, ev *events.Job) runner.Outcome {
		// Trivial driver path: emit output, publish one result ref.
		if ow := ev.Output("stdout", "building"); ow != nil {
			ow.Write([]byte("hello from the job"))
			ow.Close()
		}
		k, err := wire.ParseKey(job.Key)
		if err != nil {
			return runner.Outcome{Failed: true, Class: "hard", Stderr: err.Error()}
		}
		if _, err := rw.WriteRefs(ctx, []runner.Ref{{Name: "import-output:" + k.String(), Key: k}}); err != nil {
			return runner.Outcome{Failed: true, Class: "retryable", Stderr: err.Error()}
		}
		return runner.Outcome{OutputKey: k}
	}
	h := startHarness(t, execute)

	// The job's key is the def's real file key so the in-band verification
	// (ingest must hash to Job.Key) and the scratch union line up.
	defBytes := []byte("canonical def bytes")
	outKey, err := amber.FileKey(defBytes)
	if err != nil {
		t.Fatalf("file key: %v", err)
	}

	node := wire.NodeName(wire.KindImport, outKey)
	logCh := make(chan *nats.Msg, 16)
	if _, err := h.nc.ChanSubscribe(wire.LogsSubject(node), logCh); err != nil {
		t.Fatalf("subscribe logs: %v", err)
	}

	job := wire.Job{
		Node:     node,
		Kind:     wire.KindImport,
		Key:      outKey[:],
		Gen:      1,
		Platform: testPlatform,
		Class:    "c1-m1",
		Def:      defBytes,
		CPUMilli: 1000,
		MemBytes: 1 << 30,
	}
	h.publishJob(ctx, job)

	res := h.awaitResult(ctx, node, 1)
	if res.Class != wire.ClassOK {
		t.Fatalf("result class = %q (%s), want ok", res.Class, res.ErrSummary)
	}
	if res.Runner != "test-runner" {
		t.Fatalf("runner = %q", res.Runner)
	}
	if len(res.Refs) != 1 || res.Refs[0].Name != "import-output:"+outKey.String() {
		t.Fatalf("refs = %+v", res.Refs)
	}
	wantScratch := "runner-push/test-runner/" + node + "-1"
	if res.ScratchRef != wantScratch {
		t.Fatalf("scratchRef = %q, want %q", res.ScratchRef, wantScratch)
	}
	if res.Rusage.WallNs <= 0 {
		t.Fatalf("rusage wallNs = %d", res.Rusage.WallNs)
	}

	// The scratch push covered the proposed root's closure: its value is the
	// BuildStoreTree union of the proposed keys.
	pushedRoot, ok := h.sync.pushedRef(wantScratch)
	if !ok {
		t.Fatalf("scratch ref %q was not pushed", wantScratch)
	}
	union, err := h.st.BuildStoreTree(ctx, []key.Key{outKey})
	if err != nil {
		t.Fatalf("build union: %v", err)
	}
	if pushedRoot != union {
		t.Fatalf("pushed root = %s, want union %s", pushedRoot, union)
	}

	// In-band def landed under the local bookkeeping ref.
	if k, ok, err := h.st.GetKey(ctx, "import:"+outKey.String()); err != nil || !ok || k != outKey {
		t.Fatalf("import:K bookkeeping ref = %v %v %v", k, ok, err)
	}
	// The collect writer also wrote the result ref locally (private cache).
	if _, ok, _ := h.st.GetKey(ctx, "import-output:"+outKey.String()); !ok {
		t.Fatal("import-output:K not written to the local store")
	}

	// The job's output crossed logs.<node> as a LogChunk.
	select {
	case m := <-logCh:
		var chunk wire.LogChunk
		if err := wire.Decode(m.Data, &chunk); err != nil {
			t.Fatalf("decode log chunk: %v", err)
		}
		if chunk.Gen != 1 || chunk.Stream != "stdout" || string(chunk.Data) != "hello from the job" {
			t.Fatalf("chunk = %+v", chunk)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no log chunk arrived")
	}

	// Admission drained back to full capacity.
	waitFor(t, func() bool {
		free, inflight := h.d.adm.Snapshot()
		return inflight == 0 && free == wire.Class("c1-m2").Resources()
	}, "admission did not drain")
}

func TestLoopPullFailureIsRetryable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := startHarness(t, func(context.Context, wire.Job, runner.RefWriter, *events.Job) runner.Outcome {
		t.Error("execute must not run when pulls fail")
		return runner.Outcome{}
	})
	h.sync.mu.Lock()
	h.sync.pullErr = errors.New("server unreachable")
	h.sync.mu.Unlock()

	k, err := amber.FileKey([]byte("pinned blob"))
	if err != nil {
		t.Fatalf("file key: %v", err)
	}
	node := wire.NodeName(wire.KindBuildRun, k)
	h.publishJob(ctx, wire.Job{
		Node: node, Kind: wire.KindBuildRun, Key: k[:], Gen: 2,
		Platform: testPlatform, Class: "c1-m1",
		PullRefs: []string{"build-pinned:" + k.String()},
		CPUMilli: 1000, MemBytes: 1 << 30,
	})

	res := h.awaitResult(ctx, node, 2)
	if res.Class != wire.ClassRetryable {
		t.Fatalf("result class = %q, want retryable", res.Class)
	}
	if !strings.Contains(res.ErrSummary, "server unreachable") {
		t.Fatalf("errSummary = %q", res.ErrSummary)
	}
	if len(res.Refs) != 0 || res.ScratchRef != "" {
		t.Fatalf("failed result proposed refs: %+v", res)
	}
}

func TestLoopShutdownPublishesCancelledAndNaks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	started := make(chan struct{})
	h := startHarness(t, func(ctx context.Context, job wire.Job, rw runner.RefWriter, ev *events.Job) runner.Outcome {
		close(started)
		<-ctx.Done()
		return runner.Outcome{Cancelled: true}
	})

	k, err := amber.FileKey([]byte("long job def"))
	if err != nil {
		t.Fatalf("file key: %v", err)
	}
	node := wire.NodeName(wire.KindImport, k)
	h.publishJob(ctx, wire.Job{
		Node: node, Kind: wire.KindImport, Key: k[:], Gen: 5,
		Platform: testPlatform, Class: "c1-m1",
		CPUMilli: 1000, MemBytes: 1 << 30,
	})

	select {
	case <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("job never started")
	}
	h.cancel() // shutdown mid-job

	res := h.awaitResult(ctx, node, 5)
	if res.Class != wire.ClassCancelled {
		t.Fatalf("result class = %q, want cancelled", res.Class)
	}

	// The nak put the message back on the work queue for another runner.
	s, err := h.js.Stream(ctx, wire.StreamJobs)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("JOBS stream has %d msgs after nak, want 1 (redeliverable)", info.State.Msgs)
	}
}

func TestLoopShutdownFailureReportsCancelledNotHard(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	started := make(chan struct{})
	h := startHarness(t, func(ctx context.Context, job wire.Job, rw runner.RefWriter, ev *events.Job) runner.Outcome {
		close(started)
		<-ctx.Done()
		// A shutdown signal (terminal ^C) reaches the sandboxed child too —
		// the driver sees its process die and reports an ordinary hard
		// failure, not Cancelled. The daemon must not trust that verdict.
		return runner.Outcome{Failed: true, Class: "hard", ExitCode: -1, Stderr: "signal: interrupt"}
	})

	k, err := amber.FileKey([]byte("shutdown-killed job def"))
	if err != nil {
		t.Fatalf("file key: %v", err)
	}
	node := wire.NodeName(wire.KindImport, k)
	h.publishJob(ctx, wire.Job{
		Node: node, Kind: wire.KindImport, Key: k[:], Gen: 6,
		Platform: testPlatform, Class: "c1-m1",
		CPUMilli: 1000, MemBytes: 1 << 30,
	})

	select {
	case <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("job never started")
	}
	h.cancel() // shutdown mid-job

	res := h.awaitResult(ctx, node, 6)
	if res.Class != wire.ClassCancelled {
		t.Fatalf("result class = %q, want cancelled (a failure under a dying context is not a verdict)", res.Class)
	}

	// Cancelled results nak: the job goes back to the queue for another runner.
	s, err := h.js.Stream(ctx, wire.StreamJobs)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("JOBS stream has %d msgs after nak, want 1 (redeliverable)", info.State.Msgs)
	}
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal(msg)
}

// TestLoopCrossGenAttemptsCoexist: a redelivered stale-gen attempt of a node
// must not park the CURRENT gen's message in a nak loop (each nak burns a
// MaxDeliver delivery — a long stale attempt would poison the fresh message
// out of the work queue). Distinct gens of one node run independently; only
// a duplicate of the exact in-flight (node, gen) attempt is parked.
func TestLoopCrossGenAttemptsCoexist(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	releaseGen1 := make(chan struct{})
	gen1Started := make(chan struct{})
	var startedOnce sync.Once
	execute := func(ctx context.Context, job wire.Job, rw runner.RefWriter, ev *events.Job) runner.Outcome {
		if job.Gen == 1 {
			startedOnce.Do(func() { close(gen1Started) })
			select {
			case <-releaseGen1:
			case <-ctx.Done():
				return runner.Outcome{Cancelled: true}
			}
		}
		return runner.Outcome{}
	}
	h := startHarness(t, execute)

	k, err := amber.FileKey([]byte("cross-gen def"))
	if err != nil {
		t.Fatalf("file key: %v", err)
	}
	node := wire.NodeName(wire.KindImport, k)
	base := wire.Job{
		Node: node, Kind: wire.KindImport, Key: k[:],
		Platform: testPlatform, Class: "c0.2-m1",
		CPUMilli: 200, MemBytes: 1 << 30,
	}

	gen1 := base
	gen1.Gen = 1
	h.publishJob(ctx, gen1)
	select {
	case <-gen1Started:
	case <-time.After(30 * time.Second):
		t.Fatal("gen 1 never started")
	}

	// While gen 1 is still running, the server's re-enqueued gen 2 must be
	// dispatched, run, and report — not nak-parked behind gen 1.
	gen2 := base
	gen2.Gen = 2
	h.publishJob(ctx, gen2)
	res := h.awaitResult(ctx, node, 2)
	if res.Class != wire.ClassOK {
		t.Fatalf("gen 2 result class = %q (%s), want ok", res.Class, res.ErrSummary)
	}

	close(releaseGen1)
	if res := h.awaitResult(ctx, node, 1); res.Class != wire.ClassOK {
		t.Fatalf("gen 1 result class = %q, want ok", res.Class)
	}
	waitFor(t, func() bool {
		_, inflight := h.d.adm.Snapshot()
		return inflight == 0
	}, "admission did not drain")
}
