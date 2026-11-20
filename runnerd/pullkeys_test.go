package runnerd

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/amber-store/core/key"

	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/events"
	"github.com/jobs-build/jobs-iroh/runner"
	"github.com/jobs-build/jobs-iroh/wire"
)

// twoKeys ingests two distinct objects into st: a stale and a current
// binding for one mutable ref name.
func twoKeys(t *testing.T, st *amber.Store) (stale, current key.Key) {
	t.Helper()
	ctx := context.Background()
	var err error
	if stale, err = st.IngestFile(ctx, []byte("F before the server was wiped")); err != nil {
		t.Fatal(err)
	}
	if current, err = st.IngestFile(ctx, []byte("F the server holds now")); err != nil {
		t.Fatal(err)
	}
	return stale, current
}

// ensureRef trusts a complete local copy only when it names the key the job
// was queued against (wire.Job.PullKeys); a stale binding is re-pulled.
func TestEnsureRefRepullsStaleBinding(t *testing.T) {
	ctx := context.Background()
	st, err := amber.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	stale, current := twoKeys(t, st)
	const name = "build-from:fetcher"
	if err := st.PutRef(ctx, name, stale); err != nil {
		t.Fatal(err)
	}
	fake := newFakeSync(st)
	fake.pulls[name] = current
	d := &daemon{log: slog.New(slog.NewTextHandler(io.Discard, nil)), st: st, sync: fake}

	// No expectation (an old server): the local copy short-circuits.
	if k, err := d.ensureRef(ctx, name, nil); err != nil || k != stale {
		t.Fatalf("without PullKeys: %s %v, want the local %s", k, err, stale)
	}
	// The server bound the name elsewhere: re-pull.
	if k, err := d.ensureRef(ctx, name, current[:]); err != nil || k != current {
		t.Fatalf("stale binding: %s %v, want the server's %s", k, err, current)
	}
	if k, _, _ := st.GetKey(ctx, name); k != current {
		t.Fatalf("local ref after re-pull = %s, want %s", k, current)
	}
	// Matching copy: no pull at all.
	fake.pullErr = errors.New("must not pull")
	if k, err := d.ensureRef(ctx, name, current[:]); err != nil || k != current {
		t.Fatalf("matching binding: %s %v", k, err)
	}
}

// The whole job path: a runner whose cache outlived a server wipe holds an
// old build-from binding; the driver must see the server's (issue #22's
// "fetcher build … output not resolvable yet" on every retry).
func TestLoopPullKeysReplaceStaleLocalRef(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	seen := make(chan key.Key, 1)
	var h *testHarness
	h = startHarness(t, func(ctx context.Context, job wire.Job, rw runner.RefWriter, ev *events.Job) runner.Outcome {
		k, _, _ := h.st.GetKey(ctx, "build-from:fetcher")
		seen <- k
		return runner.Outcome{Failed: true, Class: "hard", Stderr: "stub driver"}
	})
	stale, current := twoKeys(t, h.st)
	if err := h.st.PutRef(ctx, "build-from:fetcher", stale); err != nil {
		t.Fatal(err)
	}
	h.sync.mu.Lock()
	h.sync.pulls["build-from:fetcher"] = current
	h.sync.mu.Unlock()

	k, err := amber.FileKey([]byte("import def"))
	if err != nil {
		t.Fatal(err)
	}
	node := wire.NodeName(wire.KindBuildRun, k)
	h.publishJob(ctx, wire.Job{
		Node: node, Kind: wire.KindBuildRun, Key: k[:], Gen: 1,
		Platform: testPlatform, Class: "c1-m1",
		PullRefs: []string{"build-from:fetcher"},
		PullKeys: [][]byte{current[:]},
		CPUMilli: 1000, MemBytes: 1 << 30,
	})
	select {
	case got := <-seen:
		if got != current {
			t.Fatalf("driver resolved build-from:fetcher = %s, want the server's %s (stale local %s)", got, current, stale)
		}
	case <-ctx.Done():
		t.Fatal("job never ran")
	}
	h.awaitResult(ctx, node, 1)
}
