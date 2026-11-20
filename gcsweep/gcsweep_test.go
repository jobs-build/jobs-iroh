package gcsweep

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/amber-store/core/reference"
	"github.com/amber-store/core/refstore"

	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/reftrack"
)

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// openStore opens a store under dir with n refs (named by format) all
// pointing at one real object, written in one batch.
func openStore(t *testing.T, dir, format string, n int) *amber.Store {
	t.Helper()
	st, err := amber.Open(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	k, err := st.IngestFile(context.Background(), []byte("shared content"))
	if err != nil {
		t.Fatal(err)
	}
	recs := make([]refstore.Record, n)
	for i := range recs {
		name := fmt.Sprintf(format, i)
		raw, err := reference.Reference{Name: name, Key: k[:], CreatedAt: time.Now().UnixNano()}.Encode()
		if err != nil {
			t.Fatal(err)
		}
		recs[i] = refstore.Record{Name: name, Data: raw}
	}
	if err := st.RefStore().PutBatch(recs); err != nil {
		t.Fatal(err)
	}
	return st
}

func sweeper(t *testing.T, st *amber.Store, dir string) *Sweeper {
	t.Helper()
	sw, err := New(quietLog(), st, Options{
		StoreDir:     filepath.Join(dir, "store"),
		SnapshotPath: filepath.Join(dir, "refaccess.cbor"),
		Retention:    time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	return sw
}

// An access snapshot that exists but will not load halts the GC: no sweep,
// no pin, and the file is never rewritten — sweeping from an empty tracker
// would re-seed every clock and drop every pin (issue #22).
func TestHaltsOnUnreadableSnapshot(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st := openStore(t, dir, "build-output:%d", 3)
	snap := filepath.Join(dir, "refaccess.cbor")
	garbage := []byte("not a snapshot")
	if err := os.WriteFile(snap, garbage, 0o600); err != nil {
		t.Fatal(err)
	}

	sw := sweeper(t, st, dir)
	if sw.Halted() == "" || sw.StatsSnapshot().Halted == "" {
		t.Fatal("unreadable snapshot did not halt the sweeper")
	}
	if _, err := sw.Sweep(ctx, -1, true); !errors.Is(err, ErrHalted) {
		t.Fatalf("sweep: %v, want ErrHalted", err)
	}
	if _, _, err := sw.Pin(ctx, "build-output:1"); !errors.Is(err, ErrHalted) {
		t.Fatalf("pin: %v, want ErrHalted", err)
	}
	sw.Unpin(ctx, "build-output:1")
	sw.Close()
	if got, err := os.ReadFile(snap); err != nil || !bytes.Equal(got, garbage) {
		t.Fatalf("halted sweeper rewrote the snapshot: %q %v", got, err)
	}
	if names, err := st.RefNames(ctx); err != nil || len(names) != 3 {
		t.Fatalf("halted sweeper touched refs: %v %v", names, err)
	}
}

// A missing snapshot is still a normal first start.
func TestMissingSnapshotIsAFirstStart(t *testing.T) {
	dir := t.TempDir()
	st := openStore(t, dir, "r%d", 1)
	sw := sweeper(t, st, dir)
	defer sw.Close()
	if h := sw.Halted(); h != "" {
		t.Fatalf("missing snapshot halted the sweeper: %s", h)
	}
	if _, err := sw.Sweep(context.Background(), -1, false); err != nil {
		t.Fatal(err)
	}
}

// A restart with more than 131,072 tracked refs keeps every pin and clock:
// the v0.36.0 failure mode of issue #22 was a snapshot the next start could
// not decode.
func TestRestartPreservesPinsPastDefaultLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("140k refs")
	}
	ctx := context.Background()
	dir := t.TempDir()
	const n = 140_000
	st := openStore(t, dir, "build-output:%07d", n)

	sw := sweeper(t, st, dir)
	if _, err := sw.Sweep(ctx, -1, false); err != nil { // seeding sweep
		t.Fatal(err)
	}
	if _, _, err := sw.Pin(ctx, "build-output:0000042"); err != nil {
		t.Fatal(err)
	}
	seeded, _ := sw.Entry("build-output:0100000")
	sw.Close()

	sw2 := sweeper(t, st, dir)
	defer sw2.Close()
	if h := sw2.Halted(); h != "" {
		t.Fatalf("restart halted: %s", h)
	}
	if e, ok := sw2.Entry("build-output:0000042"); !ok || !e.Pinned {
		t.Fatalf("pin lost across restart: %+v ok=%v", e, ok)
	}
	if e, ok := sw2.Entry("build-output:0100000"); !ok || !e.FirstSeen.Equal(seeded.FirstSeen) {
		t.Fatalf("clock lost across restart: %+v (was %+v) ok=%v", e, seeded, ok)
	}
	stats, err := sw2.Sweep(ctx, -1, false)
	if err != nil {
		t.Fatal(err)
	}
	if stats.RefCount != n || stats.Pinned != 1 {
		t.Fatalf("after restart sweep: refs %d pinned %d, want %d and 1", stats.RefCount, stats.Pinned, n)
	}
}

func TestRefCountWarning(t *testing.T) {
	if w := RefCountWarning(1000); w != "" {
		t.Fatalf("warned at 1000 refs: %s", w)
	}
	if w := RefCountWarning(LegacyListLimit); w == "" {
		t.Fatal("no warning at the legacy limit")
	}
}

// A halted sweeper resumes on its next sweep, without a restart, once the
// operator acts: a restored readable snapshot is loaded (its pins with it);
// a snapshot moved aside is a first start.
func TestHaltedSweeperResumes(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []string{"restored", "moved aside"} {
		t.Run(tc, func(t *testing.T) {
			dir := t.TempDir()
			st := openStore(t, dir, "build-output:%d", 3)
			snap := filepath.Join(dir, "refaccess.cbor")
			if err := os.WriteFile(snap, []byte("torn"), 0o600); err != nil {
				t.Fatal(err)
			}
			sw := sweeper(t, st, dir)
			defer sw.Close()
			if _, err := sw.Sweep(ctx, -1, false); !errors.Is(err, ErrHalted) {
				t.Fatalf("sweep before repair: %v, want ErrHalted", err)
			}
			if tc == "restored" {
				good := reftrack.New()
				good.Pin("build-output:2")
				if err := good.Flush(snap); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Rename(snap, snap+".bad"); err != nil {
				t.Fatal(err)
			}
			stats, err := sw.Sweep(ctx, -1, false)
			if err != nil || sw.Halted() != "" || stats.Halted != "" {
				t.Fatalf("sweep after repair: %v, halted %q / %q", err, sw.Halted(), stats.Halted)
			}
			if e, _ := sw.Entry("build-output:2"); e.Pinned != (tc == "restored") {
				t.Fatalf("pin after resume = %v", e.Pinned)
			}
			if _, err := os.Stat(snap); err != nil {
				t.Fatalf("resumed sweeper did not write its snapshot: %v", err)
			}
		})
	}
}
