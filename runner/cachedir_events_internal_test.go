package runner

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/events"
)

// TestSeedCachesEmitsSeededEvents: a warm seed emits the seeding phase plus
// one exec.cache/seeded with the materialized tree's bytes+files; a cold
// cache emits cold=true with zeros (cache-observability design §4, §5).
//
// (jobs' companion test that a slow seed span ticks liveness heartbeats is
// not portable here: it injected a slow Tar via a Store-interface wrapper,
// and the jobs-iroh seam is the concrete *amber.Store. The heartbeat
// machinery itself is covered by execheartbeat_internal_test.go.)
func TestSeedCachesEmitsSeededEvents(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	const platform = "linux/amd64"

	// Publish a warm state for cache "warm": 2 files, 15 bytes total.
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(src, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "sub", "b.txt"), []byte("worldworld"), 0o644); err != nil {
		t.Fatal(err)
	}
	k, err := st.IngestDir(ctx, src)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutRef(ctx, builddef.CacheRefName("warm", platform), k); err != nil {
		t.Fatal(err)
	}

	ev, sink := capJob(t)
	decls := []builddef.PinnedCache{{Path: "/w", ID: "warm"}, {Path: "/c", ID: "cold"}}
	cms, out := seedCaches(ctx, st, platform, decls, ev)
	if out != nil {
		t.Fatalf("seedCaches outcome: %+v", out)
	}
	defer removeCacheDirs(cms)

	evs := sink.events()
	if len(evs) != 3 {
		t.Fatalf("want phase + 2 cache events, got %d: %+v", len(evs), evs)
	}
	if evs[0].Type != events.TypeExecPhase || evs[0].Data["phase"] != "seeding" {
		t.Fatalf("first event must be the seeding phase: %+v", evs[0])
	}
	warm, cold := evs[1], evs[2]
	if warm.Type != events.TypeExecCache || warm.Data["cache"] != "warm" || warm.Data["stage"] != "seeded" {
		t.Fatalf("warm event wrong: %+v", warm)
	}
	if got := evInt(t, warm.Data["bytes"]); got != 15 {
		t.Fatalf("warm bytes = %d, want 15", got)
	}
	if got := evInt(t, warm.Data["files"]); got != 2 {
		t.Fatalf("warm files = %d, want 2", got)
	}
	if warm.Data["cold"] != false {
		t.Fatalf("warm cold flag wrong: %+v", warm.Data)
	}
	if ms := evInt(t, warm.Data["ms"]); ms < 0 {
		t.Fatalf("warm ms negative: %d", ms)
	}
	if cold.Data["cache"] != "cold" || cold.Data["cold"] != true ||
		evInt(t, cold.Data["bytes"]) != 0 || evInt(t, cold.Data["files"]) != 0 {
		t.Fatalf("cold event wrong: %+v", cold)
	}

	// The warm mount carries its seed key; the cold one stays zero.
	if len(cms) != 2 || cms[0].SeedKey != k {
		t.Fatalf("warm SeedKey = %+v, want %v", cms, k)
	}
	if cms[1].SeedKey != (key.Key{}) {
		t.Fatalf("cold SeedKey must stay zero, got %v", cms[1].SeedKey)
	}
}

// TestSeedCachesSeedIsLossless: the seeded tree must round-trip losslessly
// (mtimes preserved) — amber hashes mtime, so finalizeCaches' unchanged-skip
// compare (ingest key == seed key) only works over a lossless extract. This
// locks the Store.Materialize (not lossy extractTar) seeding path.
func TestSeedCachesSeedIsLossless(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	const platform = "linux/amd64"

	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "f.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := os.Chtimes(filepath.Join(src, "f.txt"), old, old); err != nil {
		t.Fatal(err)
	}
	k, err := st.IngestDir(ctx, src)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutRef(ctx, builddef.CacheRefName("c", platform), k); err != nil {
		t.Fatal(err)
	}

	cms, out := seedCaches(ctx, st, platform, []builddef.PinnedCache{{Path: "/c", ID: "c"}}, nil)
	if out != nil {
		t.Fatalf("seedCaches outcome: %+v", out)
	}
	defer removeCacheDirs(cms)

	// Re-ingesting the untouched seeded tree must reproduce the seed key.
	got, err := st.IngestDir(ctx, cms[0].HostDir)
	if err != nil {
		t.Fatal(err)
	}
	if got != k {
		t.Fatalf("seeded tree re-ingest = %v, want the seed key %v (lossless round-trip)", got, k)
	}
}

// TestSeedCachesNoDeclsNoEvents: no declared caches ⇒ no phase, no events.
func TestSeedCachesNoDeclsNoEvents(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	ev, sink := capJob(t)
	cms, out := seedCaches(ctx, st, "linux/amd64", nil, ev)
	if out != nil || len(cms) != 0 {
		t.Fatalf("unexpected: cms=%v out=%+v", cms, out)
	}
	if evs := sink.events(); len(evs) != 0 {
		t.Fatalf("want no events, got %+v", evs)
	}
}
