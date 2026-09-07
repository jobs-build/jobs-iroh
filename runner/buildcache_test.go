//go:build linux

package runner_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/runner"
	"github.com/jobs-build/jobs-iroh/sandbox"
)

const testCacheID = "test-go-cache"

// cachePinned builds a Pinned declaring one cache at /test-cache running script.
func cachePinned(script string) builddef.Pinned {
	return builddef.Pinned{
		Env:    map[string]string{},
		Script: script,
		Caches: builddef.CanonicalCaches([]builddef.PinnedCache{{Path: "/test-cache", ID: testCacheID}}),
	}
}

// runCachedBuild pins + runs one build with its own F (distinct srcTag) and
// returns the outcome.
func runCachedBuild(t *testing.T, ctx context.Context, st *amber.Store,
	rw runner.RefWriter, shellKey key.Key, platform, srcTag, script string) runner.Outcome {
	t.Helper()
	srcInput, _ := mkImportInputWithOutput(t, ctx, st, "src-fetcher",
		"https://example.com/"+srcTag+".tgz",
		"BUILD.jobs", "def build(): return struct(inputs={}, env={}, script='', runtime_deps=[])\n")
	_, f, _ := putPinnedBuild(t, ctx, st, srcInput, platform, cachePinned(script))
	brc := runner.BuildRunCfg{Platform: platform, ShellKey: shellKey, CacheDir: t.TempDir()}
	runCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	return runner.RunBuild(runCtx, st, rw, brc, runner.NamespaceBuildExecutor{}, f)
}

// cacheTreeNames lists the top-level entry names of the current cache state.
func cacheTreeNames(t *testing.T, ctx context.Context, st *amber.Store, platform string) (key.Key, map[string]bool) {
	t.Helper()
	k, ok, err := st.GetKey(ctx, builddef.CacheRefName(testCacheID, platform))
	if err != nil || !ok {
		t.Fatalf("cache ref: ok=%v err=%v", ok, err)
	}
	ents, err := st.Ls(ctx, k, "")
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, e := range ents {
		names[e.Name] = true
	}
	return k, names
}

// recordingRefWriter counts the ref names written through it.
type recordingRefWriter struct {
	runner.RefWriter
	names []string
}

func (w *recordingRefWriter) WriteRefs(ctx context.Context, refs []runner.Ref) (runner.PushTotals, error) {
	for _, r := range refs {
		w.names = append(w.names, r.Name)
	}
	return w.RefWriter.WriteRefs(ctx, refs)
}

// TestRunBuild_CacheWarmAndPrune drives the full lifecycle across three builds
// sharing one cache id (design §5): A populates, B sees A's state warm and its
// unread file is pruned from B's upload, C touches nothing so the ref is
// unchanged (§5.3 empty-skip).
func TestRunBuild_CacheWarmAndPrune(t *testing.T) {
	if !sandbox.UserNSAvailable() {
		t.Skip("user namespaces unavailable")
	}
	ctx := context.Background()
	st := buildEvalStore(t)
	platform := runner.Platform()
	shellKey := buildShellArtifact(t, ctx, st)
	rw := runner.NewLocalRefWriter(st)

	// Build A: writes two files into the cache (both freshly written => kept).
	outA := runCachedBuild(t, ctx, st, rw, shellKey, platform, "cache-a",
		`printf hot > /test-cache/keep; printf cold > /test-cache/cold; printf ok > "$out/result"`)
	if outA.Failed || outA.Decline || outA.Cancelled {
		t.Fatalf("build A: %+v", outA)
	}
	keyA, namesA := cacheTreeNames(t, ctx, st, platform)
	if !namesA["keep"] || !namesA["cold"] {
		t.Fatalf("after A: cache entries = %v", namesA)
	}

	// Build B (different F, same cache id): READS keep, never touches cold.
	// Seeding pre-ages atimes; reading keep refreshes it through the rw bind,
	// so the prune drops cold and keeps keep — this asserts the whole
	// pre-age → strictatime bind → atime-refresh → prune chain.
	outB := runCachedBuild(t, ctx, st, rw, shellKey, platform, "cache-b",
		`cp /test-cache/keep "$out/result"`)
	if outB.Failed || outB.Decline || outB.Cancelled {
		t.Fatalf("build B: %+v", outB)
	}
	if got := string(readTarEntry(t, ctx, st, outB.OutputKey, "c/result")); got != "hot" {
		t.Fatalf("B did not see A's cache: result=%q want %q", got, "hot")
	}
	keyB, namesB := cacheTreeNames(t, ctx, st, platform)
	if keyB == keyA {
		t.Fatal("cache ref should have advanced after B's prune")
	}
	if !namesB["keep"] || namesB["cold"] {
		t.Fatalf("after B: cache entries = %v (want keep only)", namesB)
	}

	// Build C: never touches the cache => pruned-to-empty => ref UNCHANGED.
	outC := runCachedBuild(t, ctx, st, rw, shellKey, platform, "cache-c",
		`printf ok > "$out/result"`)
	if outC.Failed || outC.Decline || outC.Cancelled {
		t.Fatalf("build C: %+v", outC)
	}
	keyC, _ := cacheTreeNames(t, ctx, st, platform)
	if keyC != keyB {
		t.Fatal("empty prune must keep the last published cache state")
	}
}

// TestRunBuild_CacheUnchangedAndFailure: an all-read build re-ingests to the
// seed key and writes NO cache ref (§5.3 skip); a failing build uploads
// nothing (§5.3 success-only).
func TestRunBuild_CacheUnchangedAndFailure(t *testing.T) {
	if !sandbox.UserNSAvailable() {
		t.Skip("user namespaces unavailable")
	}
	ctx := context.Background()
	st := buildEvalStore(t)
	platform := runner.Platform()
	shellKey := buildShellArtifact(t, ctx, st)
	rw := runner.NewLocalRefWriter(st)

	// Populate.
	out := runCachedBuild(t, ctx, st, rw, shellKey, platform, "cache-u1",
		`printf hot > /test-cache/keep; printf ok > "$out/result"`)
	if out.Failed {
		t.Fatalf("populate: %+v", out)
	}
	keyBefore, _ := cacheTreeNames(t, ctx, st, platform)

	// All-read build: tree round-trips to the seed key => no cache ref written.
	rec := &recordingRefWriter{RefWriter: rw}
	out = runCachedBuild(t, ctx, st, rec, shellKey, platform, "cache-u2",
		`cp /test-cache/keep "$out/result"`)
	if out.Failed {
		t.Fatalf("all-read build: %+v", out)
	}
	for _, n := range rec.names {
		if strings.HasPrefix(n, "build-cache:") {
			t.Fatalf("unchanged cache must not rewrite its ref (wrote %v)", rec.names)
		}
	}

	// Failing build: writes into the cache, then exits 1 => nothing uploaded.
	out = runCachedBuild(t, ctx, st, rw, shellKey, platform, "cache-u3",
		`printf poison > /test-cache/keep; exit 1`)
	if !out.Failed {
		t.Fatalf("expected failure, got %+v", out)
	}
	keyAfter, names := cacheTreeNames(t, ctx, st, platform)
	if keyAfter != keyBefore {
		t.Fatal("failed build must not update the cache ref")
	}
	if got := string(readTarEntry(t, ctx, st, keyAfter, "keep")); got != "hot" {
		t.Fatalf("cache content changed after failed build: %q", got)
	}
	_ = names
}
