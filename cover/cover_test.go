package cover_test

// Tests for the covered-closure walker (Walk) and the shared KP derivation
// (Derive) — sibling-sources design §5, §6, exercised in-store over trees
// built with IngestSourceDir (symlinks ride as keyless LinkTarget entries).

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/cover"
)

// newStore opens a fresh Store over a temp dir, closed at test end (the
// amber test harness's helper, local to this package).
func newStore(t *testing.T) *amber.Store {
	t.Helper()
	s, err := amber.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	return s
}

func write(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func symlink(t *testing.T, target, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatalf("symlink %s -> %s: %v", path, target, err)
	}
}

func ingest(t *testing.T, s *amber.Store, dir string) key.Key {
	t.Helper()
	root, err := s.IngestSourceDir(t.Context(), dir)
	if err != nil {
		t.Fatalf("IngestSourceDir: %v", err)
	}
	return root
}

// --- Walk ---

func TestWalkDeclaredSeeds(t *testing.T) {
	s := newStore(t)
	src := t.TempDir()
	write(t, filepath.Join(src, "services", "api", "BUILD.jobs"), []byte("recipe"))
	write(t, filepath.Join(src, "lib", "common", "go.mod"), []byte("module common"))
	write(t, filepath.Join(src, "docs", "readme.md"), []byte("docs"))
	write(t, filepath.Join(src, "extra", "other.txt"), []byte("outside"))
	root := ingest(t, s, src)

	res, err := cover.Walk(t.Context(), s, root, "services/api", []string{"lib/common", "docs/readme.md"}, nil)
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	want := []string{"docs/readme.md", "lib/common", "services/api"}
	if !slices.Equal(res.Paths, want) {
		t.Errorf("Paths = %v, want %v (dir seed + declared dir + declared file, sorted)", res.Paths, want)
	}
	if len(res.Warnings) != 0 {
		t.Errorf("Warnings = %v, want none", res.Warnings)
	}
}

func TestWalkChasesSymlinkChains(t *testing.T) {
	s := newStore(t)
	src := t.TempDir()
	// A covered dir whose link points at a sibling file...
	write(t, filepath.Join(src, "sibling", "file.txt"), []byte("target"))
	symlink(t, "../sibling/file.txt", filepath.Join(src, "covered", "link.txt"))
	write(t, filepath.Join(src, "covered", "own.txt"), []byte("own"))
	// ...and a two-hop chain of top-level links.
	write(t, filepath.Join(src, "c.txt"), []byte("end of chain"))
	symlink(t, "c.txt", filepath.Join(src, "b.txt"))
	symlink(t, "b.txt", filepath.Join(src, "a.txt"))
	root := ingest(t, s, src)

	res, err := cover.Walk(t.Context(), s, root, "covered", []string{"a.txt"}, nil)
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	want := []string{"a.txt", "b.txt", "c.txt", "covered", "sibling/file.txt"}
	if !slices.Equal(res.Paths, want) {
		t.Errorf("Paths = %v, want %v (chased targets + every intermediate link)", res.Paths, want)
	}
}

// TestWalkThroughSymlinkedDirComponent covers a declared path whose
// INTERMEDIATE component is a directory symlink: the link itself and the
// fully-resolved location must both join the closure (design §5.4 —
// component-wise resolution, whole-target lexical cleaning is wrong).
func TestWalkThroughSymlinkedDirComponent(t *testing.T) {
	s := newStore(t)
	src := t.TempDir()
	write(t, filepath.Join(src, "real", "inner.txt"), []byte("inner"))
	symlink(t, "real", filepath.Join(src, "linkdir"))
	root := ingest(t, s, src)

	res, err := cover.Walk(t.Context(), s, root, "", []string{"linkdir/inner.txt"}, nil)
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	want := []string{"linkdir", "linkdir/inner.txt", "real/inner.txt"}
	if !slices.Equal(res.Paths, want) {
		t.Errorf("Paths = %v, want %v (intermediate dir symlink + declared path + resolved target)", res.Paths, want)
	}
	// The closure must survive pruning: "linkdir/inner.txt" prunes only
	// because the intermediate link "linkdir" is itself a full cover.
	if _, err := s.PruneTree(t.Context(), root, res.Paths); err != nil {
		t.Errorf("PruneTree over the walked closure: %v", err)
	}
}

func TestWalkSymlinkCycleTerminates(t *testing.T) {
	s := newStore(t)
	src := t.TempDir()
	symlink(t, "b", filepath.Join(src, "a"))
	symlink(t, "a", filepath.Join(src, "b"))
	root := ingest(t, s, src)

	// Covering the cycle's links directly terminates via the visited set:
	// both links join the closure, both end dangling-by-cycle without error.
	res, err := cover.Walk(t.Context(), s, root, "", []string{"a"}, nil)
	if err != nil {
		t.Fatalf("Walk over link cycle: %v", err)
	}
	if want := []string{"a", "b"}; !slices.Equal(res.Paths, want) {
		t.Errorf("Paths = %v, want %v", res.Paths, want)
	}

	// A path THROUGH the cycle exhausts the per-path link budget — a loud
	// error, never a hang.
	done := make(chan error, 1)
	go func() {
		_, err := cover.Walk(t.Context(), s, root, "", []string{"a/x"}, nil)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "symlink loop") {
			t.Errorf("path through cycle: err = %v, want symlink-loop budget error", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Walk hung on a symlink cycle")
	}
}

func TestWalkDanglingInRootTarget(t *testing.T) {
	s := newStore(t)
	src := t.TempDir()
	symlink(t, "missing.txt", filepath.Join(src, "link"))
	root := ingest(t, s, src)

	res, err := cover.Walk(t.Context(), s, root, "", []string{"link"}, nil)
	if err != nil {
		t.Fatalf("Walk: %v (dangling in-root target must warn, not fail)", err)
	}
	if want := []string{"link"}; !slices.Equal(res.Paths, want) {
		t.Errorf("Paths = %v, want %v (link kept, missing target not added)", res.Paths, want)
	}
	if len(res.Warnings) != 1 || res.Warnings[0].Path != "missing.txt" {
		t.Errorf("Warnings = %+v, want one naming missing.txt", res.Warnings)
	}
}

func TestWalkEscapingTargets(t *testing.T) {
	s := newStore(t)
	src := t.TempDir()
	symlink(t, "/etc/hosts", filepath.Join(src, "link_abs"))
	symlink(t, "../outside.txt", filepath.Join(src, "link_up"))
	root := ingest(t, s, src)
	ctx := t.Context()

	if _, err := cover.Walk(ctx, s, root, "", []string{"link_abs"}, nil); err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Errorf("absolute target: err = %v, want absolute-path escape error", err)
	}
	if _, err := cover.Walk(ctx, s, root, "", []string{"link_up"}, nil); err == nil || !strings.Contains(err.Error(), "escapes the context root") {
		t.Errorf("../ target: err = %v, want escapes-root error", err)
	}

	// The per-recipe hatch: an allowed escaping link is kept verbatim (it
	// dangles in the sandbox), no target joins, no error.
	for _, link := range []string{"link_abs", "link_up"} {
		res, err := cover.Walk(ctx, s, root, "", []string{link}, []string{link})
		if err != nil {
			t.Errorf("allowed escaping %s: %v", link, err)
			continue
		}
		if want := []string{link}; !slices.Equal(res.Paths, want) {
			t.Errorf("allowed escaping %s: Paths = %v, want %v", link, res.Paths, want)
		}
	}
}

func TestWalkMissingDeclaredSourceErrors(t *testing.T) {
	s := newStore(t)
	src := t.TempDir()
	write(t, filepath.Join(src, "present.txt"), []byte("x"))
	root := ingest(t, s, src)

	_, err := cover.Walk(t.Context(), s, root, "", []string{"no/such/path"}, nil)
	if err == nil || !strings.Contains(err.Error(), `"no/such/path"`) {
		t.Errorf("missing declared source: err = %v, want error naming the path", err)
	}
	if _, err := cover.Walk(t.Context(), s, root, "", nil, nil); err == nil {
		t.Errorf("empty seed set must error")
	}
}

// TestWalkScansCoveredSubtree: a symlink buried deep inside a covered
// directory is chased and its in-root target joins the closure.
func TestWalkScansCoveredSubtree(t *testing.T) {
	s := newStore(t)
	src := t.TempDir()
	write(t, filepath.Join(src, "shared", "data.txt"), []byte("shared"))
	write(t, filepath.Join(src, "app", "sub", "deep", "file.go"), []byte("code"))
	symlink(t, "../../../shared/data.txt", filepath.Join(src, "app", "sub", "deep", "link"))
	root := ingest(t, s, src)

	res, err := cover.Walk(t.Context(), s, root, "app", nil, nil)
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	want := []string{"app", "shared/data.txt"}
	if !slices.Equal(res.Paths, want) {
		t.Errorf("Paths = %v, want %v (deep link inside the covered dir chased)", res.Paths, want)
	}
}

// --- Derive ---

func encodePinned(t *testing.T, p builddef.Pinned) []byte {
	t.Helper()
	b, err := builddef.EncodePinned(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// deriveFixture writes a small monorepo on disk (the tests mutate and
// re-ingest it).
func deriveFixture(t *testing.T) string {
	src := t.TempDir()
	write(t, filepath.Join(src, "services", "api", "BUILD.jobs"), []byte("recipe"))
	write(t, filepath.Join(src, "services", "api", "main.go"), []byte("package main"))
	write(t, filepath.Join(src, "lib", "common", "common.go"), []byte("package common"))
	write(t, filepath.Join(src, "docs", "readme.md"), []byte("docs"))
	return src
}

// touchAll bumps every mtime under src (identical bytes, fresh timestamps).
func touchAll(t *testing.T, src string) {
	t.Helper()
	future := time.Now().Add(2 * time.Hour)
	err := filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return err
		}
		return os.Chtimes(p, future, future)
	})
	if err != nil {
		t.Fatalf("chtimes walk: %v", err)
	}
}

// TestDeriveLegacyNormalizes: no Sources (a legacy or root build) — Derive
// normalizes the WHOLE env tree, so KP is immune to an mtime-only touch of
// the entire tree but re-keys on any content change.
func TestDeriveLegacyNormalizes(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	src := deriveFixture(t)
	pinned := builddef.Pinned{}
	pb := encodePinned(t, pinned)

	root1 := ingest(t, s, src)
	kp1, err := cover.Derive(ctx, s, pb, pinned, "linux/amd64", root1)
	if err != nil {
		t.Fatalf("Derive: %v", err)
	}

	touchAll(t, src)
	root2 := ingest(t, s, src)
	if root1 == root2 {
		t.Fatal("mtime touch did not re-key the source tree — fixture broken (amber hashes mtime)")
	}
	kp2, err := cover.Derive(ctx, s, pb, pinned, "linux/amd64", root2)
	if err != nil {
		t.Fatalf("Derive 2: %v", err)
	}
	if kp1 != kp2 {
		t.Errorf("mtime-only touch moved the legacy KP: %s vs %s", kp1, kp2)
	}

	write(t, filepath.Join(src, "docs", "readme.md"), []byte("docs v2"))
	kp3, err := cover.Derive(ctx, s, pb, pinned, "linux/amd64", ingest(t, s, src))
	if err != nil {
		t.Fatalf("Derive 3: %v", err)
	}
	if kp3 == kp1 {
		t.Errorf("content change did not re-key the legacy KP (whole tree is the cover)")
	}
}

// TestDeriveWidenedCutoff: with Sources set, KP moves with covered content
// and ONLY covered content — the early-cutoff property the arc exists for.
func TestDeriveWidenedCutoff(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	src := deriveFixture(t)
	pinned := builddef.Pinned{
		Sources: []string{"lib/common", "services/api"},
		Dir:     "services/api",
	}
	pb := encodePinned(t, pinned)

	kp1, err := cover.Derive(ctx, s, pb, pinned, "linux/amd64", ingest(t, s, src))
	if err != nil {
		t.Fatalf("Derive: %v", err)
	}

	// Outside the cover: docs change + mtime churn everywhere ⇒ same KP.
	write(t, filepath.Join(src, "docs", "readme.md"), []byte("docs v2"))
	write(t, filepath.Join(src, "docs", "new.md"), []byte("new page"))
	touchAll(t, src)
	kp2, err := cover.Derive(ctx, s, pb, pinned, "linux/amd64", ingest(t, s, src))
	if err != nil {
		t.Fatalf("Derive 2: %v", err)
	}
	if kp2 != kp1 {
		t.Errorf("out-of-cover change moved KP: %s vs %s", kp1, kp2)
	}

	// Inside the cover ⇒ new KP.
	write(t, filepath.Join(src, "lib", "common", "common.go"), []byte("package common // v2"))
	kp3, err := cover.Derive(ctx, s, pb, pinned, "linux/amd64", ingest(t, s, src))
	if err != nil {
		t.Fatalf("Derive 3: %v", err)
	}
	if kp3 == kp1 {
		t.Errorf("covered content change did not re-key KP")
	}
}

// TestDeriveGeneratedOverlay: Pinned.Generated participates in KP — same
// cover with different synthesized content must not join.
func TestDeriveGeneratedOverlay(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	src := deriveFixture(t)
	root := ingest(t, s, src)

	derive := func(p builddef.Pinned) key.Key {
		t.Helper()
		kp, err := cover.Derive(ctx, s, encodePinned(t, p), p, "linux/amd64", root)
		if err != nil {
			t.Fatalf("Derive: %v", err)
		}
		return kp
	}
	base := builddef.Pinned{Sources: []string{"services/api"}, Dir: "services/api"}
	genA := base
	genA.Generated = map[string][]byte{"services/api/Cargo.lock": []byte("lock-a")}
	genB := base
	genB.Generated = map[string][]byte{"services/api/Cargo.lock": []byte("lock-b")}

	kpBase, kpA, kpB, kpA2 := derive(base), derive(genA), derive(genB), derive(genA)
	if kpA == kpBase {
		t.Errorf("generated overlay did not change KP")
	}
	if kpB == kpA {
		t.Errorf("generated content change did not change KP")
	}
	if kpA2 != kpA {
		t.Errorf("identical generated overlay not deterministic: %s vs %s", kpA, kpA2)
	}
}

func TestDeriveGeneratedCap(t *testing.T) {
	s := newStore(t)
	src := deriveFixture(t)
	root := ingest(t, s, src)

	p := builddef.Pinned{
		Sources: []string{"services/api"},
		Dir:     "services/api",
		Generated: map[string][]byte{
			"a.bin": make([]byte, builddef.GeneratedMaxBytes),
			"b.bin": []byte("one over"),
		},
	}
	_, err := cover.Derive(t.Context(), s, encodePinned(t, p), p, "linux/amd64", root)
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("generated total over the cap: err = %v, want cap error", err)
	}
}

// --- WalkClosure (source-closure design §5) ---

// closureFixture lays out lib/common, services/api (with a manifest), docs.
func closureFixture(t *testing.T, s *amber.Store) key.Key {
	t.Helper()
	src := t.TempDir()
	write(t, filepath.Join(src, "lib", "common", "a.txt"), []byte("a"))
	write(t, filepath.Join(src, "services", "api", "go.mod"), []byte("module api"))
	write(t, filepath.Join(src, "docs", "x.txt"), []byte("docs"))
	return ingest(t, s, src)
}

func TestWalkClosureSeedsAndWorkdir(t *testing.T) {
	s := newStore(t)
	root := closureFixture(t, s)

	// (a) no dir seed: only the declared path is covered even with dir="".
	res, err := cover.WalkClosure(t.Context(), s, root, "", []string{"lib/common"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Paths, []string{"lib/common"}) {
		t.Fatalf("paths: %v", res.Paths)
	}

	// (b) workdir covered transitively via its own manifest.
	if _, err := cover.WalkClosure(t.Context(), s, root, "services/api",
		[]string{"lib/common", "services/api/go.mod"}, nil); err != nil {
		t.Fatalf("covered workdir rejected: %v", err)
	}

	// (c) workdir NOT covered → hard error.
	_, err = cover.WalkClosure(t.Context(), s, root, "services/api", []string{"lib/common"}, nil)
	if err == nil || !strings.Contains(err.Error(), "does not cover the build dir") {
		t.Fatalf("want workdir error, got %v", err)
	}

	// (d) ancestor cover satisfies the workdir check.
	if _, err := cover.WalkClosure(t.Context(), s, root, "services/api", []string{"services"}, nil); err != nil {
		t.Fatalf("ancestor cover rejected: %v", err)
	}

	// (e) empty closure → error.
	if _, err := cover.WalkClosure(t.Context(), s, root, "", nil, nil); err == nil {
		t.Fatal("empty closure accepted")
	}

	// (f) missing declared path → hard error (same rule as Walk seeds).
	if _, err := cover.WalkClosure(t.Context(), s, root, "", []string{"nope"}, nil); err == nil {
		t.Fatal("missing declared closure path accepted")
	}
}

func TestDeriveClosureBranch(t *testing.T) {
	s := newStore(t)
	root := closureFixture(t, s)
	viaClosure, err := cover.Derive(t.Context(), s,
		encodePinned(t, builddef.Pinned{Script: "s", Closure: []string{"lib/common"}}),
		builddef.Pinned{Script: "s", Closure: []string{"lib/common"}}, "linux/amd64", root)
	if err != nil {
		t.Fatal(err)
	}
	// The covered tree must match a Sources prune of the same list: KP trees
	// differ only through job.cbor (Pinned bytes), so compare the src/ subtree.
	viaSources, err := cover.Derive(t.Context(), s,
		encodePinned(t, builddef.Pinned{Script: "s", Closure: []string{"lib/common"}}),
		builddef.Pinned{Script: "s", Sources: []string{"lib/common"}}, "linux/amd64", root)
	if err != nil {
		t.Fatal(err)
	}
	if viaClosure != viaSources {
		t.Fatalf("closure/sources prune divergence: %s vs %s", viaClosure, viaSources)
	}

	// Both set → error.
	if _, err := cover.Derive(t.Context(), s, []byte("pb"),
		builddef.Pinned{Closure: []string{"a"}, Sources: []string{"b"}}, "linux/amd64", root); err == nil {
		t.Fatal("both Closure and Sources accepted")
	}
}
