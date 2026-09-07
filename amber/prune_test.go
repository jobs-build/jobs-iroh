package amber_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
)

// pruneFixture ingests a small monorepo-shaped tree:
//
//	services/api/{BUILD.jobs,main.go}
//	lib/common/{go.mod,common.go}
//	lib/other/other.go
//	docs/readme.md
//	link.go -> lib/common/common.go   (symlink, kept verbatim by prune)
func pruneFixture(t *testing.T, s *amber.Store) (string, key.Key) {
	t.Helper()
	src := t.TempDir()
	write(t, filepath.Join(src, "services", "api", "BUILD.jobs"), []byte("recipe"), 0o644)
	write(t, filepath.Join(src, "services", "api", "main.go"), []byte("package main"), 0o644)
	write(t, filepath.Join(src, "lib", "common", "go.mod"), []byte("module common"), 0o644)
	write(t, filepath.Join(src, "lib", "common", "common.go"), []byte("package common"), 0o755)
	write(t, filepath.Join(src, "lib", "other", "other.go"), []byte("package other"), 0o644)
	write(t, filepath.Join(src, "docs", "readme.md"), []byte("docs"), 0o644)
	if err := os.Symlink("lib/common/common.go", filepath.Join(src, "link.go")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	root, err := s.IngestSourceDir(t.Context(), src)
	if err != nil {
		t.Fatalf("IngestSourceDir: %v", err)
	}
	return src, root
}

func TestPruneTreeCoversExactlyTheClosure(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	_, root := pruneFixture(t, s)

	covered, err := s.PruneTree(ctx, root, []string{"services/api", "lib/common", "link.go"})
	if err != nil {
		t.Fatalf("PruneTree: %v", err)
	}

	if got, want := lsNames(t, s, covered, ""), []string{"lib", "link.go", "services"}; !slices.Equal(got, want) {
		t.Errorf("root entries = %v, want %v", got, want)
	}
	if got, want := lsNames(t, s, covered, "lib"), []string{"common"}; !slices.Equal(got, want) {
		t.Errorf("lib entries = %v, want %v (lib/other must be pruned)", got, want)
	}
	if got, want := lsNames(t, s, covered, "services/api"), []string{"BUILD.jobs", "main.go"}; !slices.Equal(got, want) {
		t.Errorf("services/api entries = %v, want %v", got, want)
	}
	// The symlink is kept verbatim (target string preserved, no content key).
	ents, err := s.Ls(ctx, covered, "")
	if err != nil {
		t.Fatalf("Ls: %v", err)
	}
	for _, e := range ents {
		if e.Name == "link.go" && e.LinkTarget != "lib/common/common.go" {
			t.Errorf("link.go target = %q, want lib/common/common.go", e.LinkTarget)
		}
	}
}

// TestPruneTreeMtimeImmune is the load-bearing normalization test
// (sibling-sources design §6.1): re-ingesting byte-identical content with
// fresh mtimes yields a DIFFERENT source tree key (amber hashes mtime) but
// the SAME pruned tree key — KP is a pure content+mode function.
func TestPruneTreeMtimeImmune(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	src, root1 := pruneFixture(t, s)

	covered1, err := s.PruneTree(ctx, root1, []string{"services/api", "lib/common"})
	if err != nil {
		t.Fatalf("PruneTree 1: %v", err)
	}

	// Touch every file (identical bytes, new mtimes) and re-ingest.
	future := time.Now().Add(2 * time.Hour)
	err = filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return err
		}
		return os.Chtimes(p, future, future)
	})
	if err != nil {
		t.Fatalf("chtimes walk: %v", err)
	}
	root2, err := s.IngestSourceDir(ctx, src)
	if err != nil {
		t.Fatalf("re-ingest: %v", err)
	}
	if root1 == root2 {
		t.Fatalf("re-ingest with fresh mtimes produced the same tree key — fixture broken (amber is expected to hash mtime)")
	}

	covered2, err := s.PruneTree(ctx, root2, []string{"services/api", "lib/common"})
	if err != nil {
		t.Fatalf("PruneTree 2: %v", err)
	}
	if covered1 != covered2 {
		t.Errorf("pruned keys differ across an mtime-only touch: %s vs %s", covered1, covered2)
	}

	// And a real content change inside the cover DOES re-key.
	write(t, filepath.Join(src, "lib", "common", "common.go"), []byte("package common // v2"), 0o755)
	root3, err := s.IngestSourceDir(ctx, src)
	if err != nil {
		t.Fatalf("re-ingest 3: %v", err)
	}
	covered3, err := s.PruneTree(ctx, root3, []string{"services/api", "lib/common"})
	if err != nil {
		t.Fatalf("PruneTree 3: %v", err)
	}
	if covered3 == covered1 {
		t.Errorf("content change inside the cover did not re-key the pruned tree")
	}
}

func TestPruneTreeOutsideChangeInvariant(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	src, root1 := pruneFixture(t, s)

	covered1, err := s.PruneTree(ctx, root1, []string{"services/api"})
	if err != nil {
		t.Fatalf("PruneTree: %v", err)
	}

	// Change content OUTSIDE the cover: pruned key must not move.
	write(t, filepath.Join(src, "lib", "other", "other.go"), []byte("package other // changed"), 0o644)
	write(t, filepath.Join(src, "docs", "new.md"), []byte("new doc"), 0o644)
	root2, err := s.IngestSourceDir(ctx, src)
	if err != nil {
		t.Fatalf("re-ingest: %v", err)
	}
	covered2, err := s.PruneTree(ctx, root2, []string{"services/api"})
	if err != nil {
		t.Fatalf("PruneTree 2: %v", err)
	}
	if covered1 != covered2 {
		t.Errorf("out-of-cover change moved the pruned key: %s vs %s", covered1, covered2)
	}
}

func TestPruneTreeSingleFileCover(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	_, root := pruneFixture(t, s)

	covered, err := s.PruneTree(ctx, root, []string{"lib/common/go.mod", "services/api"})
	if err != nil {
		t.Fatalf("PruneTree: %v", err)
	}
	if got, want := lsNames(t, s, covered, "lib/common"), []string{"go.mod"}; !slices.Equal(got, want) {
		t.Errorf("lib/common entries = %v, want %v (file-level cover)", got, want)
	}
}

func TestPruneTreeErrors(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	_, root := pruneFixture(t, s)

	if _, err := s.PruneTree(ctx, root, []string{"no/such/path"}); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("missing path: err = %v, want not-found", err)
	}
	if _, err := s.PruneTree(ctx, root, nil); err == nil {
		t.Errorf("empty keep set must error")
	}
	if _, err := s.PruneTree(ctx, root, []string{"."}); err == nil {
		t.Errorf("whole-root cover must error")
	}
	// Descending through a file is a caller error, not a silent skip.
	if _, err := s.PruneTree(ctx, root, []string{"docs/readme.md/inside"}); err == nil || !strings.Contains(err.Error(), "non-directory") {
		t.Errorf("descend into file: err = %v, want non-directory", err)
	}
}

func TestBuildKPTreeDeterminismAndSensitivity(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	_, root := pruneFixture(t, s)
	covered, err := s.PruneTree(ctx, root, []string{"services/api"})
	if err != nil {
		t.Fatalf("PruneTree: %v", err)
	}

	pinned := []byte("canonical-pinned-bytes")
	kp1, err := s.BuildKPTree(ctx, pinned, "linux/amd64", covered)
	if err != nil {
		t.Fatalf("BuildKPTree: %v", err)
	}
	kp2, err := s.BuildKPTree(ctx, pinned, "linux/amd64", covered)
	if err != nil {
		t.Fatalf("BuildKPTree again: %v", err)
	}
	if kp1 != kp2 {
		t.Errorf("KP not deterministic: %s vs %s", kp1, kp2)
	}

	// Platform is load-bearing (design §3.3 INV): same pinned + cover on a
	// different platform must be a different KP.
	kpArm, err := s.BuildKPTree(ctx, pinned, "linux/arm64", covered)
	if err != nil {
		t.Fatalf("BuildKPTree arm: %v", err)
	}
	if kpArm == kp1 {
		t.Errorf("cross-platform KP collision — platform entry not keying")
	}

	kpOther, err := s.BuildKPTree(ctx, []byte("other-pinned"), "linux/amd64", covered)
	if err != nil {
		t.Fatalf("BuildKPTree other: %v", err)
	}
	if kpOther == kp1 {
		t.Errorf("pinned bytes not keying KP")
	}

	// Layout check: {job.cbor, platform, src, v}.
	if got, want := lsNames(t, s, kp1, ""), []string{"job.cbor", "platform", "src", "v"}; !slices.Equal(got, want) {
		t.Errorf("KP tree entries = %v, want %v", got, want)
	}
}

// TestBuildFromTreeDirEntry locks the widened-F shape (design §3.2): dir==""
// omits the entry (root builds keep their pre-field F), dir!="" keys it.
func TestBuildFromTreeDirEntry(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	_, root := pruneFixture(t, s)

	params := []byte(`{"a":1}`)
	fRoot, err := s.BuildFromTree(ctx, root, "", params, "linux/amd64", nil)
	if err != nil {
		t.Fatalf("BuildFromTree root: %v", err)
	}
	if got, want := lsNames(t, s, fRoot, ""), []string{"env", "params", "platform"}; !slices.Equal(got, want) {
		t.Errorf("root-build F-tree entries = %v, want %v (no dir entry)", got, want)
	}

	fA, err := s.BuildFromTree(ctx, root, "services/api", params, "linux/amd64", nil)
	if err != nil {
		t.Fatalf("BuildFromTree dir: %v", err)
	}
	if got, want := lsNames(t, s, fA, ""), []string{"dir", "env", "params", "platform"}; !slices.Equal(got, want) {
		t.Errorf("widened F-tree entries = %v, want %v", got, want)
	}
	fB, err := s.BuildFromTree(ctx, root, "lib/common", params, "linux/amd64", nil)
	if err != nil {
		t.Fatalf("BuildFromTree dir B: %v", err)
	}
	if fA == fB || fA == fRoot {
		t.Errorf("dir entry not keying F: fRoot=%s fA=%s fB=%s", fRoot, fA, fB)
	}
}
