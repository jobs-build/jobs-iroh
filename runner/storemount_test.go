package runner

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
)

// storeFixture ingests a one-entry store tree and a fresh newRoot/work pair.
func storeFixture(t *testing.T) (ctx context.Context, st *amber.Store, storeKey, entryKey key.Key, newRoot, work string) {
	t.Helper()
	ctx = context.Background()
	st = openTestStore(t)

	entryDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(entryDir, "marker"), []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	var err error
	entryKey, err = st.IngestDir(ctx, entryDir)
	if err != nil {
		t.Fatal(err)
	}
	storeKey, err = st.BuildStoreTree(ctx, []key.Key{entryKey})
	if err != nil {
		t.Fatal(err)
	}

	work = t.TempDir()
	newRoot = filepath.Join(work, "root")
	if err := os.MkdirAll(newRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	return ctx, st, storeKey, entryKey, newRoot, work
}

// TestMaterializeStore asserts that materializeStore extracts a content-
// addressed store tree to disk as <dest>/<BOK>/... AND leaves the staging
// removable: BuildStoreTree records its entry dirs as read-only 0555, and a
// verbatim lossless extract would make the work-tree cleanup fail with
// EACCES (jobs' lossy extractTar added 0700 for the same reason).
func TestMaterializeStore(t *testing.T) {
	ctx, st, storeKey, entryKey, _, _ := storeFixture(t)

	parent := t.TempDir()
	dest := filepath.Join(parent, "store")
	if err := materializeStore(ctx, st, storeKey, dest); err != nil {
		t.Fatalf("materializeStore: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(dest, entryKey.String(), "marker"))
	if err != nil {
		t.Fatalf("read materialized marker: %v", err)
	}
	if string(got) != "hi" {
		t.Errorf("materialized marker = %q, want %q", got, "hi")
	}
	fi, err := os.Stat(filepath.Join(dest, entryKey.String()))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm()&0o700 != 0o700 {
		t.Errorf("staged entry dir mode = %v, want owner-writable", fi.Mode().Perm())
	}
	// The load-bearing property: the whole staging removes cleanly.
	if err := os.RemoveAll(parent); err != nil {
		t.Fatalf("staging must be removable after the job: %v", err)
	}
}

// TestProvisionStore_MaterializeROBind asserts the simplified (materialize-
// only) provisionStore: exactly one read-only bind of the staged store at
// /jobs/store, staged under the work dir so the work-tree cleanup removes it.
func TestProvisionStore_MaterializeROBind(t *testing.T) {
	ctx, st, storeKey, entryKey, newRoot, work := storeFixture(t)

	binds, err := provisionStore(ctx, st, storeKey, newRoot, work)
	if err != nil {
		t.Fatalf("provisionStore: %v", err)
	}
	if len(binds) != 1 {
		t.Fatalf("expected exactly 1 bind mount, got %d", len(binds))
	}
	b := binds[0]
	if b.Target != sandboxStoreDir {
		t.Errorf("bind Target = %q, want %q", b.Target, sandboxStoreDir)
	}
	if !b.ReadOnly {
		t.Error("materialized store bind must be read-only")
	}
	got, err := os.ReadFile(filepath.Join(b.Source, entryKey.String(), "marker"))
	if err != nil {
		t.Fatalf("read staged marker: %v", err)
	}
	if string(got) != "hi" {
		t.Errorf("staged marker = %q, want %q", got, "hi")
	}
	// The mountpoint inside newRoot exists for the sandbox bind.
	if fi, err := os.Stat(filepath.Join(newRoot, sandboxStoreDir)); err != nil || !fi.IsDir() {
		t.Fatalf("store mountpoint missing under newRoot: %v", err)
	}
	// The work tree (including the staging) removes cleanly, like the
	// executor's deferred cleanup does.
	if err := os.RemoveAll(work); err != nil {
		t.Fatalf("work tree must be removable: %v", err)
	}
}
