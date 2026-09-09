package amber_test

import (
	"bytes"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jobs-build/jobs-iroh/amber"
)

// newStore opens a fresh Store over a temp dir, closed at test end.
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

// write creates path (and parents) with data, chmod'ing explicitly so the
// test's umask cannot skew modes.
func write(t *testing.T, path string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

// compareTrees requires got to mirror want exactly: same entries (both
// directions), same types, and — checked from the want side — same regular
// file contents and permissions, same symlink targets, same dir permissions.
func compareTrees(t *testing.T, want, got string) {
	t.Helper()
	walk := func(base, other string, deep bool) {
		err := filepath.WalkDir(base, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(base, p)
			if err != nil {
				return err
			}
			if rel == "." {
				return nil
			}
			bi, err := os.Lstat(p)
			if err != nil {
				return err
			}
			oi, err := os.Lstat(filepath.Join(other, rel))
			if err != nil {
				t.Errorf("%s: missing on other side: %v", rel, err)
				return nil
			}
			if bi.Mode().Type() != oi.Mode().Type() {
				t.Errorf("%s: type %v != %v", rel, bi.Mode().Type(), oi.Mode().Type())
				return nil
			}
			if !deep {
				return nil
			}
			switch {
			case bi.Mode().IsRegular():
				if bi.Mode().Perm() != oi.Mode().Perm() {
					t.Errorf("%s: perm %v != %v", rel, bi.Mode().Perm(), oi.Mode().Perm())
				}
				bb, err := os.ReadFile(p)
				if err != nil {
					return err
				}
				ob, err := os.ReadFile(filepath.Join(other, rel))
				if err != nil {
					return err
				}
				if !bytes.Equal(bb, ob) {
					t.Errorf("%s: content differs (%d vs %d bytes)", rel, len(bb), len(ob))
				}
			case bi.Mode()&fs.ModeSymlink != 0:
				bt, err := os.Readlink(p)
				if err != nil {
					return err
				}
				ot, err := os.Readlink(filepath.Join(other, rel))
				if err != nil {
					return err
				}
				if bt != ot {
					t.Errorf("%s: symlink target %q != %q", rel, bt, ot)
				}
			case bi.IsDir():
				if bi.Mode().Perm() != oi.Mode().Perm() {
					t.Errorf("%s: dir perm %v != %v", rel, bi.Mode().Perm(), oi.Mode().Perm())
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	walk(want, got, true)
	walk(got, want, false) // no extra entries on the got side
}

func TestIngestMaterializeRoundTrip(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()

	src := t.TempDir()
	big := make([]byte, 4<<20) // multi-chunk above the 1 MiB max chunk size
	rand.New(rand.NewSource(42)).Read(big)
	write(t, filepath.Join(src, "a.txt"), []byte("hello world\n"), 0o644)
	write(t, filepath.Join(src, "bin", "run.sh"), []byte("#!/bin/sh\necho hi\n"), 0o755)
	write(t, filepath.Join(src, "empty"), nil, 0o644)
	write(t, filepath.Join(src, "big.bin"), big, 0o600)
	write(t, filepath.Join(src, "sub", "inner.txt"), []byte("inner"), 0o644)
	if err := os.MkdirAll(filepath.Join(src, "emptydir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("a.txt", filepath.Join(src, "ln")); err != nil {
		t.Fatal(err)
	}

	root, err := s.IngestDir(ctx, src)
	if err != nil {
		t.Fatalf("IngestDir: %v", err)
	}
	dest := filepath.Join(t.TempDir(), "out")
	if err := s.Materialize(ctx, root, dest); err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	compareTrees(t, src, dest)
}

func TestIdenticalContentIdenticalKeys(t *testing.T) {
	ctx := t.Context()
	s1, s2 := newStore(t), newStore(t)

	data := []byte("the quick brown fox")
	fk, err := amber.FileKey(data)
	if err != nil {
		t.Fatalf("FileKey: %v", err)
	}
	ik, err := s1.IngestFile(ctx, data)
	if err != nil {
		t.Fatalf("IngestFile: %v", err)
	}
	if fk != ik {
		t.Errorf("FileKey %s != IngestFile key %s", fk, ik)
	}

	// Two directories with identical content, structure, modes, and mtimes
	// (entry metadata is part of the tree identity, so times are pinned).
	ts := time.Unix(1_700_000_000, 0)
	mk := func() string {
		dir := t.TempDir()
		write(t, filepath.Join(dir, "a.txt"), []byte("alpha"), 0o644)
		write(t, filepath.Join(dir, "sub", "b.txt"), []byte("beta"), 0o755)
		err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			return os.Chtimes(p, ts, ts)
		})
		if err != nil {
			t.Fatal(err)
		}
		return dir
	}
	k1, err := s1.IngestDir(ctx, mk())
	if err != nil {
		t.Fatal(err)
	}
	k2, err := s2.IngestDir(ctx, mk())
	if err != nil {
		t.Fatal(err)
	}
	if k1 != k2 {
		t.Errorf("identical trees in two stores: keys differ: %s vs %s", k1, k2)
	}
}

func TestIngestDedupStats(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()

	dir := t.TempDir()
	write(t, filepath.Join(dir, "one.txt"), []byte("first file"), 0o644)
	write(t, filepath.Join(dir, "two.txt"), []byte("second file"), 0o644)

	k1, st1, err := s.IngestDirStats(ctx, dir)
	if err != nil {
		t.Fatalf("IngestDirStats: %v", err)
	}
	if st1.ObjectsStored == 0 || st1.BytesStored == 0 {
		t.Errorf("fresh ingest stored nothing: %+v", st1)
	}
	if st1.ObjectsDeduped != 0 {
		t.Errorf("fresh ingest of unique content deduped %d objects", st1.ObjectsDeduped)
	}

	k2, st2, err := s.IngestDirStats(ctx, dir)
	if err != nil {
		t.Fatalf("re-ingest: %v", err)
	}
	if k2 != k1 {
		t.Errorf("re-ingest key %s != %s", k2, k1)
	}
	if st2.ObjectsStored != 0 || st2.BytesStored != 0 {
		t.Errorf("re-ingest stored objects: %+v", st2)
	}
	if st2.ObjectsDeduped == 0 {
		t.Errorf("re-ingest deduped nothing: %+v", st2)
	}
}
