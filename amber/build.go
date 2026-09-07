package amber

// Pure tree assembly: chunk data and directories into fstree objects without
// touching any store. Ported verbatim from jobs' amber/build.go — the
// tree-assembly logic mirrors amber's own (un-importable) cmd ingest driver
// using the public fstree/chunkers building blocks. Only the emit destination
// differs between callers: ingest routes objects into the packstore, FileKey
// discards them.

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"syscall"

	"github.com/amber-store/core/amberignore"
	"github.com/amber-store/core/chunkers"
	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"golang.org/x/sys/unix"
)

// defByteOpts and the item-chunker bit width match amber's ingest defaults, so
// identical content dedups across jobs-iroh, jobs, and the amber CLI. These
// are IDENTITY-CRITICAL: every content key depends on them.
var defByteOpts = &chunkers.ByteOpts{MinSize: 32 << 10, NormalSize: 128 << 10, MaxSize: 256 << 10}

func itemChunker() chunkers.ItemChunker { return chunkers.NewItemChunker(7) }

// BuildFile chunks data into blobs (emitting each), builds the file index, and
// returns the file's root key.
func BuildFile(data []byte, emit fstree.Emit) (key.Key, error) {
	ib := fstree.NewFileIndexBuilder(itemChunker())
	saw := false
	err := chunkers.SplitBytes(bytes.NewReader(data), defByteOpts, func(chunk []byte) error {
		saw = true
		obj, err := fstree.EncodeBlob(chunk)
		if err != nil {
			return err
		}
		if err := emit(obj); err != nil {
			return err
		}
		return ib.AddChild(emit, obj.Key, nil)
	})
	if err != nil {
		return key.Key{}, err
	}
	if !saw {
		obj, err := fstree.EncodeBlob([]byte{})
		if err != nil {
			return key.Key{}, err
		}
		if err := emit(obj); err != nil {
			return key.Key{}, err
		}
		if err := ib.AddChild(emit, obj.Key, nil); err != nil {
			return key.Key{}, err
		}
	}
	return ib.Finish(emit)
}

// FileKey derives the amber file key of data without storing anything.
func FileKey(data []byte) (key.Key, error) {
	return BuildFile(data, func(fstree.Object) error { return nil })
}

// BuildDir walks dir and emits every CAS object (children before parents),
// returning the directory's root key. Supports regular files, directories, and
// symlinks (the cases an import's /output tree contains).
func BuildDir(dir string, emit fstree.Emit) (key.Key, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return key.Key{}, err
	}
	sort.Slice(ents, func(i, j int) bool { return ents[i].Name() < ents[j].Name() })

	db := fstree.NewDirBuilder(itemChunker())
	for _, de := range ents {
		full := filepath.Join(dir, de.Name())
		e, err := buildEntry(full, de.Name(), emit)
		if err != nil {
			return key.Key{}, err
		}
		if err := db.AddEntry(emit, e); err != nil {
			return key.Key{}, err
		}
	}
	return db.Finish(emit)
}

// BuildSourceDir is like BuildDir but honors .amberignore files (gitignore
// semantics: per-directory patterns compose down the tree, with negation, **
// globs, dir-only and anchored forms). Entries matched by a pattern are skipped,
// and the .amberignore control files themselves are never stored — they drive
// ingestion but are not content. Use this for ingesting *source* trees (the
// local --source build and fetcher-import output); build outputs are
// machine-generated and must be stored verbatim via BuildDir.
func BuildSourceDir(dir string, emit fstree.Emit) (key.Key, error) {
	m, err := amberignore.Root(dir)
	if err != nil {
		return key.Key{}, err
	}
	return buildDirFiltered(dir, m, emit)
}

func buildDirFiltered(dir string, m *amberignore.Matcher, emit fstree.Emit) (key.Key, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return key.Key{}, err
	}
	sort.Slice(ents, func(i, j int) bool { return ents[i].Name() < ents[j].Name() })

	db := fstree.NewDirBuilder(itemChunker())
	for _, de := range ents {
		name := de.Name()
		isDir := de.IsDir()
		// Drop the control files themselves. amber's Matcher deliberately keeps
		// .amberignore files (so a stored tree round-trips); a jobs-iroh source
		// tree must not carry ingest-control files into the CAS, so we skip them
		// here. This is why these builders exist instead of calling core's own
		// ingest package, which stores them.
		if !isDir && name == amberignore.FileName {
			continue
		}
		// VCS metadata is never build source (sibling-sources design §11.1
		// [INV]): .git — dir or worktree/submodule gitfile — is excluded
		// unconditionally at every level. Without this, repo-root contexts
		// would push packfiles (and config, which can carry credentials) into
		// the CAS and every plugin sandbox, and F would churn on every git
		// command (amber hashes mtimes; git status rewrites .git/index).
		if name == ".git" {
			continue
		}
		if m.Ignored(name, isDir) {
			continue
		}
		full := filepath.Join(dir, name)
		e, err := buildEntryWith(full, name, emit, func(subFull, subName string) (key.Key, error) {
			child, err := m.Descend(subFull, subName)
			if err != nil {
				return key.Key{}, err
			}
			return buildDirFiltered(subFull, child, emit)
		})
		if err != nil {
			return key.Key{}, err
		}
		if err := db.AddEntry(emit, e); err != nil {
			return key.Key{}, err
		}
	}
	return db.Finish(emit)
}

func buildEntry(full, name string, emit fstree.Emit) (fstree.Entry, error) {
	return buildEntryWith(full, name, emit, func(subFull, _ string) (key.Key, error) {
		return BuildDir(subFull, emit)
	})
}

// buildEntryWith lstat's full, fills the entry's mode/uid/gid/mtime, and sets
// its content per file type — delegating subdirectory recursion to buildSubdir
// so the raw (BuildDir) and .amberignore-filtered (buildDirFiltered) walks share
// one entry-building path and differ only in how they descend.
func buildEntryWith(full, name string, emit fstree.Emit, buildSubdir func(subFull, subName string) (key.Key, error)) (fstree.Entry, error) {
	info, err := os.Lstat(full)
	if err != nil {
		return fstree.Entry{}, err
	}
	e := fstree.Entry{Name: []byte(name)}
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		e.Mode = uint64(st.Mode)
		e.UID = uint64(st.Uid)
		e.GID = uint64(st.Gid)
	} else {
		e.Mode = uint64(info.Mode().Perm())
	}
	e.Mtime = info.ModTime().UnixNano()

	switch e.Mode & unix.S_IFMT {
	case unix.S_IFREG:
		data, err := os.ReadFile(full)
		if err != nil {
			return fstree.Entry{}, err
		}
		ck, err := BuildFile(data, emit)
		if err != nil {
			return fstree.Entry{}, err
		}
		e.ContentKey = ck[:]
	case unix.S_IFDIR:
		ck, err := buildSubdir(full, name)
		if err != nil {
			return fstree.Entry{}, err
		}
		e.ContentKey = ck[:]
	case unix.S_IFLNK:
		target, err := os.Readlink(full)
		if err != nil {
			return fstree.Entry{}, err
		}
		e.LinkTarget = []byte(target)
	default:
		return fstree.Entry{}, fmt.Errorf("%s: unsupported file type %#o", full, e.Mode&unix.S_IFMT)
	}
	return e, nil
}
