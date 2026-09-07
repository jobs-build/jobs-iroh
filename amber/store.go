// Package amber is the jobs-iroh content-addressed store seam over
// github.com/amber-store/core: an embedded, in-process
// store — no daemon, no HTTP, no signing. A Store owns the two core pieces
// under one directory (<dir>/packstore for objects, <dir>/refs for name→key
// references) plus the tree-assembly and ref conventions the rest of
// jobs-iroh builds on.
//
// Identity-critical parameters are pinned HERE and nowhere else: the byte
// chunker (32 KiB / 128 KiB / 256 KiB) and the item-chunker bit width (7)
// match jobs and the amber CLI, so identical content produces identical keys
// across systems and dedups against existing stores. Never change them, and
// never fall back to core ingest's nil-opts defaults (2K/10K/64K) — that
// would silently fork every content key.
//
// Concurrency (verified against packstore/refstore sources): packstore.Open
// takes an exclusive non-blocking flock on the store directory, so exactly
// one process owns a store dir — a second Open fails with "already open".
// Within that process both halves are safe for concurrent use: packstore
// reads (Get/Has/GetRecord) take only a read lock, the write path
// (Put/WriteBatch) is serialized by an append mutex; refstore reads are
// lock-free Pebble reads and writes are serialized. packstore.Put is
// idempotent — it Has-checks first and returns success without writing when
// the key is already present, and a concurrent duplicate that loses the
// append race is dropped against the active-segment index — so re-puts of
// identical content are safe and cheap. A failed fsync poisons the packstore
// write path permanently (sticky error): reads continue, writes fail until
// the store is reopened.
package amber

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/core/refstore"
)

// Store owns an open amber-store-core store: a packstore (objects) and a
// refstore (references) under one directory. It is single-process by
// construction — the packstore flocks its directory, the refstore's Pebble DB
// takes its own dir lock — and safe for concurrent use within that process.
type Store struct {
	objects *packstore.Store
	refs    *refstore.Store

	// observe, when set, is called with the name of every reference read
	// that found its record (GetRef/GetKey) — the GC access-tracking seam.
	// Set once before the store serves traffic; not synchronized.
	observe func(name string)
	// guard, when set, brackets every PutRef with the collector's
	// PrepareRef so a reference published mid-mark keeps its closure.
	guard RefGuard
}

// Open opens (creating if necessary) the store rooted at dir: objects in
// <dir>/packstore, references in <dir>/refs. Both halves are opened with
// synchronous durability (every acknowledged write is fsynced). Open fails if
// another process holds the store open.
func Open(dir string) (*Store, error) {
	for _, sub := range []string{"packstore", "refs"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, fmt.Errorf("amber: creating %s: %w", filepath.Join(dir, sub), err)
		}
	}
	objects, err := packstore.Open(filepath.Join(dir, "packstore"), packstore.WithSync(true))
	if err != nil {
		return nil, err
	}
	refs, err := refstore.Open(filepath.Join(dir, "refs"), true /* sync */)
	if err != nil {
		objects.Close()
		return nil, err
	}
	return &Store{objects: objects, refs: refs}, nil
}

// Close closes both halves (objects, then refs), releasing the directory
// locks, and joins their errors.
func (s *Store) Close() error {
	return errors.Join(s.objects.Close(), s.refs.Close())
}

// Get returns the object bytes stored under k. The returned slice is
// caller-owned. Absent keys return packstore.ErrNotFound.
func (s *Store) Get(k key.Key) ([]byte, error) {
	return s.objects.Get(k)
}

// Has reports whether an object is stored under k.
func (s *Store) Has(k key.Key) (bool, error) {
	return s.objects.Has(k)
}

// Objects exposes the underlying packstore — the escape hatch for sync
// protocols that need the raw object surface (GetRecord/Missing/WriteParallel;
// amber-store-iroh's server.New takes exactly this).
func (s *Store) Objects() *packstore.Store { return s.objects }

// RefStore exposes the underlying refstore — the raw name→record surface the
// sync protocols serve.
func (s *Store) RefStore() *refstore.Store { return s.refs }

// getFunc returns the fstree-shaped get plugged into all tree reads, bound to
// the object store and failing fast once ctx is done. The embedded packstore
// itself takes no context (reads are local mmap/pread and effectively
// instant); this wrapper is the cancellation seam for the tree walks, which
// call get once per visited object.
func (s *Store) getFunc(ctx context.Context) func(key.Key) ([]byte, error) {
	return func(k key.Key) ([]byte, error) {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		return s.objects.Get(k)
	}
}

// RefGuard is the GC write-barrier seam around reference publication.
// *gc.Collector satisfies it directly. Exactly one of commit/abort must be
// called after PrepareRef returns nil.
type RefGuard interface {
	PrepareRef(root key.Key) (commit, abort func(), err error)
}

// SetObserver installs the ref-read hook. Call before serving traffic.
func (s *Store) SetObserver(f func(name string)) { s.observe = f }

// SetRefGuard installs the reference write barrier. Call before serving
// traffic; nil keeps the unguarded behavior (runner/registry private
// stores, tests).
func (s *Store) SetRefGuard(g RefGuard) { s.guard = g }
