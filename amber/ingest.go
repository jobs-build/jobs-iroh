package amber

import (
	"context"
	"errors"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
)

// IngestStats reports what one ingest wrote: ObjectsStored/BytesStored count
// NEWLY written objects and their (pre-compression) payload bytes — a
// re-ingest of present content stores nothing — and ObjectsDeduped the
// emitted objects that were already present. Counting is exact for a single
// ingest; concurrent ingests of overlapping content may each count a shared
// object as stored while the packstore appends it once (its Put/append path
// dedups again internally).
type IngestStats struct {
	ObjectsStored  uint64
	ObjectsDeduped uint64
	BytesStored    uint64
}

// errBatchStopped signals the builder that the surrounding WriteBatch aborted
// the iteration; the batch's own (real) error supersedes it.
var errBatchStopped = errors.New("amber: write batch stopped")

// ingest runs build with an emit that routes every emitted object directly
// into the packstore — no amberpack framing, no pipe. All new objects go
// through one packstore.WriteBatch, so they are appended in emit order
// (builders emit children before parents, hence objects-before-parents holds
// on disk too) and fsynced once at the end: on success every object is
// durable before the root key is returned to the caller. A crash mid-ingest
// leaves a valid prefix of the batch — harmless under content addressing.
// Each emit Has-checks first: present objects are counted as deduped and
// skipped, new ones counted as stored (an object emitted twice within one
// ingest is appended once and counted deduped the second time, because the
// batch's append publishes it in the active index immediately).
func (s *Store) ingest(ctx context.Context, build func(emit fstree.Emit) (key.Key, error)) (key.Key, IngestStats, error) {
	var (
		root     key.Key
		stats    IngestStats
		buildErr error
	)
	batchErr := s.objects.WriteBatch(func(yield func(packstore.Object, error) bool) {
		root, buildErr = build(func(o fstree.Object) error {
			if err := context.Cause(ctx); err != nil {
				return err
			}
			has, err := s.objects.Has(o.Key)
			if err != nil {
				return err
			}
			if has {
				stats.ObjectsDeduped++
				return nil
			}
			stats.ObjectsStored++
			stats.BytesStored += uint64(len(o.Bytes))
			if !yield(packstore.Object{Key: o.Key, Data: o.Bytes}, nil) {
				return errBatchStopped
			}
			return nil
		})
	})
	if batchErr != nil {
		// The batch's error (append/fsync failure) is authoritative; the
		// builder only saw errBatchStopped.
		return key.Key{}, IngestStats{}, batchErr
	}
	if buildErr != nil {
		return key.Key{}, IngestStats{}, buildErr
	}
	return root, stats, nil
}

// IngestFile stores data as an amber file and returns its key.
func (s *Store) IngestFile(ctx context.Context, data []byte) (key.Key, error) {
	k, _, err := s.ingest(ctx, func(emit fstree.Emit) (key.Key, error) {
		return BuildFile(data, emit)
	})
	return k, err
}

// IngestDirStats is IngestDir returning the ingest's write stats
// (CAS-size observability).
func (s *Store) IngestDirStats(ctx context.Context, dir string) (key.Key, IngestStats, error) {
	return s.ingest(ctx, func(emit fstree.Emit) (key.Key, error) {
		return BuildDir(dir, emit)
	})
}

// IngestDir stores the directory tree at dir and returns its root key.
func (s *Store) IngestDir(ctx context.Context, dir string) (key.Key, error) {
	k, _, err := s.IngestDirStats(ctx, dir)
	return k, err
}

// IngestSourceDirStats is IngestSourceDir returning the ingest's write stats.
func (s *Store) IngestSourceDirStats(ctx context.Context, dir string) (key.Key, IngestStats, error) {
	return s.ingest(ctx, func(emit fstree.Emit) (key.Key, error) {
		return BuildSourceDir(dir, emit)
	})
}

// IngestSourceDir stores a source directory tree, honoring .amberignore
// (see BuildSourceDir): files matched by a pattern and the .amberignore control
// files themselves are excluded. For *source* trees only — build outputs are
// ingested verbatim via IngestDir.
func (s *Store) IngestSourceDir(ctx context.Context, dir string) (key.Key, error) {
	k, _, err := s.IngestSourceDirStats(ctx, dir)
	return k, err
}
