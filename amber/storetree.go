package amber

import (
	"context"
	"sort"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"golang.org/x/sys/unix"
)

// BuildStoreTree assembles the content-addressed /jobs/store union directory:
// one entry per distinct key in boks, named by the key's hex string and
// pointing at that already-stored subtree. Only the new directory objects are
// emitted; the referenced subtrees are shared by key, never re-ingested. The
// result is deterministic (entries sorted, fixed mode/uid/gid/mtime) so the same
// set yields a byte-identical tree — used for both build-output-deps:K and the
// build-time store mount.
func (s *Store) BuildStoreTree(ctx context.Context, boks []key.Key) (key.Key, error) {
	// Dedup by hex name; identical content (same key) collapses to one entry.
	uniq := make(map[string]key.Key, len(boks))
	for _, k := range boks {
		uniq[k.String()] = k
	}
	names := make([]string, 0, len(uniq))
	for n := range uniq {
		names = append(names, n)
	}
	sort.Strings(names) // bytewise order required by fstree.DirBuilder

	k, _, err := s.ingest(ctx, func(emit fstree.Emit) (key.Key, error) {
		db := fstree.NewDirBuilder(itemChunker())
		for _, n := range names {
			k := uniq[n]
			// An artifact root is a directory; a file-shaped artifact (rare) is a
			// regular file. Derive the entry type from the key's CAS kind.
			mode := uint64(unix.S_IFDIR | 0o555)
			if t := k.Type(); t == key.FileNode || t == key.Blob {
				mode = unix.S_IFREG | 0o444
			}
			if err := db.AddEntry(emit, fstree.Entry{
				Name:       []byte(n),
				Mode:       mode,
				ContentKey: k[:],
			}); err != nil {
				return key.Key{}, err
			}
		}
		return db.Finish(emit)
	})
	return k, err
}
