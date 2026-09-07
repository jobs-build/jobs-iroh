package amber

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"golang.org/x/sys/unix"
)

// CoverEpochNano is the normalized mtime stamped on every entry of a covered
// (pruned) tree: 1980-01-01T00:00:00Z, the ZIP epoch — the reproducible-build
// convention, chosen so downstream archive formats that cannot represent
// earlier dates never underflow (sibling-sources design §2, §6.1).
const CoverEpochNano int64 = 315532800 * 1_000_000_000

// pruneNode is one level of the keep-set trie. full marks a path that is
// covered in its entirety (everything beneath it is kept); children narrow
// the coverage below a partially-covered directory.
type pruneNode struct {
	full     bool
	children map[string]*pruneNode
}

func (n *pruneNode) add(segs []string) {
	if n.full {
		return // already fully covered by a shorter path
	}
	if len(segs) == 0 {
		n.full, n.children = true, nil
		return
	}
	if n.children == nil {
		n.children = make(map[string]*pruneNode)
	}
	c := n.children[segs[0]]
	if c == nil {
		c = &pruneNode{}
		n.children[segs[0]] = c
	}
	c.add(segs[1:])
}

// PruneTree builds the covered tree of the sibling-sources design (§6.1): the
// subtree of root containing exactly the root-relative paths in keep (files,
// symlinks, or whole directories), with normalized metadata — uid/gid zeroed,
// mtime = CoverEpochNano, modes preserved, xattrs carried verbatim.
//
// EVERY directory object in the covered closure is re-emitted: entry metadata
// (mode/uid/gid/mtime) lives in the PARENT directory object, so normalization
// cannot share even a fully-covered subtree by its original key. File content
// objects (blobs/file nodes) are shared by key untouched — the cost is
// O(directories in the closure), and the result is a pure function of
// (root, keep), immune to mtime churn by construction.
//
// Every keep path must exist in root: the pin stage validates declared
// sources loudly, and chased symlink targets are filtered to existing paths
// by the closure walker before pruning — a miss here is therefore a bug or a
// store race, reported as an error, never skipped. keep = ["" or "."] (or an
// empty keep) is rejected: covering the whole tree defeats the cutoff and is
// always a caller mistake.
func (s *Store) PruneTree(ctx context.Context, root key.Key, keep []string) (key.Key, error) {
	trie := &pruneNode{}
	for _, p := range keep {
		p = strings.Trim(path.Clean("/"+p), "/") // normalize; collapse any stray . or ..
		if p == "" {
			return key.Key{}, fmt.Errorf("prune: covered path %q resolves to the context root", p)
		}
		trie.add(strings.Split(p, "/"))
	}
	if len(trie.children) == 0 {
		return key.Key{}, fmt.Errorf("prune: empty keep set")
	}
	k, _, err := s.ingest(ctx, func(emit fstree.Emit) (key.Key, error) {
		return s.pruneDir(ctx, emit, root, trie, ".")
	})
	return k, err
}

// NormalizeTree re-emits the WHOLE tree at root with normalized metadata
// (uid/gid zeroed, mtime = CoverEpochNano, modes preserved) — the covered
// tree of a root build, whose cover is the entire env (sibling-sources
// design §6.2: root builds get the same mtime-immune buildrun memo). File
// content objects are shared by key; every directory object re-emits.
func (s *Store) NormalizeTree(ctx context.Context, root key.Key) (key.Key, error) {
	k, _, err := s.ingest(ctx, func(emit fstree.Emit) (key.Key, error) {
		return s.pruneDir(ctx, emit, root, &pruneNode{full: true}, ".")
	})
	return k, err
}

func (s *Store) pruneDir(ctx context.Context, emit fstree.Emit, dirKey key.Key, node *pruneNode, at string) (key.Key, error) {
	ents, err := fstree.CollectEntries(dirKey, s.getFunc(ctx))
	if err != nil {
		return key.Key{}, fmt.Errorf("prune: reading %s: %w", at, err)
	}
	db := fstree.NewDirBuilder(itemChunker())
	matched := make(map[string]bool, len(node.children))
	for _, e := range ents {
		name := string(e.Name)
		var child *pruneNode
		switch {
		case node.full:
			child = node // everything below is covered
		case node.children[name] != nil:
			child = node.children[name]
			matched[name] = true
		default:
			continue // not covered
		}

		fe := e // copy the full entry: modes and xattrs ride along verbatim
		fe.UID, fe.GID, fe.Mtime = 0, 0, CoverEpochNano

		if fe.Mode&unix.S_IFMT == unix.S_IFDIR {
			ck, err := key.Parse(e.ContentKey)
			if err != nil {
				return key.Key{}, fmt.Errorf("prune: %s/%s: content key: %w", at, name, err)
			}
			nk, err := s.pruneDir(ctx, emit, ck, child, path.Join(at, name))
			if err != nil {
				return key.Key{}, err
			}
			fe.ContentKey = nk[:]
		} else if !child.full {
			// A keep path descends INTO a non-directory (e.g. "a/b" where a/b's
			// prefix "a" is a file). The closure walker resolves symlink
			// components before pruning, so this is a caller error.
			return key.Key{}, fmt.Errorf("prune: covered path descends into non-directory %s/%s", at, name)
		}
		if err := db.AddEntry(emit, fe); err != nil {
			return key.Key{}, err
		}
	}
	for cname := range node.children {
		if !matched[cname] {
			return key.Key{}, fmt.Errorf("prune: covered path %s not found in tree", path.Join(at, cname))
		}
	}
	return db.Finish(emit)
}
