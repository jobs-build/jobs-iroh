package amber

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"golang.org/x/sys/unix"
)

// overlayNode is one level of the generated-files trie: either a file (leaf,
// content set) or a directory of children.
type overlayNode struct {
	content  []byte // non-nil => file leaf
	isFile   bool
	children map[string]*overlayNode
}

// OverlayTree grafts generated files onto the tree at root (sibling-sources
// design §7): each files entry (root-relative slash path → content) replaces
// or adds a regular file at that path, creating intermediate directories as
// needed. Synthetic entries are normalized like the covered tree: mode 0444
// files / 0555 dirs, uid/gid 0, mtime CoverEpochNano — deterministic, so the
// overlaid tree is a pure function of (root, files). A generated path that
// collides with an existing DIRECTORY is an error (silently shadowing a
// covered subtree would hide real inputs from KP).
func (s *Store) OverlayTree(ctx context.Context, root key.Key, files map[string][]byte) (key.Key, error) {
	if len(files) == 0 {
		return root, nil
	}
	trie := &overlayNode{children: map[string]*overlayNode{}}
	for p, content := range files {
		p = strings.Trim(path.Clean("/"+p), "/")
		if p == "" {
			return key.Key{}, fmt.Errorf("overlay: generated path resolves to the root")
		}
		n := trie
		segs := strings.Split(p, "/")
		for i, seg := range segs {
			if n.isFile {
				return key.Key{}, fmt.Errorf("overlay: generated path %q descends into generated file", p)
			}
			if n.children == nil {
				n.children = map[string]*overlayNode{}
			}
			c := n.children[seg]
			if c == nil {
				c = &overlayNode{}
				n.children[seg] = c
			}
			if i == len(segs)-1 {
				if c.children != nil || c.isFile {
					return key.Key{}, fmt.Errorf("overlay: generated path %q conflicts with another generated entry", p)
				}
				c.isFile, c.content = true, content
			}
			n = c
		}
	}
	k, _, err := s.ingest(ctx, func(emit fstree.Emit) (key.Key, error) {
		return s.overlayDir(ctx, emit, root, trie, ".")
	})
	return k, err
}

func (s *Store) overlayDir(ctx context.Context, emit fstree.Emit, dirKey key.Key, node *overlayNode, at string) (key.Key, error) {
	ents, err := fstree.CollectEntries(dirKey, s.getFunc(ctx))
	if err != nil {
		return key.Key{}, fmt.Errorf("overlay: reading %s: %w", at, err)
	}
	byName := make(map[string]fstree.Entry, len(ents))
	names := make([]string, 0, len(ents)+len(node.children))
	for _, e := range ents {
		byName[string(e.Name)] = e
		names = append(names, string(e.Name))
	}
	for cname := range node.children {
		if _, exists := byName[cname]; !exists {
			names = append(names, cname)
		}
	}
	sort.Strings(names) // bytewise order for DirBuilder

	db := fstree.NewDirBuilder(itemChunker())
	for _, name := range names {
		e, exists := byName[name]
		child := node.children[name]
		switch {
		case child == nil:
			// untouched existing entry, verbatim
		case child.isFile:
			if exists && e.Mode&unix.S_IFMT == unix.S_IFDIR {
				return key.Key{}, fmt.Errorf("overlay: generated file %s replaces a covered directory", path.Join(at, name))
			}
			fk, err := BuildFile(child.content, emit)
			if err != nil {
				return key.Key{}, err
			}
			e = fstree.Entry{Name: []byte(name), Mode: unix.S_IFREG | 0o444, Mtime: CoverEpochNano, ContentKey: fk[:]}
		default: // descend
			if exists {
				if e.Mode&unix.S_IFMT != unix.S_IFDIR {
					return key.Key{}, fmt.Errorf("overlay: generated path descends into non-directory %s", path.Join(at, name))
				}
				ck, err := key.Parse(e.ContentKey)
				if err != nil {
					return key.Key{}, fmt.Errorf("overlay: %s/%s: content key: %w", at, name, err)
				}
				nk, err := s.overlayDir(ctx, emit, ck, child, path.Join(at, name))
				if err != nil {
					return key.Key{}, err
				}
				e.ContentKey = nk[:]
			} else {
				nk, err := s.overlayNewDir(emit, child)
				if err != nil {
					return key.Key{}, err
				}
				e = fstree.Entry{Name: []byte(name), Mode: unix.S_IFDIR | 0o555, Mtime: CoverEpochNano, ContentKey: nk[:]}
			}
		}
		if err := db.AddEntry(emit, e); err != nil {
			return key.Key{}, err
		}
	}
	return db.Finish(emit)
}

// overlayNewDir emits a directory that exists only in the overlay.
func (s *Store) overlayNewDir(emit fstree.Emit, node *overlayNode) (key.Key, error) {
	names := make([]string, 0, len(node.children))
	for n := range node.children {
		names = append(names, n)
	}
	sort.Strings(names)
	db := fstree.NewDirBuilder(itemChunker())
	for _, name := range names {
		child := node.children[name]
		var e fstree.Entry
		if child.isFile {
			fk, err := BuildFile(child.content, emit)
			if err != nil {
				return key.Key{}, err
			}
			e = fstree.Entry{Name: []byte(name), Mode: unix.S_IFREG | 0o444, Mtime: CoverEpochNano, ContentKey: fk[:]}
		} else {
			nk, err := s.overlayNewDir(emit, child)
			if err != nil {
				return key.Key{}, err
			}
			e = fstree.Entry{Name: []byte(name), Mode: unix.S_IFDIR | 0o555, Mtime: CoverEpochNano, ContentKey: nk[:]}
		}
		if err := db.AddEntry(emit, e); err != nil {
			return key.Key{}, err
		}
	}
	return db.Finish(emit)
}
