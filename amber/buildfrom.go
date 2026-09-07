package amber

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"golang.org/x/sys/unix"
)

// BuildFromTree assembles the build-from environment tree (build-from design
// §4; widened by the sibling-sources design §3.2): an env/ directory
// referencing the source subtree by key, plus params and platform files, a
// dir file (only when dir != "" — a CtxWidened def carries its build dir in
// F; legacy narrow defs pass "" and keep their pre-field F byte-identical),
// and (only when override != nil) a top-level BUILD.jobs override. Only the
// new directory + small file objects are emitted; env/ is shared by key,
// never re-ingested. Deterministic: the same inputs yield a byte-identical F.
func (s *Store) BuildFromTree(ctx context.Context, env key.Key, dir string, params []byte, platform string, override []byte) (key.Key, error) {
	type ent struct {
		name string
		data []byte // nil => directory referenced by subKey
		sub  key.Key
		dir  bool
	}
	entries := []ent{
		{name: "env", sub: env, dir: true},
		{name: "params", data: params},
		{name: "platform", data: []byte(platform)},
	}
	if dir != "" {
		entries = append(entries, ent{name: "dir", data: []byte(dir)})
	}
	if override != nil {
		entries = append(entries, ent{name: "BUILD.jobs", data: override})
	}
	// DirBuilder requires entries in bytewise name order.
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })

	k, _, err := s.ingest(ctx, func(emit fstree.Emit) (key.Key, error) {
		db := fstree.NewDirBuilder(itemChunker())
		for _, e := range entries {
			fe := fstree.Entry{Name: []byte(e.name)}
			if e.dir {
				fe.Mode = uint64(unix.S_IFDIR | 0o555)
				fe.ContentKey = e.sub[:]
			} else {
				fk, err := BuildFile(e.data, emit)
				if err != nil {
					return key.Key{}, err
				}
				fe.Mode = uint64(unix.S_IFREG | 0o444)
				fe.ContentKey = fk[:]
			}
			if err := db.AddEntry(emit, fe); err != nil {
				return key.Key{}, err
			}
		}
		return db.Finish(emit)
	})
	return k, err
}

// ResolveBuildOutput resolves a build's identity K to its build-output tree key,
// following the build-from:K → F bridge (build-from design §2.1). ok=false if
// either ref is absent. Use everywhere a BUILD's output is consumed (source,
// plugin, input).
func (s *Store) ResolveBuildOutput(ctx context.Context, k key.Key) (key.Key, bool, error) {
	f, ok, err := s.GetKey(ctx, "build-from:"+k.String())
	if err != nil || !ok {
		return key.Key{}, false, err
	}
	return s.GetKey(ctx, "build-output:"+f.String())
}

// ResolveBuildOutputDeps resolves a build K to its materialized runtime-closure
// tree via the same build-from:K → F bridge.
func (s *Store) ResolveBuildOutputDeps(ctx context.Context, k key.Key) (key.Key, bool, error) {
	f, ok, err := s.GetKey(ctx, "build-from:"+k.String())
	if err != nil || !ok {
		return key.Key{}, false, err
	}
	return s.GetKey(ctx, "build-output-deps:"+f.String())
}

// ErrNoArtifact marks a build output that resolved but has no c/ subtree — a
// structurally malformed artifact, permanent for that output (vs. a missing
// ref, which is a sync race and comes back as ok=false, nil).
var ErrNoArtifact = errors.New("build output has no c/ artifact")

// ResolveBuildArtifact resolves the c/ output artifact of the build identified
// by definition key k — the fetcher-artifact resolution (a tree with fetch at
// ITS root, not the whole {c/} output tree): build-from:K → F → build-output:F,
// with a build-output:K fallback for local builds without the bridge, then the
// c/ subtree (recipe-declared-fetchers design §6). Single-store: all refs are
// read locally — a consumer needing remote content must sync it in before
// resolving (jobs' RemoteSync plumbing has no counterpart here).
func (s *Store) ResolveBuildArtifact(ctx context.Context, k key.Key) (key.Key, bool, error) {
	out, ok, err := s.ResolveBuildOutput(ctx, k)
	if err != nil {
		return key.Key{}, false, fmt.Errorf("resolve build output for %s: %w", k, err)
	}
	if !ok {
		out, ok, err = s.GetKey(ctx, "build-output:"+k.String())
		if err != nil {
			return key.Key{}, false, fmt.Errorf("resolve build-output:%s: %w", k, err)
		}
	}
	if !ok {
		return key.Key{}, false, nil
	}
	ak, found, err := s.TreeSubdir(ctx, out, "c")
	if err != nil {
		return key.Key{}, false, err
	}
	if !found {
		// The output exists but is not artifact-shaped — a malformed build, not
		// a sync race. Distinguishable via errors.Is so callers fail hard
		// instead of retrying forever.
		return key.Key{}, false, fmt.Errorf("%w (build %s output %s)", ErrNoArtifact, k, out)
	}
	return ak, true, nil
}

// TreeSubdir returns the content key of the named immediate entry (a
// subdirectory or a file) of the tree at root. found=false when root has no
// such entry.
func (s *Store) TreeSubdir(ctx context.Context, root key.Key, name string) (key.Key, bool, error) {
	ent, err := fstree.LookupEntry(root, []byte(name), s.getFunc(ctx))
	if errors.Is(err, fstree.ErrNotFound) {
		return key.Key{}, false, nil
	}
	if err != nil {
		return key.Key{}, false, err
	}
	k, err := key.Parse(ent.ContentKey)
	if err != nil {
		return key.Key{}, false, fmt.Errorf("invalid content key for %q: %w", name, err)
	}
	return k, true, nil
}
