package amber

import (
	"context"
	"sort"
	"strconv"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"golang.org/x/sys/unix"
)

// KPVersion is the closure/prune algorithm version stamped into every KP tree
// (the v entry) and carried in the pin-cover ref name (pin-cover/<v>:F).
// Derived pin-cover bindings persist forever — pin never re-runs once
// build-pinned:F exists — so only a version bump can supersede bindings
// produced by a buggy or semantically-changed walker. Bump on ANY semantic
// change to the closure walk (runner/cover) or PruneTree normalization —
// i.e. any change to what an EXISTING Pinned/declaration derives; a branch
// that only consumes a new Pinned field leaves old bindings valid and needs
// no bump (source-closure design §7.1 — the runner ALPN fences the pin-time
// skew instead). Starts at 2, matching builddef.CtxWidened (sibling-sources
// design §3.3).
const KPVersion = 2

// BuildKPTree assembles the KP identity tree (sibling-sources design §3.3):
//
//	{ job.cbor  — the canonical Pinned bytes, verbatim
//	  platform  — the placement platform string
//	  v         — KPVersion as ASCII
//	  src/      — the covered source tree (PruneTree output), by key }
//
// Its root key IS KP: the buildrun node key, the doneness/memo ref suffix,
// and the join point across arbitrary out-of-cover context changes. platform
// is load-bearing — Pinned has no platform field (the shell resolves at pull
// time by node platform), so without this entry a platform-blind recipe would
// collide amd64/arm64 onto one KP and memo hits would serve wrong binaries.
// Deterministic: same inputs, byte-identical KP.
func (s *Store) BuildKPTree(ctx context.Context, pinned []byte, platform string, src key.Key) (key.Key, error) {
	type ent struct {
		name string
		data []byte
		sub  key.Key
		dir  bool
	}
	entries := []ent{
		{name: "job.cbor", data: pinned},
		{name: "platform", data: []byte(platform)},
		{name: "v", data: []byte(strconv.Itoa(KPVersion))},
		{name: "src", sub: src, dir: true},
	}
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
