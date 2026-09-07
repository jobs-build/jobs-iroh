// Package cover computes the covered source closure and the KP identity of a
// widened-context build (sibling-sources design §5, §6). It is
// IDENTITY-CRITICAL SHARED CODE: the pin runner expands the closure once
// (Walk → Pinned.Sources, or WalkClosure → Pinned.Closure for complete
// covers, source-closure design §5); the server and the local pipeline both
// derive KP from the pinned blob via Derive — the same implementation on
// every path, pinned like the chunker params. amber.KPVersion versions the
// semantics; bump it on ANY behavioral change to how a given Pinned derives
// (a new branch that only fires on a new Pinned field does not re-derive old
// pins and needs no bump — source-closure design §7.1; the runner ALPN
// fences version skew at pin time).
package cover

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/builddef"
	"golang.org/x/sys/unix"
)

// linkBudget bounds symlink traversals per resolved path (the Linux ELOOP
// convention, design §5.4).
const linkBudget = 40

// KPTreeRef names the closure carrier for one KP tree: "kp-tree/<KP>" → KP
// (the f-tree/ pattern one stage later — sibling-sources design §4).
func KPTreeRef(kp key.Key) string { return "kp-tree/" + kp.String() }

// PinCoverRef names the F → KP bridge, versioned by the closure-algorithm
// version: "pin-cover/<v>:<F>" → KP. Derived bindings persist forever (pin
// never re-runs once build-pinned:F exists), so a semantic change to the
// walk or prune bumps amber.KPVersion and supersedes stale bindings.
func PinCoverRef(f key.Key) string {
	return fmt.Sprintf("pin-cover/%d:%s", amber.KPVersion, f.String())
}

// Warning is a non-fatal closure-walk finding surfaced to the build log.
type Warning struct{ Path, Msg string }

// WalkResult is the expanded closure: the root-relative covered paths
// (declared seeds + every chased symlink and target), sorted and deduped —
// the exact value of Pinned.Sources — plus the walk warnings.
type WalkResult struct {
	Paths    []string
	Warnings []Warning
}

// Walk expands the covered closure of a widened build (design §5): seeds are
// the build dir (always) plus the declared source paths (root-relative,
// pre-normalized by the recipe layer). Every declared path must exist —
// missing declared paths are errors. The walk then chases symlinks
// component-wise in-store: every intermediate and final symlink joins the
// closure together with its target; in-root targets that do not exist are
// kept-and-warned (today's dangling-link semantics; .amberignore legitimately
// manufactures them); targets escaping the context root are errors unless
// the link's path is listed in allowEscaping (the link is then kept verbatim
// and dangles in the sandbox).
func Walk(ctx context.Context, st *amber.Store, contextRoot key.Key, dir string, declared, allowEscaping []string) (WalkResult, error) {
	w := &walker{
		ctx: ctx, st: st, root: contextRoot,
		covered: map[string]bool{},
		visited: map[string]bool{},
		allow:   map[string]bool{},
	}
	for _, p := range allowEscaping {
		w.allow[p] = true
	}

	seeds := make([]string, 0, len(declared)+1)
	if dir != "" {
		seeds = append(seeds, dir)
	}
	seeds = append(seeds, declared...)
	if len(seeds) == 0 {
		return WalkResult{}, fmt.Errorf("cover: empty seed set (no dir, no sources)")
	}
	for _, s := range seeds {
		if err := w.cover(s, true); err != nil {
			return WalkResult{}, err
		}
	}

	out := make([]string, 0, len(w.covered))
	for p := range w.covered {
		out = append(out, p)
	}
	sort.Strings(out)
	return WalkResult{Paths: out, Warnings: w.warnings}, nil
}

// WalkClosure expands a COMPLETE declared cover (source-closure design §5):
// unlike Walk, the build dir is NOT auto-seeded — the declared paths are the
// whole cover. Same expansion semantics otherwise (missing declared ⇒ error,
// component-wise symlink chase, escape policy via allowEscaping). After
// expansion the closure must cover the build dir (a covered path at, under,
// or above dir) or the pruned tree would lack the sandbox workdir — a
// pin-time error, never a runtime cd failure (§5.3 [INV]). Root builds
// (dir == "") satisfy the check trivially.
func WalkClosure(ctx context.Context, st *amber.Store, contextRoot key.Key, dir string, declared, allowEscaping []string) (WalkResult, error) {
	if len(declared) == 0 {
		return WalkResult{}, fmt.Errorf("cover: empty closure")
	}
	w := &walker{
		ctx: ctx, st: st, root: contextRoot,
		covered: map[string]bool{},
		visited: map[string]bool{},
		allow:   map[string]bool{},
	}
	for _, p := range allowEscaping {
		w.allow[p] = true
	}
	for _, s := range declared {
		if err := w.cover(s, true); err != nil {
			return WalkResult{}, err
		}
	}
	if dir != "" {
		ok := false
		for p := range w.covered {
			if p == dir || strings.HasPrefix(p, dir+"/") || strings.HasPrefix(dir, p+"/") {
				ok = true
				break
			}
		}
		if !ok {
			return WalkResult{}, fmt.Errorf("cover: closure does not cover the build dir %q — the sandbox workdir would not exist", dir)
		}
	}
	out := make([]string, 0, len(w.covered))
	for p := range w.covered {
		out = append(out, p)
	}
	sort.Strings(out)
	return WalkResult{Paths: out, Warnings: w.warnings}, nil
}

type walker struct {
	ctx      context.Context
	st       *amber.Store
	root     key.Key
	covered  map[string]bool // paths that land in Pinned.Sources / Pinned.Closure
	visited  map[string]bool // recursion guard for cover()
	allow    map[string]bool // link paths allowed to escape the root
	warnings []Warning
}

// cover adds one root-relative path to the closure: resolves its components
// (chasing symlinks), records it, and — when it names a directory — scans the
// subtree for further symlinks. declared marks seed paths (missing ⇒ error);
// chased targets degrade to a warning instead.
func (w *walker) cover(p string, declared bool) error {
	if w.visited[p] {
		return nil
	}
	w.visited[p] = true

	resolved, e, err := w.resolve(p)
	if err != nil {
		return err
	}
	if e == nil {
		if declared {
			return fmt.Errorf("cover: declared source %q not found in the context", p)
		}
		w.warnings = append(w.warnings, Warning{Path: p, Msg: "chased symlink target does not exist in the context; link kept dangling"})
		return nil
	}
	w.covered[p] = true
	if resolved != p {
		// The path traversed symlinked components; the fully-resolved location
		// must be covered too or the chain dangles after pruning.
		if err := w.cover(resolved, false); err != nil {
			return err
		}
	}

	switch e.Mode & unix.S_IFMT {
	case unix.S_IFLNK:
		return w.chaseLink(resolved, e.LinkTarget)
	case unix.S_IFDIR:
		return w.scanDir(resolved, e.Key)
	}
	return nil
}

// chaseLink handles one covered symlink at linkPath (root-relative, already
// component-resolved up to the link itself).
func (w *walker) chaseLink(linkPath, target string) error {
	if strings.HasPrefix(target, "/") {
		if w.allow[linkPath] {
			return nil // intentionally dangling
		}
		return fmt.Errorf("cover: symlink %q targets absolute path %q (escapes the context; add it to sources_allow_escaping to keep it dangling)", linkPath, target)
	}
	t := path.Clean(path.Join(path.Dir(linkPath), target))
	if t == ".." || strings.HasPrefix(t, "../") {
		if w.allow[linkPath] {
			return nil
		}
		return fmt.Errorf("cover: symlink %q target %q escapes the context root (add it to sources_allow_escaping to keep it dangling)", linkPath, target)
	}
	return w.cover(t, false)
}

// scanDir walks a covered directory subtree in-store, chasing every symlink
// entry found beneath it.
func (w *walker) scanDir(dirPath string, dirKey key.Key) error {
	ents, err := w.st.Ls(w.ctx, dirKey, "")
	if err != nil {
		return fmt.Errorf("cover: listing %s: %w", dirPath, err)
	}
	for _, e := range ents {
		p := path.Join(dirPath, e.Name)
		switch e.Mode & unix.S_IFMT {
		case unix.S_IFLNK:
			if err := w.chaseLink(p, e.LinkTarget); err != nil {
				return err
			}
		case unix.S_IFDIR:
			if err := w.scanDir(p, e.Key); err != nil {
				return err
			}
		}
	}
	return nil
}

// resolve walks p from the context root component-wise, chasing symlinked
// components with a link budget. Returns the fully-resolved root-relative
// path and its entry, or (path, nil, nil) when a component does not exist.
// Every intermediate symlink encountered is added to the closure — the
// pruned tree must contain the whole chain or runtime resolution dangles.
func (w *walker) resolve(p string) (string, *amber.Entry, error) {
	segs := strings.Split(p, "/")
	cur := "" // resolved prefix
	curKey := w.root
	budget := linkBudget
	for i := 0; i < len(segs); i++ {
		seg := segs[i]
		if seg == "" || seg == "." {
			continue
		}
		ents, err := w.st.Ls(w.ctx, curKey, "")
		if err != nil {
			return "", nil, fmt.Errorf("cover: listing %s: %w", cur, err)
		}
		var e *amber.Entry
		for j := range ents {
			if ents[j].Name == seg {
				e = &ents[j]
				break
			}
		}
		if e == nil {
			return p, nil, nil // component missing
		}
		full := path.Join(cur, seg)
		last := i == len(segs)-1

		if e.Mode&unix.S_IFMT == unix.S_IFLNK {
			if budget--; budget < 0 {
				return "", nil, fmt.Errorf("cover: symlink loop resolving %q (budget %d exceeded)", p, linkBudget)
			}
			// The link itself joins the closure (intermediate or final).
			w.covered[full] = true
			if last {
				return full, e, nil // caller chases the final link
			}
			// Intermediate link: retarget the remaining walk.
			t := e.LinkTarget
			if strings.HasPrefix(t, "/") {
				return "", nil, fmt.Errorf("cover: path %q traverses symlink %q with absolute target %q (escapes the context)", p, full, t)
			}
			nt := path.Clean(path.Join(cur, t))
			if nt == ".." || strings.HasPrefix(nt, "../") {
				return "", nil, fmt.Errorf("cover: path %q traverses symlink %q whose target escapes the context root", p, full)
			}
			rest := append(strings.Split(nt, "/"), segs[i+1:]...)
			segs, i = rest, -1
			cur, curKey = "", w.root
			continue
		}

		if last {
			return full, e, nil
		}
		if e.Mode&unix.S_IFMT != unix.S_IFDIR {
			return "", nil, fmt.Errorf("cover: path %q descends into non-directory %q", p, full)
		}
		cur, curKey = full, e.Key
	}
	// p resolved to the context root itself ("" after cleaning) — reject.
	return "", nil, fmt.Errorf("cover: path %q resolves to the context root", p)
}

// Derive computes KP from a pinned blob — the SINGLE implementation used by
// the pin runner, the server's pin-commit derivation, and the local
// pipeline's memo (design §6.2, §6.3). Pinned.Closure (a COMPLETE cover,
// source-closure design §6) branches first; then Pinned.Sources (widened
// build: prune the context to the pre-expanded closure — no walking here);
// else the covered tree is the whole env normalized (legacy/root builds get
// the same mtime-immune buildrun memo). Generated overlays after the prune,
// and {job.cbor, platform, v, src/} assembles into KP.
func Derive(ctx context.Context, st *amber.Store, pinnedBytes []byte, p builddef.Pinned, platform string, contextRoot key.Key) (key.Key, error) {
	if total := generatedSize(p.Generated); total > builddef.GeneratedMaxBytes {
		return key.Key{}, fmt.Errorf("cover: generated sources total %d bytes exceeds the %d cap", total, builddef.GeneratedMaxBytes)
	}
	if len(p.Closure) > 0 && len(p.Sources) > 0 {
		return key.Key{}, fmt.Errorf("cover: pinned declares both Closure and Sources")
	}
	var covered key.Key
	var err error
	switch {
	case len(p.Closure) > 0:
		// Complete cover (source-closure design §6): same prune, closure list.
		covered, err = st.PruneTree(ctx, contextRoot, p.Closure)
	case len(p.Sources) > 0:
		covered, err = st.PruneTree(ctx, contextRoot, p.Sources)
	default:
		covered, err = st.NormalizeTree(ctx, contextRoot)
	}
	if err != nil {
		return key.Key{}, err
	}
	if len(p.Generated) > 0 {
		covered, err = st.OverlayTree(ctx, covered, p.Generated)
		if err != nil {
			return key.Key{}, err
		}
	}
	return st.BuildKPTree(ctx, pinnedBytes, platform, covered)
}

func generatedSize(g map[string][]byte) int {
	total := 0
	for _, b := range g {
		total += len(b)
	}
	return total
}
