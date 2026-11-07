package tui

// Build-graph folding for the build view (docs/design/2026-08-19-remote-
// build-tui.md §2): the snapshot's raw scheduler nodes fold into logical
// rows — one per buildvalue/testvalue/import — with the value's stage chain
// (buildfrom/pluginresolve/pin/buildrun, or …/testrun for a test) collapsed
// into the row's state. A testvalue's inputs are its own deps (tests design
// §5), so they fold as direct children. Pure functions over api.Snapshot,
// so every rule is table-testable.
//
// FoldSnapshot/FlattenTree and their row types are EXPORTED API: external
// jobs-iroh frontends (jobs-build/assimilate's deploy TUI) fold snapshots
// with the same rules instead of duplicating them — keep the folding
// semantics backward-compatible or version the change loudly.

import (
	"sort"
	"strings"

	"github.com/jobs-build/jobs-iroh/api"
	"github.com/jobs-build/jobs-iroh/wire"
)

// stageNames are the display names of the chain stages (+ import).
var stageNames = map[string]string{
	wire.KindBuildFrom:     "eval",
	wire.KindPluginResolve: "resolve",
	wire.KindPin:           "pin",
	wire.KindBuildRun:      "build",
	wire.KindTestRun:       "test",
	wire.KindImport:        "fetch",
}

// chainOrder is the value stage order — folds pick the FIRST match in this
// order so a verdict is deterministic when several stages qualify. A chain
// holds a buildrun or a testrun, never both.
var chainOrder = []string{wire.KindBuildFrom, wire.KindPluginResolve, wire.KindPin, wire.KindBuildRun, wire.KindTestRun}

// BuildRow is one logical row: a buildvalue or testvalue (with its chain
// folded in) or an import.
type BuildRow struct {
	Node      string // the row's own node name
	Kind      string // wire.KindBuildValue | wire.KindTestValue | wire.KindImport
	Label     string
	Phase     string // folded wire phase
	Stage     string // active/failed stage name (eval|resolve|pin|build|fetch)
	ElapsedMs int64  // running: the active stage's; done: the buildrun's
	Cached    bool
	Err       string
	Runner    string
	LogNode   string   // node whose output the pane shows ("" = none)
	Children  []string // child row node names, display order (label, name)
}

// BuildGraph is the folded snapshot: logical rows plus the display roots.
type BuildGraph struct {
	Rows  map[string]*BuildRow // by logical node name
	Roots []string             // in-degree-0 rows, display order
}

// SnapshotHasGraph reports whether the snapshot carries dependency edges. A
// single-node closure folds fine without any (the fast-pathed target); a
// multi-node closure without edges is an old server → callers fall back to
// the flat view.
func SnapshotHasGraph(snap api.Snapshot) bool {
	if len(snap.Nodes) <= 1 {
		return true
	}
	for _, n := range snap.Nodes {
		if len(n.Deps) > 0 {
			return true
		}
	}
	return false
}

// FoldSnapshot folds the snapshot's raw nodes into the logical row graph.
func FoldSnapshot(snap api.Snapshot) *BuildGraph {
	byName := make(map[string]api.NodeSnap, len(snap.Nodes))
	for _, n := range snap.Nodes {
		byName[n.Node] = n
	}
	kindOf := func(name string) string {
		kind, _, err := wire.ParseNodeName(name)
		if err != nil {
			return ""
		}
		return kind
	}
	logical := func(kind string) bool {
		return kind == wire.KindBuildValue || kind == wire.KindTestValue || kind == wire.KindImport
	}

	g := &BuildGraph{Rows: map[string]*BuildRow{}}
	for _, n := range snap.Nodes {
		kind := kindOf(n.Node)
		if !logical(kind) {
			continue
		}
		row := &BuildRow{Node: n.Node, Kind: kind, Label: n.Label}

		// Split deps into the stage chain and direct logical children
		// (imports dep straight on their fetcher buildvalue; buildvalues on
		// their chain — direct logical deps are kept defensively).
		chain := map[string]api.NodeSnap{} // by kind
		var kids []string
		for _, d := range n.Deps {
			dk := kindOf(d)
			switch {
			case logical(dk):
				kids = append(kids, d)
			case dk != "":
				if dn, ok := byName[d]; ok {
					chain[dk] = dn
				}
			}
		}
		// Chain nodes' logical deps are the row's children.
		for _, ck := range chainOrder {
			cn, ok := chain[ck]
			if !ok {
				continue
			}
			for _, d := range cn.Deps {
				if logical(kindOf(d)) {
					kids = append(kids, d)
				}
			}
		}
		row.Children = orderChildren(dedup(kids), byName)

		if kind == wire.KindImport {
			row.Phase, row.Stage = n.Phase, stageNames[wire.KindImport]
			row.ElapsedMs, row.Cached = n.ElapsedMs, n.Cached
			row.Err, row.Runner = n.ErrSummary, n.Runner
			row.LogNode = n.Node
			g.Rows[n.Node] = row
			continue
		}
		foldChain(row, n, chain)
		g.Rows[n.Node] = row
	}

	g.Roots = rootsOf(g)
	return g
}

// foldChain derives a buildvalue row's verdict from its stage chain
// (design §2 precedence: chain failure → own verdict → running → queued →
// done → waiting).
func foldChain(row *BuildRow, own api.NodeSnap, chain map[string]api.NodeSnap) {
	// The run stage: a buildvalue's buildrun, a testvalue's testrun.
	br, hasBR := chain[wire.KindBuildRun]
	if !hasBR {
		br, hasBR = chain[wire.KindTestRun]
	}

	// The output pane's target, best available: failed stage → active stage
	// → the run stage (stored logs of the run). Falls out of the fold below
	// for the verdict rows; done/waiting rows default to the run stage.
	if hasBR {
		row.LogNode = br.Node
	}

	for _, ck := range chainOrder {
		if cn, ok := chain[ck]; ok && cn.Phase == wire.PhaseFailed {
			row.Phase, row.Stage = wire.PhaseFailed, stageNames[ck]
			row.Err, row.Runner, row.ElapsedMs = cn.ErrSummary, cn.Runner, cn.ElapsedMs
			row.LogNode = cn.Node
			return
		}
	}
	switch own.Phase {
	case wire.PhaseFailed, wire.PhaseCancelled, wire.PhaseUpstream:
		row.Phase, row.Err = own.Phase, own.ErrSummary
		return
	}
	for _, ck := range chainOrder {
		if cn, ok := chain[ck]; ok && (cn.Phase == wire.PhaseRunning || cn.Phase == wire.PhasePublishing) {
			row.Phase, row.Stage = cn.Phase, stageNames[ck]
			row.Runner, row.ElapsedMs = cn.Runner, cn.ElapsedMs
			row.LogNode = cn.Node
			return
		}
	}
	for _, ck := range chainOrder {
		if cn, ok := chain[ck]; ok && cn.Phase == wire.PhaseQueued {
			row.Phase, row.Stage = wire.PhaseQueued, stageNames[ck]
			return
		}
	}
	if own.Phase == wire.PhaseDone {
		row.Phase = wire.PhaseDone
		row.Cached = own.Cached || (hasBR && br.Cached)
		if hasBR {
			row.ElapsedMs = br.ElapsedMs
		}
		return
	}
	row.Phase = wire.PhaseWaiting
}

// rootsOf returns the in-degree-0 logical rows (the target; a forest only
// on malformed snapshots), ordered like children. A cyclic malformed graph
// with no in-degree-0 row falls back to the name-sorted first row so the
// tree always renders something.
func rootsOf(g *BuildGraph) []string {
	child := map[string]bool{}
	for _, r := range g.Rows {
		for _, c := range r.Children {
			child[c] = true
		}
	}
	var roots []string
	for name := range g.Rows {
		if !child[name] {
			roots = append(roots, name)
		}
	}
	if len(roots) == 0 && len(g.Rows) > 0 {
		for name := range g.Rows {
			roots = append(roots, name)
		}
		sort.Strings(roots)
		roots = roots[:1]
	}
	byName := map[string]api.NodeSnap{}
	for name, r := range g.Rows {
		byName[name] = api.NodeSnap{Node: name, Label: r.Label}
	}
	return orderChildren(roots, byName)
}

// orderChildren sorts row names for display: by label, then name.
func orderChildren(names []string, byName map[string]api.NodeSnap) []string {
	sort.Slice(names, func(i, j int) bool {
		li, lj := byName[names[i]].Label, byName[names[j]].Label
		if li != lj {
			return li < lj
		}
		return names[i] < names[j]
	})
	return names
}

func dedup(names []string) []string {
	seen := map[string]bool{}
	out := names[:0]
	for _, n := range names {
		if !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	return out
}

// TreeRow is one visible line of the flattened tree.
type TreeRow struct {
	Path     string // "/"-joined node names root→row (expansion identity)
	Node     string
	Depth    int
	HasKids  bool
	Expanded bool
}

// maxTreeDepth bounds flatten recursion (cycle/malformed-snapshot guard on
// top of the on-path check).
const maxTreeDepth = 64

// FlattenTree renders the DAG as a tree: shared subtrees repeat under every
// parent; expansion is per path. exp overrides the default (expanded,
// except cached-done rows); missing paths use the default.
func FlattenTree(g *BuildGraph, exp map[string]bool) []TreeRow {
	var out []TreeRow
	var walk func(name, parentPath string, depth int)
	walk = func(name, parentPath string, depth int) {
		row, ok := g.Rows[name]
		if !ok || depth > maxTreeDepth {
			return
		}
		// On-path check: a malformed cycle repeats a name in its own
		// ancestry — skip rather than recurse forever.
		if strings.Contains(parentPath+"/", "/"+name+"/") {
			return
		}
		path := parentPath + "/" + name
		expanded := len(row.Children) > 0
		if expanded {
			if v, ok := exp[path]; ok {
				expanded = v
			} else {
				expanded = !(row.Phase == wire.PhaseDone && row.Cached)
			}
		}
		out = append(out, TreeRow{Path: path, Node: name, Depth: depth, HasKids: len(row.Children) > 0, Expanded: expanded})
		if expanded {
			for _, c := range row.Children {
				walk(c, path, depth+1)
			}
		}
	}
	for _, r := range g.Roots {
		walk(r, "", 0)
	}
	return out
}

// RequestKind names what a snapshot's request is for display: "test" when
// its closure holds a testvalue (a test request's target; a build never
// depends on one), else "build". Old servers and empty snapshots read as
// builds.
func RequestKind(snap api.Snapshot) string {
	for _, n := range snap.Nodes {
		if strings.HasPrefix(n.Node, wire.KindTestValue+"_") {
			return "test"
		}
	}
	return "build"
}
