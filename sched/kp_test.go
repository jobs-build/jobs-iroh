package sched

// KP-layer tests (sibling-sources design §6.3, §8, §10): crash-window heals
// of the derived refs and the F-aliases, platform distinctness of the KP
// binding, and sub-build cycle detection. Uses sched_test.go's env harness.

import (
	"strings"
	"testing"

	"github.com/amber-store/core/key"

	"github.com/jobs-build/jobs-iroh/api"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/wire"
)

// runChain submits defBytes, waits for the terminal snapshot and asserts it
// is done, returning (K, F, KP).
func (e *env) runChain(defBytes []byte) (k, f, kp key.Key) {
	e.t.Helper()
	sub, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: defBytes})
	if err != nil {
		e.t.Fatalf("submit: %v", err)
	}
	if snap := e.watchTerminal(sub.RequestID); snap.Phase != "done" {
		e.t.Fatalf("terminal phase = %s, want done (%+v)", snap.Phase, snap)
	}
	k, err = wire.ParseKey(sub.K)
	if err != nil {
		e.t.Fatal(err)
	}
	var ok bool
	if f, ok = e.getRef("build-from:" + k.String()); !ok {
		e.t.Fatal("build-from:K not written")
	}
	if kp, ok = e.getRef(PinCoverRef(f)); !ok {
		e.t.Fatal("pin-cover/<v>:F not written")
	}
	// Forget the request so a resubmit re-creates the nodes: a done node with
	// live interest would satisfy the join without ever re-checking refs.
	if err := e.s.Delete(e.ctx, sub.RequestID); err != nil {
		e.t.Fatalf("delete request: %v", err)
	}
	if stats := e.s.Stats(); stats.NodesTracked != 0 {
		e.t.Fatalf("NodesTracked = %d after delete, want 0", stats.NodesTracked)
	}
	return k, f, kp
}

func (e *env) deleteRefs(names ...string) {
	e.t.Helper()
	for _, name := range names {
		if err := e.st.DeleteRef(e.ctx, name); err != nil {
			e.t.Fatalf("delete ref %s: %v", name, err)
		}
	}
}

func (e *env) mustRef(name string) key.Key {
	e.t.Helper()
	k, ok := e.getRef(name)
	if !ok {
		e.t.Fatalf("ref %s missing", name)
	}
	return k
}

// TestKPDerivedRefHeal simulates the pin-commit crash window (§6.3 [INV]):
// build-pinned:F exists but the derived refs (pin-cover/<v>:F,
// build-pinned:<KP>, kp-tree/<KP>) are gone. The F-aliases are removed too so
// the buildvalue's doneness fast-path (build-output:F) misses and the
// pipeline actually re-walks — with them present a resubmit is legitimately
// free and never needs KP. The resubmit must complete via resolveKPLocked's
// on-demand re-derivation — same KP, all refs re-written, and NO stage re-run
// (build-output:<KP>, the memo, is intact).
func TestKPDerivedRefHeal(t *testing.T) {
	e := newEnv(t)
	fr := e.startRunner([]wire.Class{"c0.2-m1", "c1-m1"}, e.standardHandler(builddef.Pinned{}))

	defBytes := e.treeDef("kp-heal-derived")
	_, f, kp := e.runChain(defBytes)
	pinnedKey := e.mustRef("build-pinned:" + f.String())
	outKP := e.mustRef("build-output:" + kp.String())
	depsKP := e.mustRef("build-output-deps:" + kp.String())

	e.deleteRefs(
		PinCoverRef(f), "build-pinned:"+kp.String(), KPTreeRef(kp),
		"build-output:"+f.String(), "build-output-deps:"+f.String(),
	)

	sub, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: defBytes})
	if err != nil {
		t.Fatalf("resubmit: %v", err)
	}
	if snap := e.watchTerminal(sub.RequestID); snap.Phase != "done" {
		t.Fatalf("healed resubmit phase = %s, want done (%+v)", snap.Phase, snap)
	}

	// The derived refs are re-written, converging on the SAME binding.
	if kp2 := e.mustRef(PinCoverRef(f)); kp2 != kp {
		t.Errorf("healed pin-cover -> %s, want %s (derivation must be pure)", kp2, kp)
	}
	if kt := e.mustRef(KPTreeRef(kp)); kt != kp {
		t.Errorf("healed kp-tree/%s -> %s, want name==value", kp, kt)
	}
	if pk := e.mustRef("build-pinned:" + kp.String()); pk != pinnedKey {
		t.Errorf("healed build-pinned:KP -> %s, want the pinned blob %s", pk, pinnedKey)
	}
	// The F-aliases are re-written from the intact KP pair.
	if av := e.mustRef("build-output:" + f.String()); av != outKP {
		t.Errorf("healed build-output:F -> %s, want %s", av, outKP)
	}
	if av := e.mustRef("build-output-deps:" + f.String()); av != depsKP {
		t.Errorf("healed build-output-deps:F -> %s, want %s", av, depsKP)
	}
	// The heal is pure ref surgery: no stage ran a second time.
	for _, kind := range []string{wire.KindBuildFrom, wire.KindPluginResolve, wire.KindPin, wire.KindBuildRun} {
		if jobs := fr.byKind(kind); len(jobs) != 1 {
			t.Errorf("%s ran %d times, want 1 (heal must not re-run work)", kind, len(jobs))
		}
	}
}

// TestFAliasHeal deletes ONLY the F-aliases after a full success (the §10.2
// rule-4 crash window: server died between the KP refs and the aliases). The
// resubmit completes with the aliases re-written by ensureFAliasesLocked —
// and buildrun never re-runs, because build-output:<KP> is the memo.
func TestFAliasHeal(t *testing.T) {
	e := newEnv(t)
	fr := e.startRunner([]wire.Class{"c0.2-m1", "c1-m1"}, e.standardHandler(builddef.Pinned{}))

	defBytes := e.treeDef("kp-heal-aliases")
	_, f, kp := e.runChain(defBytes)
	outKP := e.mustRef("build-output:" + kp.String())
	depsKP := e.mustRef("build-output-deps:" + kp.String())

	e.deleteRefs("build-output:"+f.String(), "build-output-deps:"+f.String())

	sub, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: defBytes})
	if err != nil {
		t.Fatalf("resubmit: %v", err)
	}
	if snap := e.watchTerminal(sub.RequestID); snap.Phase != "done" {
		t.Fatalf("alias-heal resubmit phase = %s, want done (%+v)", snap.Phase, snap)
	}
	if av := e.mustRef("build-output:" + f.String()); av != outKP {
		t.Errorf("re-written build-output:F -> %s, want the KP output %s", av, outKP)
	}
	if av := e.mustRef("build-output-deps:" + f.String()); av != depsKP {
		t.Errorf("re-written build-output-deps:F -> %s, want the KP deps %s", av, depsKP)
	}
	if jobs := fr.byKind(wire.KindBuildRun); len(jobs) != 1 {
		t.Errorf("buildrun ran %d times, want 1 (the memo serves the alias heal)", len(jobs))
	}
}

// TestKPPlatformDistinct pins the §3.3 [INV]: an identical pure-script build
// (byte-identical pinned blob, identical env tree) on two platforms must bind
// to two DIFFERENT KPs — the platform entry of the KP tree is load-bearing,
// or cross-platform memo hits would serve wrong binaries.
func TestKPPlatformDistinct(t *testing.T) {
	e := newEnv(t)

	// One FIXED env tree for both platforms — the fake buildfrom splices it
	// under the job's own platform, so F differs only by the platform file
	// and the covered tree is identical.
	envT := e.ingestTree("shared env for both platforms")
	handler := func(job wire.Job) *wire.Result {
		fh, err := wire.ParseKey(job.Key)
		if err != nil {
			e.t.Errorf("job %s: bad key: %v", job.Node, err)
			return nil
		}
		f := fh.String()
		switch job.Kind {
		case wire.KindBuildFrom:
			ftree, err := e.st.BuildFromTree(e.ctx, envT, "", []byte("params"), job.Platform, nil)
			if err != nil {
				e.t.Errorf("fake BuildFromTree: %v", err)
				return nil
			}
			return okResult(job,
				ref("build-from:"+f, ftree),
				ref("build-from-tree:"+ftree.String(), ftree))
		case wire.KindPluginResolve:
			body, err := builddef.EncodePluginResolved(builddef.PluginResolved{})
			if err != nil {
				e.t.Error(err)
				return nil
			}
			return okResult(job, ref("build-plugin-resolved:"+f, e.ingestFile(body)))
		case wire.KindPin:
			body, err := builddef.EncodePinned(builddef.Pinned{})
			if err != nil {
				e.t.Error(err)
				return nil
			}
			return okResult(job, ref("build-pinned:"+f, e.ingestFile(body)))
		case wire.KindBuildRun:
			deps := e.ingestFile([]byte("deps " + job.Node))
			out := e.ingestFile([]byte("out " + job.Node))
			return okResult(job,
				ref("build-output-deps:"+f, deps),
				ref("build-output:"+f, out))
		default:
			e.t.Errorf("unexpected job kind %s", job.Kind)
			return nil
		}
	}
	classes := []wire.Class{"c0.2-m1", "c1-m1"}
	frAmd := e.startRunnerOn("linux/amd64", classes, handler)
	frArm := e.startRunnerOn("linux/arm64", classes, handler)

	// Same source tree, two platforms: only the def's platform field differs.
	tree := e.ingestTree("platform-distinct source")
	src, err := builddef.TreeInput(tree)
	if err != nil {
		t.Fatal(err)
	}
	submit := func(platform string) key.Key {
		t.Helper()
		def := builddef.Definition{Source: src, Platform: platform, Params: []byte{0xf6}}
		b, err := def.Canonical()
		if err != nil {
			t.Fatal(err)
		}
		sub, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: b})
		if err != nil {
			t.Fatalf("submit %s: %v", platform, err)
		}
		if snap := e.watchTerminal(sub.RequestID); snap.Phase != "done" {
			t.Fatalf("%s phase = %s, want done (%+v)", platform, snap.Phase, snap)
		}
		k, err := wire.ParseKey(sub.K)
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	kAmd := submit("linux/amd64")
	kArm := submit("linux/arm64")

	fAmd := e.mustRef("build-from:" + kAmd.String())
	fArm := e.mustRef("build-from:" + kArm.String())
	if fAmd == fArm {
		t.Fatal("two platforms produced one F — fixture broken")
	}
	// Precondition of the invariant: the pinned blob is byte-identical.
	if e.mustRef("build-pinned:"+fAmd.String()) != e.mustRef("build-pinned:"+fArm.String()) {
		t.Fatal("pin blobs differ across platforms — fixture broken (they must be identical for the test to bite)")
	}
	kpAmd := e.mustRef(PinCoverRef(fAmd))
	kpArm := e.mustRef(PinCoverRef(fArm))
	if kpAmd == kpArm {
		t.Fatalf("cross-platform KP collision: %s (platform entry not keying the KP tree)", kpAmd)
	}
	// Each platform's buildrun ran once, on its own KP-named node.
	for _, c := range []struct {
		fr *fakeRunner
		kp key.Key
	}{{frAmd, kpAmd}, {frArm, kpArm}} {
		jobs := c.fr.byKind(wire.KindBuildRun)
		if len(jobs) != 1 {
			t.Fatalf("buildrun jobs = %d, want 1 per platform", len(jobs))
		}
		if want := wire.NodeName(wire.KindBuildRun, c.kp); jobs[0].Node != want {
			t.Errorf("buildrun node = %s, want %s", jobs[0].Node, want)
		}
	}
}

// TestSubbuildCycleDetection unit-tests requireInputLocked's cycle check
// (design §8 [INV]) over hand-built nodes: a buildrun whose dependent
// ancestry already contains the buildvalue it is about to require (A
// subbuilds B, B subbuilds A) must hard-fail with the full node chain —
// mutual-wait cycles are not expressible via Submit, so the graph is wired
// directly.
func TestSubbuildCycleDetection(t *testing.T) {
	e := newEnv(t)
	s := e.s

	defA := e.treeDef("cycle-A")
	kA, err := (builddef.Input{Kind: builddef.KindBuild, Definition: defA}).Key()
	if err != nil {
		t.Fatal(err)
	}
	defC := e.treeDef("cycle-unrelated")
	kC, err := (builddef.Input{Kind: builddef.KindBuild, Definition: defC}).Key()
	if err != nil {
		t.Fatal(err)
	}
	kB := e.ingestFile([]byte("K of sibling B"))
	kpA := e.ingestFile([]byte("KP of A's buildrun"))
	kpB := e.ingestFile([]byte("KP of B's buildrun"))

	s.mu.Lock()
	mk := func(kind string, k key.Key) *node {
		n := &node{
			id: nodeID{kind: kind, key: k}, name: wire.NodeName(kind, k),
			phase: wire.PhaseWaiting, deps: map[*node]struct{}{},
			dependents: map[*node]struct{}{}, interest: map[string]struct{}{},
		}
		s.nodes[n.id] = n
		return n
	}
	link := func(parent, dep *node) {
		parent.deps[dep] = struct{}{}
		dep.dependents[parent] = struct{}{}
	}
	// bv_KA → buildrun_KPA → bv_KB → buildrun_KPB: A's buildrun already
	// waits on B's value node.
	bvA := mk(wire.KindBuildValue, kA)
	runA := mk(wire.KindBuildRun, kpA)
	bvB := mk(wire.KindBuildValue, kB)
	runB := mk(wire.KindBuildRun, kpB)
	link(bvA, runA)
	link(runA, bvB)
	link(bvB, runB)

	// B's buildrun now requires build(defA) — bv_KA is in its dependent
	// ancestry, closing the cycle.
	s.requireInputLocked(runB, builddef.Input{Kind: builddef.KindBuild, Definition: defA}, "")
	phase, summary := runB.phase, runB.errSummary
	_, cycleNodeCreated := s.nodes[nodeID{kind: wire.KindBuildValue, key: kA}]
	cycleNodeIsBvA := s.nodes[nodeID{kind: wire.KindBuildValue, key: kA}] == bvA

	// Control: requiring an UNRELATED build input from the intact runA joins
	// normally and fails nothing.
	s.requireInputLocked(runA, builddef.Input{Kind: builddef.KindBuild, Definition: defC}, "")
	_, unrelatedCreated := s.nodes[nodeID{kind: wire.KindBuildValue, key: kC}]
	phaseA := runA.phase
	s.mu.Unlock()

	if phase != wire.PhaseFailed {
		t.Fatalf("runB phase = %s, want failed (cycle undetected — silent mutual wait)", phase)
	}
	if !strings.Contains(summary, "sub-build cycle") {
		t.Fatalf("errSummary = %q, want a sub-build cycle error", summary)
	}
	// The error names the full chain, target…requirer.
	for _, n := range []*node{bvA, runA, bvB, runB} {
		if !strings.Contains(summary, n.name) {
			t.Errorf("cycle chain %q missing node %s", summary, n.name)
		}
	}
	// The cycle-closing edge was refused: bvA gained no new dependent and no
	// node was created or replaced.
	if !cycleNodeCreated || !cycleNodeIsBvA {
		t.Errorf("cycle target node table entry disturbed (created=%v, same=%v)", cycleNodeCreated, cycleNodeIsBvA)
	}
	if _, ok := bvA.dependents[runB]; ok {
		t.Errorf("cycle edge runB -> bvA was linked despite detection")
	}
	if !unrelatedCreated {
		t.Errorf("unrelated input was not required (over-eager cycle detection)")
	}
	if phaseA == wire.PhaseFailed {
		t.Errorf("runA failed on a non-cycle require")
	}
}

// TestKPDerivedRefHealClosure is TestKPDerivedRefHeal with a CLOSURE-carrying
// Pinned (source-closure design §11): the server-side derivation must honor
// Pinned.Closure (PruneTree branch) through the full pipeline, and the §6.3
// crash-window re-derivation must converge on the same KP without re-running
// any stage.
func TestKPDerivedRefHealClosure(t *testing.T) {
	e := newEnv(t)
	fr := e.startRunner([]wire.Class{"c0.2-m1", "c1-m1"},
		e.standardHandler(builddef.Pinned{Closure: []string{"file.txt"}}))

	defBytes := e.treeDef("kp-heal-closure")
	_, f, kp := e.runChain(defBytes)
	pinnedKey := e.mustRef("build-pinned:" + f.String())
	outKP := e.mustRef("build-output:" + kp.String())

	e.deleteRefs(
		PinCoverRef(f), "build-pinned:"+kp.String(), KPTreeRef(kp),
		"build-output:"+f.String(), "build-output-deps:"+f.String(),
	)

	sub, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: defBytes})
	if err != nil {
		t.Fatalf("resubmit: %v", err)
	}
	if snap := e.watchTerminal(sub.RequestID); snap.Phase != "done" {
		t.Fatalf("healed resubmit phase = %s, want done (%+v)", snap.Phase, snap)
	}

	if kp2 := e.mustRef(PinCoverRef(f)); kp2 != kp {
		t.Errorf("healed pin-cover -> %s, want %s (closure derivation must be pure)", kp2, kp)
	}
	if pk := e.mustRef("build-pinned:" + kp.String()); pk != pinnedKey {
		t.Errorf("healed build-pinned:KP -> %s, want %s", pk, pinnedKey)
	}
	if av := e.mustRef("build-output:" + f.String()); av != outKP {
		t.Errorf("healed build-output:F -> %s, want %s", av, outKP)
	}
	for _, kind := range []string{wire.KindBuildFrom, wire.KindPluginResolve, wire.KindPin, wire.KindBuildRun} {
		if jobs := fr.byKind(kind); len(jobs) != 1 {
			t.Errorf("%s ran %d times, want 1 (heal must not re-run work)", kind, len(jobs))
		}
	}
}
