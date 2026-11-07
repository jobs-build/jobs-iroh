package sched

// Test-pipeline tests (tests design §5, §10): the testvalue chain through
// the fake runner, the F-level memo, KT stability across a context change
// (the whole point of keying tests by resolved inputs), the derived-ref
// heal, failure semantics, and the request's test marker. Uses
// sched_test.go's env harness.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/amber-store/core/key"

	"github.com/jobs-build/jobs-iroh/api"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/cover"
	"github.com/jobs-build/jobs-iroh/importdef"
	"github.com/jobs-build/jobs-iroh/wire"
)

// testDef builds a canonical tree-source TEST definition over a fresh
// source tree with the given marker (distinct markers ⇒ distinct K and F).
func (e *env) testDef(marker string) []byte {
	e.t.Helper()
	src, err := builddef.TreeInput(e.ingestTree(marker))
	if err != nil {
		e.t.Fatal(err)
	}
	def := builddef.Definition{
		Source:   src,
		Platform: testPlatform,
		Params:   []byte{0xf6},
		Test:     true,
	}
	b, err := def.Canonical()
	if err != nil {
		e.t.Fatal(err)
	}
	return b
}

// toolchainInput is a pinned import input every test chain shares: the
// fake import handler writes "imported <node>" for it, so its output — and
// with it KT — is stable across chains.
func toolchainInput(t *testing.T) builddef.PinnedInput {
	t.Helper()
	p, err := importdef.CanonicalParams(map[string]any{"url": "https://e/tc.tgz"})
	if err != nil {
		t.Fatal(err)
	}
	canon, err := importdef.Definition{Fetcher: "tarball+https", Params: p, Platform: testPlatform}.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	return builddef.PinnedInput{Name: "toolchain", Kind: builddef.KindImport, Definition: canon}
}

// testChainOpts parametrizes testHandler. Every hook is optional.
type testChainOpts struct {
	buildPinned builddef.Pinned                       // what pin publishes for build F-trees
	testPinned  func(f key.Key) builddef.Pinned       // what pin publishes for test F-trees
	testEnv     func(node string) key.Key             // the env tree a test buildfrom assembles
	buildOutput func(node string) (out, deps key.Key) // the buildrun output pair
	testFail    bool                                  // every testrun reports a hard failure
}

// testHandler extends standardHandler for test chains: buildfrom assembles
// a TEST F-tree for a test definition (the in-band def says which), the pin
// stage publishes the test pinned for test F-trees and the build pinned
// otherwise, and testrun records test-passed:<KT> → KT (the KT tree already
// exists on the server — the scheduler derived it).
func (e *env) testHandler(o testChainOpts) func(wire.Job) *wire.Result {
	std := e.standardHandler(o.buildPinned)
	return func(job wire.Job) *wire.Result {
		fh, err := wire.ParseKey(job.Key)
		if err != nil {
			e.t.Errorf("job %s: bad key: %v", job.Node, err)
			return nil
		}
		f := fh.String()
		switch job.Kind {
		case wire.KindBuildFrom:
			def, err := builddef.DecodeDefinition(job.Def)
			if err != nil {
				e.t.Errorf("decode in-band def: %v", err)
				return nil
			}
			if !def.Test {
				return std(job)
			}
			var envT key.Key
			if o.testEnv != nil {
				envT = o.testEnv(job.Node)
			} else {
				envT = e.ingestTree("env for " + job.Node)
			}
			ftree, err := e.st.TestFromTree(e.ctx, envT, "", []byte("params"), testPlatform, []byte("def test(): pass"))
			if err != nil {
				e.t.Errorf("fake TestFromTree: %v", err)
				return nil
			}
			return okResult(job,
				ref("build-from:"+f, ftree),
				ref("build-from-tree:"+ftree.String(), ftree))
		case wire.KindPin:
			pinned := o.buildPinned
			if _, isTest, _ := e.st.TreeSubdir(e.ctx, fh, "TESTS.jobs"); isTest && o.testPinned != nil {
				pinned = o.testPinned(fh)
			}
			body, err := builddef.EncodePinned(pinned)
			if err != nil {
				e.t.Error(err)
				return nil
			}
			return okResult(job, ref("build-pinned:"+f, e.ingestFile(body)))
		case wire.KindBuildRun:
			if o.buildOutput == nil {
				return std(job)
			}
			out, deps := o.buildOutput(job.Node)
			return okResult(job,
				ref("build-output-deps:"+f, deps),
				ref("build-output:"+f, out))
		case wire.KindTestRun:
			if o.testFail {
				return &wire.Result{Class: wire.ClassHard, Exit: 1, ErrSummary: "testing: 1 test failed"}
			}
			return okResult(job, ref(cover.TestPassedRef(fh), fh))
		default:
			return std(job)
		}
	}
}

// fixedTestPinned returns a testPinned hook publishing p for every test F.
func fixedTestPinned(p builddef.Pinned) func(key.Key) builddef.Pinned {
	return func(key.Key) builddef.Pinned { return p }
}

// ingestFiles writes the given path → content map as a tree in the server
// store.
func (e *env) ingestFiles(files map[string]string) key.Key {
	e.t.Helper()
	dir := e.t.TempDir()
	for p, c := range files {
		full := filepath.Join(dir, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			e.t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(c), 0o644); err != nil {
			e.t.Fatal(err)
		}
	}
	k, err := e.st.IngestDir(e.ctx, dir)
	if err != nil {
		e.t.Fatalf("ingest dir: %v", err)
	}
	return k
}

// runTestChain submits a test def, waits for done, and returns (K, F, KT),
// deleting the request so a resubmit re-creates the nodes.
func (e *env) runTestChain(defBytes []byte) (k, f, kt key.Key) {
	e.t.Helper()
	sub, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: defBytes})
	if err != nil {
		e.t.Fatalf("submit: %v", err)
	}
	snap := e.watchTerminal(sub.RequestID)
	if snap.Phase != "done" {
		e.t.Fatalf("terminal phase = %s, want done (%+v)", snap.Phase, snap)
	}
	k, err = wire.ParseKey(sub.K)
	if err != nil {
		e.t.Fatal(err)
	}
	f = e.mustRef("build-from:" + k.String())
	kt = e.mustRef(cover.TestPassedRef(f))
	if err := e.s.Delete(e.ctx, sub.RequestID); err != nil {
		e.t.Fatalf("delete request: %v", err)
	}
	return k, f, kt
}

func nodeByKind(snap api.Snapshot, kind string) (api.NodeSnap, bool) {
	for _, n := range snap.Nodes {
		if strings.HasPrefix(n.Node, kind+"_") {
			return n, true
		}
	}
	return api.NodeSnap{}, false
}

func TestTestChainRefsAndGraph(t *testing.T) {
	e := newEnv(t)
	testPinned := builddef.Pinned{Script: "go test ./...", Inputs: []builddef.PinnedInput{toolchainInput(t)}}
	fr := e.startRunner([]wire.Class{"c0.2-m1", "c1-m1"}, e.testHandler(testChainOpts{testPinned: fixedTestPinned(testPinned)}))

	defBytes := e.testDef("test-chain")
	sub, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: defBytes, Label: "svc"})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	snap := e.watchTerminal(sub.RequestID)
	if snap.Phase != "done" {
		t.Fatalf("phase = %s (%+v)", snap.Phase, snap)
	}
	k, _ := wire.ParseKey(sub.K)

	// Bookkeeping + the whole ref family.
	e.mustRef("test:" + k.String())
	if _, ok := e.getRef("build:" + k.String()); ok {
		t.Error("test submit wrote a build:K bookkeeping ref")
	}
	f := e.mustRef("build-from:" + k.String())
	kp := e.mustRef(PinCoverRef(f))
	kt := e.mustRef(cover.TestPassedRef(f))
	if kt == kp || kt == f {
		t.Fatalf("KT %s collides with KP %s / F %s", kt, kp, f)
	}
	if got := e.mustRef(cover.TestPassedRef(kt)); got != kt {
		t.Errorf("test-passed:<KT> → %s, want KT itself", got)
	}
	if got := e.mustRef(KTTreeRef(kt)); got != kt {
		t.Errorf("kt-tree carrier → %s, want name==value", got)
	}
	if got, want := e.mustRef("build-pinned:"+kt.String()), e.mustRef("build-pinned:"+f.String()); got != want {
		t.Errorf("build-pinned:<KT> → %s, want the pinned blob %s", got, want)
	}
	// No build output anywhere for the test F / KP.
	for _, name := range []string{"build-output:" + f.String(), "build-output:" + kp.String()} {
		if _, ok := e.getRef(name); ok {
			t.Errorf("test chain wrote %s", name)
		}
	}

	// Graph shape: the testvalue owns the chain AND the input (tests design
	// §5) — the import hangs under the testvalue, not under the testrun only.
	tv, ok := nodeByKind(snap, wire.KindTestValue)
	if !ok {
		t.Fatalf("no testvalue in snapshot %+v", snap)
	}
	imp, ok := nodeByKind(snap, wire.KindImport)
	if !ok {
		t.Fatalf("no import in snapshot %+v", snap)
	}
	var hasImp, hasRun bool
	for _, d := range tv.Deps {
		hasImp = hasImp || d == imp.Node
		hasRun = hasRun || strings.HasPrefix(d, wire.KindTestRun+"_")
	}
	if !hasImp || !hasRun {
		t.Errorf("testvalue deps = %v, want the import and the testrun", tv.Deps)
	}
	if tv.Label != "svc" {
		t.Errorf("testvalue label = %q, want the submit label", tv.Label)
	}
	run, _ := nodeByKind(snap, wire.KindTestRun)
	if run.Node != wire.NodeName(wire.KindTestRun, kt) {
		t.Errorf("testrun node = %s, want %s", run.Node, wire.NodeName(wire.KindTestRun, kt))
	}

	// The testrun job pulled the KT carrier + alias and the input's output.
	jobs := fr.byKind(wire.KindTestRun)
	if len(jobs) != 1 {
		t.Fatalf("testrun jobs = %d, want 1", len(jobs))
	}
	tcK, _ := (builddef.Input{Kind: builddef.KindImport, Definition: toolchainInput(t).Definition}).Key()
	for _, want := range []string{KTTreeRef(kt), "build-pinned:" + kt.String(), "import-output:" + tcK.String()} {
		found := false
		for _, p := range jobs[0].PullRefs {
			found = found || p == want
		}
		if !found {
			t.Errorf("testrun PullRefs %v lack %s", jobs[0].PullRefs, want)
		}
	}
	if jobs[0].Kind != wire.KindTestRun || jobs[0].Class != "c1-m1" {
		t.Errorf("testrun job = %+v, want the build-sized class", jobs[0])
	}

	// The request is marked as a test.
	var found bool
	for _, r := range e.s.Requests() {
		if r.RequestID == sub.RequestID {
			found = true
			if !r.Test {
				t.Error("RequestInfo.Test = false for a test request")
			}
		}
	}
	if !found {
		t.Error("request not listed")
	}
}

func TestTestMemoResubmitRunsNothing(t *testing.T) {
	e := newEnv(t)
	testPinned := builddef.Pinned{Script: "go test ./...", Inputs: []builddef.PinnedInput{toolchainInput(t)}}
	fr := e.startRunner([]wire.Class{"c0.2-m1", "c1-m1"}, e.testHandler(testChainOpts{testPinned: fixedTestPinned(testPinned)}))

	defBytes := e.testDef("test-memo")
	_, f, kt := e.runTestChain(defBytes)

	sub, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: defBytes})
	if err != nil {
		t.Fatalf("resubmit: %v", err)
	}
	snap := e.watchTerminal(sub.RequestID)
	if snap.Phase != "done" {
		t.Fatalf("resubmit phase = %s (%+v)", snap.Phase, snap)
	}
	tv, _ := nodeByKind(snap, wire.KindTestValue)
	if !tv.Cached || len(snap.Nodes) != 1 {
		t.Fatalf("resubmit did not fast-path on test-passed:F: cached=%v nodes=%d", tv.Cached, len(snap.Nodes))
	}
	for _, kind := range []string{wire.KindBuildFrom, wire.KindPluginResolve, wire.KindPin, wire.KindImport, wire.KindTestRun} {
		if n := len(fr.byKind(kind)); n != 1 {
			t.Errorf("%s ran %d times, want 1", kind, n)
		}
	}
	if got := e.mustRef(cover.TestPassedRef(f)); got != kt {
		t.Errorf("alias changed on resubmit: %s vs %s", got, kt)
	}
}

// TestTestKTStableAcrossContextChange is the feature's reason to exist: a
// second test definition over a DIFFERENT source tree (a new commit) whose
// pinned test covers an unchanged subset and whose `build` input — a
// DIFFERENT definition, since it embeds the new tree — resolves to the same
// artifact must land on the same KT and fast-path the testrun: new K, new
// F, eval stages re-run, the build input re-runs (its fake has no cover),
// tests do NOT.
func TestTestKTStableAcrossContextChange(t *testing.T) {
	e := newEnv(t)
	// The build input's output is a constant artifact tree ({c/bin}) + a
	// constant deps tree: a different K and KP, the same content.
	outTree := e.ingestFiles(map[string]string{"c/bin": "app-v1"})
	depsTree := e.ingestFile([]byte("deps-v1"))
	opts := testChainOpts{
		// Covered subset identical, the rest of the context churns.
		testEnv: func(node string) key.Key {
			return e.ingestFiles(map[string]string{"covered/t.txt": "same", "other.txt": "env for " + node})
		},
		// The pinned test: closure over the covered subset, the toolchain
		// import, and the directory's build — a tree-source build def over
		// THIS F-tree's env, exactly what the predeclared `build` constructs.
		testPinned: func(f key.Key) builddef.Pinned {
			env, ok, err := e.st.TreeSubdir(e.ctx, f, "env")
			if err != nil || !ok {
				e.t.Errorf("F-tree env: ok=%v err=%v", ok, err)
			}
			src, err := builddef.TreeInput(env)
			if err != nil {
				e.t.Error(err)
			}
			app, err := builddef.Definition{Source: src, Platform: testPlatform, Params: []byte{0xf6}}.Canonical()
			if err != nil {
				e.t.Error(err)
			}
			return builddef.Pinned{
				Script:  "go test ./...",
				Closure: []string{"covered"},
				Inputs: builddef.CanonicalPinnedInputs([]builddef.PinnedInput{
					toolchainInput(t),
					{Name: "app", Kind: builddef.KindBuild, Definition: app},
				}),
			}
		},
		buildOutput: func(string) (key.Key, key.Key) { return outTree, depsTree },
	}
	fr := e.startRunner([]wire.Class{"c0.2-m1", "c1-m1"}, e.testHandler(opts))

	k1, f1, kt1 := e.runTestChain(e.testDef("commit-1"))

	def2 := e.testDef("commit-2")
	sub, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: def2})
	if err != nil {
		t.Fatalf("submit 2: %v", err)
	}
	snap := e.watchTerminal(sub.RequestID)
	if snap.Phase != "done" {
		t.Fatalf("phase = %s (%+v)", snap.Phase, snap)
	}
	k2, _ := wire.ParseKey(sub.K)
	f2 := e.mustRef("build-from:" + k2.String())
	if k2 == k1 || f2 == f1 {
		t.Fatal("second def did not re-key K/F — fixture broken")
	}
	// The test's pinned bytes differ (the app input's definition embeds the
	// new env), so KP differs — and KT does not.
	if kp1, kp2 := e.mustRef(PinCoverRef(f1)), e.mustRef(PinCoverRef(f2)); kp1 == kp2 {
		t.Fatal("KP unchanged across the context change — fixture broken (the build input's definition must differ)")
	}
	if kt2 := e.mustRef(cover.TestPassedRef(f2)); kt2 != kt1 {
		t.Fatalf("KT differs across a context change with identical resolved inputs: %s vs %s", kt2, kt1)
	}
	run, ok := nodeByKind(snap, wire.KindTestRun)
	if !ok || !run.Cached || run.Phase != wire.PhaseDone {
		t.Fatalf("testrun not fast-pathed: %+v", run)
	}
	if n := len(fr.byKind(wire.KindTestRun)); n != 1 {
		t.Errorf("testrun ran %d times, want 1", n)
	}
	// The build input re-ran (new K, and this fake declares no cover) and
	// the eval stages ran for both the test and its build input per chain.
	if n := len(fr.byKind(wire.KindBuildRun)); n != 2 {
		t.Errorf("buildrun ran %d times, want 2", n)
	}
	for _, kind := range []string{wire.KindBuildFrom, wire.KindPluginResolve, wire.KindPin} {
		if n := len(fr.byKind(kind)); n != 4 {
			t.Errorf("%s ran %d times, want 4 (eval churn is accepted)", kind, n)
		}
	}
}

func TestTestDerivedRefHeal(t *testing.T) {
	e := newEnv(t)
	testPinned := builddef.Pinned{Script: "go test ./...", Inputs: []builddef.PinnedInput{toolchainInput(t)}}
	fr := e.startRunner([]wire.Class{"c0.2-m1", "c1-m1"}, e.testHandler(testChainOpts{testPinned: fixedTestPinned(testPinned)}))

	defBytes := e.testDef("test-heal")
	_, f, kt := e.runTestChain(defBytes)
	pinnedKey := e.mustRef("build-pinned:" + f.String())

	// Crash window: the memo survives, every derived/alias ref is gone.
	e.deleteRefs(cover.TestPassedRef(f), "build-pinned:"+kt.String(), KTTreeRef(kt), PinCoverRef(f))

	sub, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: defBytes})
	if err != nil {
		t.Fatalf("resubmit: %v", err)
	}
	if snap := e.watchTerminal(sub.RequestID); snap.Phase != "done" {
		t.Fatalf("healed resubmit phase = %s (%+v)", snap.Phase, snap)
	}
	if got := e.mustRef(cover.TestPassedRef(f)); got != kt {
		t.Errorf("healed alias → %s, want %s (derivation must be pure)", got, kt)
	}
	if got := e.mustRef(KTTreeRef(kt)); got != kt {
		t.Errorf("healed kt-tree → %s", got)
	}
	if got := e.mustRef("build-pinned:" + kt.String()); got != pinnedKey {
		t.Errorf("healed build-pinned:<KT> → %s, want %s", got, pinnedKey)
	}
	for _, kind := range []string{wire.KindBuildFrom, wire.KindPluginResolve, wire.KindPin, wire.KindTestRun} {
		if n := len(fr.byKind(kind)); n != 1 {
			t.Errorf("%s ran %d times, want 1 (heal must not re-run work)", kind, n)
		}
	}
}

func TestTestFailureLeavesNoRefAndReruns(t *testing.T) {
	e := newEnv(t)
	testPinned := builddef.Pinned{Script: "go test ./...", Inputs: []builddef.PinnedInput{toolchainInput(t)}}
	fr := e.startRunner([]wire.Class{"c0.2-m1", "c1-m1"}, e.testHandler(testChainOpts{testPinned: fixedTestPinned(testPinned), testFail: true}))

	defBytes := e.testDef("test-fail")
	sub, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: defBytes})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	snap := e.watchTerminal(sub.RequestID)
	if snap.Phase != "failed" {
		t.Fatalf("phase = %s, want failed (%+v)", snap.Phase, snap)
	}
	run, _ := nodeByKind(snap, wire.KindTestRun)
	if run.Phase != wire.PhaseFailed || !strings.Contains(run.ErrSummary, "1 test failed") {
		t.Errorf("testrun = %+v", run)
	}
	tv, _ := nodeByKind(snap, wire.KindTestValue)
	if tv.Phase != wire.PhaseUpstream {
		t.Errorf("testvalue phase = %s, want failed-upstream", tv.Phase)
	}
	k, _ := wire.ParseKey(sub.K)
	f := e.mustRef("build-from:" + k.String())
	if _, ok := e.getRef(cover.TestPassedRef(f)); ok {
		t.Error("failed test wrote test-passed:F")
	}
	refs, _ := e.st.ListRefs(e.ctx)
	for _, r := range refs {
		if strings.HasPrefix(r.Name, "test-passed:") {
			t.Errorf("failed test left %s", r.Name)
		}
	}
	// Retry is resubmit: the testrun runs again (hard failures are sticky
	// only while watched).
	if err := e.s.Delete(e.ctx, sub.RequestID); err != nil {
		t.Fatal(err)
	}
	sub2, err := e.s.Submit(e.ctx, api.SubmitRequest{Def: defBytes})
	if err != nil {
		t.Fatalf("resubmit: %v", err)
	}
	if snap := e.watchTerminal(sub2.RequestID); snap.Phase != "failed" {
		t.Fatalf("resubmit phase = %s", snap.Phase)
	}
	if n := len(fr.byKind(wire.KindTestRun)); n != 2 {
		t.Errorf("testrun ran %d times across two submits, want 2", n)
	}
}
