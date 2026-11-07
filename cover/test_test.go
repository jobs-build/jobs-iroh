package cover_test

// KT derivation (tests design §4.2): determinism, independence from input
// DEFINITIONS (only resolved content keys matter), sensitivity to resolved
// content and to the script, and insensitivity to resources.

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/cover"
	"github.com/jobs-build/jobs-iroh/importdef"
)

// importPinned is a pinned import input named name over url, with its
// import-output:K ref pointing at out.
func importPinned(t *testing.T, s *amber.Store, name, url string, out key.Key) builddef.PinnedInput {
	t.Helper()
	p, err := importdef.CanonicalParams(map[string]any{"url": url})
	if err != nil {
		t.Fatal(err)
	}
	canon, err := importdef.Definition{Fetcher: "tarball+https", Params: p, Platform: "linux/amd64"}.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	in := builddef.Input{Kind: builddef.KindImport, Definition: canon}
	k, err := in.Key()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PutRef(t.Context(), "import-output:"+k.String(), out); err != nil {
		t.Fatal(err)
	}
	return builddef.PinnedInput{Name: name, Kind: in.Kind, Definition: in.Definition}
}

// buildPinned is a pinned build input named name over a tree source with
// the given marker (distinct markers ⇒ distinct K), whose two-hop refs point
// at outTree ({c/…}) and deps.
func buildPinned(t *testing.T, s *amber.Store, name, marker string, outTree, deps key.Key) builddef.PinnedInput {
	t.Helper()
	ctx := t.Context()
	src := t.TempDir()
	write(t, filepath.Join(src, "marker"), []byte(marker))
	tree := ingest(t, s, src)
	ti, err := builddef.TreeInput(tree)
	if err != nil {
		t.Fatal(err)
	}
	canon, err := builddef.Definition{Source: ti, Platform: "linux/amd64", Params: []byte{0xf6}}.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	in := builddef.Input{Kind: builddef.KindBuild, Definition: canon}
	k, err := in.Key()
	if err != nil {
		t.Fatal(err)
	}
	f, err := amber.FileKey([]byte("f for " + marker))
	if err != nil {
		t.Fatal(err)
	}
	for n, v := range map[string]key.Key{
		"build-from:" + k.String():        f,
		"build-output:" + f.String():      outTree,
		"build-output-deps:" + f.String(): deps,
	} {
		if err := s.PutRef(ctx, n, v); err != nil {
			t.Fatal(err)
		}
	}
	return builddef.PinnedInput{Name: name, Kind: in.Kind, Definition: in.Definition}
}

// outputTree ingests {c/<name>: content} and returns the tree plus its c/ key.
func outputTree(t *testing.T, s *amber.Store, content string) (tree, c key.Key) {
	t.Helper()
	dir := t.TempDir()
	write(t, filepath.Join(dir, "c", "bin"), []byte(content))
	tree = ingest(t, s, dir)
	var ok bool
	var err error
	c, ok, err = s.TreeSubdir(t.Context(), tree, "c")
	if err != nil || !ok {
		t.Fatalf("c/ subtree: ok=%v err=%v", ok, err)
	}
	return tree, c
}

func fileKey(t *testing.T, s *amber.Store, data string) key.Key {
	t.Helper()
	k, err := s.IngestFile(t.Context(), []byte(data))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// kpFor derives KP for a pinned test over a small context tree.
func kpFor(t *testing.T, s *amber.Store, p builddef.Pinned) key.Key {
	t.Helper()
	src := t.TempDir()
	write(t, filepath.Join(src, "TESTS.jobs"), []byte("def test(): pass"))
	write(t, filepath.Join(src, "t", "x_test.go"), []byte("package t"))
	root := ingest(t, s, src)
	kp, err := cover.Derive(t.Context(), s, encodePinned(t, p), p, "linux/amd64", root)
	if err != nil {
		t.Fatalf("Derive: %v", err)
	}
	return kp
}

func deriveKT(t *testing.T, s *amber.Store, kp key.Key, p builddef.Pinned) key.Key {
	t.Helper()
	resolved, err := cover.ResolveInputs(t.Context(), s, p.Inputs)
	if err != nil {
		t.Fatalf("ResolveInputs: %v", err)
	}
	kt, err := cover.DeriveTest(t.Context(), s, kp, p, "linux/amd64", resolved)
	if err != nil {
		t.Fatalf("DeriveTest: %v", err)
	}
	return kt
}

func TestDeriveTestKeysByResolvedContentNotDefinitions(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	outTree, c := outputTree(t, s, "binary-v1")
	deps := fileKey(t, s, "deps-v1")
	tc := fileKey(t, s, "toolchain")

	base := builddef.Pinned{
		Env:    map[string]string{"A": "1"},
		Script: "run tests",
		Inputs: builddef.CanonicalPinnedInputs([]builddef.PinnedInput{
			buildPinned(t, s, "app", "commit-1", outTree, deps),
			importPinned(t, s, "toolchain", "https://e/tc.tgz", tc),
		}),
	}
	kp := kpFor(t, s, base)
	kt1 := deriveKT(t, s, kp, base)
	if kt1 == kp {
		t.Fatal("KT equals KP")
	}
	if again := deriveKT(t, s, kp, base); again != kt1 {
		t.Fatal("DeriveTest is not deterministic")
	}

	// The KT tree is KP-shaped: {job.cbor, platform, src/, v}, src/ shared
	// by key with the KP tree, job.cbor a TestJob naming the resolved keys.
	kpSrc, _, _ := s.TreeSubdir(ctx, kp, "src")
	ktSrc, ok, err := s.TreeSubdir(ctx, kt1, "src")
	if err != nil || !ok || ktSrc != kpSrc {
		t.Fatalf("KT src/ = %s (ok=%v err=%v), want the KP tree's %s", ktSrc, ok, err, kpSrc)
	}
	jobKey, ok, err := s.TreeSubdir(ctx, kt1, "job.cbor")
	if err != nil || !ok {
		t.Fatalf("KT job.cbor: ok=%v err=%v", ok, err)
	}
	jobBytes, err := s.ReadFile(ctx, jobKey)
	if err != nil {
		t.Fatal(err)
	}
	job, err := builddef.DecodeTestJob(jobBytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(job.Inputs) != 2 || job.Inputs[0].Name != "app" || string(job.Inputs[0].Out) != string(c[:]) ||
		string(job.Inputs[0].Deps) != string(deps[:]) || job.Inputs[1].Name != "toolchain" || string(job.Inputs[1].Out) != string(tc[:]) {
		t.Fatalf("TestJob inputs = %+v", job.Inputs)
	}
	if job.Script != "run tests" || job.Env["A"] != "1" {
		t.Fatalf("TestJob = %+v", job)
	}

	// A DIFFERENT build definition (another commit's tree ⇒ new K) whose
	// output resolves to the SAME content: same KT — the memo survives
	// out-of-cover context churn (design §1).
	sameOut := base
	sameOut.Inputs = builddef.CanonicalPinnedInputs([]builddef.PinnedInput{
		buildPinned(t, s, "app", "commit-2", outTree, deps),
		base.Inputs[1],
	})
	if kt2 := deriveKT(t, s, kp, sameOut); kt2 != kt1 {
		t.Errorf("KT changed with the input DEFINITION only: %s vs %s", kt2, kt1)
	}

	// Different output content ⇒ different KT.
	outTree2, _ := outputTree(t, s, "binary-v2")
	newOut := base
	newOut.Inputs = builddef.CanonicalPinnedInputs([]builddef.PinnedInput{
		buildPinned(t, s, "app", "commit-3", outTree2, deps),
		base.Inputs[1],
	})
	if kt3 := deriveKT(t, s, kp, newOut); kt3 == kt1 {
		t.Error("KT unchanged though the input's artifact changed")
	}

	// Different runtime closure (same artifact) ⇒ different KT.
	newDeps := base
	newDeps.Inputs = builddef.CanonicalPinnedInputs([]builddef.PinnedInput{
		buildPinned(t, s, "app", "commit-4", outTree, fileKey(t, s, "deps-v2")),
		base.Inputs[1],
	})
	if kt4 := deriveKT(t, s, kp, newDeps); kt4 == kt1 {
		t.Error("KT unchanged though the input's runtime closure changed")
	}

	// Script change ⇒ different KT; resources or timeout change ⇒ SAME KT.
	script := base
	script.Script = "run tests -v"
	if kt5 := deriveKT(t, s, kp, script); kt5 == kt1 {
		t.Error("KT unchanged though the script changed")
	}
	res := base
	res.Resources = &builddef.PinnedResources{CPUMilli: 4000, MemBytes: 8 << 30}
	if kt6 := deriveKT(t, s, kp, res); kt6 != kt1 {
		t.Error("KT changed with resources — raising memory must not re-run a passed test")
	}
	tm := base
	tm.TimeoutSec = 3600
	if kt7 := deriveKT(t, s, kp, tm); kt7 != kt1 {
		t.Error("KT changed with timeout — raising a passed test's limit must not re-run it")
	}
}

func TestResolveInputsUnresolved(t *testing.T) {
	s := newStore(t)
	p, err := importdef.CanonicalParams(map[string]any{"url": "https://e/none.tgz"})
	if err != nil {
		t.Fatal(err)
	}
	canon, err := importdef.Definition{Fetcher: "tarball+https", Params: p, Platform: "linux/amd64"}.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	_, err = cover.ResolveInputs(t.Context(), s, []builddef.PinnedInput{{Name: "x", Kind: builddef.KindImport, Definition: canon}})
	if !errors.Is(err, cover.ErrInputUnresolved) {
		t.Fatalf("err = %v, want ErrInputUnresolved", err)
	}
}

func TestDeriveTestNeedsKPSrc(t *testing.T) {
	s := newStore(t)
	notKP := fileKey(t, s, "not a kp tree")
	_, err := cover.DeriveTest(t.Context(), s, notKP, builddef.Pinned{}, "linux/amd64", nil)
	if err == nil {
		t.Fatal("DeriveTest accepted a non-KP tree")
	}
}
