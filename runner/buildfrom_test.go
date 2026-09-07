//go:build linux

package runner_test

import (
	"context"
	"testing"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/importdef"
	"github.com/jobs-build/jobs-iroh/runner"
)

func TestRunBuildFrom_importSourceProducesF(t *testing.T) {
	ctx := context.Background()
	st := buildEvalStore(t)
	platform := runner.Platform()

	// Source import + its output tree (with a BUILD.jobs at dir "").
	importParams, _ := importdef.CanonicalParams(map[string]any{"url": "https://example.com/x.tgz"})
	srcDef := importdef.Definition{Fetcher: "test-fetcher", Params: importParams}
	srcCanon, _ := srcDef.Canonical()
	srcImportKey, _ := st.IngestFile(ctx, srcCanon)
	sourceInput := builddef.Input{Kind: builddef.KindImport, Definition: srcCanon}
	sourceOutputKey := ingestSourceTree(t, ctx, st, map[string]string{
		"BUILD.jobs": "def build(): return struct(inputs={}, env={}, script=\"\", runtime_deps=[])\n",
	})
	if err := st.PutRef(ctx, "import-output:"+srcImportKey.String(), sourceOutputKey); err != nil {
		t.Fatal(err)
	}

	// Build def, two builds: identical content -> same F.
	_, buildK := makeBuildDef(t, ctx, st, sourceInput, platform)

	brc := runner.BuildRunCfg{Platform: platform, CacheDir: t.TempDir()}
	out := runner.RunBuildFrom(ctx, st, runner.NewLocalRefWriter(st), brc, buildK)
	if out.Failed || out.Decline || out.Cancelled {
		t.Fatalf("RunBuildFrom: %+v", out)
	}
	f, ok, err := st.GetKey(ctx, "build-from:"+buildK.String())
	if err != nil || !ok {
		t.Fatalf("build-from ref missing: ok=%v err=%v", ok, err)
	}
	if f != out.OutputKey {
		t.Fatalf("ref %s != reported %s", f, out.OutputKey)
	}
	// Determinism: re-run yields the same F.
	out2 := runner.RunBuildFrom(ctx, st, runner.NewLocalRefWriter(st), brc, buildK)
	if out2.OutputKey != out.OutputKey {
		t.Fatalf("non-deterministic F: %s != %s", out2.OutputKey, out.OutputKey)
	}
}

// putBuildDef ingests a build Definition with the given recipe selector options
// and publishes build:<K>, returning K.
func putBuildDef(t *testing.T, ctx context.Context, st *amber.Store, source builddef.Input, platform, dir, buildJobs, buildFile string) key.Key {
	t.Helper()
	params, err := importdef.CanonicalParams(nil)
	if err != nil {
		t.Fatal(err)
	}
	var override []byte
	if buildJobs != "" {
		override = []byte(buildJobs)
	}
	def := builddef.Definition{Source: source, Dir: dir, Platform: platform, Params: params, BuildJobs: override, BuildFile: buildFile}
	canon, err := def.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	k, err := st.IngestFile(ctx, canon)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutRef(ctx, "build:"+k.String(), k); err != nil {
		t.Fatal(err)
	}
	return k
}

func runBuildFromOK(t *testing.T, ctx context.Context, st *amber.Store, brc runner.BuildRunCfg, k key.Key) key.Key {
	t.Helper()
	out := runner.RunBuildFrom(ctx, st, runner.NewLocalRefWriter(st), brc, k)
	if out.Failed || out.Decline || out.Cancelled {
		t.Fatalf("RunBuildFrom: %+v", out)
	}
	return out.OutputKey
}

// A build_file selects an alternative recipe in the source; the resulting F must
// equal an INLINE override of that same recipe content over the same source (the
// recipe is normalized into the canonical top-level BUILD.jobs), and must differ
// from the default-BUILD.jobs build.
func TestRunBuildFrom_buildFileSelectsAltRecipe(t *testing.T) {
	ctx := context.Background()
	st := buildEvalStore(t)
	platform := runner.Platform()
	brc := runner.BuildRunCfg{Platform: platform, CacheDir: t.TempDir()}

	const recipeA = "# recipe A\n"
	const recipeB = "# recipe B\n"

	importParams, _ := importdef.CanonicalParams(map[string]any{"url": "https://example.com/x.tgz"})
	srcCanon, _ := importdef.Definition{Fetcher: "test-fetcher", Params: importParams}.Canonical()
	srcImportKey, _ := st.IngestFile(ctx, srcCanon)
	source := builddef.Input{Kind: builddef.KindImport, Definition: srcCanon}
	outTree := ingestSourceTree(t, ctx, st, map[string]string{
		"BUILD.jobs": recipeA,
		"app.jobs":   recipeB,
	})
	if err := st.PutRef(ctx, "import-output:"+srcImportKey.String(), outTree); err != nil {
		t.Fatal(err)
	}

	fAlt := runBuildFromOK(t, ctx, st, brc, putBuildDef(t, ctx, st, source, platform, "", "", "app.jobs"))
	fInline := runBuildFromOK(t, ctx, st, brc, putBuildDef(t, ctx, st, source, platform, "", recipeB, ""))
	if fAlt != fInline {
		t.Fatalf("build_file selection must equal inline override of the same content: %s != %s", fAlt, fInline)
	}
	fPlain := runBuildFromOK(t, ctx, st, brc, putBuildDef(t, ctx, st, source, platform, "", "", ""))
	if fAlt == fPlain {
		t.Fatalf("build_file=app.jobs must change F vs the default BUILD.jobs, both %s", fAlt)
	}
}

func TestRunBuildFromWritesBuildFromTreeRef(t *testing.T) {
	ctx := context.Background()
	st := buildEvalStore(t)

	srcKey := ingestSourceTree(t, ctx, st, map[string]string{
		"BUILD.jobs": "def build(): return struct(inputs={}, env={}, script=\"\", runtime_deps=[])\n",
	})
	srcInput, err := builddef.TreeInput(srcKey)
	if err != nil {
		t.Fatal(err)
	}
	_, buildK := makeBuildDef(t, ctx, st, srcInput, runner.Platform())

	out := runner.RunBuildFrom(ctx, st, runner.NewLocalRefWriter(st), runner.BuildRunCfg{}, buildK)
	if out.Failed || out.Decline || out.Cancelled {
		t.Fatalf("RunBuildFrom: %+v", out)
	}
	f := out.OutputKey

	gotFromK, ok, err := st.GetKey(ctx, "build-from:"+buildK.String())
	if err != nil || !ok || gotFromK != f {
		t.Fatalf("build-from:K = %v ok=%v err=%v, want %v", gotFromK, ok, err, f)
	}
	gotTree, ok, err := st.GetKey(ctx, "build-from-tree:"+f.String())
	if err != nil || !ok {
		t.Fatalf("build-from-tree:F not found: ok=%v err=%v", ok, err)
	}
	if gotTree != f {
		t.Fatalf("build-from-tree:F = %v, want %v", gotTree, f)
	}
}

// An explicit build_file that does not exist in the source is a hard error — no
// silent fallback to BUILD.jobs.
func TestRunBuildFrom_buildFileMissingFails(t *testing.T) {
	ctx := context.Background()
	st := buildEvalStore(t)
	platform := runner.Platform()
	brc := runner.BuildRunCfg{Platform: platform, CacheDir: t.TempDir()}

	importParams, _ := importdef.CanonicalParams(map[string]any{"url": "https://example.com/x.tgz"})
	srcCanon, _ := importdef.Definition{Fetcher: "test-fetcher", Params: importParams}.Canonical()
	srcImportKey, _ := st.IngestFile(ctx, srcCanon)
	source := builddef.Input{Kind: builddef.KindImport, Definition: srcCanon}
	outTree := ingestSourceTree(t, ctx, st, map[string]string{"BUILD.jobs": "# A\n"})
	if err := st.PutRef(ctx, "import-output:"+srcImportKey.String(), outTree); err != nil {
		t.Fatal(err)
	}

	out := runner.RunBuildFrom(ctx, st, runner.NewLocalRefWriter(st), brc, putBuildDef(t, ctx, st, source, platform, "", "", "nope.jobs"))
	if !out.Failed {
		t.Fatalf("RunBuildFrom with a missing build_file must fail, got %+v", out)
	}
}
