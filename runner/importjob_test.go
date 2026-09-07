package runner_test

import (
	"archive/tar"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/importdef"
	"github.com/jobs-build/jobs-iroh/runner"
)

// buildEvalStore opens a fresh single-process store under a test temp dir —
// jobs' ambertest harness collapsed to amber.Open (no daemon, no signer).
func buildEvalStore(t *testing.T) *amber.Store {
	t.Helper()
	st, err := amber.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// registerFetcher ingests a fetcher dir holding an executable fetch script and
// publishes fetcher:<name>:<platform>.
func registerFetcher(t *testing.T, ctx context.Context, st *amber.Store, name, platform, script string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "fetch"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	k, err := st.IngestDir(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutRef(ctx, "fetcher:"+name+":"+platform, k); err != nil {
		t.Fatal(err)
	}
}

func ingestDef(t *testing.T, ctx context.Context, st *amber.Store, fetcher string, tags []string) key.Key {
	t.Helper()
	p, err := importdef.CanonicalParams(map[string]any{"url": "https://example.com/x.tgz"})
	if err != nil {
		t.Fatal(err)
	}
	def := importdef.Definition{Fetcher: fetcher, Params: p, RequiredTags: tags}
	canon, err := def.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	k, err := st.IngestFile(ctx, canon)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestRunImport_HappyPath(t *testing.T) {
	st := buildEvalStore(t)
	ctx := context.Background()
	registerFetcher(t, ctx, st, "test", runner.Platform(),
		"#!/bin/sh\necho hello > \"$JOBS_OUTPUT_DIR/data.txt\"\n")
	k := ingestDef(t, ctx, st, "test", nil)

	out := runner.RunImport(ctx, st, runner.NewLocalRefWriter(st), runner.Subprocess{}, t.TempDir(), k, nil, nil)
	if out.Failed || out.Decline || out.Cancelled {
		t.Fatalf("unexpected outcome: %+v", out)
	}
	if out.OutputKey == (key.Key{}) {
		t.Fatal("empty output key")
	}
	got, ok, err := st.GetKey(ctx, "import-output:"+k.String())
	if err != nil || !ok {
		t.Fatalf("import-output ref missing: ok=%v err=%v", ok, err)
	}
	if got != out.OutputKey {
		t.Fatalf("ref key %s != reported %s", got, out.OutputKey)
	}
	// Output content is present (a Tar of the tree drains cleanly).
	rc, err := st.Tar(ctx, out.OutputKey, "")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, rc)
	rc.Close()
}

// tarEntryBasenames drains a Tar of the tree at root and returns the set of
// file basenames it contains (prefix-agnostic across amber's tar naming).
func tarEntryBasenames(t *testing.T, ctx context.Context, st *amber.Store, root key.Key) map[string]bool {
	t.Helper()
	rc, err := st.Tar(ctx, root, "")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	names := map[string]bool{}
	tr := tar.NewReader(rc)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		names[filepath.Base(h.Name)] = true
	}
	return names
}

// A fetcher whose output carries a .amberignore must have it honored at ingest:
// matched files are excluded and the .amberignore itself never enters the CAS.
func TestRunImport_HonorsAmberignore(t *testing.T) {
	st := buildEvalStore(t)
	ctx := context.Background()
	registerFetcher(t, ctx, st, "ignore", runner.Platform(),
		"#!/bin/sh\n"+
			"printf '*.log\\n' > \"$JOBS_OUTPUT_DIR/.amberignore\"\n"+
			"echo keep > \"$JOBS_OUTPUT_DIR/keep.txt\"\n"+
			"echo drop > \"$JOBS_OUTPUT_DIR/drop.log\"\n")
	k := ingestDef(t, ctx, st, "ignore", nil)

	out := runner.RunImport(ctx, st, runner.NewLocalRefWriter(st), runner.Subprocess{}, t.TempDir(), k, nil, nil)
	if out.Failed || out.Decline || out.Cancelled {
		t.Fatalf("unexpected outcome: %+v", out)
	}

	names := tarEntryBasenames(t, ctx, st, out.OutputKey)
	if names["drop.log"] {
		t.Errorf("drop.log present in import output; .amberignore not honored (entries: %v)", names)
	}
	if names[".amberignore"] {
		t.Errorf(".amberignore leaked into import output (entries: %v)", names)
	}
	if !names["keep.txt"] {
		t.Errorf("keep.txt missing from import output (entries: %v)", names)
	}
}

func TestRunImport_HardFailOnNonzeroExit(t *testing.T) {
	st := buildEvalStore(t)
	ctx := context.Background()
	registerFetcher(t, ctx, st, "boom", runner.Platform(), "#!/bin/sh\necho nope >&2\nexit 1\n")
	k := ingestDef(t, ctx, st, "boom", nil)

	out := runner.RunImport(ctx, st, runner.NewLocalRefWriter(st), runner.Subprocess{}, t.TempDir(), k, nil, nil)
	if !out.Failed || out.Class != "hard" || out.ExitCode != 1 {
		t.Fatalf("expected hard fail exit 1, got %+v", out)
	}
}

func TestRunImport_RetryableOnExit75(t *testing.T) {
	st := buildEvalStore(t)
	ctx := context.Background()
	registerFetcher(t, ctx, st, "temp", runner.Platform(), "#!/bin/sh\nexit 75\n")
	k := ingestDef(t, ctx, st, "temp", nil)

	out := runner.RunImport(ctx, st, runner.NewLocalRefWriter(st), runner.Subprocess{}, t.TempDir(), k, nil, nil)
	if !out.Failed || out.Class != "retryable" {
		t.Fatalf("expected retryable, got %+v", out)
	}
}

// publishFetcherBuildOutput ingests {c/fetch} as a build output and wires the
// two-hop refs for the build definition key fk, returning nothing — RunImport
// resolves it via Store.ResolveBuildArtifact.
func publishFetcherBuildOutput(t *testing.T, ctx context.Context, st *amber.Store, fk key.Key, script string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "c"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "c", "fetch"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	out, err := st.IngestDir(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	f, err := st.IngestFile(ctx, []byte("from-tree stand-in for "+fk.String()))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutRef(ctx, "build-from:"+fk.String(), f); err != nil {
		t.Fatal(err)
	}
	if err := st.PutRef(ctx, "build-output:"+f.String(), out); err != nil {
		t.Fatal(err)
	}
}

func TestRunImport_FetcherDefResolvesByContent(t *testing.T) {
	st := buildEvalStore(t)
	ctx := context.Background()

	fb, err := builddef.FetcherBuild("https://example.com/fetcher-src.tar.gz", "aa", runner.Platform())
	if err != nil {
		t.Fatal(err)
	}
	fbCanon, err := fb.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	fk, err := (builddef.Input{Kind: builddef.KindBuild, Definition: fbCanon}).Key()
	if err != nil {
		t.Fatal(err)
	}
	publishFetcherBuildOutput(t, ctx, st, fk,
		"#!/bin/sh\necho built-by-def > \"$JOBS_OUTPUT_DIR/data.txt\"\n")

	p, err := importdef.CanonicalParams(map[string]any{"url": "https://example.com/x.tgz"})
	if err != nil {
		t.Fatal(err)
	}
	def := importdef.Definition{Fetcher: "declared", Params: p, Platform: runner.Platform(), FetcherDef: fbCanon}
	canon, err := def.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	k, err := st.IngestFile(ctx, canon)
	if err != nil {
		t.Fatal(err)
	}

	out := runner.RunImport(ctx, st, runner.NewLocalRefWriter(st), runner.Subprocess{}, t.TempDir(), k, nil, nil)
	if out.Failed || out.Decline || out.Cancelled {
		t.Fatalf("unexpected outcome: %+v", out)
	}
	if _, ok, err := st.GetKey(ctx, "import-output:"+k.String()); err != nil || !ok {
		t.Fatalf("import-output ref missing: ok=%v err=%v", ok, err)
	}
}

func TestRunImport_MissingNamedFetcherFailsHard(t *testing.T) {
	st := buildEvalStore(t)
	ctx := context.Background()
	k := ingestDef(t, ctx, st, "no-such-fetcher", nil)

	out := runner.RunImport(ctx, st, runner.NewLocalRefWriter(st), runner.Subprocess{}, t.TempDir(), k, nil, nil)
	if out.Decline {
		t.Fatalf("missing named fetcher must not decline (nothing will provision it): %+v", out)
	}
	if !out.Failed || out.Class != "hard" {
		t.Fatalf("want hard failure, got %+v", out)
	}
}
