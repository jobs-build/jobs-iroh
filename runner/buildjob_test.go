//go:build linux

package runner_test

import (
	"archive/tar"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/cover"
	"github.com/jobs-build/jobs-iroh/importdef"
	"github.com/jobs-build/jobs-iroh/runner"
	"github.com/jobs-build/jobs-iroh/sandbox"
)

// --- SandboxedPluginCaller e2e ---
// Proves the eval-side hermetic plugin call end to end: MaterializeSource lays
// a source subtree on disk, SandboxedPluginCaller materializes the plugin
// artifact + shell read-only at /jobs/store, binds the source read-only, runs
// the plugin BINARY net-free with the CBOR request on stdin, and decodes the
// CBOR list it writes to stdout. (jobs' Ginkgo Describe, converted to plain
// tests — jobs-iroh's runner suite is plain `testing`.)

// ingestPlugin writes an executable ./plugin script into a fresh dir and
// ingests it, yielding the PluginKey.
func ingestPlugin(t *testing.T, ctx context.Context, st *amber.Store, script string) key.Key {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "plugin"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	k, err := st.IngestDir(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// materializeSourceDir lays a tiny source tree on disk via MaterializeSource
// (going through an ingested source subtree, like the real eval path).
func materializeSourceDir(t *testing.T, ctx context.Context, st *amber.Store) (root string) {
	t.Helper()
	srcIn := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcIn, "marker.txt"), []byte("src"), 0o644); err != nil {
		t.Fatal(err)
	}
	srcKey, err := st.IngestDir(ctx, srcIn)
	if err != nil {
		t.Fatal(err)
	}
	root, _, cleanup, err := runner.MaterializeSource(ctx, st, srcKey, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cleanup)
	return root
}

// TestSandboxedPluginCaller_NetFree: the plugin's #!/jobs/shell/bin/bash
// shebang resolves bash INSIDE the sandbox (via the /jobs/shell compat
// symlink). The script drains the CBOR request on stdin and emits a CBOR list
// encoding whether 8.8.8.8:53 was reachable:
//
//	net BLOCKED (the hermetic, expected case) => ["ok"]   (\201 \142 'o' 'k')
//	net OPEN    (a sandbox failure)           => ["open"]
//
// Returning ["ok"] therefore proves BOTH the round-trip and net=none.
func TestSandboxedPluginCaller_NetFree(t *testing.T) {
	if !sandbox.UserNSAvailable() {
		t.Skip("user namespaces unavailable")
	}
	ctx := context.Background()
	st := buildEvalStore(t)
	shellKey := buildShellArtifact(t, ctx, st)

	pluginKey := ingestPlugin(t, ctx, st, "#!/jobs/shell/bin/bash\n"+
		"cat >/dev/null\n"+
		"if ( exec 3<>/dev/tcp/8.8.8.8/53 ) 2>/dev/null; then\n"+
		"  printf '\\201\\144open'\n"+
		"else\n"+
		"  printf '\\201\\142ok'\n"+
		"fi\n")

	sourceDir := materializeSourceDir(t, ctx, st)

	cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	caller := runner.SandboxedPluginCaller{
		Cl:        st,
		PluginKey: pluginKey,
		SourceDir: sourceDir,
		ShellKey:  shellKey,
		Ctx:       cctx,
	}

	resp, err := caller.Call(map[string]any{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	list, ok := resp.([]any)
	if !ok {
		t.Fatalf("response must be a CBOR list, got %#v", resp)
	}
	if len(list) != 1 {
		t.Fatalf("response list length %d, want 1: %#v", len(list), list)
	}
	// ["ok"] (not ["open"]) proves the plugin ran AND the network was blocked.
	if list[0] != "ok" {
		t.Fatalf("plugin must observe net=none (got %#v)", resp)
	}
}

// TestSandboxedPluginCaller_DepMounts: proves BOTH exposure paths of
// resolution-deps design §3.3: the tree is mounted read-only at
// /jobs/deps/aux (cat), and the CBOR request carries the in-sandbox path
// (grep -a for the path string in the raw request bytes saved to the
// writable /tmp).
// NOTE: the plugin sandbox has no /dev (unlike the build sandbox), so the
// script must not redirect to /dev/null — a failed redirect aborts the
// command substitution.
func TestSandboxedPluginCaller_DepMounts(t *testing.T) {
	if !sandbox.UserNSAvailable() {
		t.Skip("user namespaces unavailable")
	}
	ctx := context.Background()
	st := buildEvalStore(t)
	shellKey := buildShellArtifact(t, ctx, st)

	pluginKey := ingestPlugin(t, ctx, st, "#!/jobs/shell/bin/bash\n"+
		"cat > /tmp/req\n"+
		"if [ \"$(cat /jobs/deps/aux/data.txt)\" != \"hello-dep\" ]; then\n"+
		"  printf '\\201\\145nomnt'\n"+
		"elif ! grep -q '/jobs/deps/aux' /tmp/req; then\n"+
		"  printf '\\201\\144norq'\n"+
		"else\n"+
		"  printf '\\201\\142ok'\n"+
		"fi\n")

	depDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(depDir, "data.txt"), []byte("hello-dep"), 0o644); err != nil {
		t.Fatal(err)
	}

	cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	caller := runner.SandboxedPluginCaller{
		Cl:        st,
		PluginKey: pluginKey,
		SourceDir: materializeSourceDir(t, ctx, st),
		ShellKey:  shellKey,
		Ctx:       cctx,
		DepDirs:   map[string]string{"aux": depDir},
	}

	resp, err := caller.Call(map[string]any{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	list, ok := resp.([]any)
	if !ok {
		t.Fatalf("response must be a CBOR list, got %#v", resp)
	}
	if len(list) != 1 || list[0] != "ok" {
		t.Fatalf("plugin must see the dep mount + request path (got %#v)", resp)
	}
}

// --- RunBuild ---
// Plain Test* functions. They construct a real build-pinned:F directly —
// ingest a builddef.Pinned with one import input, an env, and a $out-writing
// script — publish build:K + build-pinned:F with the matching import-output
// trees present, then drive RunBuild with the real NamespaceBuildExecutor and
// assert the finalized c/ tree.

// readTarEntry reads the file at relPath ("c/result") from the tree rooted at
// root, returning its bytes. Fails the test if absent.
func readTarEntry(t *testing.T, ctx context.Context, st *amber.Store, root key.Key, relPath string) []byte {
	t.Helper()
	rc, err := st.Tar(ctx, root, "")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	want := filepath.Clean(relPath)
	tr := tar.NewReader(rc)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			t.Fatalf("%s not found in tree", relPath)
		}
		if err != nil {
			t.Fatal(err)
		}
		if filepath.Clean(hdr.Name) == want {
			b, err := io.ReadAll(tr)
			if err != nil {
				t.Fatal(err)
			}
			return b
		}
	}
}

// mkImportInputWithOutput creates an import definition with the given fetcher and
// URL, ingests its definition, ingests a one-file output tree, and publishes
// import-output:<K> for it. Returns the Input and its identity K.
func mkImportInputWithOutput(t *testing.T, ctx context.Context, st *amber.Store, fetcher, url, fileName, fileContent string) (builddef.Input, key.Key) {
	t.Helper()
	params, err := importdef.CanonicalParams(map[string]any{"url": url})
	if err != nil {
		t.Fatal(err)
	}
	def := importdef.Definition{Fetcher: fetcher, Params: params}
	canon, err := def.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.IngestFile(ctx, canon); err != nil {
		t.Fatal(err)
	}
	in := builddef.Input{Kind: builddef.KindImport, Definition: canon}
	k, err := in.Key()
	if err != nil {
		t.Fatal(err)
	}
	// Output tree for this import.
	outDir := t.TempDir()
	if fileName != "" {
		if err := os.WriteFile(filepath.Join(outDir, fileName), []byte(fileContent), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	outKey, err := st.IngestDir(ctx, outDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutRef(ctx, "import-output:"+k.String(), outKey); err != nil {
		t.Fatal(err)
	}
	return in, k
}

// putPinnedBuild assembles build:K (the pre-pinning definition) for the given
// source input, runs RunBuildFrom to get F, ingests+publishes build-pinned:F
// with the given inputs/env/script/runtimeDeps, then derives KP the production
// way (cover.Derive + the build-pinned:<KP> alias — sibling-sources design
// §10.1: RunBuild is KP-keyed). Returns (buildK, KP, defBytes) — the returned
// key is what RunBuild consumes and what build-output is published under.
func putPinnedBuild(t *testing.T, ctx context.Context, st *amber.Store, sourceInput builddef.Input, platform string, pinned builddef.Pinned) (buildK key.Key, kp key.Key, defBytes []byte) {
	t.Helper()
	defBytes, buildK = makeBuildDef(t, ctx, st, sourceInput, platform)

	// Run build-from to get F (the content-addressed env tree).
	brc := runner.BuildRunCfg{Platform: platform, CacheDir: t.TempDir()}
	bfOut := runner.RunBuildFrom(ctx, st, runner.NewLocalRefWriter(st), brc, buildK)
	if bfOut.Failed || bfOut.Decline || bfOut.Cancelled {
		t.Fatalf("RunBuildFrom: %+v", bfOut)
	}
	f := bfOut.OutputKey

	// Ingest each pinned input's definition (objects-before-ref, as RunPin does).
	for _, in := range pinned.Inputs {
		if _, err := st.IngestFile(ctx, in.Definition); err != nil {
			t.Fatal(err)
		}
	}
	pinnedBytes, err := builddef.EncodePinned(pinned)
	if err != nil {
		t.Fatal(err)
	}
	r, err := st.IngestFile(ctx, pinnedBytes)
	if err != nil {
		t.Fatal(err)
	}
	// Publish build-pinned:F (keyed by F, not by buildK).
	if err := st.PutRef(ctx, "build-pinned:"+f.String(), r); err != nil {
		t.Fatal(err)
	}
	// Derive KP + the build-pinned:<KP> alias, as pin commit / driveFStages do.
	envKey, ok, err := st.TreeSubdir(ctx, f, "env")
	if err != nil || !ok {
		t.Fatalf("F-tree env: ok=%v err=%v", ok, err)
	}
	kp, err = cover.Derive(ctx, st, pinnedBytes, pinned, platform, envKey)
	if err != nil {
		t.Fatalf("cover.Derive: %v", err)
	}
	if err := st.PutRef(ctx, "build-pinned:"+kp.String(), r); err != nil {
		t.Fatal(err)
	}
	return buildK, kp, defBytes
}

// TestRunBuild drives the full build run stage: a real build-pinned:F with one
// import input and a toolchain-less script that writes $out/result, run through
// the NamespaceBuildExecutor (materialized /jobs/store — jobs-iroh's only
// mode). Asserts build-output:F exists and its c/result holds the script's
// bytes.
func TestRunBuild(t *testing.T) {
	if !sandbox.UserNSAvailable() {
		t.Skip("user namespaces unavailable")
	}

	ctx := context.Background()
	st := buildEvalStore(t)
	platform := runner.Platform()
	shellKey := buildShellArtifact(t, ctx, st)

	// Source: an import whose output tree carries a BUILD.jobs (required by
	// RunBuildFrom to construct F). Content is irrelevant to the script.
	srcInput, _ := mkImportInputWithOutput(t, ctx, st, "src-fetcher", "https://example.com/src.tgz",
		"BUILD.jobs", "def build(): return struct(inputs={}, env={}, script='', runtime_deps=[])\n")

	// One build input: an import artifact present in the /jobs/store union.
	depInput, _ := mkImportInputWithOutput(t, ctx, st, "dep-fetcher", "https://example.com/dep.tgz", "data", "depdata")

	pinned := builddef.Pinned{
		Inputs: builddef.CanonicalPinnedInputs([]builddef.PinnedInput{
			{Name: "dep", Kind: depInput.Kind, Definition: depInput.Definition},
		}),
		Env:         map[string]string{"GREETING": "ok"},
		Script:      `printf '%s' "$GREETING" > "$out/result"`,
		RuntimeDeps: nil,
	}
	_, f, _ := putPinnedBuild(t, ctx, st, srcInput, platform, pinned)

	brc := runner.BuildRunCfg{Platform: platform, ShellKey: shellKey, CacheDir: t.TempDir()}
	runCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	out := runner.RunBuild(runCtx, st, runner.NewLocalRefWriter(st), brc, runner.NamespaceBuildExecutor{}, f)
	if out.Decline || out.Cancelled {
		t.Fatalf("unexpected decline/cancel: %+v", out)
	}
	if out.Failed {
		t.Fatalf("RunBuild failed: phase=%s class=%s exit=%d stderr=%s", out.Phase, out.Class, out.ExitCode, out.Stderr)
	}
	if out.OutputKey == (key.Key{}) {
		t.Fatal("empty OutputKey")
	}

	// build-output:F (not build-output:K) is published.
	gotKey, ok, err := st.GetKey(ctx, "build-output:"+f.String())
	if err != nil || !ok {
		t.Fatalf("build-output ref missing: ok=%v err=%v", ok, err)
	}
	if gotKey != out.OutputKey {
		t.Fatalf("ref key %s != reported %s", gotKey, out.OutputKey)
	}

	// c/result holds what the script wrote.
	if got := string(readTarEntry(t, ctx, st, gotKey, "c/result")); got != "ok" {
		t.Fatalf("c/result = %q, want %q", got, "ok")
	}
}

// TestRunBuild_BuildOutputDeps asserts the materialized runtime closure: a
// pinned build whose runtimeDeps lists one import input must publish
// build-output-deps:F — a /jobs/store tree containing exactly that import's BOK
// (its import-output key; an import dep is a leaf).
func TestRunBuild_BuildOutputDeps(t *testing.T) {
	if !sandbox.UserNSAvailable() {
		t.Skip("user namespaces unavailable")
	}

	ctx := context.Background()
	st := buildEvalStore(t)
	platform := runner.Platform()
	shellKey := buildShellArtifact(t, ctx, st)

	// Source must include BUILD.jobs for RunBuildFrom to succeed.
	srcInput, _ := mkImportInputWithOutput(t, ctx, st, "src-fetcher", "https://example.com/src2.tgz",
		"BUILD.jobs", "def build(): return struct(inputs={}, env={}, script='', runtime_deps=[])\n")
	depInput, depK := mkImportInputWithOutput(t, ctx, st, "rt-fetcher", "https://example.com/rt.tgz", "lib", "rtdata")
	depOutKey, ok, err := st.GetKey(ctx, "import-output:"+depK.String())
	if err != nil || !ok {
		t.Fatalf("dep import-output missing: ok=%v err=%v", ok, err)
	}

	pinned := builddef.Pinned{
		Inputs: builddef.CanonicalPinnedInputs([]builddef.PinnedInput{
			{Name: "lib", Kind: depInput.Kind, Definition: depInput.Definition},
		}),
		Env:         map[string]string{},
		Script:      `printf '%s' done > "$out/result"`,
		RuntimeDeps: builddef.SortKeys([][]byte{depK[:]}),
	}
	_, f, _ := putPinnedBuild(t, ctx, st, srcInput, platform, pinned)

	brc := runner.BuildRunCfg{Platform: platform, ShellKey: shellKey, CacheDir: t.TempDir()}
	runCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	out := runner.RunBuild(runCtx, st, runner.NewLocalRefWriter(st), brc, runner.NamespaceBuildExecutor{}, f)
	if out.Failed || out.Decline || out.Cancelled {
		t.Fatalf("RunBuild failed: %+v", out)
	}

	// build-output-deps:F is published and is a store tree with exactly the dep's
	// BOK (= its import-output key) at /jobs/store/<BOK>.
	depsKey, ok, err := st.GetKey(ctx, "build-output-deps:"+f.String())
	if err != nil || !ok {
		t.Fatalf("build-output-deps ref missing: ok=%v err=%v", ok, err)
	}
	ents, err := st.Ls(ctx, depsKey, "")
	if err != nil {
		t.Fatalf("ls build-output-deps: %v", err)
	}
	if len(ents) != 1 {
		t.Fatalf("expected 1 closure entry, got %d: %+v", len(ents), ents)
	}
	if ents[0].Name != depOutKey.String() {
		t.Fatalf("closure entry name = %q, want BOK %q", ents[0].Name, depOutKey.String())
	}
	if ents[0].Key != depOutKey {
		t.Fatalf("closure entry points at %q, want %q", ents[0].Key, depOutKey)
	}
}
