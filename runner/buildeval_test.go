//go:build linux

package runner_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/amber-store/core/key"
	"github.com/fxamacker/cbor/v2"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/importdef"
	"github.com/jobs-build/jobs-iroh/runner"
	"github.com/jobs-build/jobs-iroh/sandbox"
)

// ingestSourceTree ingests a source directory into amber and returns its key.
func ingestSourceTree(t *testing.T, ctx context.Context, st *amber.Store, files map[string]string) key.Key {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	k, err := st.IngestDir(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// makeBuildDef creates a canonical build Definition and ingests it, publishing
// build:<K> and returning (defBytes, K).
func makeBuildDef(t *testing.T, ctx context.Context, st *amber.Store, sourceInput builddef.Input, platform string) (defBytes []byte, k key.Key) {
	t.Helper()
	params, err := importdef.CanonicalParams(nil)
	if err != nil {
		t.Fatal(err)
	}
	def := builddef.Definition{
		Source:   sourceInput,
		Dir:      "",
		Platform: platform,
		Params:   params,
	}
	canon, err := def.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	k, err = st.IngestFile(ctx, canon)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutRef(ctx, "build:"+k.String(), k); err != nil {
		t.Fatal(err)
	}
	return canon, k
}

// buildShellArtifact runs the hostshell fetcher and ingests the result.
func buildShellArtifact(t *testing.T, ctx context.Context, st *amber.Store) key.Key {
	t.Helper()
	out := t.TempDir()
	fetch, err := filepath.Abs("../fetchers/hostshell/fetch")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(fetch)
	cmd.Env = append(os.Environ(), "JOBS_OUTPUT_DIR="+out)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("hostshell fetcher failed: %v", err)
	}
	k, err := st.IngestDir(ctx, out)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// TestRunPluginResolve exercises RunPluginResolve end-to-end:
//   - source subtree contains a BUILD.jobs with plugins() returning one imp()
//   - RunPluginResolve should produce build-plugin-resolved:F with the plugins map
func TestRunPluginResolve(t *testing.T) {
	ctx := context.Background()
	st := buildEvalStore(t)
	platform := runner.Platform()

	// 1. Import definition for the source input.
	importParams, err := importdef.CanonicalParams(map[string]any{"url": "https://example.com/x.tgz"})
	if err != nil {
		t.Fatal(err)
	}
	srcImportDef := importdef.Definition{Fetcher: "test-fetcher", Params: importParams}
	srcImportCanon, err := srcImportDef.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	srcImportKey, err := st.IngestFile(ctx, srcImportCanon)
	if err != nil {
		t.Fatal(err)
	}
	sourceInput := builddef.Input{Kind: builddef.KindImport, Definition: srcImportCanon}

	// 2. Import definition for the plugin.
	pluginImportParams, err := importdef.CanonicalParams(map[string]any{"url": "https://example.com/plugin.tgz"})
	if err != nil {
		t.Fatal(err)
	}
	pluginImportDef := importdef.Definition{Fetcher: "plugin-fetcher", Params: pluginImportParams, Platform: platform}
	pluginImportCanon, err := pluginImportDef.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	// Ingest the plugin definition to satisfy objects-before-ref.
	if _, err := st.IngestFile(ctx, pluginImportCanon); err != nil {
		t.Fatal(err)
	}

	// 3. Source tree with BUILD.jobs that declares one plugin via imp().
	buildJobs := `
def plugins():
    return {"p": imp(fetcher="plugin-fetcher", params={"url": "https://example.com/plugin.tgz"})}

def build():
    return struct(inputs={}, env={}, script="echo hello", runtime_deps=[])
`
	sourceOutputKey := ingestSourceTree(t, ctx, st, map[string]string{
		"BUILD.jobs": buildJobs,
	})
	// Publish import-output:<srcImportKey> -> sourceOutputKey.
	if err := st.PutRef(ctx, "import-output:"+srcImportKey.String(), sourceOutputKey); err != nil {
		t.Fatal(err)
	}

	// 4. Build definition.
	_, buildK := makeBuildDef(t, ctx, st, sourceInput, platform)

	// 5. Run build-from to get F, then plugin-resolve keyed by F.
	brc := runner.BuildRunCfg{Platform: platform, CacheDir: t.TempDir()}
	bfOut := runner.RunBuildFrom(ctx, st, runner.NewLocalRefWriter(st), brc, buildK)
	if bfOut.Failed {
		t.Fatalf("RunBuildFrom failed: %+v", bfOut)
	}
	f := bfOut.OutputKey

	out := runner.RunPluginResolve(ctx, st, runner.NewLocalRefWriter(st), brc, f)
	if out.Decline || out.Cancelled {
		t.Fatalf("unexpected decline/cancel: %+v", out)
	}
	if out.Failed {
		t.Fatalf("RunPluginResolve failed: phase=%s class=%s stderr=%s", out.Phase, out.Class, out.Stderr)
	}
	if out.OutputKey == (key.Key{}) {
		t.Fatal("empty OutputKey")
	}

	// 6. Verify build-plugin-resolved:F exists and contains the plugin "p".
	gotKey, ok, err := st.GetKey(ctx, "build-plugin-resolved:"+f.String())
	if err != nil || !ok {
		t.Fatalf("build-plugin-resolved ref missing: ok=%v err=%v", ok, err)
	}
	if gotKey != out.OutputKey {
		t.Fatalf("ref key %s != reported %s", gotKey, out.OutputKey)
	}

	body, err := st.ReadFile(ctx, gotKey)
	if err != nil {
		t.Fatal(err)
	}
	pr, err := builddef.DecodePluginResolved(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(pr.Plugins) != 1 {
		t.Fatalf("expected 1 plugin, got %d: %v", len(pr.Plugins), pr.Plugins)
	}
	pInput, ok := pr.Plugins["p"]
	if !ok {
		t.Fatalf("plugin 'p' not found in plugins map: %v", pr.Plugins)
	}
	if pInput.Kind != builddef.KindImport {
		t.Fatalf("expected import kind, got %q", pInput.Kind)
	}
	// The plugin import def is pinned to the build's platform
	// (import-platform-pinning design; imp() pins at construction and the
	// RunPluginResolve normalization is the stage-level guarantee).
	pDef, err := importdef.Decode(pInput.Definition)
	if err != nil {
		t.Fatal(err)
	}
	if pDef.Platform != platform {
		t.Fatalf("plugin import Platform=%q want %q", pDef.Platform, platform)
	}
}

// TestRunPin exercises RunPin end-to-end:
//  1. Calls RunPluginResolve to produce build-plugin-resolved:F.
//  2. Calls RunPin with a plugin artifact (a shell script that returns a CBOR
//     list) inside a materialized-store user-namespace sandbox.
//  3. Verifies build-pinned:F is written, decodable, and DETERMINISTIC (second
//     RunPin produces a byte-identical ref).
//
// The plugin artifact uses a #!/jobs/shell/bin/bash shebang, which requires the
// shell artifact (vendored bash+coreutils via hostshell fetcher). This test is
// skipped if user namespaces are unavailable.
func TestRunPin(t *testing.T) {
	if !sandbox.UserNSAvailable() {
		t.Skip("user namespaces unavailable")
	}

	ctx := context.Background()
	st := buildEvalStore(t)
	platform := runner.Platform()

	// ---- Shell artifact (needed by SandboxedPluginCaller for /jobs/shell) ----
	shellKey := buildShellArtifact(t, ctx, st)

	// ---- Source import definition ----
	importParams, err := importdef.CanonicalParams(map[string]any{"url": "https://example.com/src.tgz"})
	if err != nil {
		t.Fatal(err)
	}
	srcImportDef := importdef.Definition{Fetcher: "test-fetcher", Params: importParams}
	srcImportCanon, err := srcImportDef.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	srcImportKey, err := st.IngestFile(ctx, srcImportCanon)
	if err != nil {
		t.Fatal(err)
	}
	sourceInput := builddef.Input{Kind: builddef.KindImport, Definition: srcImportCanon}

	// ---- Plugin import definition ----
	pluginImportParams, err := importdef.CanonicalParams(map[string]any{"url": "https://example.com/myplugin.tgz"})
	if err != nil {
		t.Fatal(err)
	}
	pluginImportDef := importdef.Definition{Fetcher: "plugin-fetcher", Params: pluginImportParams, Platform: platform}
	pluginImportCanon, err := pluginImportDef.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	pluginInput := builddef.Input{Kind: builddef.KindImport, Definition: pluginImportCanon}
	pluginImportK, err := pluginInput.Key()
	if err != nil {
		t.Fatal(err)
	}

	// ---- Plugin artifact: a bash script using #!/jobs/shell/bin/bash ----
	// The script drains stdin (CBOR request) and emits a CBOR list holding one
	// Input spec: an import naming "gomod" — platform-less, no FetcherDef —
	// exactly what a real plugin emits. The artifact also bundles fetchers.toml
	// pinning gomod, so the pin-stage rehydration must stamp the platform AND
	// inject the FetcherDef (recipe-declared-fetchers design §7).
	emittedParams, err := importdef.CanonicalParams(map[string]any{"module": "example.com/m", "version": "v1"})
	if err != nil {
		t.Fatal(err)
	}
	emittedDef, err := importdef.Definition{Fetcher: "gomod", Params: emittedParams}.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	pluginResponse, err := cbor.Marshal([]any{map[string]any{"kind": builddef.KindImport, "definition": emittedDef}})
	if err != nil {
		t.Fatal(err)
	}
	pluginScript := buildCBOREmitScript(pluginResponse)
	pluginDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(pluginDir, "plugin"), []byte(pluginScript), 0o755); err != nil {
		t.Fatal(err)
	}
	pluginPins := `[[fetcher]]
name   = "gomod"
url    = "https://example.com/fetcher-gomod.tar.gz"
sha256 = "ab12"
`
	if err := os.WriteFile(filepath.Join(pluginDir, "fetchers.toml"), []byte(pluginPins), 0o644); err != nil {
		t.Fatal(err)
	}
	pluginArtifactKey, err := st.IngestDir(ctx, pluginDir)
	if err != nil {
		t.Fatal(err)
	}
	// Publish import-output:<pluginImportK> -> pluginArtifactKey.
	if err := st.PutRef(ctx, "import-output:"+pluginImportK.String(), pluginArtifactKey); err != nil {
		t.Fatal(err)
	}

	// ---- BUILD.jobs ----
	// This file has BOTH def plugins(): and def build():.
	// EvalPlugins calls plugins() to get the plugin declaration.
	// EvalBuild strips def plugins(): from the AST before compiling (via
	// execRecipeForBuild in recipe.go), so plugins["p"] in build() resolves
	// to the injected pluginsMapping (the live callers), not the file function.
	//
	// The build() function:
	//   - calls plugins["p"](x=1) and uses its emitted import as an input (the
	//     bundled-pin FetcherDef injection path)
	//   - uses a direct imp() for a second input
	//   - asserts env and script are carried through to build-pinned:F
	buildJobs := `
def plugins():
    return {"p": imp(fetcher="plugin-fetcher", params={"url": "https://example.com/myplugin.tgz"})}

def build():
    mods = plugins["p"](x=1)  # emits one gomod import (see the artifact's fetchers.toml)
    dep = imp(fetcher="dep-fetcher", params={"url": "https://example.com/dep.tgz"})
    return struct(
        inputs={"dep": dep, "m": mods[0]},
        env={"FOO": "bar"},
        script="echo build",
        runtime_deps=[],
    )
`

	// ---- Source tree ----
	sourceOutputKey := ingestSourceTree(t, ctx, st, map[string]string{
		"BUILD.jobs": buildJobs,
	})
	if err := st.PutRef(ctx, "import-output:"+srcImportKey.String(), sourceOutputKey); err != nil {
		t.Fatal(err)
	}

	// ---- Build definition ----
	_, buildK := makeBuildDef(t, ctx, st, sourceInput, platform)

	// ---- BuildRunCfg ----
	brc := runner.BuildRunCfg{
		Platform: platform,
		ShellKey: shellKey,
		CacheDir: t.TempDir(),
	}

	// ---- Step 0: RunBuildFrom to get F ----
	bfCtx, bfCancel := context.WithTimeout(ctx, 30*time.Second)
	defer bfCancel()
	bfOut := runner.RunBuildFrom(bfCtx, st, runner.NewLocalRefWriter(st), brc, buildK)
	if bfOut.Decline || bfOut.Cancelled || bfOut.Failed {
		t.Fatalf("RunBuildFrom failed: %+v", bfOut)
	}
	f := bfOut.OutputKey

	// ---- Step 1: RunPluginResolve ----
	prCtx, prCancel := context.WithTimeout(ctx, 30*time.Second)
	defer prCancel()
	prOut := runner.RunPluginResolve(prCtx, st, runner.NewLocalRefWriter(st), brc, f)
	if prOut.Decline || prOut.Cancelled {
		t.Fatalf("unexpected decline/cancel from RunPluginResolve: %+v", prOut)
	}
	if prOut.Failed {
		t.Fatalf("RunPluginResolve failed: phase=%s class=%s stderr=%s", prOut.Phase, prOut.Class, prOut.Stderr)
	}

	// ---- Step 2: RunPin ----
	// Ingest the dep import definition before RunPin (objects-before-ref).
	depImportParams, err := importdef.CanonicalParams(map[string]any{"url": "https://example.com/dep.tgz"})
	if err != nil {
		t.Fatal(err)
	}
	depImportDef := importdef.Definition{Fetcher: "dep-fetcher", Params: depImportParams, Platform: platform}
	depImportCanon, err := depImportDef.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.IngestFile(ctx, depImportCanon); err != nil {
		t.Fatal(err)
	}

	pinCtx, pinCancel := context.WithTimeout(ctx, 60*time.Second)
	defer pinCancel()
	pinOut := runner.RunPin(pinCtx, st, runner.NewLocalRefWriter(st), brc, f)
	if pinOut.Decline || pinOut.Cancelled {
		t.Fatalf("unexpected decline/cancel from RunPin: %+v", pinOut)
	}
	if pinOut.Failed {
		t.Fatalf("RunPin failed: phase=%s class=%s stderr=%s", pinOut.Phase, pinOut.Class, pinOut.Stderr)
	}
	if pinOut.OutputKey == (key.Key{}) {
		t.Fatal("empty OutputKey from RunPin")
	}

	// ---- Verify build-pinned:F ----
	pinnedKey, ok, err := st.GetKey(ctx, "build-pinned:"+f.String())
	if err != nil || !ok {
		t.Fatalf("build-pinned ref missing: ok=%v err=%v", ok, err)
	}
	if pinnedKey != pinOut.OutputKey {
		t.Fatalf("ref key %s != reported %s", pinnedKey, pinOut.OutputKey)
	}

	pinnedBytes, err := st.ReadFile(ctx, pinnedKey)
	if err != nil {
		t.Fatal(err)
	}

	pinned, err := builddef.DecodePinned(pinnedBytes)
	if err != nil {
		t.Fatalf("DecodePinned: %v", err)
	}
	if pinned.Script != "echo build" {
		t.Fatalf("unexpected script: %q", pinned.Script)
	}
	if pinned.Env["FOO"] != "bar" {
		t.Fatalf("unexpected env: %v", pinned.Env)
	}
	if len(pinned.Inputs) != 2 {
		t.Fatalf("expected 2 inputs, got %d", len(pinned.Inputs))
	}
	// Every import entering Pinned.Inputs is pinned to the build's platform
	// (import-platform-pinning design).
	var emittedPinned *importdef.Definition
	for _, in := range pinned.Inputs {
		if in.Kind != builddef.KindImport {
			continue
		}
		def, err := importdef.Decode(in.Definition)
		if err != nil {
			t.Fatal(err)
		}
		if def.Platform != platform {
			t.Fatalf("pinned input %s: Platform=%q want %q", in.Name, def.Platform, platform)
		}
		if in.Name == "m" {
			d := def
			emittedPinned = &d
		}
	}
	// The plugin-emitted gomod import must carry the FetcherDef instantiated
	// from the artifact's bundled fetchers.toml (recipe-declared-fetchers §7).
	if emittedPinned == nil {
		t.Fatal("emitted input m missing from Pinned.Inputs")
	}
	wantFB, err := builddef.FetcherBuild("https://example.com/fetcher-gomod.tar.gz", "ab12", platform)
	if err != nil {
		t.Fatal(err)
	}
	wantFBCanon, err := wantFB.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(emittedPinned.FetcherDef, wantFBCanon) {
		t.Fatal("emitted import must carry the bundled pin's FetcherDef")
	}

	// ---- Determinism: second RunPin must produce a byte-identical ref ----
	pin2Ctx, pin2Cancel := context.WithTimeout(ctx, 60*time.Second)
	defer pin2Cancel()
	pinOut2 := runner.RunPin(pin2Ctx, st, runner.NewLocalRefWriter(st), brc, f)
	if pinOut2.Failed {
		t.Fatalf("RunPin (second run) failed: %+v", pinOut2)
	}
	if pinOut2.OutputKey != pinOut.OutputKey {
		t.Fatalf("non-deterministic RunPin: first=%s second=%s", pinOut.OutputKey, pinOut2.OutputKey)
	}
}

// buildCBOREmitScript returns a shell script that emits the given raw bytes to
// stdout and drains stdin. Uses #!/jobs/shell/bin/bash and printf with octal
// escapes (matching the pattern in buildjob_test.go).
func buildCBOREmitScript(data []byte) string {
	var buf bytes.Buffer
	buf.WriteString("#!/jobs/shell/bin/bash\ncat >/dev/null\nprintf '")
	for _, b := range data {
		buf.WriteString(`\`)
		buf.WriteString(octal3(b))
	}
	buf.WriteString("'\n")
	return buf.String()
}

func octal3(b byte) string {
	s := [3]byte{
		'0' + (b>>6)&7,
		'0' + (b>>3)&7,
		'0' + b&7,
	}
	return string(s[:])
}

// TestRunPluginResolve_PublishesDeps: a plugins() struct return declaring
// resolution deps must publish them in build-plugin-resolved:F, with each dep
// definition ingested (objects-before-ref).
func TestRunPluginResolve_PublishesDeps(t *testing.T) {
	ctx := context.Background()
	st := buildEvalStore(t)
	platform := runner.Platform()

	importParams, err := importdef.CanonicalParams(map[string]any{"url": "https://example.com/src.tgz"})
	if err != nil {
		t.Fatal(err)
	}
	srcImportDef := importdef.Definition{Fetcher: "test-fetcher", Params: importParams}
	srcImportCanon, err := srcImportDef.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	srcImportKey, err := st.IngestFile(ctx, srcImportCanon)
	if err != nil {
		t.Fatal(err)
	}
	sourceInput := builddef.Input{Kind: builddef.KindImport, Definition: srcImportCanon}

	buildJobs := `
def plugins():
    return struct(
        deps = {"aux": imp(fetcher = "tarball+https", params = {"url": "https://e/aux.tgz"})},
    )

def build():
    return struct(inputs = {}, env = {}, script = "", runtime_deps = [])
`
	sourceOutputKey := ingestSourceTree(t, ctx, st, map[string]string{"BUILD.jobs": buildJobs})
	if err := st.PutRef(ctx, "import-output:"+srcImportKey.String(), sourceOutputKey); err != nil {
		t.Fatal(err)
	}

	_, buildK := makeBuildDef(t, ctx, st, sourceInput, platform)
	brc := runner.BuildRunCfg{Platform: platform, CacheDir: t.TempDir()}
	bfOut := runner.RunBuildFrom(ctx, st, runner.NewLocalRefWriter(st), brc, buildK)
	if bfOut.Failed {
		t.Fatalf("RunBuildFrom failed: %+v", bfOut)
	}
	f := bfOut.OutputKey

	out := runner.RunPluginResolve(ctx, st, runner.NewLocalRefWriter(st), brc, f)
	if out.Failed {
		t.Fatalf("RunPluginResolve failed: phase=%s stderr=%s", out.Phase, out.Stderr)
	}

	gotKey, ok, err := st.GetKey(ctx, "build-plugin-resolved:"+f.String())
	if err != nil || !ok {
		t.Fatalf("build-plugin-resolved ref missing: ok=%v err=%v", ok, err)
	}
	body, err := st.ReadFile(ctx, gotKey)
	if err != nil {
		t.Fatal(err)
	}
	pr, err := builddef.DecodePluginResolved(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(pr.Deps) != 1 || pr.Deps["aux"].Kind != builddef.KindImport {
		t.Fatalf("deps not published: %+v", pr)
	}
	// The dep definition object must be present in the store.
	depK, err := pr.Deps["aux"].Key()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.ReadFile(ctx, depK); err != nil {
		t.Fatalf("dep definition not ingested: %v", err)
	}
}

// TestRunPin_MaterializesResolutionDeps: the pin stage must materialize each
// declared resolution dep's output and expose it to build() via the deps
// handle (read + path) — resolution-deps design §6.2.
func TestRunPin_MaterializesResolutionDeps(t *testing.T) {
	ctx := context.Background()
	st := buildEvalStore(t)
	platform := runner.Platform()

	// ---- Source import ----
	importParams, err := importdef.CanonicalParams(map[string]any{"url": "https://example.com/depsrc.tgz"})
	if err != nil {
		t.Fatal(err)
	}
	srcImportDef := importdef.Definition{Fetcher: "test-fetcher", Params: importParams}
	srcImportCanon, err := srcImportDef.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	srcImportKey, err := st.IngestFile(ctx, srcImportCanon)
	if err != nil {
		t.Fatal(err)
	}
	sourceInput := builddef.Input{Kind: builddef.KindImport, Definition: srcImportCanon}

	// ---- The dep import (what imp() in the recipe constructs: platform-pinned) ----
	depParams, err := importdef.CanonicalParams(map[string]any{"url": "https://e/aux2.tgz"})
	if err != nil {
		t.Fatal(err)
	}
	depCanon, err := importdef.Definition{Fetcher: "tarball+https", Params: depParams, Platform: platform}.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	depK, err := (builddef.Input{Kind: builddef.KindImport, Definition: depCanon}).Key()
	if err != nil {
		t.Fatal(err)
	}
	// Its output: a tree with data.txt.
	depOut := ingestSourceTree(t, ctx, st, map[string]string{"data.txt": "hello-dep"})
	if err := st.PutRef(ctx, "import-output:"+depK.String(), depOut); err != nil {
		t.Fatal(err)
	}

	buildJobs := `
def plugins():
    return struct(deps = {"aux": imp(fetcher = "tarball+https", params = {"url": "https://e/aux2.tgz"})})

def build():
    return struct(
        inputs = {},
        env = {"AUX": str(deps["aux"].read("data.txt")), "AUXPATH": deps["aux"].path("data.txt")},
        script = "",
        runtime_deps = [],
    )
`
	sourceOutputKey := ingestSourceTree(t, ctx, st, map[string]string{"BUILD.jobs": buildJobs})
	if err := st.PutRef(ctx, "import-output:"+srcImportKey.String(), sourceOutputKey); err != nil {
		t.Fatal(err)
	}

	_, buildK := makeBuildDef(t, ctx, st, sourceInput, platform)
	brc := runner.BuildRunCfg{Platform: platform, CacheDir: t.TempDir()}
	rw := runner.NewLocalRefWriter(st)
	bfOut := runner.RunBuildFrom(ctx, st, rw, brc, buildK)
	if bfOut.Failed {
		t.Fatalf("RunBuildFrom failed: %+v", bfOut)
	}
	f := bfOut.OutputKey
	if out := runner.RunPluginResolve(ctx, st, rw, brc, f); out.Failed {
		t.Fatalf("RunPluginResolve failed: %+v", out)
	}

	out := runner.RunPin(ctx, st, rw, brc, f)
	if out.Failed {
		t.Fatalf("RunPin failed: phase=%s class=%s stderr=%s", out.Phase, out.Class, out.Stderr)
	}

	pinnedKey, ok, err := st.GetKey(ctx, "build-pinned:"+f.String())
	if err != nil || !ok {
		t.Fatalf("build-pinned ref missing: ok=%v err=%v", ok, err)
	}
	b, err := st.ReadFile(ctx, pinnedKey)
	if err != nil {
		t.Fatal(err)
	}
	pinned, err := builddef.DecodePinned(b)
	if err != nil {
		t.Fatal(err)
	}
	if pinned.Env["AUX"] != "hello-dep" {
		t.Fatalf("env AUX = %q, want hello-dep", pinned.Env["AUX"])
	}
	if pinned.Env["AUXPATH"] != "/jobs/deps/aux/data.txt" {
		t.Fatalf("env AUXPATH = %q", pinned.Env["AUXPATH"])
	}
}
