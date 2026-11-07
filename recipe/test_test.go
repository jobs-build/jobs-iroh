package recipe

// Test-recipe evaluation (tests design §3): test() entry, the predeclared
// `build` input and its exact definition, the runtime_deps rejection, and
// the build()-shadowing guard.

import (
	"bytes"
	"strings"
	"testing"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/importdef"
)

func fakeTreeKey(t *testing.T, payload string) key.Key {
	t.Helper()
	k, err := key.New(key.Blob, uint64(len(payload)), []byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

const testsRecipe = `
def plugins():
    return {}

def test():
    tc = imp(fetcher = "tarball+https", params = {"url": "https://e/tc.tgz"})
    return struct(
        inputs = {"app": build, "toolchain": tc},
        env = {"GOFLAGS": "-mod=mod"},
        script = "go test ./...",
        resources = struct(cpu = "2000m"),
        name = "unit tests",
    )
`

func TestEvalTest_BuildInputIsThisDirectorysBuild(t *testing.T) {
	ctxKey := fakeTreeKey(t, "context tree")
	rootKey := fakeTreeKey(t, "build root subtree")
	c := cfg(t, MapSource{})
	c.ContextKey, c.SourceContentKey, c.Dir = ctxKey, rootKey, "services/api"

	res, err := EvalTest(c, []byte(testsRecipe), nil)
	if err != nil {
		t.Fatalf("EvalTest: %v", err)
	}
	if len(res.Inputs) != 2 || res.Script != "go test ./..." || res.Name != "unit tests" || res.Resources == nil || res.Resources.CPUMilli != 2000 {
		t.Fatalf("result = %+v", res)
	}
	if len(res.RuntimeDeps) != 0 {
		t.Fatalf("runtime deps = %v, want none", res.RuntimeDeps)
	}
	var app builddef.Input
	for _, in := range res.Inputs {
		if in.Name == "app" {
			app = in.In
		}
	}
	if app.Kind != builddef.KindBuild {
		t.Fatalf("app input kind = %q", app.Kind)
	}
	// Field for field the widened definition clientcli's treeDefinition
	// builds for (context root, dir, platform, params): tree source over the
	// WHOLE context, dir, ctx: 2, the test's own params, default recipe.
	ti, err := builddef.TreeInput(ctxKey)
	if err != nil {
		t.Fatal(err)
	}
	want, err := builddef.Definition{Source: ti, Dir: "services/api", Platform: c.Platform, Params: c.Params, Ctx: builddef.CtxWidened}.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(app.Definition, want) {
		t.Fatalf("build input definition differs from the directory's build definition:\n got %x\nwant %x", app.Definition, want)
	}
}

func TestEvalTest_RootBuildInput(t *testing.T) {
	rootKey := fakeTreeKey(t, "root tree")
	c := cfg(t, MapSource{})
	c.SourceContentKey = rootKey // root test: no widened context, Dir ""

	res, err := EvalTest(c, []byte(testsRecipe), nil)
	if err != nil {
		t.Fatalf("EvalTest: %v", err)
	}
	var app builddef.Input
	for _, in := range res.Inputs {
		if in.Name == "app" {
			app = in.In
		}
	}
	ti, err := builddef.TreeInput(rootKey)
	if err != nil {
		t.Fatal(err)
	}
	want, err := builddef.Definition{Source: ti, Platform: c.Platform, Params: c.Params}.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(app.Definition, want) {
		t.Fatalf("root build input definition mismatch:\n got %x\nwant %x", app.Definition, want)
	}
}

func TestEvalTest_BuildUnavailableWithoutKeys(t *testing.T) {
	_, err := EvalTest(cfg(t, MapSource{}), []byte(testsRecipe), nil)
	if err == nil || !strings.Contains(err.Error(), "build is unavailable") {
		t.Fatalf("err = %v, want a build-unavailable error", err)
	}
}

func TestEvalTest_PluginsStageSeesBuild(t *testing.T) {
	// plugins() compiles the whole file: `build` must resolve there too.
	c := cfg(t, MapSource{})
	c.Test = true
	res, err := EvalPlugins(c, []byte(testsRecipe))
	if err != nil {
		t.Fatalf("EvalPlugins in test mode: %v", err)
	}
	if len(res.Plugins) != 0 {
		t.Fatalf("plugins = %v", res.Plugins)
	}
	// Without test mode the name is undefined — a TESTS.jobs cannot be
	// evaluated as a build.
	if _, err := EvalPlugins(cfg(t, MapSource{}), []byte(testsRecipe)); err == nil {
		t.Fatal("build resolved outside test mode")
	}
}

func TestEvalTest_RejectsRuntimeDepsAndTuples(t *testing.T) {
	c := cfg(t, MapSource{})
	c.SourceContentKey = fakeTreeKey(t, "r")
	_, err := EvalTest(c, []byte(`
def test():
    return struct(inputs = {}, env = {}, script = "x", runtime_deps = [])
`), nil)
	if err == nil || !strings.Contains(err.Error(), "runtime_deps") {
		t.Fatalf("err = %v, want a runtime_deps rejection", err)
	}
	_, err = EvalTest(c, []byte(`
def test():
    return ({}, {}, "x", [])
`), nil)
	if err == nil || !strings.Contains(err.Error(), "must return a struct") {
		t.Fatalf("err = %v, want a tuple rejection", err)
	}
}

func TestEvalTest_MissingTestAndBuildDefGuard(t *testing.T) {
	c := cfg(t, MapSource{})
	c.SourceContentKey = fakeTreeKey(t, "r")
	_, err := EvalTest(c, []byte(`
def build():
    return struct(inputs = {}, env = {}, script = "x", runtime_deps = [])
`), nil)
	if err == nil || !strings.Contains(err.Error(), "must not define build()") {
		t.Fatalf("err = %v, want the build() guard", err)
	}
	_, err = EvalTest(c, []byte("x = 1\n"), nil)
	if err == nil || !strings.Contains(err.Error(), "no test() function") {
		t.Fatalf("err = %v, want a missing-test() error", err)
	}
}

func TestEvalBuild_UnaffectedByTestMode(t *testing.T) {
	// A BUILD.jobs still evaluates exactly as before: no `build` predeclared
	// (the def is the only build), runtime_deps required.
	p, err := importdef.CanonicalParams(nil)
	if err != nil {
		t.Fatal(err)
	}
	c := EvalConfig{Platform: "linux/amd64", Params: p, Source: MapSource{}}
	res, err := EvalBuild(c, []byte(`
def build():
    return struct(inputs = {}, env = {}, script = "b", runtime_deps = [])
`), nil)
	if err != nil || res.Script != "b" {
		t.Fatalf("EvalBuild: %v (%+v)", err, res)
	}
	if _, err := EvalBuild(c, []byte(`
def build():
    return struct(inputs = {}, env = {}, script = "b")
`), nil); err == nil || !strings.Contains(err.Error(), "runtime_deps") {
		t.Fatalf("err = %v, want missing runtime_deps", err)
	}
}

func TestEvalTest_Timeout(t *testing.T) {
	c := cfg(t, MapSource{})
	c.SourceContentKey = fakeTreeKey(t, "r")
	eval := func(timeout string) (BuildResult, error) {
		return EvalTest(c, []byte(`
def test():
    return struct(inputs = {}, env = {}, script = "x", timeout = `+timeout+`)
`), nil)
	}
	// Omitted ⇒ 0 (the runner's default); Go duration syntax ⇒ whole seconds.
	res, err := EvalTest(c, []byte(`
def test():
    return struct(inputs = {}, env = {}, script = "x")
`), nil)
	if err != nil || res.TimeoutSec != 0 {
		t.Fatalf("omitted: %v (%d)", err, res.TimeoutSec)
	}
	for in, want := range map[string]int64{`"90s"`: 90, `"10m"`: 600, `"1h30m"`: 5400} {
		res, err := eval(in)
		if err != nil || res.TimeoutSec != want {
			t.Errorf("timeout %s: %v, got %d want %d", in, err, res.TimeoutSec, want)
		}
	}
	for in, want := range map[string]string{
		`"soon"`:  "timeout",
		`"0s"`:    "at least 1s",
		`"500ms"`: "at least 1s",
		`"1.5s"`:  "whole number of seconds",
		`90`:      "duration string",
	} {
		if _, err := eval(in); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("timeout %s: err = %v, want %q", in, err, want)
		}
	}

	// build() rejects the field rather than ignoring it.
	p, err := importdef.CanonicalParams(nil)
	if err != nil {
		t.Fatal(err)
	}
	bc := EvalConfig{Platform: "linux/amd64", Params: p, Source: MapSource{}}
	if _, err := EvalBuild(bc, []byte(`
def build():
    return struct(inputs = {}, env = {}, script = "b", runtime_deps = [], timeout = "10m")
`), nil); err == nil || !strings.Contains(err.Error(), "build() must not declare timeout") {
		t.Fatalf("build() timeout: err = %v, want a rejection", err)
	}
}
