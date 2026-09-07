package recipe

import (
	"strings"
	"testing"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/importdef"
	"go.starlark.net/starlark"
)

func TestNormalizeSourcePath(t *testing.T) {
	cases := []struct {
		p, dir, want, wantErr string
	}{
		{"//lib/common", "services/api", "lib/common", ""},
		{"//pom.xml", "modules/core", "pom.xml", ""},
		{"../../lib/common", "services/api", "lib/common", ""},
		{"proto", "services/api", "services/api/proto", ""},
		{"..", "services", "", "context root"},
		{"//.", "x", "", "context root"},
		{"//..", "x", "", "escapes"},
		{"../../../up", "a/b", "", "escapes"},
		{"/abs", "x", "", "absolute"},
		{"", "x", "", "non-empty"},
		{"//lib/../lib/common", "x", "lib/common", ""},
	}
	for _, c := range cases {
		got, err := NormalizeSourcePath(c.p, c.dir)
		if c.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("NormalizeSourcePath(%q, %q) err = %v, want containing %q", c.p, c.dir, err, c.wantErr)
			}
			continue
		}
		if err != nil {
			t.Errorf("NormalizeSourcePath(%q, %q): %v", c.p, c.dir, err)
			continue
		}
		if got != c.want {
			t.Errorf("NormalizeSourcePath(%q, %q) = %q, want %q", c.p, c.dir, got, c.want)
		}
	}
}

// widenedCfg is a build-stage eval config with a (fake) widened context.
func widenedCfg(t *testing.T, dir string) EvalConfig {
	t.Helper()
	p, err := importdef.CanonicalParams(nil)
	if err != nil {
		t.Fatal(err)
	}
	var ck key.Key
	ck[0] = 0xaa
	return EvalConfig{
		Platform:   "linux/amd64",
		Params:     p,
		Source:     MapSource{},
		ContextKey: ck,
		Dir:        dir,
	}
}

func subbuildDef(t *testing.T, sb *starlark.Builtin, dir string) builddef.Definition {
	t.Helper()
	v := callBuiltin(t, sb, []starlark.Tuple{{starlark.String("dir"), starlark.String(dir)}})
	in, ok := v.(*Input)
	if !ok {
		t.Fatalf("subbuild returned %T", v)
	}
	def, err := builddef.DecodeDefinition(in.In.Definition)
	if err != nil {
		t.Fatal(err)
	}
	return def
}

func subbuildErr(sb *starlark.Builtin, dir string) error {
	_, err := starlark.Call(&starlark.Thread{Name: "t"}, sb, nil,
		[]starlark.Tuple{{starlark.String("dir"), starlark.String(dir)}})
	return err
}

func TestEvalBuildSourcesAndGenerated(t *testing.T) {
	recipeSrc := []byte(`
def build():
    return struct(
        inputs = {},
        env = {},
        script = "true",
        runtime_deps = [],
        sources = ["//lib/common", "../shared", "protos"],
        generated = {"//Cargo.lock": "pruned-lock-bytes"},
        sources_allow_escaping = ["//lib/common/weird-link"],
    )
`)
	res, err := EvalBuild(widenedCfg(t, "services/api"), recipeSrc, nil)
	if err != nil {
		t.Fatalf("EvalBuild: %v", err)
	}
	wantSources := []string{"lib/common", "services/shared", "services/api/protos"}
	if len(res.Sources) != len(wantSources) {
		t.Fatalf("Sources = %v, want %v", res.Sources, wantSources)
	}
	for i, w := range wantSources {
		if res.Sources[i] != w {
			t.Errorf("Sources[%d] = %q, want %q", i, res.Sources[i], w)
		}
	}
	if string(res.Generated["Cargo.lock"]) != "pruned-lock-bytes" {
		t.Errorf("Generated = %v, want Cargo.lock -> pruned-lock-bytes", res.Generated)
	}
	if len(res.AllowEscaping) != 1 || res.AllowEscaping[0] != "lib/common/weird-link" {
		t.Errorf("AllowEscaping = %v", res.AllowEscaping)
	}
}

func TestEvalBuildSourcesWithoutContextFails(t *testing.T) {
	recipeSrc := []byte(`
def build():
    return struct(inputs = {}, env = {}, script = "true", runtime_deps = [], sources = ["//lib"])
`)
	cfg := widenedCfg(t, "")
	cfg.ContextKey = key.Key{}
	_, err := EvalBuild(cfg, recipeSrc, nil)
	if err == nil || !strings.Contains(err.Error(), "no widened context") {
		t.Fatalf("want no-widened-context error, got %v", err)
	}
}

func TestEvalBuildSourceEscapeFails(t *testing.T) {
	recipeSrc := []byte(`
def build():
    return struct(inputs = {}, env = {}, script = "true", runtime_deps = [], sources = ["../../../outside"])
`)
	_, err := EvalBuild(widenedCfg(t, "a/b"), recipeSrc, nil)
	if err == nil || !strings.Contains(err.Error(), "escapes the context root") {
		t.Fatalf("want escape error, got %v", err)
	}
}

func TestSubbuildRootRelative(t *testing.T) {
	root, err := amber.FileKey([]byte("build-root"))
	if err != nil {
		t.Fatal(err)
	}
	ctxKey, err := amber.FileKey([]byte("context-root"))
	if err != nil {
		t.Fatal(err)
	}
	sb := makeSubbuild("linux/amd64", root, ctxKey)

	// //-path resolves against the context tree with ctx: 2.
	def := subbuildDef(t, sb, "//lib/x")
	if def.Dir != "lib/x" {
		t.Errorf("dir = %q, want lib/x", def.Dir)
	}
	if def.Ctx != builddef.CtxWidened {
		t.Errorf("ctx = %d, want %d (CtxWidened)", def.Ctx, builddef.CtxWidened)
	}
	if tk, err := builddef.DecodeTreeKey(def.Source.Definition); err != nil || tk != ctxKey {
		t.Errorf("//-subbuild source = %s (err %v), want context key %s", tk, err, ctxKey)
	}

	// Descendant path resolves against the build root, also ctx: 2 (dir != "").
	def2 := subbuildDef(t, sb, "below/here")
	if tk2, err := builddef.DecodeTreeKey(def2.Source.Definition); err != nil || tk2 != root {
		t.Errorf("descendant subbuild source = %s (err %v), want build root %s", tk2, err, root)
	}
	if def2.Ctx != builddef.CtxWidened {
		t.Errorf("descendant ctx = %d, want %d", def2.Ctx, builddef.CtxWidened)
	}

	// //-path without a context errors.
	sbNoCtx := makeSubbuild("linux/amd64", root, key.Key{})
	if err := subbuildErr(sbNoCtx, "//lib/x"); err == nil || !strings.Contains(err.Error(), "widened context") {
		t.Errorf("//-path without context: err = %v, want widened-context error", err)
	}

	// Upward traversal is still rejected in both forms.
	if err := subbuildErr(sb, "../up"); err == nil {
		t.Errorf("../ subbuild must error")
	}
	if err := subbuildErr(sb, "//../up"); err == nil {
		t.Errorf("//../ subbuild must error")
	}
}

// rootCfg is a build-stage eval config for a ROOT build: no widened context.
func rootCfg(t *testing.T) EvalConfig {
	t.Helper()
	cfg := widenedCfg(t, "")
	cfg.ContextKey = key.Key{}
	return cfg
}

func TestEvalBuildClosure(t *testing.T) {
	// (a) closure decodes + normalizes; allowed at ROOT (zero ContextKey).
	src := []byte(`
def build():
    return struct(inputs={}, env={}, script="s", runtime_deps=[],
                  closure=["//lib/common", "cmd/foo", "//go.mod"])
`)
	res, err := EvalBuild(rootCfg(t), src, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"lib/common", "cmd/foo", "go.mod"}
	if len(res.Closure) != len(want) {
		t.Fatalf("closure: got %v want %v", res.Closure, want)
	}
	for i, w := range want {
		if res.Closure[i] != w {
			t.Errorf("closure[%d] = %q, want %q", i, res.Closure[i], w)
		}
	}

	// (b) closure normalizes against dir in a widened build.
	widened := []byte(`
def build():
    return struct(inputs={}, env={}, script="s", runtime_deps=[],
                  closure=["../shared", "go.mod"])
`)
	wres, err := EvalBuild(widenedCfg(t, "services/api"), widened, nil)
	if err != nil {
		t.Fatal(err)
	}
	wantW := []string{"services/shared", "services/api/go.mod"}
	for i, w := range wantW {
		if wres.Closure[i] != w {
			t.Errorf("widened closure[%d] = %q, want %q", i, wres.Closure[i], w)
		}
	}

	// (c) closure + sources together → error naming both.
	both := []byte(`
def build():
    return struct(inputs={}, env={}, script="s", runtime_deps=[],
                  closure=["//a"], sources=["//b"])
`)
	if _, err := EvalBuild(widenedCfg(t, "x"), both, nil); err == nil ||
		!strings.Contains(err.Error(), "both closure and sources") {
		t.Fatalf("want both-declared error, got %v", err)
	}

	// (d) generated WITH closure at root is allowed.
	gen := []byte(`
def build():
    return struct(inputs={}, env={}, script="s", runtime_deps=[],
                  closure=["//lib"], generated={"//lib/gen.txt": "x"})
`)
	gres, err := EvalBuild(rootCfg(t), gen, nil)
	if err != nil {
		t.Fatalf("generated+closure at root: %v", err)
	}
	if string(gres.Generated["lib/gen.txt"]) != "x" {
		t.Errorf("generated = %v", gres.Generated)
	}

	// (e) generated WITHOUT closure at root still rejected.
	genOnly := []byte(`
def build():
    return struct(inputs={}, env={}, script="s", runtime_deps=[],
                  generated={"//lib/gen.txt": "x"})
`)
	if _, err := EvalBuild(rootCfg(t), genOnly, nil); err == nil {
		t.Fatal("generated without closure/context: want error")
	}

	// (f) sources at root still rejected (unchanged rule).
	srcOnly := []byte(`
def build():
    return struct(inputs={}, env={}, script="s", runtime_deps=[], sources=["//lib"])
`)
	if _, err := EvalBuild(rootCfg(t), srcOnly, nil); err == nil ||
		!strings.Contains(err.Error(), "no widened context") {
		t.Fatal("sources at root accepted")
	}
}
