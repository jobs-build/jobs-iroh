package recipe

import (
	"fmt"

	"github.com/amber-store/core/key"
	"github.com/fxamacker/cbor/v2"
	"github.com/jobs-build/jobs-iroh/builddef"
	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

// Test-recipe support (tests design §3): a TESTS.jobs defines test() instead
// of build() and may name the directory's own build as an input through the
// predeclared `build` value.

// selfBuildInput constructs the predeclared `build` Input of a test recipe:
// the build of the SAME (context, dir, platform, params) with the default
// BUILD.jobs — field for field the definition jobs-client build/remote-build
// constructs for that directory (clientcli treeDefinition, local
// localBuildFrom), so the test's input joins the user's own build node. A
// widened test (Dir != "") addresses the whole context with ctx: 2; a root
// test addresses the build-root tree with no ctx, exactly like every other
// def constructor.
func (cfg EvalConfig) selfBuildInput() (starlark.Value, error) {
	srcKey, ctxv := cfg.ContextKey, builddef.CtxWidened
	if cfg.Dir == "" {
		srcKey, ctxv = cfg.SourceContentKey, 0
	}
	if srcKey == (key.Key{}) {
		return buildUnavailable{}, nil
	}
	src, err := builddef.TreeInput(srcKey)
	if err != nil {
		return nil, err
	}
	params := cbor.RawMessage(cfg.Params)
	if len(params) == 0 {
		params = cbor.RawMessage{0xf6} // canonical null: no params
	}
	canon, err := builddef.Definition{
		Source: src, Dir: cfg.Dir, Platform: cfg.Platform, Params: params, Ctx: ctxv,
	}.Canonical()
	if err != nil {
		return nil, err
	}
	return &Input{In: builddef.Input{Kind: builddef.KindBuild, Definition: canon}}, nil
}

// buildUnavailable is the `build` predeclared value of a test evaluation
// that has no source content key (a caller without a materialized F-tree —
// unit tests of the recipe layer). The name must still resolve at compile
// time; using it as an input is a clear error instead of a type mismatch.
type buildUnavailable struct{}

var _ starlark.Value = buildUnavailable{}

func (buildUnavailable) String() string        { return "build" }
func (buildUnavailable) Type() string          { return "build" }
func (buildUnavailable) Freeze()               {}
func (buildUnavailable) Truth() starlark.Bool  { return starlark.True }
func (buildUnavailable) Hash() (uint32, error) { return 0, fmt.Errorf("build is unhashable") }

// rejectBuildDef fails a test recipe that defines a global `build`: in a
// TESTS.jobs `build` is the predeclared input, and a def shadowing it would
// be silently ignored by the pipeline (nothing calls build() in a test).
func rejectBuildDef(f *syntax.File) error {
	for _, stmt := range f.Stmts {
		if def, ok := stmt.(*syntax.DefStmt); ok && def.Name.Name == "build" {
			return fmt.Errorf("%s:%d: a test recipe must not define build() — `build` is the predeclared input naming this directory's build; define test() instead", f.Path, def.Name.NamePos.Line)
		}
	}
	return nil
}

// EvalTest evaluates a TESTS.jobs's `test()` entry with the resolved plugins
// live (tests design §3): the same evaluation as EvalBuild — same
// predeclared symbols, plus `build` — with the entry renamed and
// `runtime_deps` rejected (a test has no output to ship a closure with).
func EvalTest(cfg EvalConfig, recipeSrc []byte, plugins map[string]PluginSpec) (BuildResult, error) {
	cfg.Test = true
	return evalEntry(cfg, recipeSrc, plugins, "test")
}
