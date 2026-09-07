package recipe

import (
	"fmt"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/resources"
	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
	"go.starlark.net/syntax"
)

// EvalConfig is the shared context for a recipe evaluation (build.md §3.1).
// Params is the build definition's params as canonical CBOR.
type EvalConfig struct {
	Platform string
	Params   []byte
	Source   Source
	// SourceContentKey is the amber content key of the build root (the dir
	// subtree). subbuild() addresses descendants relative to it. Zero when a
	// caller does not provide it (subbuild() then errors if used).
	SourceContentKey key.Key
	// ContextKey is the whole-context tree key of a widened build
	// (sibling-sources design §3.1) — the tree //-rooted subbuild() paths and
	// build() sources= resolve against. Zero for legacy narrow evaluations
	// (//-paths then error).
	ContextKey key.Key
	// Dir is the build dir within the context ("" for root builds); ../-sugar
	// source paths normalize against it.
	Dir string
	// SeedFetchers are the import-capable seed fetcher names (bootstrap
	// SeedFetcherNames); a plugin-emitted import naming one passes through
	// without a FetcherDef, anything else must match the plugin's bundled pins
	// (recipe-declared-fetchers design §7).
	SeedFetchers []string
	// Deps are the materialized resolution deps at pin time (resolution-deps
	// design §3.2): name → file access + the fixed in-plugin-sandbox path.
	// nil on the plugins() stage and for dep-less builds.
	Deps map[string]DepSource
}

// newThread returns a hermetic Starlark thread: print is captured (no stdout),
// there is no load() resolver, and the runtime is otherwise pure (build.md §3.1).
func newThread() *starlark.Thread {
	return &starlark.Thread{Name: "recipe", Print: func(*starlark.Thread, string) {}}
}

// basePredeclared builds the globals common to both stages: platform, params,
// source, imp, bld, struct (build.md §3.1). struct is included here (not only
// in EvalBuild) because Starlark checks all name references in function bodies
// at compile time (ExecFile), so a BUILD.jobs that defines both plugins() and
// build() using struct would fail during EvalPlugins if struct were absent.
// EvalBuild re-sets struct to the same value (idempotent); no behaviour changes.
func (cfg EvalConfig) basePredeclared() (starlark.StringDict, error) {
	d, err := builddef.Definition{Params: cfg.Params}.ParamsValue()
	if err != nil {
		return nil, fmt.Errorf("decode params: %w", err)
	}
	// Params are opaque user data — never rehydrate-pinned (importPlatform "").
	pv, err := toStarlark(d, "")
	if err != nil {
		return nil, fmt.Errorf("params to starlark: %w", err)
	}
	return starlark.StringDict{
		"platform": starlark.String(cfg.Platform),
		"params":   pv,
		"source":   sourceValue{src: cfg.Source},
		"imp":      makeImp(cfg.Platform),
		"bld":      makeBld(cfg.Platform),
		"fetcher":  makeFetcher(cfg.Platform),
		"subbuild": makeSubbuild(cfg.Platform, cfg.SourceContentKey, cfg.ContextKey),
		"struct":   starlark.NewBuiltin("struct", starlarkstruct.Make),
		// deps must be predeclared in BOTH stages (the same compile-time name
		// check that keeps struct here); at the plugins() stage it is a stub
		// that errors on use — deps are declared there, consumed by build().
		// EvalBuild re-sets it to the live depsMapping.
		"deps": depsUnavailable{},
	}, nil
}

// execRecipe runs the recipe file and returns its module globals.
func execRecipe(thread *starlark.Thread, recipeSrc []byte, pre starlark.StringDict) (starlark.StringDict, error) {
	globals, err := starlark.ExecFile(thread, "BUILD.jobs", recipeSrc, pre)
	if err != nil {
		return nil, err
	}
	return globals, nil
}

// execRecipeForBuild is like execRecipe but strips any top-level
// `def plugins():` function from the AST before compiling, so that the
// predeclared `plugins` mapping (the live plugin callers) is not shadowed by
// the recipe's plugin-declaration function. This allows a BUILD.jobs to contain
// both `def plugins():` (for the plugin-resolve stage) and `def build():` (for
// the pin stage) without naming conflicts. See build.md §3.1, §7.
func execRecipeForBuild(thread *starlark.Thread, recipeSrc []byte, pre starlark.StringDict) (starlark.StringDict, error) {
	opts := syntax.LegacyFileOptions()
	f, err := opts.Parse("BUILD.jobs", recipeSrc, 0)
	if err != nil {
		return nil, err
	}
	// Filter out any top-level `def plugins():` statement so that the predeclared
	// `plugins` mapping (live callers) is resolved from predeclared rather than
	// being shadowed by the file's own `plugins` name.
	filtered := f.Stmts[:0]
	for _, stmt := range f.Stmts {
		def, ok := stmt.(*syntax.DefStmt)
		if ok && def.Name.Name == "plugins" {
			continue // drop the declaration function for the build stage
		}
		filtered = append(filtered, stmt)
	}
	f.Stmts = filtered

	prog, err := starlark.FileProgram(f, pre.Has)
	if err != nil {
		return nil, err
	}
	globals, err := prog.Init(thread, pre)
	globals.Freeze()
	return globals, err
}

// PluginsResult is the decoded return of plugins(): the declared plugins and
// the resolution deps (resolution-deps design §3.1), each name → complete
// Input. A bare-dict return means plugins only.
type PluginsResult struct {
	Plugins map[string]builddef.Input
	Deps    map[string]builddef.Input
}

// EvalPlugins evaluates the recipe's `plugins()` entry in a plugin-free runtime
// (build.md §5). The return is either a dict {name: Input} (plugins only) or
// struct(plugins = {...}, deps = {...}) (resolution-deps design §3.1). An
// absent plugins() yields an empty result.
func EvalPlugins(cfg EvalConfig, recipeSrc []byte) (PluginsResult, error) {
	thread := newThread()
	pre, err := cfg.basePredeclared()
	if err != nil {
		return PluginsResult{}, err
	}
	globals, err := execRecipe(thread, recipeSrc, pre)
	if err != nil {
		return PluginsResult{}, err
	}
	fn, ok := globals["plugins"]
	if !ok {
		return PluginsResult{Plugins: map[string]builddef.Input{}}, nil
	}
	res, err := starlark.Call(thread, fn, nil, nil)
	if err != nil {
		return PluginsResult{}, err
	}
	out := PluginsResult{Plugins: map[string]builddef.Input{}}
	switch t := res.(type) {
	case *starlark.Dict:
		out.Plugins, err = pinnedInputDict(t, cfg.Platform, "plugins()")
		if err != nil {
			return PluginsResult{}, err
		}
	case *starlarkstruct.Struct:
		// Strict field set: a typo'd field (dep=, pluginz=) must fail here at
		// the resolve stage, not surface later as a confusing missing-dep
		// error at pin — or worse, silently drop a declaration.
		for _, an := range t.AttrNames() {
			if an != "plugins" && an != "deps" {
				return PluginsResult{}, fmt.Errorf("plugins() struct field %q is not recognized (want plugins=..., deps=...)", an)
			}
		}
		if pv, perr := t.Attr("plugins"); perr == nil {
			d, ok := pv.(*starlark.Dict)
			if !ok {
				return PluginsResult{}, fmt.Errorf("plugins() struct field plugins must be a dict, got %s", pv.Type())
			}
			out.Plugins, err = pinnedInputDict(d, cfg.Platform, "plugins()")
			if err != nil {
				return PluginsResult{}, err
			}
		}
		if dv, derr := t.Attr("deps"); derr == nil {
			d, ok := dv.(*starlark.Dict)
			if !ok {
				return PluginsResult{}, fmt.Errorf("plugins() struct field deps must be a dict, got %s", dv.Type())
			}
			out.Deps, err = pinnedInputDict(d, cfg.Platform, "plugins() deps")
			if err != nil {
				return PluginsResult{}, err
			}
			for name := range out.Deps {
				if err := builddef.ValidateDepName(name); err != nil {
					return PluginsResult{}, fmt.Errorf("plugins() deps: %w", err)
				}
			}
		}
	default:
		return PluginsResult{}, fmt.Errorf("plugins() must return a mapping or struct(plugins=..., deps=...), got %s", res.Type())
	}
	return out, nil
}

// pinnedInputDict decodes a Starlark dict {name: Input} and applies the
// stage-level pin guarantee (import-platform-pinning) to every value: no
// platform-less import leaves a recipe evaluation. imp() and plugin
// rehydration already pin at construction, so the rehydrate is normally a
// no-op — it closes the remaining construction paths (e.g. an Input smuggled
// through params, which rehydrate unpinned by design). Shared by the plugins
// and deps maps of plugins()' return.
func pinnedInputDict(dict *starlark.Dict, platform, what string) (map[string]builddef.Input, error) {
	out := make(map[string]builddef.Input, dict.Len())
	for _, item := range dict.Items() {
		name, ok := starlark.AsString(item[0])
		if !ok {
			return nil, fmt.Errorf("%s key %v is not a string", what, item[0])
		}
		in, ok := item[1].(*Input)
		if !ok {
			return nil, fmt.Errorf("%s[%q] is not an Input (got %s)", what, name, item[1].Type())
		}
		pinned, perr := rehydrator{platform: platform}.rehydrateInput(in.In)
		if perr != nil {
			return nil, fmt.Errorf("%s[%q]: %w", what, name, perr)
		}
		out[name] = pinned
	}
	return out, nil
}

// NamedInput is one entry of build()'s `inputs` map: the recipe-chosen name
// (the JOBS_DEPS key the build script looks up) and the input it refers to
// (build.md §3.1, §7).
type NamedInput struct {
	Name string
	In   builddef.Input
}

// BuildResult is the decoded return of build() (build.md §7). Inputs is the
// named `inputs` map flattened to a slice; RuntimeDeps is the subset that ships
// with the artifact, given by Input value (matched to inputs by identity).
type BuildResult struct {
	Inputs      []NamedInput
	Env         map[string]string
	Script      string
	RuntimeDeps []builddef.Input
	// Caches are the optional persistent build-cache declarations
	// (build-cache design §3); nil when the recipe declares none.
	Caches []builddef.PinnedCache
	// Resources is the optional recipe-declared CPU/RAM requirement
	// (multi-job-runner design); nil when the recipe omits `resources`. Baked
	// into Pinned.Resources at pin time (F-deterministic; not part of identity).
	Resources *builddef.PinnedResources
	// Name is the optional recipe-declared human-readable build name, surfaced
	// in the jobs-console dashboard. Display metadata ONLY: it is never written
	// into Pinned and never affects F/identity or dedup.
	Name string
	// Sources are the declared covered source paths of a widened build
	// (sibling-sources design §5.2), normalized to root-relative form (the
	// // prefix stripped, ../-sugar resolved against the build dir). nil when
	// the recipe declares none.
	Sources []string
	// Closure is the COMPLETE declared cover (source-closure design §3): the
	// build dir is NOT auto-seeded and the covered tree is exactly this list.
	// Mutually exclusive with Sources; allowed for root builds. Normalized
	// root-relative like Sources.
	Closure []string
	// Generated are pin-synthesized files overlaid onto the covered tree
	// (design §7): normalized root-relative path → content bytes.
	Generated map[string][]byte
	// AllowEscaping lists in-root symlink paths whose targets are allowed to
	// escape the context root (kept verbatim, dangling in the sandbox —
	// design §5.4). Normalized root-relative.
	AllowEscaping []string
}

// Validate enforces the structural rule that runtimeDeps ⊆ inputs (build.md §7,
// §11 — a violation is a hard failure), and delegates cache-declaration
// validation to builddef.ValidateCaches (build-cache design §3).
func (r BuildResult) Validate() error {
	have := make(map[string]bool, len(r.Inputs))
	for _, in := range r.Inputs {
		k, err := in.In.Key()
		if err != nil {
			return err
		}
		have[in.In.Kind+"|"+k.String()] = true
	}
	for _, d := range r.RuntimeDeps {
		k, err := d.Key()
		if err != nil {
			return err
		}
		if !have[d.Kind+"|"+k.String()] {
			return fmt.Errorf("runtime_dep %s-%s is not in inputs", d.Kind, k.String())
		}
	}
	if err := builddef.ValidateCaches(r.Caches); err != nil {
		return err
	}
	return nil
}

// EvalBuild evaluates the recipe's `build()` entry with the resolved plugins
// live, returning the discovered inputs, env, script, and runtime-dep subset
// (build.md §7). A `struct(...)` return is conventional; a 4-tuple
// (inputs, env, script, runtime_deps) is also accepted.
//
// EvalBuild uses execRecipeForBuild which strips any `def plugins():` from the
// AST before compilation, preventing the recipe's plugin-declaration function
// from shadowing the injected live `plugins` mapping (build.md §3.1, §7).
func EvalBuild(cfg EvalConfig, recipeSrc []byte, plugins map[string]PluginSpec) (BuildResult, error) {
	thread := newThread()
	pre, err := cfg.basePredeclared()
	if err != nil {
		return BuildResult{}, err
	}
	if plugins == nil {
		plugins = map[string]PluginSpec{}
	}
	seeds := make(map[string]bool, len(cfg.SeedFetchers))
	for _, s := range cfg.SeedFetchers {
		seeds[s] = true
	}
	pre["plugins"] = pluginsMapping{specs: plugins, platform: cfg.Platform, seeds: seeds}
	pre["deps"] = depsMapping{deps: cfg.Deps}

	globals, err := execRecipeForBuild(thread, recipeSrc, pre)
	if err != nil {
		return BuildResult{}, err
	}
	fn, ok := globals["build"]
	if !ok {
		return BuildResult{}, fmt.Errorf("recipe defines no build() function")
	}
	res, err := starlark.Call(thread, fn, nil, nil)
	if err != nil {
		return BuildResult{}, err
	}
	out, err := decodeBuildResult(res)
	if err != nil {
		return BuildResult{}, err
	}
	// Stage-level pin guarantee (import-platform-pinning): no platform-less
	// import leaves a recipe evaluation — Pinned.Inputs and RuntimeDeps are
	// normalized together so the runtime_deps ⊆ inputs key check (Validate)
	// stays consistent. imp() and plugin rehydration already pin at
	// construction, so this is normally a no-op; it closes the remaining
	// construction paths (e.g. an Input smuggled through params, which
	// rehydrate unpinned by design). Creation-time only: frozen Pinned trees
	// read back elsewhere are never rewritten.
	for i, in := range out.Inputs {
		pinned, perr := rehydrator{platform: cfg.Platform}.rehydrateInput(in.In)
		if perr != nil {
			return BuildResult{}, fmt.Errorf("input %q: %w", in.Name, perr)
		}
		out.Inputs[i].In = pinned
	}
	for i, d := range out.RuntimeDeps {
		pinned, perr := rehydrator{platform: cfg.Platform}.rehydrateInput(d)
		if perr != nil {
			return BuildResult{}, fmt.Errorf("runtime_dep: %w", perr)
		}
		out.RuntimeDeps[i] = pinned
	}
	// Resolve declared covered paths to root-relative form against the build
	// dir (sibling-sources design §5.2, source-closure design §3). closure is
	// a complete cover and is allowed everywhere — root builds included (the
	// gate lift). sources still needs a widened context; generated and
	// sources_allow_escaping ride with either a widened context or a closure.
	widened := cfg.ContextKey != (key.Key{})
	if len(out.Closure) > 0 && len(out.Sources) > 0 {
		return BuildResult{}, fmt.Errorf("build() declares both closure and sources — closure is a complete cover; drop sources")
	}
	if len(out.Sources) > 0 && !widened {
		return BuildResult{}, fmt.Errorf("build() declares sources/generated but this build has no widened context (submit from a repo root, or drop the declarations)")
	}
	if (len(out.Generated) > 0 || len(out.AllowEscaping) > 0) && !widened && len(out.Closure) == 0 {
		return BuildResult{}, fmt.Errorf("build() declares generated/sources_allow_escaping but this build has neither a widened context nor a closure")
	}
	if len(out.Sources) > 0 || len(out.Generated) > 0 || len(out.AllowEscaping) > 0 || len(out.Closure) > 0 {
		if err := normalizeBuildSources(&out, cfg.Dir); err != nil {
			return BuildResult{}, err
		}
	}
	return out, nil
}

// decodeBuildResult accepts a *starlarkstruct.Struct or a 4-tuple.
func decodeBuildResult(v starlark.Value) (BuildResult, error) {
	var inputsV, envV, scriptV, rtV, cachesV, resourcesV, nameV starlark.Value
	var sourcesV, closureV, generatedV, allowEscV starlark.Value
	switch t := v.(type) {
	case *starlarkstruct.Struct:
		get := func(field string) (starlark.Value, error) {
			val, err := t.Attr(field)
			if err != nil {
				return nil, fmt.Errorf("build() result missing %q", field)
			}
			return val, nil
		}
		var err error
		if inputsV, err = get("inputs"); err != nil {
			return BuildResult{}, err
		}
		if envV, err = get("env"); err != nil {
			return BuildResult{}, err
		}
		if scriptV, err = get("script"); err != nil {
			return BuildResult{}, err
		}
		if rtV, err = get("runtime_deps"); err != nil {
			return BuildResult{}, err
		}
		// caches is OPTIONAL (build-cache design §3); struct form only — the
		// legacy 4-tuple form cannot carry it. A missing attr returns an error
		// from t.Attr, which here means "absent", not a failure.
		if cv, cerr := t.Attr("caches"); cerr == nil {
			cachesV = cv
		}
		// resources is OPTIONAL (multi-job-runner design); struct form only.
		if rv, rerr := t.Attr("resources"); rerr == nil {
			resourcesV = rv
		}
		// name is OPTIONAL (display-only build name); struct form only. A
		// missing attr returns an error from t.Attr, meaning "absent".
		if nv, nerr := t.Attr("name"); nerr == nil {
			nameV = nv
		}
		// sources / closure / generated / sources_allow_escaping are OPTIONAL
		// (sibling-sources design §5.2, §7; source-closure design §3);
		// struct form only.
		if sv, serr := t.Attr("sources"); serr == nil {
			sourcesV = sv
		}
		if cv, cerr := t.Attr("closure"); cerr == nil {
			closureV = cv
		}
		if gv, gerr := t.Attr("generated"); gerr == nil {
			generatedV = gv
		}
		if av, aerr := t.Attr("sources_allow_escaping"); aerr == nil {
			allowEscV = av
		}
	case starlark.Tuple:
		if t.Len() != 4 {
			return BuildResult{}, fmt.Errorf("build() tuple must have 4 elements, got %d", t.Len())
		}
		inputsV, envV, scriptV, rtV = t.Index(0), t.Index(1), t.Index(2), t.Index(3)
	default:
		return BuildResult{}, fmt.Errorf("build() must return a struct or 4-tuple, got %s", v.Type())
	}

	inputs, err := inputMapValue(inputsV)
	if err != nil {
		return BuildResult{}, fmt.Errorf("build() inputs: %w", err)
	}
	rtDeps, err := inputListValue(rtV)
	if err != nil {
		return BuildResult{}, fmt.Errorf("build() runtime_deps: %w", err)
	}
	env, err := stringMap(envV)
	if err != nil {
		return BuildResult{}, fmt.Errorf("build() env: %w", err)
	}
	script, ok := starlark.AsString(scriptV)
	if !ok {
		return BuildResult{}, fmt.Errorf("build() script must be a string, got %s", scriptV.Type())
	}
	var caches []builddef.PinnedCache
	if cachesV != nil {
		caches, err = cacheMapValue(cachesV)
		if err != nil {
			return BuildResult{}, fmt.Errorf("build() caches: %w", err)
		}
	}
	var res *builddef.PinnedResources
	if resourcesV != nil {
		res, err = decodeResources(resourcesV)
		if err != nil {
			return BuildResult{}, fmt.Errorf("build() resources: %w", err)
		}
	}
	var name string
	if nameV != nil {
		s, ok := starlark.AsString(nameV)
		if !ok {
			return BuildResult{}, fmt.Errorf("build() name must be a string, got %s", nameV.Type())
		}
		name = s
	}
	var sources, closure, allowEsc []string
	var generated map[string][]byte
	if sourcesV != nil {
		if sources, err = decodeSources(sourcesV); err != nil {
			return BuildResult{}, fmt.Errorf("build() sources: %w", err)
		}
	}
	if closureV != nil {
		if closure, err = decodeSources(closureV); err != nil {
			return BuildResult{}, fmt.Errorf("build() closure: %w", err)
		}
	}
	if allowEscV != nil {
		if allowEsc, err = decodeSources(allowEscV); err != nil {
			return BuildResult{}, fmt.Errorf("build() sources_allow_escaping: %w", err)
		}
	}
	if generatedV != nil {
		if generated, err = decodeGenerated(generatedV); err != nil {
			return BuildResult{}, fmt.Errorf("build() generated: %w", err)
		}
	}
	return BuildResult{Inputs: inputs, Env: env, Script: script, RuntimeDeps: rtDeps, Caches: caches, Resources: res, Name: name,
		Sources: sources, Closure: closure, Generated: generated, AllowEscaping: allowEsc}, nil
}

// decodeResources reads a Starlark struct(cpu="...", memory="...") into a
// builddef.PinnedResources (multi-job-runner design). Both fields are optional;
// a missing field stays zero.
func decodeResources(v starlark.Value) (*builddef.PinnedResources, error) {
	st, ok := v.(*starlarkstruct.Struct)
	if !ok {
		return nil, fmt.Errorf("resources must be a struct, got %s", v.Type())
	}
	out := &builddef.PinnedResources{}
	if cv, err := st.Attr("cpu"); err == nil {
		s, ok := starlark.AsString(cv)
		if !ok {
			return nil, fmt.Errorf("resources.cpu must be a string, got %s", cv.Type())
		}
		m, err := resources.ParseCPU(s)
		if err != nil {
			return nil, err
		}
		out.CPUMilli = m
	}
	if mv, err := st.Attr("memory"); err == nil {
		s, ok := starlark.AsString(mv)
		if !ok {
			return nil, fmt.Errorf("resources.memory must be a string, got %s", mv.Type())
		}
		b, err := resources.ParseMem(s)
		if err != nil {
			return nil, err
		}
		out.MemBytes = b
	}
	return out, nil
}

// inputMapValue converts build()'s `inputs` — a Starlark dict {name: Input} — to
// []NamedInput. Keys must be non-empty strings (the JOBS_DEPS lookup keys);
// values must be Inputs. The dict guarantees name uniqueness.
func inputMapValue(v starlark.Value) ([]NamedInput, error) {
	d, ok := v.(*starlark.Dict)
	if !ok {
		return nil, fmt.Errorf("expected a dict {name: Input}, got %s", v.Type())
	}
	out := make([]NamedInput, 0, d.Len())
	for _, item := range d.Items() {
		name, ok := starlark.AsString(item[0])
		if !ok {
			return nil, fmt.Errorf("input key %v is not a string", item[0])
		}
		if name == "" {
			return nil, fmt.Errorf("input name must be non-empty")
		}
		in, ok := item[1].(*Input)
		if !ok {
			return nil, fmt.Errorf("input %q is not an Input (got %s)", name, item[1].Type())
		}
		out = append(out, NamedInput{Name: name, In: in.In})
	}
	return out, nil
}

// inputListValue converts a Starlark list/tuple of *Input to []builddef.Input.
func inputListValue(v starlark.Value) ([]builddef.Input, error) {
	it, ok := v.(starlark.Iterable)
	if !ok {
		return nil, fmt.Errorf("expected a list of Inputs, got %s", v.Type())
	}
	var out []builddef.Input
	iter := it.Iterate()
	defer iter.Done()
	var e starlark.Value
	for iter.Next(&e) {
		in, ok := e.(*Input)
		if !ok {
			return nil, fmt.Errorf("element is not an Input (got %s)", e.Type())
		}
		out = append(out, in.In)
	}
	return out, nil
}

// stringMap converts a Starlark dict {string: string} to map[string]string.
func stringMap(v starlark.Value) (map[string]string, error) {
	d, ok := v.(*starlark.Dict)
	if !ok {
		return nil, fmt.Errorf("expected a dict, got %s", v.Type())
	}
	out := make(map[string]string, d.Len())
	for _, item := range d.Items() {
		ks, ok := starlark.AsString(item[0])
		if !ok {
			return nil, fmt.Errorf("key %v is not a string", item[0])
		}
		vs, ok := starlark.AsString(item[1])
		if !ok {
			return nil, fmt.Errorf("value for %q is not a string", ks)
		}
		out[ks] = vs
	}
	return out, nil
}

// cacheMapValue converts build()'s `caches` — a Starlark dict
// {mountPath: cacheID} — to []builddef.PinnedCache (build-cache design §3).
// Keying by path makes duplicate mount paths structurally impossible; all
// other rules (id charset, reserved paths, dup ids, nesting) are enforced by
// builddef.ValidateCaches via BuildResult.Validate.
func cacheMapValue(v starlark.Value) ([]builddef.PinnedCache, error) {
	d, ok := v.(*starlark.Dict)
	if !ok {
		return nil, fmt.Errorf("expected a dict {path: id}, got %s", v.Type())
	}
	out := make([]builddef.PinnedCache, 0, d.Len())
	for _, item := range d.Items() {
		p, ok := starlark.AsString(item[0])
		if !ok {
			return nil, fmt.Errorf("cache path %v is not a string", item[0])
		}
		id, ok := starlark.AsString(item[1])
		if !ok {
			return nil, fmt.Errorf("cache id for %q is not a string", p)
		}
		out = append(out, builddef.PinnedCache{Path: p, ID: id})
	}
	return out, nil
}
