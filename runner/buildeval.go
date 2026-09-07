package runner

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/bootstrap"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/cover"
	"github.com/jobs-build/jobs-iroh/events"
	"github.com/jobs-build/jobs-iroh/recipe"
)

// BuildRunCfg carries the build-eval-stage configuration: platform, the
// vendored shell artifact key (resolved at startup), and a cache directory.
type BuildRunCfg struct {
	Platform string
	ShellKey key.Key
	CacheDir string
	// MemoryMaxBytes hard-caps the build sandbox's memory via cgroup memory.max
	// (multi-job-runner design); 0 = unlimited. CPU is reservation-only and is
	// never capped here.
	MemoryMaxBytes int64
	// Events is the per-job event scope (build-events design §9); nil (the
	// local path, tests) no-ops all emission.
	Events *events.Job
}

// RunPluginResolve executes one plugin-resolve job, keyed by F (build-from
// design §7): load the F-tree env, evaluate plugins(), and publish
// build-plugin-resolved:F = {plugins}.
func RunPluginResolve(ctx context.Context, st *amber.Store, rw RefWriter, brc BuildRunCfg, f key.Key) Outcome {
	env, out := loadBuildFromEnv(ctx, st, f)
	if out != nil {
		return *out
	}
	defer env.cleanup()

	pres, err := recipe.EvalPlugins(recipe.EvalConfig{
		Platform:         env.Platform,
		Params:           env.Params,
		Source:           env.Source,
		SourceContentKey: env.SourceContentKey,
	}, env.Recipe)
	if err != nil {
		return hard("evaluating", "recipe plugins(): "+err.Error(), 0)
	}
	plugins := pres.Plugins

	// No pin loop here (import-platform-pinning): EvalPlugins itself guarantees
	// every KindImport in the returned map is pinned to the build's platform
	// (recipe/recipe.go — imp() and plugin rehydration pin at construction, the
	// eval boundary normalizes the rest). Frozen plugin-resolved trees read back
	// in RunPin are never rewritten — their imports resolve under their original K.

	// Plugins and resolution deps are handled identically here: ingest each
	// definition (objects-before-ref) and publish any tree-source refs.
	pluginInputs := make([]builddef.Input, 0, len(plugins)+len(pres.Deps))
	for _, in := range plugins {
		pluginInputs = append(pluginInputs, in)
	}
	for _, in := range pres.Deps {
		pluginInputs = append(pluginInputs, in)
	}
	for _, in := range pluginInputs {
		if _, err := st.IngestFile(ctx, in.Definition); err != nil {
			return retryable("ingesting", err)
		}
	}
	if o := writeTreeSourceRefs(ctx, st, rw, pluginInputs); o != nil {
		return *o
	}

	// An empty deps map is normalized to nil so a dep-less resolve stays
	// byte-identical to the pre-deps encoding (omitempty).
	deps := pres.Deps
	if len(deps) == 0 {
		deps = nil
	}
	body, err := builddef.EncodePluginResolved(builddef.PluginResolved{Plugins: plugins, Deps: deps})
	if err != nil {
		return retryable("encoding", err)
	}
	r, err := st.IngestFile(ctx, body)
	if err != nil {
		return retryable("ingesting", err)
	}
	if err := rw.WriteRef(ctx, "build-plugin-resolved:"+f.String(), r); err != nil {
		return pushOutcome("pushing", err)
	}
	return Outcome{OutputKey: r}
}

// RunPin executes one pin job, keyed by F (build-from design §7): load the
// F-tree env + build-plugin-resolved:F, evaluate build() with live plugins, and
// publish build-pinned:F.
func RunPin(ctx context.Context, st *amber.Store, rw RefWriter, brc BuildRunCfg, f key.Key) Outcome {
	env, out := loadBuildFromEnv(ctx, st, f)
	if out != nil {
		return *out
	}
	defer env.cleanup()

	prKey, ok, err := st.GetKey(ctx, "build-plugin-resolved:"+f.String())
	if err != nil {
		return retryable("pulling", err)
	}
	if !ok {
		return retryable("pulling", errors.New("build-plugin-resolved:"+f.String()+" not found"))
	}
	prBytes, err := pullFileBytes(ctx, st, prKey)
	if err != nil {
		return retryable("pulling", err)
	}
	pr, err := builddef.DecodePluginResolved(prBytes)
	if err != nil {
		return hard("pulling", "decode plugin-resolved: "+err.Error(), 0)
	}

	// Resolution deps (resolution-deps design §6.2): resolve + materialize each
	// declared dep's output before evaluating build(), exposing it to the
	// Starlark deps handle and to every plugin call sandbox.
	depDirs, depsCleanup, o := materializeDeps(ctx, st, pr.Deps)
	if o != nil {
		return *o
	}
	defer depsCleanup()
	evalDeps := make(map[string]recipe.DepSource, len(depDirs))
	for name, dir := range depDirs {
		evalDeps[name] = recipe.DepSource{Source: diskSource{root: dir}, SandboxPath: pluginDepsDir + "/" + name}
	}

	var pluginErrSink io.Writer
	if ow := brc.Events.Output("stderr", "plugin"); ow != nil {
		pluginErrSink = ow
		defer ow.Close()
	}
	// Plugins see the WHOLE context read-only (sibling-sources design §9) —
	// that visibility is what enables manifest-driven sibling discovery — with
	// the build dir carried in the request so they know where the consumer
	// package lives. Legacy builds: CtxRoot == SrcRoot, Dir == "".
	callers, err := buildPluginCallers(ctx, st, pr.Plugins, env.CtxRoot, env.Dir, brc.ShellKey, pluginErrSink, depDirs)
	if err != nil {
		var pke pluginKeyError
		if errors.As(err, &pke) {
			return hard("resolving", err.Error(), 0)
		}
		return retryable("resolving", err)
	}

	res, err := recipe.EvalBuild(recipe.EvalConfig{
		Platform:         env.Platform,
		Params:           env.Params,
		Source:           env.Source,
		SourceContentKey: env.SourceContentKey,
		ContextKey:       env.ContextKey,
		Dir:              env.Dir,
		SeedFetchers:     bootstrap.SeedFetcherNames(),
		Deps:             evalDeps,
	}, env.Recipe, callers)
	if err != nil {
		var pe *recipe.PluginError
		if errors.As(err, &pe) && pe.Retryable {
			return retryable("evaluating", err)
		}
		return hard("evaluating", "recipe build(): "+err.Error(), 0)
	}

	if err := res.Validate(); err != nil {
		return hard("validating", err.Error(), 0)
	}

	// Covered closure (sibling-sources design §5; source-closure design §5):
	// expand declared + discovered paths once, here — symlink chasing, escape
	// validation, dangling warnings. The EXPANDED set lands in Pinned.Sources
	// (or Pinned.Closure for complete covers), so every later KP derivation
	// (server pin-commit, local memo) is a pure prune+assemble, never a walk.
	var covered, closure []string
	switch {
	case len(res.Closure) > 0:
		// Complete cover (source-closure design §5): dir is not auto-seeded;
		// root builds (no widened context) walk the build-root tree — the
		// same tree Derive later receives as the F env/.
		root := env.ContextKey
		if root == (key.Key{}) {
			root = env.SourceContentKey
		}
		walk, werr := cover.WalkClosure(ctx, st, root, env.Dir, res.Closure, res.AllowEscaping)
		if werr != nil {
			return hard("covering", werr.Error(), 0)
		}
		if len(walk.Warnings) > 0 && pluginErrSink != nil {
			for _, w := range walk.Warnings {
				fmt.Fprintf(pluginErrSink, "cover: %s: %s\n", w.Path, w.Msg)
			}
		}
		closure = walk.Paths
	case env.ContextKey != (key.Key{}):
		walk, werr := cover.Walk(ctx, st, env.ContextKey, env.Dir, res.Sources, res.AllowEscaping)
		if werr != nil {
			return hard("covering", werr.Error(), 0)
		}
		if len(walk.Warnings) > 0 && pluginErrSink != nil {
			for _, w := range walk.Warnings {
				fmt.Fprintf(pluginErrSink, "cover: %s: %s\n", w.Path, w.Msg)
			}
		}
		covered = walk.Paths
	case len(res.Sources) > 0:
		return hard("covering", "recipe declares sources= but the build has no widened context", 0)
	}

	pinnedInputs := make([]builddef.PinnedInput, 0, len(res.Inputs))
	for _, in := range res.Inputs {
		pinnedInputs = append(pinnedInputs, builddef.PinnedInput{
			Name:       in.Name,
			Kind:       in.In.Kind,
			Definition: in.In.Definition,
		})
	}
	treeInputs := make([]builddef.Input, len(res.Inputs))
	for i, in := range res.Inputs {
		treeInputs[i] = in.In
	}
	if o := writeTreeSourceRefs(ctx, st, rw, treeInputs); o != nil {
		return *o
	}
	rtKeys := make([][]byte, 0, len(res.RuntimeDeps))
	for _, d := range res.RuntimeDeps {
		dk, err := d.Key()
		if err != nil {
			return hard("assembling", "runtimeDep key: "+err.Error(), 0)
		}
		rtKeys = append(rtKeys, dk[:])
	}
	pinned := builddef.Pinned{
		Inputs:      builddef.CanonicalPinnedInputs(pinnedInputs),
		Env:         res.Env,
		Script:      res.Script,
		RuntimeDeps: builddef.SortKeys(rtKeys),
		Caches:      builddef.CanonicalCaches(res.Caches),
		Resources:   res.Resources,
		Sources:     builddef.CanonicalSources(covered),
		Closure:     builddef.CanonicalSources(closure),
		Dir:         env.Dir,
		Generated:   res.Generated,
	}
	for _, in := range pinned.Inputs {
		if _, err := st.IngestFile(ctx, in.Definition); err != nil {
			return retryable("ingesting", err)
		}
	}
	pinnedBytes, err := builddef.EncodePinned(pinned)
	if err != nil {
		return retryable("encoding", err)
	}
	r, err := st.IngestFile(ctx, pinnedBytes)
	if err != nil {
		return retryable("ingesting", err)
	}
	// Publish via WriteRefs (one-entry batch) so the recipe-declared display
	// name rides along as the ref's Label. The server attributes it to the
	// target build|K for display; it never enters the pinned bytes above, so
	// F/identity is unaffected (res.Name is deliberately absent from the
	// builddef.Pinned literal).
	if _, err := rw.WriteRefs(ctx, []Ref{{Name: "build-pinned:" + f.String(), Key: r, Label: res.Name}}); err != nil {
		return pushOutcome("pushing", err)
	}
	return Outcome{OutputKey: r}
}

// writeTreeSourceRefs publishes build-from-tree:<tk> -> tk for every distinct
// KindTree source found among inputs, so a downstream subbuild's build-from can
// resolve tk's closure by name (tk == the parent F-tree's env subtree, local
// here). Idempotent; a push failure -> retryable Outcome.
func writeTreeSourceRefs(ctx context.Context, st *amber.Store, rw RefWriter, inputs []builddef.Input) *Outcome {
	seen := map[key.Key]bool{}
	for _, in := range inputs {
		if in.Kind != builddef.KindBuild {
			continue
		}
		def, err := builddef.DecodeDefinition(in.Definition)
		if err != nil || def.Source.Kind != builddef.KindTree {
			continue
		}
		tk, err := builddef.DecodeTreeKey(def.Source.Definition)
		if err != nil || seen[tk] {
			continue
		}
		seen[tk] = true
		if err := rw.WriteRef(ctx, "build-from-tree:"+tk.String(), tk); err != nil {
			o := pushOutcome("pushing tree source ref", err)
			return &o
		}
	}
	return nil
}

// pullAndDecodeDefinition reads the build definition bytes at K from the local
// store and decodes them.
// Returns (bytes, def, nil) on success; returns (nil, zero, &Outcome) on error.
func pullAndDecodeDefinition(ctx context.Context, st *amber.Store, k key.Key) ([]byte, builddef.Definition, *Outcome) {
	defBytes, err := pullFileBytes(ctx, st, k)
	if err != nil {
		o := retryable("pulling", err)
		return nil, builddef.Definition{}, &o
	}
	def, err := builddef.DecodeDefinition(defBytes)
	if err != nil {
		o := hard("pulling", "decode definition: "+err.Error(), 0)
		return nil, builddef.Definition{}, &o
	}
	return defBytes, def, nil
}

// pullFileBytes reads all bytes from st.File(ctx, k).
func pullFileBytes(ctx context.Context, st *amber.Store, k key.Key) ([]byte, error) {
	rc, err := st.File(ctx, k)
	if err != nil {
		return nil, err
	}
	b, err := io.ReadAll(rc)
	rc.Close()
	return b, err
}

// pluginKeyError marks a plugin whose definition could not be keyed — a hard
// (non-retryable) failure, distinct from a missing/unresolvable plugin output.
type pluginKeyError struct{ err error }

func (e pluginKeyError) Error() string { return e.err.Error() }
func (e pluginKeyError) Unwrap() error { return e.err }

// buildPluginCallers resolves each resolved plugin to its output tree and returns
// a hermetic PluginSpec per plugin (build.md §6): a SandboxedPluginCaller plus
// the fetcher-pin bundle the artifact ships (recipe-declared-fetchers §7). A
// plugin is tried as an import-output first, then a build-output. srcRoot is
// the materialized source bound read-only into each call; depDirs are the
// materialized resolution deps, each bound read-only at /jobs/deps/<name>
// (resolution-deps design §3.3).
func buildPluginCallers(ctx context.Context, st *amber.Store, plugins map[string]builddef.Input, srcRoot, dir string, shellKey key.Key, errSink io.Writer, depDirs map[string]string) (map[string]recipe.PluginSpec, error) {
	specs := make(map[string]recipe.PluginSpec, len(plugins))
	for name, in := range plugins {
		pk, err := in.Key()
		if err != nil {
			return nil, pluginKeyError{fmt.Errorf("plugin %s key: %w", name, err)}
		}
		pluginOut, ok, err := st.GetKey(ctx, "import-output:"+pk.String())
		if err != nil {
			return nil, err
		}
		if !ok {
			pluginOut, ok, err = st.ResolveBuildOutput(ctx, pk)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, fmt.Errorf("no output for plugin %s (key %s)", name, pk.String())
			}
		}
		pins, err := readPluginPins(ctx, st, pluginOut)
		if err != nil {
			// readPluginPins wraps a malformed bundle (the plugin artifact's
			// fault) in pluginKeyError → hard at the caller; store I/O errors
			// come back plain and stay retryable, like every other artifact
			// read in this loop.
			return nil, fmt.Errorf("plugin %s: %w", name, err)
		}
		specs[name] = recipe.PluginSpec{
			Caller: SandboxedPluginCaller{
				Cl: st, PluginKey: pluginOut, SourceDir: srcRoot, Dir: dir, ShellKey: shellKey, Ctx: ctx,
				StderrSink: errSink, DepDirs: depDirs,
			},
			Pins: pins,
		}
	}
	return specs, nil
}

// readPluginPins loads the pin bundle a plugin artifact ships at its root
// (recipe.PinsFileName). Absent file ⇒ nil pins (the plugin emits only seed
// fetchers, or none). A bundle that exists but does not parse is the plugin
// artifact's fault — wrapped in pluginKeyError so the pin fails HARD; store
// I/O errors return plain (retryable at the caller).
func readPluginPins(ctx context.Context, st *amber.Store, artifact key.Key) (*recipe.FetcherPins, error) {
	k, found, err := st.TreeSubdir(ctx, artifact, recipe.PinsFileName)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	rc, err := st.File(ctx, k)
	if err != nil {
		return nil, err
	}
	b, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return nil, err
	}
	pins, err := recipe.ParseFetcherPins(b)
	if err != nil {
		return nil, pluginKeyError{fmt.Errorf("%s: %w", recipe.PinsFileName, err)}
	}
	return pins, nil
}
