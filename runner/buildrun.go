package runner

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/builddef"
)

// assembleBuildSpec reads build-pinned:<KP> and assembles the hermetic build
// sandbox spec (build-from design §7, §8; sibling-sources design §9): the
// /jobs/store union (every input's closure + the shell), the JOBS_DEPS
// name→path map, and the covered source — the KP tree's src/ subtree
// (SourceKey: kp, SourceDir: "src"), with $SRC/CWD at the pinned Dir. Since
// the sibling-sources arc EVERY buildrun is KP-keyed (root builds cover the
// whole env, normalized); the build-pinned:<KP> alias is written at pin
// commit (server) / KP derivation (local). Shared by RunBuild and the local
// jobs-develop shell. Also returns the decoded Pinned so RunBuild reuses it
// for runtimeDeps. Returns a *Outcome on failure (runner convention).
func assembleBuildSpec(ctx context.Context, st *amber.Store, brc BuildRunCfg, kp key.Key) (BuildSpec, builddef.Pinned, *Outcome) {
	pinnedKey, ok, err := st.GetKey(ctx, "build-pinned:"+kp.String())
	if err != nil {
		o := retryable("pulling", err)
		return BuildSpec{}, builddef.Pinned{}, &o
	}
	if !ok {
		o := retryable("pulling", errors.New("build-pinned:"+kp.String()+" not found (not pinned yet)"))
		return BuildSpec{}, builddef.Pinned{}, &o
	}
	pinnedBytes, err := pullFileBytes(ctx, st, pinnedKey)
	if err != nil {
		o := retryable("pulling", err)
		return BuildSpec{}, builddef.Pinned{}, &o
	}
	pinned, err := builddef.DecodePinned(pinnedBytes)
	if err != nil {
		o := hard("pulling", "decode build-pinned: "+err.Error(), 0)
		return BuildSpec{}, builddef.Pinned{}, &o
	}

	jobsDeps := make(map[string]string, len(pinned.Inputs))
	storeBOKs, out := collectStore(ctx, st, pinned.Inputs, jobsDeps)
	if out != nil {
		return BuildSpec{}, builddef.Pinned{}, out
	}
	storeBOKs = append(storeBOKs, brc.ShellKey)
	storeKey, err := st.BuildStoreTree(ctx, storeBOKs)
	if err != nil {
		o := retryable("assembling store", err)
		return BuildSpec{}, builddef.Pinned{}, &o
	}
	caches, out := seedCaches(ctx, st, brc.Platform, pinned.Caches, brc.Events)
	if out != nil {
		return BuildSpec{}, builddef.Pinned{}, out
	}
	return BuildSpec{
		StoreKey:       storeKey,
		ShellBOK:       brc.ShellKey,
		JobsDeps:       jobsDeps,
		SourceKey:      kp,
		SourceDir:      "src",
		Workdir:        pinned.Dir,
		Env:            pinned.Env,
		Script:         pinned.Script,
		Caches:         caches,
		MemoryMaxBytes: brc.MemoryMaxBytes,
	}, pinned, nil
}

// RunBuild executes one build run job, keyed by KP (sibling-sources design
// §10.1): pull build-pinned:<KP>, resolve inputs (two-hop for build inputs),
// assemble the /jobs/store union + JOBS_DEPS, run the hermetic script with
// $SRC/CWD at the covered tree's build dir, and publish
// build-output-deps:<KP> then build-output:<KP> ({c/}) — deps strictly first:
// build-output:<KP> alone is the doneness marker AND the cross-context memo,
// so it must never become visible before its runtime closure.
func RunBuild(ctx context.Context, st *amber.Store, rw RefWriter, brc BuildRunCfg, ex BuildExecutor, kp key.Key) Outcome {
	ev := brc.Events
	ev.Phase("assembling")
	spec, pinned, out := assembleBuildSpec(ctx, st, brc, kp)
	if out != nil {
		return *out
	}
	defer removeCacheDirs(spec.Caches)

	// Capture the build script's full output as events (build-events §9). The
	// executor emits its own "materializing"/"building" phases via spec.Events
	// — "building" only once the script actually starts.
	spec.Events = ev
	// Key the job-cgroup registry for live process snapshots (issue #97); the
	// scheduler offers builds by F, so the node string is F-keyed.
	spec.Node = "build|" + kp.String()
	if ow := ev.Output("stdout", "building"); ow != nil {
		spec.StdoutSink = ow
		defer ow.Close()
	}
	if ow := ev.Output("stderr", "building"); ow != nil {
		spec.StderrSink = ow
		defer ow.Close()
	}

	buildStart := time.Now()
	res, err := ex.RunBuild(ctx, st, spec)
	if err != nil {
		if ctx.Err() != nil {
			return Outcome{Cancelled: true}
		}
		return retryable("building", err)
	}
	if res.ExitCode != 0 {
		return Outcome{Failed: true, Class: "hard", ExitCode: res.ExitCode, Phase: "building", Stderr: res.StderrTail}
	}

	// Success only (design §5.3): prune each cache to what the build accessed
	// and collect the build-cache refs to publish; a failed build never
	// touches the shared cache state (the deferred removeCacheDirs discards
	// the host dirs).
	ev.Phase("finalizing")
	cacheRefs, out := finalizeCaches(ctx, st, spec.Caches, brc.Platform, buildStart, ev)
	if out != nil {
		return *out
	}

	r, ing, out := finalizeOutput(ctx, st, res.OutDir)
	if out != nil {
		return *out
	}

	rtInputs, out := runtimeDepInputs(pinned)
	if out != nil {
		return *out
	}
	depBOKs, out := collectStore(ctx, st, rtInputs, nil)
	if out != nil {
		return *out
	}
	depsKey, err := st.BuildStoreTree(ctx, depBOKs)
	if err != nil {
		return retryable("assembling deps", err)
	}

	// One ordered batch (design §5.3). RefWriter implementations publish
	// entries sequentially and stop on the first error, so the standing
	// invariant holds: build-output-deps:F (the runtime closure) lands before
	// build-output:F — doneness is derived from build-output:F alone (two-hop
	// resolution), so a crash between the two never leaves a done-looking
	// build whose runtime closure is missing. Cache refs go first: they gate
	// nothing, and output-visible ⇒ everything else landed.
	refs := append(cacheRefs,
		Ref{Name: "build-output-deps:" + kp.String(), Key: depsKey},
		Ref{Name: "build-output:" + kp.String(), Key: r},
	)
	ev.Phase("pushing")
	pt, err := rw.WriteRefs(ctx, refs)
	if err != nil {
		return pushOutcome("pushing", err)
	}
	emitCAS(ev, ing, pt)
	return Outcome{OutputKey: r}
}

// storePath is the in-sandbox path of a dependency, keyed by its content (BOK):
// /jobs/store/<BOK> (build.md §2).
func storePath(bok key.Key) string { return "/jobs/store/" + bok.String() }

// collectConcurrency bounds how many inputs collectStore resolves at once.
// Resolution is local in jobs-iroh (the runner syncs missing objects in before
// a job), but the resolve of each input is still independent I/O — the bound
// keeps a many-input build from stampeding the store while preserving the
// concurrent shape (jobs issue: ~100 imports resolved sequentially made the
// assembling phase scale linearly with the input count).
const collectConcurrency = 8

// collectStore resolves a set of pinned inputs to the BOKs that must populate a
// /jobs/store union: each input's own BOK plus its already-materialized runtime
// closure (build-output-deps for a build input; nothing for an import leaf). When
// jobsDeps is non-nil it is filled with name→/jobs/store/<BOK> for each input
// (the build-time JOBS_DEPS map). Result BOKs may repeat; BuildStoreTree dedups.
// Inputs are independent, so they resolve concurrently (bounded); on failure the
// first outcome wins and cancels the rest — the root cause, not a cancellation
// artifact, reaches the scheduler.
func collectStore(ctx context.Context, st *amber.Store, inputs []builddef.PinnedInput, jobsDeps map[string]string) ([]key.Key, *Outcome) {
	type resolved struct {
		bok  key.Key
		deps []key.Key
	}
	results := make([]resolved, len(inputs))
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg       sync.WaitGroup
		sem      = make(chan struct{}, collectConcurrency)
		failOnce sync.Once
		failed   *Outcome
	)
	fail := func(o *Outcome) {
		failOnce.Do(func() { failed = o; cancel() })
	}
	for i, pi := range inputs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-cctx.Done():
				return
			}
			defer func() { <-sem }()
			bok, hardOut := resolveInputOutputKey(cctx, st, pi)
			if hardOut != nil {
				fail(hardOut)
				return
			}
			depBOKs, depOut := resolveInputDeps(cctx, st, pi)
			if depOut != nil {
				fail(depOut)
				return
			}
			results[i] = resolved{bok: bok, deps: depBOKs}
		}()
	}
	wg.Wait()
	if failed != nil {
		return nil, failed
	}
	boks := make([]key.Key, 0, len(inputs))
	for i, pi := range inputs {
		if jobsDeps != nil {
			jobsDeps[pi.Name] = storePath(results[i].bok)
		}
		boks = append(boks, results[i].bok)
		boks = append(boks, results[i].deps...)
	}
	return boks, nil
}

// runtimeDepInputs maps each pinned RuntimeDep (an identity K) back to its
// PinnedInput (kind + definition), so its BOK + closure can be resolved like any
// input. A runtimeDep absent from inputs is a malformed description (hard, §11).
func runtimeDepInputs(pinned builddef.Pinned) ([]builddef.PinnedInput, *Outcome) {
	byKey := make(map[string]builddef.PinnedInput, len(pinned.Inputs))
	for _, pi := range pinned.Inputs {
		ik, err := (builddef.Input{Kind: pi.Kind, Definition: pi.Definition}).Key()
		if err != nil {
			o := hard("assembling", "input key: "+err.Error(), 0)
			return nil, &o
		}
		byKey[string(ik[:])] = pi
	}
	out := make([]builddef.PinnedInput, 0, len(pinned.RuntimeDeps))
	for _, dk := range pinned.RuntimeDeps {
		pi, ok := byKey[string(dk)]
		if !ok {
			o := hard("assembling", "runtimeDep is not among inputs: "+hex.EncodeToString(dk), 0)
			return nil, &o
		}
		out = append(out, pi)
	}
	return out, nil
}

// resolveInputOutputKey resolves one pinned input to its artifact content key
// (BOK) — the tree the executor mounts at /jobs/store/<BOK>. For an import that
// is the import's whole output tree; for a build it is the `c/` subtree of the
// build output (§4, §9). Returns a *Outcome on error (hard for an unknown
// kind/derivation, retryable for a missing ref).
func resolveInputOutputKey(ctx context.Context, st *amber.Store, pi builddef.PinnedInput) (key.Key, *Outcome) {
	in := builddef.Input{Kind: pi.Kind, Definition: pi.Definition}
	ik, err := in.Key()
	if err != nil {
		o := hard("resolving", "input key: "+err.Error(), 0)
		return key.Key{}, &o
	}
	switch pi.Kind {
	case builddef.KindImport:
		outKey, ok, err := st.GetKey(ctx, "import-output:"+ik.String())
		if err != nil {
			o := retryable("resolving", err)
			return key.Key{}, &o
		}
		if !ok {
			o := retryable("resolving", errors.New("import-output:"+ik.String()+" not found"))
			return key.Key{}, &o
		}
		return outKey, nil
	case builddef.KindBuild:
		outKey, ok, err := st.ResolveBuildOutput(ctx, ik)
		if err != nil {
			o := retryable("resolving", err)
			return key.Key{}, &o
		}
		if !ok {
			o := retryable("resolving", errors.New("build output for "+ik.String()+" not found"))
			return key.Key{}, &o
		}
		cKey, err := resolveSubdirKey(ctx, st, outKey, "c")
		if err != nil {
			o := retryable("resolving", err)
			return key.Key{}, &o
		}
		return cKey, nil
	default:
		// Unreachable backstop: pinned.Inputs only holds Inputs a recipe builtin
		// produced (imp→import, bld/subbuild→build), so a "tree" input — built only
		// by subbuild as a build's Source, never bound into Starlark — can never
		// appear as a top-level input here (subbuild design §3/§8).
		o := hard("resolving", "unknown input kind: "+pi.Kind, 0)
		return key.Key{}, &o
	}
}

// resolveInputDeps returns the BOKs of a build input's already-materialized
// runtime closure (the entries of its build-output-deps:ik store tree). An
// import is a closure leaf and contributes nothing. The closure tree is flat
// (one entry per BOK), so its entries ARE the transitive set (§5).
func resolveInputDeps(ctx context.Context, st *amber.Store, pi builddef.PinnedInput) ([]key.Key, *Outcome) {
	// Only a build input carries a runtime closure; an import is a leaf. (A "tree"
	// input never reaches here either — it is only a build's Source, never a
	// top-level pinned input — and it would likewise leaf.)
	if pi.Kind != builddef.KindBuild {
		return nil, nil
	}
	ik, err := (builddef.Input{Kind: pi.Kind, Definition: pi.Definition}).Key()
	if err != nil {
		o := hard("resolving", "input key: "+err.Error(), 0)
		return nil, &o
	}
	depsTree, ok, err := st.ResolveBuildOutputDeps(ctx, ik)
	if err != nil {
		o := retryable("resolving", err)
		return nil, &o
	}
	if !ok {
		o := retryable("resolving", errors.New("build-output-deps for "+ik.String()+" not found"))
		return nil, &o
	}
	ents, err := st.Ls(ctx, depsTree, "")
	if err != nil {
		o := retryable("resolving", err)
		return nil, &o
	}
	boks := make([]key.Key, 0, len(ents))
	for _, e := range ents {
		bok, err := parseStoreEntryKey(e.Name)
		if err != nil {
			o := hard("resolving", "deps entry name: "+err.Error(), 0)
			return nil, &o
		}
		boks = append(boks, bok)
	}
	return boks, nil
}

// parseStoreEntryKey parses a /jobs/store entry name (hex of a BOK) into a key.
func parseStoreEntryKey(name string) (key.Key, error) {
	b, err := hex.DecodeString(name)
	if err != nil {
		return key.Key{}, err
	}
	return key.Parse(b)
}

// finalizeOutput assembles and ingests the structured build output tree (design
// §8): a temp dir T with T/c/ = a copy of outDir. The `meta` sibling is gone —
// provenance is recoverable from F and the build-from tree. Removes T and the
// executor's build work tree before returning. Also returns the ingest's
// write stats (issue #95, exec.cas).
func finalizeOutput(ctx context.Context, st *amber.Store, outDir string) (key.Key, amber.IngestStats, *Outcome) {
	workRoot := filepath.Dir(filepath.Dir(filepath.Dir(outDir)))
	defer os.RemoveAll(workRoot)

	T, err := os.MkdirTemp("", "jobs-build-out-")
	if err != nil {
		o := retryable("finalizing", err)
		return key.Key{}, amber.IngestStats{}, &o
	}
	defer os.RemoveAll(T)

	if err := os.CopyFS(filepath.Join(T, "c"), os.DirFS(outDir)); err != nil {
		o := retryable("finalizing", err)
		return key.Key{}, amber.IngestStats{}, &o
	}
	r, ing, err := st.IngestDirStats(ctx, T)
	if err != nil {
		o := retryable("ingesting", err)
		return key.Key{}, amber.IngestStats{}, &o
	}
	return r, ing, nil
}
