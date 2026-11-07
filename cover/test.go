package cover

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/builddef"
)

// Test identity (tests design §4.2). A test's run identity KT is derived
// from the pinned job AFTER every input is done: the inputs are resolved to
// the content the sandbox will mount, and a TestJob record replaces the
// definition-carrying Pinned as job.cbor. Like Derive, this is
// IDENTITY-CRITICAL SHARED CODE: the server (testvalue advance) and the
// local pipeline run the same functions.

// KTTreeRef names the closure carrier for one KT tree: "kt-tree/<KT>" → KT
// (the kp-tree/ pattern for the test-run key; the runner pulls the covered
// sources by this name).
func KTTreeRef(kt key.Key) string { return "kt-tree/" + kt.String() }

// TestPassedRef names the test memo for a KT (or its F alias): "test-passed:<k>"
// → KT. The doneness marker of a testrun and the cross-context memo, one and
// the same; absent for a failed test, so failures re-run on the next submit.
func TestPassedRef(k key.Key) string { return "test-passed:" + k.String() }

// ErrInputUnresolved marks a pinned input whose output refs are not (yet) in
// the store — not built, or a sync race. errors.Is-able.
var ErrInputUnresolved = errors.New("cover: input output not resolvable")

// ResolveInputs resolves every pinned input to content (tests design §4.2),
// in pinned (name-sorted) order: an import's output tree; a build's c/
// artifact plus its build-output-deps closure tree, both through the
// build-from:K → F bridge — the same reads the runner's collectStore
// performs, so KT names exactly what the sandbox mounts. A missing ref is
// ErrInputUnresolved (callers derive KT only after every input is done, so
// it signals an inconsistency, never a wait).
func ResolveInputs(ctx context.Context, st *amber.Store, inputs []builddef.PinnedInput) ([]builddef.ResolvedInput, error) {
	out := make([]builddef.ResolvedInput, 0, len(inputs))
	for _, pi := range inputs {
		in := builddef.Input{Kind: pi.Kind, Definition: pi.Definition}
		ik, err := in.Key()
		if err != nil {
			return nil, fmt.Errorf("input %q key: %w", pi.Name, err)
		}
		r := builddef.ResolvedInput{Name: pi.Name, Kind: pi.Kind}
		switch pi.Kind {
		case builddef.KindImport:
			k, ok, err := st.GetKey(ctx, "import-output:"+ik.String())
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, fmt.Errorf("input %q: import-output:%s: %w", pi.Name, ik, ErrInputUnresolved)
			}
			r.Out = k[:]
		case builddef.KindBuild:
			outTree, ok, err := st.ResolveBuildOutput(ctx, ik)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, fmt.Errorf("input %q: build output for %s: %w", pi.Name, ik, ErrInputUnresolved)
			}
			c, found, err := st.TreeSubdir(ctx, outTree, "c")
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, fmt.Errorf("input %q: %w (build %s output %s)", pi.Name, amber.ErrNoArtifact, ik, outTree)
			}
			deps, ok, err := st.ResolveBuildOutputDeps(ctx, ik)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, fmt.Errorf("input %q: build-output-deps for %s: %w", pi.Name, ik, ErrInputUnresolved)
			}
			r.Out, r.Deps = c[:], deps[:]
		default:
			return nil, fmt.Errorf("input %q: unsupported pinned input kind %q", pi.Name, pi.Kind)
		}
		out = append(out, r)
	}
	return out, nil
}

// DeriveTest computes KT (tests design §4.2) from the pinned test at KP and
// its resolved inputs: {job.cbor = canonical TestJob, platform, v, src/}
// where src/ is spliced by key from the KP tree (the same pruned, normalized
// cover — and the same KPVersion — as the build-side derivation). Resources
// are left out by design. Deterministic: same inputs ⇒ byte-identical KT.
func DeriveTest(ctx context.Context, st *amber.Store, kp key.Key, p builddef.Pinned, platform string, resolved []builddef.ResolvedInput) (key.Key, error) {
	src, ok, err := st.TreeSubdir(ctx, kp, "src")
	if err != nil {
		return key.Key{}, fmt.Errorf("cover: KP tree %s src/: %w", kp, err)
	}
	if !ok {
		return key.Key{}, fmt.Errorf("cover: KP tree %s has no src/ entry", kp)
	}
	inputs := append([]builddef.ResolvedInput(nil), resolved...)
	sort.SliceStable(inputs, func(i, j int) bool { return inputs[i].Name < inputs[j].Name })
	job, err := builddef.EncodeTestJob(builddef.TestJob{
		Inputs: inputs,
		Env:    p.Env,
		Script: p.Script,
		Caches: p.Caches,
		Dir:    p.Dir,
	})
	if err != nil {
		return key.Key{}, fmt.Errorf("cover: encode test job: %w", err)
	}
	return st.BuildKPTree(ctx, job, platform, src)
}
