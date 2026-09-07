package runner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/builddef"
)

// RunBuildFrom executes one build-from job (build-from design §3;
// sibling-sources design §3.2): pull build:K, resolve the source content
// tree, normalize the buildJobs override against the build root, assemble +
// ingest the F-tree, and publish build-from:K → F. For a legacy def the dir
// subtree is spliced as env/ (byte-identical F to before); for a CtxWidened
// def env/ is the WHOLE context tree and dir rides as an F-tree entry, so
// the eval stages see siblings. Hermetic, network-free, tagless,
// platform-independent.
func RunBuildFrom(ctx context.Context, st *amber.Store, rw RefWriter, brc BuildRunCfg, k key.Key) Outcome {
	if o := ensureBuildDef(ctx, st, k); o != nil {
		return *o
	}
	_, def, out := pullAndDecodeDefinition(ctx, st, k)
	if out != nil {
		return *out
	}
	if err := builddef.ValidateCtx(def.Ctx); err != nil {
		return hard("resolving", err.Error(), 0)
	}

	contentTree, out := resolveSourceContentTree(ctx, st, def.Source)
	if out != nil {
		return *out
	}
	// Both modes resolve the build root: legacy splices it as env/; widened
	// validates dir exists and anchors the override comparison there.
	buildRoot, err := resolveSubtreeKey(ctx, st, contentTree, def.Dir)
	if err != nil {
		return hard("resolving", "dir not found in source: "+err.Error(), 0)
	}

	override, rerr := resolveRecipeOverride(ctx, st, buildRoot, def.BuildFile, def.BuildJobs)
	if rerr != nil {
		return hard("resolving", rerr.Error(), 0)
	}

	envKey, dirEntry := buildRoot, ""
	if def.Ctx == builddef.CtxWidened {
		envKey, dirEntry = contentTree, def.Dir
	}
	f, err := st.BuildFromTree(ctx, envKey, dirEntry, def.Params, def.Platform, override)
	if err != nil {
		return retryable("assembling", err)
	}
	// build-from:K and its self-referential build-from-tree:F (a second,
	// F-keyed ref to the same F-tree, so a downstream F-stage — which knows F,
	// not K — can resolve F's closure by an F-derivable name) MUST travel as
	// one batch: the server's name-gating only permits a build-from-tree:F
	// entry that matches the build-from:K value reported in the SAME WriteRefs
	// message (sched gate), so two independent WriteRef calls would have the
	// second one rejected.
	if _, err := rw.WriteRefs(ctx, []Ref{
		{Name: "build-from:" + k.String(), Key: f},
		{Name: "build-from-tree:" + f.String(), Key: f},
	}); err != nil {
		return pushOutcome("pushing", err)
	}
	return Outcome{OutputKey: f}
}

// resolveRecipeOverride computes the build-from override recipe: the bytes to
// splice as the F-tree's top-level BUILD.jobs, or nil to leave the build
// root's BUILD.jobs as the effective recipe. Shared by RunBuildFrom (server)
// and localBuildFrom (local) so both compute an IDENTICAL F for the same
// environment (the cache-join invariant). envKey is the BUILD ROOT subtree —
// the dir subtree in both modes (a widened caller resolves it from the whole
// context solely for this comparison; file reads here are build-root-relative
// either way). The effective recipe is the inline override if set, else
// <root>/<buildFile> when buildFile != "" (which MUST exist — no silent
// fallback), else <root>/BUILD.jobs. It is spliced only when it differs from
// <root>/BUILD.jobs (absent counts as differing) — that omission is what
// makes equivalent builds JOIN. buildJobs and a non-empty buildFile are
// mutually exclusive and rejected earlier (recipe builtins / submit handler).
func resolveRecipeOverride(ctx context.Context, st *amber.Store, envKey key.Key, buildFile string, inline []byte) ([]byte, error) {
	effective := inline
	if len(effective) == 0 && buildFile != "" {
		b, err := readTreeFile(ctx, st, envKey, buildFile)
		if err != nil {
			return nil, fmt.Errorf("recipe not found at %q in build root: %w", buildFile, err)
		}
		effective = b
	}
	// The canonical comparison base is always env/BUILD.jobs.
	envRecipe, rerr := readTreeFile(ctx, st, envKey, "BUILD.jobs")
	hasEnvRecipe := rerr == nil
	if len(effective) > 0 && !(hasEnvRecipe && bytes.Equal(effective, envRecipe)) {
		return effective, nil
	}
	if !hasEnvRecipe {
		return nil, errors.New("no BUILD.jobs in source and no override/build_file")
	}
	return nil, nil
}

// resolveSourceContentTree resolves a source Input to its CONTENT tree key: the
// raw output tree for an import, or the c/ subtree of the (two-hop) build output
// for a build (design §2.1, §3).
func resolveSourceContentTree(ctx context.Context, st *amber.Store, source builddef.Input) (key.Key, *Outcome) {
	srcK, err := source.Key()
	if err != nil {
		o := hard("resolving", "source key: "+err.Error(), 0)
		return key.Key{}, &o
	}
	switch source.Kind {
	case builddef.KindImport:
		out, ok, err := st.GetKey(ctx, "import-output:"+srcK.String())
		if err != nil {
			o := retryable("resolving", err)
			return key.Key{}, &o
		}
		if !ok {
			o := retryable("resolving", errors.New("import-output:"+srcK.String()+" not found (source not built)"))
			return key.Key{}, &o
		}
		return out, nil
	case builddef.KindBuild:
		out, ok, err := st.ResolveBuildOutput(ctx, srcK)
		if err != nil {
			o := retryable("resolving", err)
			return key.Key{}, &o
		}
		if !ok {
			o := retryable("resolving", errors.New("build output for source "+srcK.String()+" not found"))
			return key.Key{}, &o
		}
		cKey, err := resolveSubdirKey(ctx, st, out, "c")
		if err != nil {
			o := retryable("resolving", err)
			return key.Key{}, &o
		}
		return cKey, nil
	case builddef.KindTree:
		tk, err := builddef.DecodeTreeKey(source.Definition)
		if err != nil {
			o := hard("resolving", "decode tree source: "+err.Error(), 0)
			return key.Key{}, &o
		}
		// tk is a raw subtree of the parent F-tree; the emitting stage wrote
		// build-from-tree:<tk> so the closure is resolvable by name.
		if o := ensureBuildFromTree(ctx, st, tk); o != nil {
			return key.Key{}, o
		}
		if _, err := st.Ls(ctx, tk, ""); err != nil {
			o := retryable("resolving", err)
			return key.Key{}, &o
		}
		return tk, nil
	default:
		o := hard("resolving", "unknown source kind: "+source.Kind, 0)
		return key.Key{}, &o
	}
}

// resolveSubtreeKey walks dir (slash-separated) from root, returning the content
// key of the subtree at that path. An empty dir returns root.
func resolveSubtreeKey(ctx context.Context, st *amber.Store, root key.Key, dir string) (key.Key, error) {
	cur := root
	for _, seg := range strings.Split(dir, "/") {
		if seg == "" {
			continue
		}
		next, err := resolveSubdirKey(ctx, st, cur, seg)
		if err != nil {
			return key.Key{}, err
		}
		cur = next
	}
	return cur, nil
}
