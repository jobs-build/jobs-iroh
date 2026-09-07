package runner

import (
	"context"
	"os"
	"path/filepath"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/recipe"
)

// buildFromEnv is the materialized eval environment of a build-from tree at F:
// the build-root source, the on-disk context root (for plugin sandboxes), and
// the platform, params, dir, and effective recipe read from the F-tree
// (build-from design §4, §7; sibling-sources design §3.2). For a legacy
// F-tree env/ IS the build root (Dir == "", ContextKey zero); for a widened
// F-tree env/ is the whole context, the build root is env/<dir>, and
// ContextKey/Dir expose the context to //-subbuilds and the closure walker.
type buildFromEnv struct {
	Source           recipe.Source
	SrcRoot          string // on-disk BUILD ROOT (recipe source.read anchor)
	CtxRoot          string // on-disk context root (plugin sandbox mount; == SrcRoot when legacy)
	Platform         string
	Params           []byte
	Recipe           []byte
	Dir              string  // build dir within the context ("" legacy/root)
	SourceContentKey key.Key // the BUILD ROOT subtree content key (subbuild's anchor)
	ContextKey       key.Key // the whole-context tree key (zero for legacy F-trees)
	cleanup          func()
}

// loadBuildFromEnv tars the whole build-from tree at f to disk and reads its
// pieces. The recipe is the top-level BUILD.jobs override if present, else
// <build root>/BUILD.jobs. Returns a *Outcome on error (the cleanup is a
// no-op then).
func loadBuildFromEnv(ctx context.Context, st *amber.Store, f key.Key) (buildFromEnv, *Outcome) {
	if o := ensureBuildFromTree(ctx, st, f); o != nil {
		return buildFromEnv{}, o
	}
	root, err := os.MkdirTemp("", "jobs-buildfrom-")
	if err != nil {
		o := retryable("materializing", err)
		return buildFromEnv{}, &o
	}
	cleanup := func() { _ = os.RemoveAll(root) }

	rc, err := st.Tar(ctx, f, "")
	if err != nil {
		cleanup()
		o := retryable("materializing", err)
		return buildFromEnv{}, &o
	}
	if err := extractTar(rc, root); err != nil {
		rc.Close()
		cleanup()
		o := retryable("materializing", err)
		return buildFromEnv{}, &o
	}
	rc.Close()

	envKey, err := resolveSubdirKey(ctx, st, f, "env")
	if err != nil {
		cleanup()
		o := hard("materializing", "build-from tree missing env: "+err.Error(), 0)
		return buildFromEnv{}, &o
	}

	// The dir entry marks a widened F-tree (sibling-sources design §3.2):
	// env/ is the whole context, the build root is env/<dir>.
	var dir string
	if b, err := os.ReadFile(filepath.Join(root, "dir")); err == nil {
		dir = string(b)
	}
	ctxRoot := filepath.Join(root, "env")
	srcRoot := ctxRoot
	srcContentKey, contextKey := envKey, key.Key{}
	if dir != "" {
		srcRoot = filepath.Join(ctxRoot, filepath.FromSlash(dir))
		if fi, serr := os.Stat(srcRoot); serr != nil || !fi.IsDir() {
			cleanup()
			o := hard("materializing", "build-from tree dir "+dir+" not found under env", 0)
			return buildFromEnv{}, &o
		}
		contextKey = envKey
		srcContentKey, err = resolveSubtreeKey(ctx, st, envKey, dir)
		if err != nil {
			cleanup()
			o := hard("materializing", "build-from tree dir subtree: "+err.Error(), 0)
			return buildFromEnv{}, &o
		}
	}
	src := diskSource{root: srcRoot}

	params, err := os.ReadFile(filepath.Join(root, "params"))
	if err != nil {
		cleanup()
		o := hard("materializing", "build-from tree missing params: "+err.Error(), 0)
		return buildFromEnv{}, &o
	}
	platform, err := os.ReadFile(filepath.Join(root, "platform"))
	if err != nil {
		cleanup()
		o := hard("materializing", "build-from tree missing platform: "+err.Error(), 0)
		return buildFromEnv{}, &o
	}

	var recipeSrc []byte
	if b, err := os.ReadFile(filepath.Join(root, "BUILD.jobs")); err == nil {
		recipeSrc = b // top-level override
	} else if b, err := src.Read("BUILD.jobs"); err == nil {
		recipeSrc = b // <build root>/BUILD.jobs
	} else {
		cleanup()
		o := hard("materializing", "no effective BUILD.jobs in build-from tree", 0)
		return buildFromEnv{}, &o
	}

	return buildFromEnv{
		Source:           src,
		SrcRoot:          srcRoot,
		CtxRoot:          ctxRoot,
		Platform:         string(platform),
		Params:           params,
		Recipe:           recipeSrc,
		Dir:              dir,
		SourceContentKey: srcContentKey,
		ContextKey:       contextKey,
		cleanup:          cleanup,
	}, nil
}

// ensureBuildFromTree verifies the self-referential build-from-tree:F ref is
// readable before a raw content-key read of F's tree (loadBuildFromEnv's
// st.Tar, or the build executor's SourceKey:f mount). Single-store world:
// there is no closure pull (jobs pulled the ref through RemoteSync here) — the
// ref being absent is NOT an error (F may be local-only, e.g. on the local
// paths); only a genuine ref-store failure becomes a retryable Outcome.
func ensureBuildFromTree(ctx context.Context, st *amber.Store, f key.Key) *Outcome {
	if _, _, err := st.GetKey(ctx, "build-from-tree:"+f.String()); err != nil {
		o := retryable("pulling build-from tree", err)
		return &o
	}
	return nil
}

// ensureBuildDef verifies the build:K bookkeeping ref is readable before
// pullAndDecodeDefinition's st.File(K). Single-store analogue of jobs'
// closure pull: the ref being absent is NOT an error (K's def objects may be
// present without it); a ref-store failure is retryable.
func ensureBuildDef(ctx context.Context, st *amber.Store, k key.Key) *Outcome {
	if _, _, err := st.GetKey(ctx, "build:"+k.String()); err != nil {
		o := retryable("pulling build def", err)
		return &o
	}
	return nil
}
