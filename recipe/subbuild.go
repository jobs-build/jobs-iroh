package recipe

import (
	"fmt"
	"strings"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/builddef"
	"go.starlark.net/starlark"
)

// makeSubbuild returns the `subbuild(dir, platform=platform, params=None,
// build_jobs=None)` builtin: construct a build Input for a directory of the
// current build's source. Two forms (sibling-sources design §8):
//
//   - "path/below"  — a strict DESCENDANT of the build root; the sub-build's
//     source is a tree Input addressing the build-root content
//     (sourceContentKey). The descendant form stays cycle-free by
//     construction (a build depends only on builds strictly below it).
//   - "//any/path"  — ROOT-RELATIVE within the widened context: the source is
//     a tree Input addressing the WHOLE context (contextKey), dir the stripped
//     path. Sibling builds join per-context-commit on K and per-cover on KP;
//     cycles become expressible and are caught by the scheduler's
//     ancestry check at node creation.
//
// See docs/superpowers/specs/2026-06-25-subbuild-descendant-inputs-design.md
// and docs/design/2026-07-26-sibling-sources.md §8.
func makeSubbuild(platform string, sourceContentKey, contextKey key.Key) *starlark.Builtin {
	return starlark.NewBuiltin("subbuild", func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var dir string
		plat := platform
		var params starlark.Value
		var buildJobs string
		var buildFile string
		if err := starlark.UnpackArgs("subbuild", args, kwargs,
			"dir", &dir, "platform?", &plat, "params?", &params, "build_jobs?", &buildJobs, "build_file?", &buildFile); err != nil {
			return nil, err
		}
		srcKey := sourceContentKey
		if strings.HasPrefix(dir, "//") {
			dir = strings.TrimPrefix(dir, "//")
			srcKey = contextKey
			if srcKey == (key.Key{}) {
				return nil, fmt.Errorf("subbuild: //-paths need a widened context (this build has none)")
			}
		}
		if srcKey == (key.Key{}) {
			return nil, fmt.Errorf("subbuild: unavailable (no build-root content key in this evaluation)")
		}
		if err := validateDescendant(dir); err != nil {
			return nil, fmt.Errorf("subbuild: %w", err)
		}
		src, err := builddef.TreeInput(srcKey)
		if err != nil {
			return nil, fmt.Errorf("subbuild: %w", err)
		}
		return newBuildInput(src, dir, plat, params, buildJobs, buildFile)
	})
}

// validateBuildFile checks an optional recipe path (build_file): empty is allowed
// (the build-from stage then defaults to BUILD.jobs); otherwise it must be a clean
// relative path WITHIN the build root — no leading slash and no ".", "..", or empty
// segments. Mirrors validateDescendant but permits the path to name a file and
// tolerates the empty (unset) value.
func validateBuildFile(p string) error {
	if p == "" {
		return nil
	}
	if strings.HasPrefix(p, "/") {
		return fmt.Errorf("build_file %q must be relative (no leading slash)", p)
	}
	for _, seg := range strings.Split(p, "/") {
		switch seg {
		case "":
			return fmt.Errorf("build_file %q has an empty segment", p)
		case ".", "..":
			return fmt.Errorf("build_file %q must not contain %q segments", p, seg)
		}
	}
	return nil
}

// validateDescendant checks that dir names a strict descendant of the build
// root: non-empty, relative, slash-separated, with no ".", "..", empty, or
// leading-slash segments. This is what keeps the sub-build graph acyclic — a
// build may depend only on builds strictly below it.
func validateDescendant(dir string) error {
	if dir == "" {
		return fmt.Errorf("dir must be non-empty")
	}
	if strings.HasPrefix(dir, "/") {
		return fmt.Errorf("dir %q must be relative (no leading slash)", dir)
	}
	for _, seg := range strings.Split(dir, "/") {
		switch seg {
		case "":
			return fmt.Errorf("dir %q has an empty segment", dir)
		case ".", "..":
			return fmt.Errorf("dir %q must not contain %q segments", dir, seg)
		}
	}
	return nil
}
