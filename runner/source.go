package runner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/recipe"
)

// MaterializeSource extracts a build's source subtree (sourceOutputKey at dir)
// onto disk in a fresh temp directory and returns a recipe.Source backed by it.
//
// The eval stages (plugin-resolve, pin) need the source tree on the host both to
// answer source.read/exists in-process AND to bind it read-only into each plugin
// sandbox (build.md §3.1, §6). cleanup removes the temp tree; the caller must
// call it once the source is no longer needed (after all plugin calls).
//
// On any error the temp tree is removed before returning, so a non-nil error
// leaves nothing to clean up (cleanup is a no-op in that case).
func MaterializeSource(ctx context.Context, st *amber.Store, sourceOutputKey key.Key, dir string) (root string, src recipe.Source, cleanup func(), err error) {
	root, err = os.MkdirTemp("", "jobs-source-")
	if err != nil {
		return "", nil, func() {}, fmt.Errorf("create source temp dir: %w", err)
	}
	// On any failure past this point, drop the temp tree and hand back a no-op
	// cleanup so the caller never double-frees.
	cleanupTemp := func() { _ = os.RemoveAll(root) }

	rc, err := st.Tar(ctx, sourceOutputKey, dir)
	if err != nil {
		cleanupTemp()
		return "", nil, func() {}, fmt.Errorf("tar source %s/%s: %w", sourceOutputKey, dir, err)
	}
	if err := extractTar(rc, root); err != nil {
		rc.Close()
		cleanupTemp()
		return "", nil, func() {}, fmt.Errorf("extract source: %w", err)
	}
	rc.Close()

	return root, diskSource{root: root}, cleanupTemp, nil
}

// diskSource is a recipe.Source reading files from a materialized source tree on
// disk. Paths are slash-separated and relative to root; a cleaned path that
// would escape root is rejected.
type diskSource struct{ root string }

var _ recipe.Source = diskSource{}

// resolve joins p under root, rejecting any path that escapes the tree.
func (s diskSource) resolve(p string) (string, error) {
	// filepath.Clean("/"+p) collapses .. and absolute components into a path
	// rooted at "/", which we then re-root under s.root; the suffix check
	// guards against a residual escape (e.g. root itself vs a sibling prefix).
	full := filepath.Join(s.root, filepath.Clean("/"+p))
	cleanRoot := filepath.Clean(s.root)
	if full != cleanRoot && !strings.HasPrefix(full, cleanRoot+string(os.PathSeparator)) {
		return "", fmt.Errorf("source path escapes tree: %q", p)
	}
	// The lexical check cannot see symlinks INSIDE the tree: a materialized
	// tree may carry `go.sum -> /etc/passwd` (a resolution dep is untrusted
	// third-party content, and this read runs on the runner HOST, outside any
	// sandbox). Resolve the real path and require it to stay under the real
	// root. EvalSymlinks also fails for a missing file — the same not-found
	// the caller would have reported at open time.
	realRoot, err := filepath.EvalSymlinks(cleanRoot)
	if err != nil {
		return "", err
	}
	realFull, err := filepath.EvalSymlinks(full)
	if err != nil {
		return "", err
	}
	if realFull != realRoot && !strings.HasPrefix(realFull, realRoot+string(os.PathSeparator)) {
		return "", fmt.Errorf("source path %q resolves outside the tree", p)
	}
	return realFull, nil
}

func (s diskSource) Read(p string) ([]byte, error) {
	full, err := s.resolve(p)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(full)
}

func (s diskSource) Exists(p string) bool {
	full, err := s.resolve(p)
	if err != nil {
		return false
	}
	_, err = os.Stat(full)
	return err == nil
}
