//go:build !linux

package runner

import (
	"context"
	"os"
	"path/filepath"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
)

// ResolveFetcher resolves fetcher:<name>:<platform> and extracts the artifact
// into cacheDir/<key> (reused across jobs), returning the extracted directory.
// The returned cleanup is a no-op (the cache entry is intentionally retained).
// found is false when no such fetcher ref exists for this platform (→ Decline).
// Single-store: a plain local ref read (see fetcher_linux.go).
func ResolveFetcher(ctx context.Context, st *amber.Store, cacheDir, name, platform string) (dir string, cleanup func(), found bool, err error) {
	k, ok, err := st.GetKey(ctx, "fetcher:"+name+":"+platform)
	if err != nil {
		return "", nil, false, err
	}
	if !ok {
		return "", nil, false, nil
	}
	dir, cleanup, err = ResolveFetcherArtifact(ctx, st, cacheDir, k, name)
	if err != nil {
		return "", nil, false, err
	}
	return dir, cleanup, true, nil
}

// ResolveFetcherArtifact extracts the fetcher artifact at content key k into
// cacheDir/<key> (reused across jobs; keyed by content, so distinct fetcher
// versions never share a dir) and returns it. This is the materialize half of
// ResolveFetcher, shared with the recipe-declared-fetcher path (design §6).
// name is used only in error messages; the cleanup is a no-op (the cache entry
// is intentionally retained).
func ResolveFetcherArtifact(ctx context.Context, st *amber.Store, cacheDir string, k key.Key, name string) (dir string, cleanup func(), err error) {
	_ = name
	dir = filepath.Join(cacheDir, k.String())
	if fi, statErr := os.Stat(filepath.Join(dir, "fetch")); statErr == nil && !fi.IsDir() {
		return dir, func() {}, nil // already cached
	}

	tmp := dir + ".tmp"
	if err := os.RemoveAll(tmp); err != nil {
		return "", nil, err
	}
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		return "", nil, err
	}
	rc, err := st.Tar(ctx, k, "")
	if err != nil {
		return "", nil, err
	}
	defer rc.Close()
	if err := extractTar(rc, tmp); err != nil {
		os.RemoveAll(tmp)
		return "", nil, err
	}
	if err := os.Rename(tmp, dir); err != nil {
		if _, statErr := os.Stat(dir); statErr == nil {
			os.RemoveAll(tmp) // lost a race; the other extraction wins
			return dir, func() {}, nil
		}
		return "", nil, err
	}
	return dir, func() {}, nil
}
