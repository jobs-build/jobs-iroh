//go:build linux

package runner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
)

// ResolveFetcher resolves fetcher:<name>:<platform> and materializes the
// artifact to a real on-disk directory, returning it (its root holds ./fetch)
// and a cleanup that removes it. The dir lives on the host: the import
// executor runs without pivot_root (CgroupExecutor) or as a plain subprocess
// (develop), so it sees the dir directly and execs ./fetch from it. found is
// false when no such fetcher ref exists for this platform (→ Decline).
//
// Single-store: a plain local ref read (jobs' remote-first ReadKeyFresh has no
// counterpart here — mutable fetcher refs arrive by whole-ref sync before the
// job runs).
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

// ResolveFetcherArtifact makes the fetcher artifact at content key k usable as
// an on-disk directory (whose root holds ./fetch), returning the directory and
// a cleanup. This is the materialize half of ResolveFetcher, shared with the
// recipe-declared-fetcher path where k comes from the import's FetcherDef
// build output (design §6) rather than a fetcher: ref. name is used only in
// error messages.
func ResolveFetcherArtifact(ctx context.Context, st *amber.Store, cacheDir string, k key.Key, name string) (dir string, cleanup func(), err error) {
	// Hold the materialized artifact under a temp dir on the cache filesystem
	// (cacheDir; os.TempDir() when unset), torn down by cleanup.
	if cacheDir != "" {
		if err := os.MkdirAll(cacheDir, 0o755); err != nil {
			return "", nil, err
		}
	}
	fwork, err := os.MkdirTemp(cacheDir, "fetcher-")
	if err != nil {
		return "", nil, err
	}
	mp := filepath.Join(fwork, "mnt")
	// Materialize the artifact to disk — `./fetch` is exec'd from here, so a
	// real on-disk dir is the reliable path (jobs-iroh ships no FUSE).
	if err := materializeStore(ctx, st, k, mp); err != nil {
		os.RemoveAll(fwork)
		return "", nil, fmt.Errorf("materialize fetcher %s: %w", name, err)
	}
	cleanup = func() { _ = os.RemoveAll(fwork) }
	return mp, cleanup, nil
}
