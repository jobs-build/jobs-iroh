package runner

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/sandbox"
)

// jobs-iroh ships no FUSE: every store mount is materialize-to-disk (jobs'
// default mode; the JOBS_STORE_MOUNT switch, the FUSE probe, and amberfuse
// were dropped in the port — see docs/research/jobs-sandbox.md §8).

// materializeStore extracts the content-addressed store tree storeKey to dest
// (created if needed) as dest/<BOK>/..., the on-disk store staging for the
// sandbox and fetcher paths. The extraction is amber's in-process lossless
// Materialize (tarexport|tarextract); afterwards every directory is made
// owner-writable, because store trees carry read-only 0555 directories that
// would otherwise make the caller's work-tree cleanup fail with EACCES —
// jobs' lossy extractTar achieved the same via MkdirAll(mode|0700).
func materializeStore(ctx context.Context, st *amber.Store, storeKey key.Key, dest string) error {
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return fmt.Errorf("mkdir store dir %s: %w", dest, err)
	}
	if err := st.Materialize(ctx, storeKey, dest); err != nil {
		return fmt.Errorf("materialize store %s: %w", storeKey, err)
	}
	if err := makeDirsOwnerWritable(dest); err != nil {
		return fmt.Errorf("materialize store %s: %w", storeKey, err)
	}
	return nil
}

// makeDirsOwnerWritable walks dir and adds owner rwx to every directory
// (pre-order, so a read-only parent is opened up before its children are
// visited). File modes are left exactly as materialized.
func makeDirsOwnerWritable(dir string) error {
	return filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.Mode().Perm()&0o700 == 0o700 {
			return nil
		}
		return os.Chmod(p, info.Mode().Perm()|0o700)
	})
}

// provisionStore makes storeKey available read-only at newRoot/jobs/store:
// the tree is materialized into a staging dir under workDir (so the caller's
// work-tree cleanup removes it) and returned as a read-only bind mount for
// the sandbox config. This is jobs' provisionStore collapsed to its
// materialize half — no FUSE mounts to hand back, so the caller has nothing
// to release beyond the work tree itself.
func provisionStore(ctx context.Context, st *amber.Store, storeKey key.Key, newRoot, workDir string) ([]sandbox.Mount, error) {
	mp := filepath.Join(newRoot, sandboxStoreDir)
	if err := os.MkdirAll(mp, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir store mountpoint: %w", err)
	}
	staging := filepath.Join(workDir, "store")
	if err := materializeStore(ctx, st, storeKey, staging); err != nil {
		return nil, err
	}
	return []sandbox.Mount{{Source: staging, Target: sandboxStoreDir, ReadOnly: true}}, nil
}
