package runner

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/events"
)

// CacheMount is one seeded build cache (build-cache design §5): ID/Path from
// the pinned declaration, HostDir the on-disk seeded state (always a real
// directory — atime pruning needs a disk-backed rw bind), SeedKey the amber
// key the state was seeded from (zero: cold start).
type CacheMount struct {
	ID      string
	Path    string
	HostDir string
	SeedKey key.Key
}

// seedCaches materializes each declared cache's last published state
// (build-cache:<id>:<platform>) into a fresh host dir and pre-ages atimes so
// any in-build access is observable (design §5.1). A missing ref means a cold
// start (empty dir, logged upstream by the ref simply not existing); a store
// error fails the stage loudly, matching the input-pull policy. On error
// every already-created dir is removed.
//
// The extract MUST be the LOSSLESS path (Store.Materialize = amber's
// tarexport|tarextract, not the runner's lossy extractTar): amber hashes
// mtime, so the skip-if-unchanged comparison in finalizeCaches needs a
// lossless metadata round-trip.
//
// Emits the "seeding" phase and one exec.cache/seeded event per cache
// (cache-observability design §4, §5); ev is nil-safe (local paths).
func seedCaches(ctx context.Context, st *amber.Store, platform string, decls []builddef.PinnedCache, ev *events.Job) ([]CacheMount, *Outcome) {
	if len(decls) > 0 {
		ev.Phase("seeding") // ends when the executor emits "materializing"
		// Liveness ticks while the seed states extract — a big cache seed is
		// otherwise event-silent for its whole span (issue #101).
		defer startPhaseHeartbeat(ev, "seeding", execHeartbeatInterval)()
	}
	var cms []CacheMount
	fail := func(err error) ([]CacheMount, *Outcome) {
		removeCacheDirs(cms)
		o := retryable("seeding cache", err)
		return nil, &o
	}
	for _, d := range decls {
		start := time.Now()
		dir, err := os.MkdirTemp("", "jobs-cache-")
		if err != nil {
			return fail(err)
		}
		cms = append(cms, CacheMount{ID: d.ID, Path: d.Path, HostDir: dir})
		cm := &cms[len(cms)-1]

		k, ok, err := st.GetKey(ctx, builddef.CacheRefName(d.ID, platform))
		if err != nil {
			return fail(err)
		}
		if !ok {
			ev.CacheSeeded(d.ID, time.Since(start).Milliseconds(), 0, 0, true)
			continue // cold start: empty cache dir
		}
		if err := st.Materialize(ctx, k, dir); err != nil {
			return fail(err)
		}
		if err := preAgeAtimes(dir); err != nil {
			return fail(err)
		}
		cm.SeedKey = k
		files, bytes, err := dirStats(dir)
		if err != nil {
			return fail(err)
		}
		ev.CacheSeeded(d.ID, time.Since(start).Milliseconds(), bytes, files, false)
	}
	return cms, nil
}

// preAgeAtimes sets every regular file's atime to the epoch, preserving mtime,
// so that any access during the build refreshes the atime even under default
// relatime (epoch is both older than the file's mtime and older than 24h —
// the two relatime update conditions). Symlinks are skipped (os.Chtimes
// follows them; their atime is not used by pruning anyway).
func preAgeAtimes(dir string) error {
	return filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		return os.Chtimes(p, time.Unix(1, 0), info.ModTime())
	})
}

// dirStats sums dir's regular files (count + bytes) — the same population
// pruning acts on, so seeded/finalized sizes line up with prune effects.
func dirStats(dir string) (files int, bytes int64, err error) {
	err = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		files++
		bytes += info.Size()
		return nil
	})
	return files, bytes, err
}

// removeCacheDirs removes every cache host dir — the post-build/post-develop
// cleanup (the ref publish, when any, happens before this on the success path).
func removeCacheDirs(cms []CacheMount) {
	for _, cm := range cms {
		if cm.HostDir != "" {
			_ = os.RemoveAll(cm.HostDir)
		}
	}
}
