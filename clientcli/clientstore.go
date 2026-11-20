package clientcli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/gcsweep"
)

// testStore, when non-nil, bypasses the data-dir open entirely (no flock, no
// on-disk store): the test harness pre-opens one amber.Open(t.TempDir())
// store and shares it across in-process CLI invocations.
var testStore *amber.Store

// defaultDataDir is the client data dir when neither --data-dir nor
// JOBS_DATA_DIR is set: $XDG_DATA_HOME/jobs-iroh, else
// ~/.local/share/jobs-iroh.
func defaultDataDir() string {
	if x := os.Getenv("XDG_DATA_HOME"); x != "" {
		return filepath.Join(x, "jobs-iroh")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".jobs-iroh-data"
	}
	return filepath.Join(home, ".local", "share", "jobs-iroh")
}

// clientStore is the end-user CLI's store access: an embedded
// amber-store-core store under <data-dir>/store plus the fetcher cache
// <data-dir>/cache, serialized against concurrent jobs-client processes by
// acquireStoreLock. Single-store world: no remotes, no grants, no transport
// identity — jobs' Embedded/GrantHolder plumbing has no counterpart here.
type clientStore struct {
	Store    *amber.Store
	CacheDir string
	dataDir  string
	mode     lockMode // the flock mode this store was opened with; MaybeGC self-disables unless lockExclusive
	release  func()
	closeFn  func() error
	gc       *gcsweep.Sweeper // nil = disabled (JOBS_GC_RETENTION=0, testStore path, or construction failure)
}

// openClientStore locks the data dir (flock FIRST — the embedded packstore's
// own flock is non-blocking and would fail instead of waiting) and opens the
// embedded store. Callers must defer Close.
func openClientStore(dataDir string, mode lockMode) (*clientStore, error) {
	// Absolutize: the fetcher cache under this dir becomes sandbox exec
	// targets, and the sandbox child chdirs before exec'ing — a cwd-relative
	// --data-dir would make those command paths resolve wrong (ENOENT).
	if abs, err := filepath.Abs(dataDir); err == nil {
		dataDir = abs
	}
	cacheDir := filepath.Join(dataDir, "cache")
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}
	if testStore != nil {
		return &clientStore{Store: testStore, CacheDir: cacheDir, dataDir: dataDir, mode: mode,
			release: func() {}, closeFn: func() error { return nil }}, nil
	}
	release, err := acquireStoreLock(dataDir, mode)
	if err != nil {
		return nil, err
	}
	st, err := amber.Open(filepath.Join(dataDir, "store"))
	if err != nil {
		release()
		return nil, fmt.Errorf("open embedded store: %w", err)
	}
	cs := &clientStore{Store: st, CacheDir: cacheDir, dataDir: dataDir, mode: mode,
		release: release, closeFn: st.Close}
	if ret := clientGCRetention(); ret > 0 {
		sw, err := gcsweep.New(slog.Default(), st, gcsweep.Options{
			StoreDir:     filepath.Join(dataDir, "store"),
			SnapshotPath: filepath.Join(dataDir, "refaccess.cbor"),
			CacheDir:     cacheDir,
			Retention:    ret,
		})
		if err != nil {
			// GC must never block a build: warn and run without it.
			fmt.Fprintf(os.Stderr, "gc disabled for this run: %v\n", err)
		} else {
			cs.gc = sw
		}
	}
	return cs, nil
}

// Close closes the sweeper first (flushes the tracker snapshot), then the
// store, then releases the flock.
func (cs *clientStore) Close() {
	if cs.gc != nil {
		cs.gc.Close()
	}
	_ = cs.closeFn()
	cs.release()
}

// gcCheckEvery is how often the auto sweep is allowed to run; the stamp
// file's mtime is the record.
const gcCheckEvery = 24 * time.Hour

// MaybeGC runs the opportunistic sweep: at most once per gcCheckEvery
// (stamp-gated), after a command's main work, while the flock is still
// held. Silent unless something was reclaimed; never fails the command.
func (cs *clientStore) MaybeGC(ctx context.Context) {
	if cs.gc == nil {
		return
	}
	// The sweep mutates shared state (ref deletes, pack compaction, tree
	// removal), which the shared-flock contract at storelock.go reserves for
	// exclusive holders; a remote-build's due sweep is simply picked up by
	// the next exclusive command.
	if cs.mode != lockExclusive {
		return
	}
	stamp := filepath.Join(cs.dataDir, "gc.stamp")
	if info, err := os.Stat(stamp); err == nil && time.Since(info.ModTime()) < gcCheckEvery {
		return
	}
	stats, err := cs.gc.Sweep(ctx, -1, false)
	if errors.Is(err, gcsweep.ErrHalted) {
		// Said on every exclusive command until the operator acts: the
		// local GC will not run again by itself.
		fmt.Fprintf(os.Stderr, "WARNING: local %v\n", err)
		return
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "gc sweep failed (will retry within %s): %v\n", gcCheckEvery, err)
		return
	}
	_ = os.WriteFile(stamp, nil, 0o644) // mtime is the record; content unused
	if stats.ExpiredLast > 0 || stats.LastCycleFreed > 0 || stats.TreesRemoved > 0 {
		fmt.Fprintf(os.Stderr, "gc: expired %d refs, freed %d bytes, removed %d orphaned trees\n",
			stats.ExpiredLast, stats.LastCycleFreed, stats.TreesRemoved)
	}
}
