// Package gcsweep is the host-agnostic GC engine designed to be embedded by
// jobs-server, jobs-runner and jobs-client: a reftrack access tracker + an
// amber-store-core mark-sweep collector, driven by one sweep pipeline
// (reconcile → expire → advisory status → conditional cycle → flush).
// Hosts construct a Sweeper over their open amber store (which installs
// the store's read observer and PutRef guard), then either Start the
// periodic loop (server, runner) or call Sweep directly (client).
// Extracted verbatim from serve/gc.go (v0.30.0); the sweep semantics —
// protected classes, build-output family ordering and pin mirroring,
// failed-output sibling skip, pin-race re-check — are unchanged.
package gcsweep

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/amber-store/core/gc"

	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/reftrack"
)

// gcOutputPrefix/gcDepsPrefix mirror reftrack's own (unexported)
// build-output:/build-output-deps: family prefixes. reftrack keeps
// familySibling private, so the sweep loop below derives the same mapping
// locally rather than exporting it.
const (
	gcOutputPrefix = "build-output:"
	gcDepsPrefix   = "build-output-deps:"
)

// depsOutputName returns the build-output:X sibling of a build-output-deps:X
// name, or ok=false if name isn't a deps name.
func depsOutputName(name string) (string, bool) {
	s, ok := strings.CutPrefix(name, gcDepsPrefix)
	if !ok {
		return "", false
	}
	return gcOutputPrefix + s, true
}

// Options configures a Sweeper.
type Options struct {
	StoreDir     string // amber store root; the collector opens <StoreDir>/closures
	SnapshotPath string // reftrack snapshot file (e.g. <data-dir>/refaccess.cbor)
	CacheDir     string // set to enable the trees sweep (trees.go); empty disables it

	Retention time.Duration
	Interval  time.Duration
	Rate      int64
	MinFree   uint64
}

// Stats reports the GC/auto-cleanup state as of the last sweep — no mark
// walk runs on the stats path. Field names mirror api.GCStats exactly (plus
// the two Trees fields, populated by the trees sweep when Options.CacheDir
// is set, zero otherwise) so hosts can map this 1:1 onto their wire shape.
type Stats struct {
	RetentionNs  int64
	LastSweepNs  int64 // 0 = no sweep yet
	ExpiredLast  int
	ExpiredTotal int // since boot
	Pinned       int
	RefCount     int
	DiskBytes    int64
	LiveBytes    int64
	GarbageBytes int64

	LastCycleNs     int64 // start of the last cycle
	LastCycleReaped int
	LastCycleFreed  int64
	LastCycleWallNs int64
	LastError       string

	TreesRemoved       int
	FetcherDirsRemoved int
}

// Sweeper owns the GC feature: the access tracker, the mark-sweep
// collector, and the periodic sweep. Construct with New over an already-open
// amber store; the caller wires any host-side hooks (Touch/TouchAll/
// MarkPinned/Guard) and starts the loop (Start) or drives Sweep directly.
type Sweeper struct {
	log       *slog.Logger
	store     *amber.Store
	storeDir  string
	snapPath  string // <data-dir>/refaccess.cbor
	cacheDir  string // enables the trees sweep (trees.go) when non-empty
	tracker   *reftrack.Tracker
	coll      *gc.Collector
	retention time.Duration
	interval  time.Duration
	minFree   uint64

	sweepMu sync.Mutex // serializes sweeps (hourly loop vs admin gc)

	mu    sync.Mutex // guards stats
	stats Stats

	stop context.CancelFunc
	done chan struct{}
}

// New opens the collector next to the already-open store, loads the
// tracker snapshot, and installs the store hooks. The caller wires any
// additional host hooks and starts the loop.
func New(log *slog.Logger, store *amber.Store, opts Options) (*Sweeper, error) {
	tracker := reftrack.New()
	snapPath := opts.SnapshotPath
	if err := tracker.Load(snapPath); err != nil {
		// Corrupt snapshot: start empty (refs re-seed at the next sweep),
		// never fatal — access data is approximately reconstructible.
		log.Warn("gc: tracker snapshot unreadable; starting empty", "error", err)
	}
	coll, err := gc.Open(filepath.Join(opts.StoreDir, "closures"), store.Objects(), store.RefStore(), gc.Options{
		Rate:    opts.Rate,
		MinFree: opts.MinFree,
	})
	if err != nil {
		return nil, fmt.Errorf("open gc collector: %w", err)
	}
	g := &Sweeper{
		log:       log,
		store:     store,
		storeDir:  opts.StoreDir,
		snapPath:  snapPath,
		cacheDir:  opts.CacheDir,
		tracker:   tracker,
		coll:      coll,
		retention: opts.Retention,
		interval:  opts.Interval,
		minFree:   opts.MinFree,
	}
	g.stats.RetentionNs = int64(opts.Retention)
	store.SetObserver(tracker.Touch)
	store.SetRefGuard(coll)
	return g, nil
}

// Start launches the periodic sweep loop.
func (g *Sweeper) Start(ctx context.Context) {
	// Default the loop period here, not only in hosts: a zero interval would panic time.NewTicker inside the goroutine.
	if g.interval <= 0 {
		g.interval = time.Hour
	}
	loopCtx, cancel := context.WithCancel(ctx)
	g.stop = cancel
	g.done = make(chan struct{})
	go g.loop(loopCtx)
}

func (g *Sweeper) loop(ctx context.Context) {
	defer close(g.done)
	t := time.NewTicker(g.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := g.Sweep(ctx, -1, false); err != nil && ctx.Err() == nil {
				g.log.Warn("gc sweep failed; next tick retries", "error", err)
			}
		}
	}
}

// Close stops the loop, closes the collector (waiting out a running cycle)
// and flushes the tracker. Runs before the store closes.
func (g *Sweeper) Close() {
	if g.stop != nil {
		g.stop()
		<-g.done
	}
	// Wait out a concurrently running (e.g. admin-triggered) Sweep — it must
	// not touch the store after Close returns. No deadlock: the loop is
	// already joined above, so nothing new can start this lock.
	g.sweepMu.Lock()
	defer g.sweepMu.Unlock()
	if err := g.coll.Close(); err != nil {
		g.log.Warn("gc collector close", "error", err)
	}
	if err := g.tracker.Flush(g.snapPath); err != nil {
		g.log.Warn("gc tracker flush", "error", err)
	}
}

// Sweep is one full tick: reconcile the tracker with the ref listing,
// expire cold refs, score the store (advisory mark), run a cycle when
// worth it (or force=true), flush the tracker, log and return the stats.
// garbage >= 0 forces that selection line on the cycle; -1 = policy.
func (g *Sweeper) Sweep(ctx context.Context, garbage float64, force bool) (Stats, error) {
	g.sweepMu.Lock()
	defer g.sweepMu.Unlock()
	now := time.Now()

	// 1. Seed & prune.
	refs, err := g.store.ListRefs(ctx)
	if err != nil {
		return g.failSweep(fmt.Errorf("list refs: %w", err))
	}
	names := make([]string, len(refs))
	for i, r := range refs {
		names[i] = r.Name
	}
	g.tracker.Reconcile(names, now)

	// 2. Expire (output before deps — Expired orders them). failedOutputs
	// tracks build-output:X names whose delete failed this sweep, so the
	// loop below skips their build-output-deps:X sibling (which sorts
	// later in the same pass) instead of deleting it — deps must never go
	// missing while their output survives, mirroring "deps strictly
	// before output".
	expired := g.tracker.Expired(g.retention, now)
	failedOutputs := map[string]bool{}
	deleted := 0
	for _, name := range expired {
		if out, ok := depsOutputName(name); ok && failedOutputs[out] {
			g.log.Debug("gc: skipping deps ref whose output failed to delete this sweep", "ref", name, "output", out)
			continue
		}
		// Re-fetch immediately before deleting, not just at Expired()'s
		// snapshot time: a TPin/admin pin may have landed in between, or
		// the entry may already be gone (e.g. a concurrent admin gc).
		e, ok := g.tracker.Get(name)
		if !ok || e.Pinned {
			continue
		}
		if err := g.store.DeleteRef(ctx, name); err != nil {
			g.log.Warn("gc: delete expired ref", "ref", name, "error", err)
			if strings.HasPrefix(name, gcOutputPrefix) {
				failedOutputs[name] = true
			}
			continue
		}
		g.tracker.Forget(name)
		deleted++
		g.log.Debug("gc: expired ref deleted", "ref", name,
			"idle", now.Sub(e.LastAccess).Round(time.Minute))
	}

	// 3. Score, then cycle when worth it. The advisory mark walk is the
	// price of the garbage-percentage stat; ingests ride the write barrier.
	st, err := g.coll.Status(ctx)
	if err != nil {
		return g.failSweep(fmt.Errorf("gc status: %w", err))
	}
	frac := 0.0
	if tot := st.LiveBytes + st.GarbageBytes; tot > 0 {
		frac = float64(st.GarbageBytes) / float64(tot)
	}
	run := force || deleted > 0 || frac >= gc.DefaultGarbage || freeBelow(g.storeDir, g.minFree)
	var cycle gc.CycleStats
	var cycleErr error
	if run {
		cycle, cycleErr = g.coll.Run(ctx, garbage)
	}

	// Trees sweep (trees.go); no-op when CacheDir is unset.
	treesRemoved, fetcherRemoved := g.sweepTrees()

	// 4. Persist & report.
	if err := g.tracker.Flush(g.snapPath); err != nil {
		g.log.Warn("gc: tracker flush", "error", err)
	}
	total, pinned := g.tracker.Counts()
	disk := DirSize(g.storeDir)

	g.mu.Lock()
	g.stats.LastSweepNs = now.UnixNano()
	g.stats.ExpiredLast = deleted
	g.stats.ExpiredTotal += deleted
	g.stats.Pinned = pinned
	g.stats.RefCount = total
	g.stats.DiskBytes = disk
	g.stats.LiveBytes = st.LiveBytes
	g.stats.GarbageBytes = st.GarbageBytes
	g.stats.LastError = ""
	g.stats.TreesRemoved = treesRemoved
	g.stats.FetcherDirsRemoved = fetcherRemoved
	if cycleErr != nil {
		g.stats.LastError = cycleErr.Error()
	}
	if run && cycleErr == nil {
		g.stats.LastCycleNs = cycle.Start.UnixNano()
		g.stats.LastCycleReaped = len(cycle.Reaped)
		g.stats.LastCycleFreed = cycle.FreedBytes
		g.stats.LastCycleWallNs = int64(cycle.Duration)
	}
	out := g.stats
	g.mu.Unlock()

	args := []any{
		"disk", out.DiskBytes, "refs", total, "pinned", pinned,
		"expired", deleted, "live", st.LiveBytes, "garbage", st.GarbageBytes,
		"garbagePct", fmt.Sprintf("%.1f%%", frac*100),
	}
	if run {
		args = append(args, "reaped", len(cycle.Reaped), "freed", cycle.FreedBytes,
			"cycle", cycle.Duration.Round(time.Millisecond))
	}
	if g.cacheDir != "" {
		args = append(args, "trees", treesRemoved)
	}
	if cycleErr != nil {
		args = append(args, "cycleError", cycleErr)
	}
	g.log.Info("gc sweep", args...)
	return out, cycleErr
}

// failSweep records a sweep-level failure in the stats and returns it.
func (g *Sweeper) failSweep(err error) (Stats, error) {
	g.mu.Lock()
	g.stats.LastError = err.Error()
	out := g.stats
	g.mu.Unlock()
	return out, err
}

// StatsSnapshot returns the last-known stats — no walk, no lock beyond the
// stats mutex (the numbers are "as of the last sweep").
func (g *Sweeper) StatsSnapshot() Stats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.stats
}

// Entry exposes one tracker entry for the refs listing.
func (g *Sweeper) Entry(name string) (reftrack.Entry, bool) {
	return g.tracker.Get(name)
}

// Touch, TouchAll and MarkPinned expose the tracker to host-side hooks
// (the server wires amberiroh pull/pin callbacks and the scheduler's
// runner-report forwarding through these).
func (g *Sweeper) Touch(name string)       { g.tracker.Touch(name) }
func (g *Sweeper) TouchAll(names []string) { g.tracker.TouchAll(names) }
func (g *Sweeper) MarkPinned(name string)  { g.tracker.Pin(name) }

// Guard is the collector as a reference write barrier, for hosts that
// write refs outside amber.PutRef (the server's amberiroh push path).
func (g *Sweeper) Guard() amber.RefGuard { return g.coll }

// Pin marks an existing ref kept-forever and returns its record and entry.
func (g *Sweeper) Pin(ctx context.Context, name string) (amber.RefInfo, reftrack.Entry, error) {
	ri, err := g.store.GetRef(ctx, name)
	if err != nil {
		return amber.RefInfo{}, reftrack.Entry{}, err
	}
	g.tracker.Pin(name)
	if err := g.tracker.Flush(g.snapPath); err != nil {
		g.log.Warn("gc: tracker flush after pin", "error", err)
	}
	e, _ := g.tracker.Get(name)
	return ri, e, nil
}

// Unpin clears the flag (always succeeds; the ref may already be gone).
// The returned bool reports whether the ref record still exists.
func (g *Sweeper) Unpin(ctx context.Context, name string) (amber.RefInfo, reftrack.Entry, bool) {
	g.tracker.Unpin(name)
	if err := g.tracker.Flush(g.snapPath); err != nil {
		g.log.Warn("gc: tracker flush after unpin", "error", err)
	}
	// GetRef first: it fires the store observer, so the entry read below reflects this unpin's own access.
	ri, err := g.store.GetRef(ctx, name)
	if err != nil {
		return amber.RefInfo{Name: name}, reftrack.Entry{}, false
	}
	e, _ := g.tracker.Get(name)
	return ri, e, true
}

// DirSize sums the regular-file bytes under dir, best-effort: stats belong
// to observation, so a racing compaction or permission hiccup yields a
// smaller number, never an error.
func DirSize(dir string) int64 {
	var total int64
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() {
			return nil
		}
		if info, err := d.Info(); err == nil {
			total += info.Size()
		}
		return nil
	})
	return total
}

// freeBelow reports whether the filesystem holding path has less than min
// bytes free; min 0 means 5% of the filesystem (the collector's own
// pressure line). Portable across linux and darwin.
//
// This intentionally mirrors amber-store-core/gc's unexported freeBelow
// (same 5% default, same pressure-line role) — keep the two in sync.
func freeBelow(path string, min uint64) bool {
	var st unix.Statfs_t
	if unix.Statfs(path, &st) != nil {
		return false
	}
	if min == 0 {
		min = uint64(st.Blocks) * uint64(st.Bsize) / 20
	}
	return uint64(st.Bavail)*uint64(st.Bsize) < min
}
