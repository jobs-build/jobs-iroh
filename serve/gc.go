package serve

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/api"
	"github.com/jobs-build/jobs-iroh/gcsweep"
	"github.com/jobs-build/jobs-iroh/reftrack"
)

// gcTestCapture, when set (tests only), receives the gcRunner Run wires.
// Callers wire this to a buffered(1) channel, which supports exactly one
// GC-enabled server per test process: a second such server started before
// the first capture is drained would block on the send inside Run.
var gcTestCapture func(*gcRunner)

// gcRunner adapts the shared gcsweep.Sweeper to the admin API surface:
// api.GCStats/api.RefInfo mapping and the serve-side hook wiring points.
// One per server; nil when GC is disabled (Options.GCRetention == 0).
type gcRunner struct {
	sweeper *gcsweep.Sweeper
}

func newGCRunner(log *slog.Logger, store *amber.Store, dataDir, storeDir string, opts Options) (*gcRunner, error) {
	sw, err := gcsweep.New(log, store, gcsweep.Options{
		StoreDir:     storeDir,
		SnapshotPath: filepath.Join(dataDir, "refaccess.cbor"),
		Retention:    opts.GCRetention,
		Interval:     opts.GCInterval,
		Rate:         opts.GCRate,
		MinFree:      opts.GCMinFree,
	})
	if err != nil {
		return nil, err
	}
	return &gcRunner{sweeper: sw}, nil
}

func (g *gcRunner) start(ctx context.Context) { g.sweeper.Start(ctx) }
func (g *gcRunner) Close()                    { g.sweeper.Close() }

func (g *gcRunner) Sweep(ctx context.Context, garbage float64, force bool) (api.GCStats, error) {
	st, err := g.sweeper.Sweep(ctx, garbage, force)
	return toAPIGCStats(st), err
}

func (g *gcRunner) StatsSnapshot() api.GCStats { return toAPIGCStats(g.sweeper.StatsSnapshot()) }

func (g *gcRunner) Entry(name string) (reftrack.Entry, bool) { return g.sweeper.Entry(name) }

func (g *gcRunner) Pin(ctx context.Context, name string) (api.RefInfo, error) {
	ri, e, err := g.sweeper.Pin(ctx, name)
	if err != nil {
		return api.RefInfo{}, err
	}
	return refRow(ri, e), nil
}

// Unpin fails with gcsweep.ErrHalted while the GC is halted, like Pin: the
// change could not be persisted.
func (g *gcRunner) Unpin(ctx context.Context, name string) (api.RefInfo, error) {
	if h := g.sweeper.Halted(); h != "" {
		return api.RefInfo{}, fmt.Errorf("%w: %s", gcsweep.ErrHalted, h)
	}
	ri, e, ok := g.sweeper.Unpin(ctx, name)
	if !ok {
		return api.RefInfo{Name: name}, nil
	}
	return refRow(ri, e), nil
}

func refRow(ri amber.RefInfo, e reftrack.Entry) api.RefInfo {
	return api.RefInfo{
		Name: ri.Name, Key: ri.Key[:], CreatedNs: ri.CreatedAt.UnixNano(),
		LastAccessNs: e.LastAccess.UnixNano(), Pinned: e.Pinned,
	}
}

// toAPIGCStats maps the shared stats onto the frozen admin wire shape.
func toAPIGCStats(s gcsweep.Stats) api.GCStats {
	return api.GCStats{
		RetentionNs: s.RetentionNs, LastSweepNs: s.LastSweepNs,
		ExpiredLast: s.ExpiredLast, ExpiredTotal: s.ExpiredTotal,
		Pinned: s.Pinned, RefCount: s.RefCount, DiskBytes: s.DiskBytes,
		LiveBytes: s.LiveBytes, GarbageBytes: s.GarbageBytes,
		LastCycleNs: s.LastCycleNs, LastCycleReaped: s.LastCycleReaped,
		LastCycleFreed: s.LastCycleFreed, LastCycleWallNs: s.LastCycleWallNs,
		LastError: s.LastError, Halted: s.Halted,
	}
}
