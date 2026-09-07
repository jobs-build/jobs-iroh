package runnerd

import (
	"context"
	"sync"

	"github.com/amber-store/core/key"

	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/runner"
	"github.com/jobs-build/jobs-iroh/wire"
)

// collectRefWriter is the runner daemon's runner.RefWriter: it writes each
// result ref into the runner's PRIVATE local store (so this runner's own
// future jobs short-circuit on them — ensureBuildFromTree, input resolution,
// doneness fast paths — exactly what LocalRefWriter gives the local path) AND
// collects name+key proposals in order for the wire.Result. The server never
// sees these local refs; it gates the proposals and writes its own.
//
// Batch invariants (RefWriter contract): entries are written sequentially,
// stopping on the first error, so RunBuild's ordered [caches…,
// build-output-deps:F, build-output:F] and RunBuildFrom's joint
// build-from:K + build-from-tree:F batches keep their order in the proposal
// list the server gate validates.
type collectRefWriter struct {
	st *amber.Store

	mu   sync.Mutex
	refs []wire.RefProposal
}

var _ runner.RefWriter = (*collectRefWriter)(nil)

func newCollectRefWriter(st *amber.Store) *collectRefWriter {
	return &collectRefWriter{st: st}
}

func (w *collectRefWriter) WriteRef(ctx context.Context, name string, k key.Key) error {
	if err := w.st.PutRef(ctx, name, k); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.refs = append(w.refs, wire.RefProposal{Name: name, Key: k[:]})
	return nil
}

func (w *collectRefWriter) WriteRefs(ctx context.Context, refs []runner.Ref) (runner.PushTotals, error) {
	for _, r := range refs {
		if err := w.st.PutRef(ctx, r.Name, r.Key); err != nil {
			return runner.PushTotals{}, err
		}
		w.mu.Lock()
		w.refs = append(w.refs, wire.RefProposal{Name: r.Name, Key: r.Key[:], Label: r.Label})
		w.mu.Unlock()
	}
	return runner.PushTotals{}, nil
}

// proposals snapshots the collected batch, in write order.
func (w *collectRefWriter) proposals() []wire.RefProposal {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]wire.RefProposal, len(w.refs))
	copy(out, w.refs)
	return out
}
