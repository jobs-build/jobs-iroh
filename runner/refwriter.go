package runner

import (
	"context"
	"errors"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
)

// Ref names one result ref (name -> key) for a WriteRefs batch.
type Ref struct {
	Name string
	Key  key.Key
	// Label is an optional display name (build-pinned:F only) a remote result
	// publisher may forward for UI purposes — never identity. Ignored by
	// LocalRefWriter.
	Label string
}

// PushTotals sums the server object-closure pushes of one WriteRefs batch
// (CAS-size observability, issue #95). ObjectsTotal counts objects reachable
// from the batch's refs (summed per ref — objects shared between refs count
// once per ref), ObjectsPushed the ones the server was missing (uploaded),
// BytesPushed their payload bytes. Reported=false means no push stats exist
// (local mode — LocalRefWriter never pushes). Observability only — callers
// must never gate behavior on these numbers.
type PushTotals struct {
	ObjectsTotal  int
	ObjectsPushed int
	BytesPushed   int64
	Reported      bool
}

// RefWriter is how stage drivers publish result refs. Two shapes exist:
// LocalRefWriter (local builds — write straight into the local store) and the
// runner's remote result publisher (push objects to the server, report
// name+key over NATS; the server gates and writes the refs).
//
// Batch invariants, frozen from jobs:
//
//   - WriteRefs publishes multiple result refs for the node as one batch.
//     This matters for the remote writer: the server's name-gating validates
//     a build-from-tree:F entry against the build-from:K value reported in
//     the SAME message — so RunBuildFrom's build-from:K + its
//     self-referential build-from-tree:F MUST travel together, one WriteRefs
//     call. Implementations with no such joint constraint (LocalRefWriter)
//     may just perform each write independently.
//
//   - Implementations publish the entries SEQUENTIALLY and stop on the first
//     error, so RunBuild's ordered batch [caches…, build-output-deps:F,
//     build-output:F] lands deps-before-output — doneness is derived from
//     build-output:F alone, so a crash between the two never yields a
//     done-looking build with a missing runtime closure.
//
// The returned PushTotals sums any server-push CAS stats observed while
// writing the batch (issue #95) — Reported is false when no such stats exist.
type RefWriter interface {
	// WriteRef publishes one result ref for the node being executed.
	WriteRef(ctx context.Context, name string, k key.Key) error
	// WriteRefs publishes multiple result refs for the node as one batch (see
	// the interface doc for the joint-validation and ordering invariants).
	WriteRefs(ctx context.Context, refs []Ref) (PushTotals, error)
}

// LocalRefWriter writes result refs straight into the local store — jobs'
// SignerRefWriter with signing removed: jobs-iroh refs are UNSIGNED
// reference records and the objects are already local, so there is nothing
// to sign, push, or await.
type LocalRefWriter struct {
	st *amber.Store
}

var _ RefWriter = (*LocalRefWriter)(nil)

// NewLocalRefWriter wraps st as a RefWriter.
func NewLocalRefWriter(st *amber.Store) *LocalRefWriter {
	return &LocalRefWriter{st: st}
}

// WriteRef publishes name -> k into the local store.
func (w *LocalRefWriter) WriteRef(ctx context.Context, name string, k key.Key) error {
	return w.st.PutRef(ctx, name, k)
}

// WriteRefs writes each ref in order, stopping on the first error (the
// ordered-batch invariant). LocalRefWriter never observes server-push stats,
// so the returned PushTotals is always the zero value (Reported=false).
func (w *LocalRefWriter) WriteRefs(ctx context.Context, refs []Ref) (PushTotals, error) {
	for _, r := range refs {
		if err := w.WriteRef(ctx, r.Name, r.Key); err != nil {
			return PushTotals{}, err
		}
	}
	return PushTotals{}, nil
}

// errRefsNotAssigned marks a WriteRefs batch declined because the node is not
// assigned to this runner: the duplicate-execution-loser signature. The job
// lost its race and must end silently — reporting it as a failure paints a
// phantom permanent failure onto a DONE node (issue #152). Produced by the
// remote result publisher; kept here as the publish-tail classification
// vocabulary pushOutcome keys on.
var errRefsNotAssigned = errors.New("refs batch declined: node not assigned to this runner")

// errRefsAckTimeout marks a WriteRefs ack wait that expired: a CONTROL-PLANE
// verdict (the server is congested or the link is bad), saying nothing about
// the job itself — it must not burn the job's retry budget (issue #153).
var errRefsAckTimeout = errors.New("refs ack wait timed out")
