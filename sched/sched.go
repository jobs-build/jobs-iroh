// Package sched is the single-process jobs-iroh scheduler: an in-memory
// dependency graph over the server's amber store, driven by the embedded
// NATS layout of docs/design/2026-07-23-sched-and-wire.md.
//
// Everything follows from one law (docs/research/jobs-scheduler-semantics.md):
// doneness is derived, never stored — a node is DONE iff its output ref
// exists in the CAS, and running the same job twice is wasteful but never
// wrong. The node table keyed (kind, key) with get-or-create semantics IS
// the join; states run waiting → ready → queued → running → publishing →
// done | failed, with ready instantaneous in-process. The buildvalue
// orchestrator (never placed) walks buildfrom → pluginresolve → pin →
// buildrun sequentially; consumers depend on buildvalue.
//
// Crash recovery: New replays nothing. Requests are in-memory (mirrored to
// the status KV for observation only) and are re-created by resubmit; the
// doneness fast-path at node creation makes a resubmitted finished closure
// free. Results published while the server was down are re-consumed via the
// durable RESULTS consumer, and results for unknown nodes are dropped.
package sched

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/wire"
)

// Tunables (ported jobs production defaults).
const (
	maxConsecRetryable = 3                      // retryable budget before hard-fail
	maxConsecControl   = 20                     // control-class consecutive cap
	defaultRetryBase   = time.Second            // backoff 1s·2^(n−1), capped 30s
	snapshotTick       = 250 * time.Millisecond // watcher coalescing (≤4/s)

	logHeadCap = 1 << 20 // per-(node,gen) captured output head
	logTailCap = 3 << 20 // per-(node,gen) captured output tail ring

	// Durable failure-record bounds. One record is one NATS message, so the
	// log snapshot must fit the embedded server's default 1MiB max_payload
	// with room for the Result and CBOR overhead.
	failureLogHeadCap = 256 << 10 // durable snapshot: first bytes of the head
	failureLogTailCap = 512 << 10 // durable snapshot: last bytes of the tail
	failuresPerNode   = 8         // FAILURES MaxMsgsPerSubject: last N failed attempts
	failuresMaxBytes  = 1 << 30   // whole-stream backstop

	diagnoseDefaultAttempts = 4 // Diagnose: attempts per node when unspecified
	diagnoseMaxNodes        = 4 // Diagnose: nodes per reply (MaxFrame budget)

	jobsAckWait       = 90 * time.Second // runner heartbeats msg.InProgress every 5s
	jobsMaxDeliver    = 25               // poison backstop; the node FSM owns retries
	jobsMaxAckPending = 4096             // slots gate concurrency, not the consumer

	// No-runners policy (issue #8). A runner is live while its last
	// hello/heartbeat is within runnerLiveWindow (3 missed 5s heartbeats);
	// runnerd re-hellos on every NATS (re)connect, so a server restart
	// re-learns platforms. Submits for a platform with no live runner are
	// rejected outright; an accepted request whose platform loses its last
	// live runner fails after noRunnerFailAfter of continuous absence,
	// checked every noRunnerCheckEvery.
	runnerLiveWindow   = 15 * time.Second
	noRunnerFailAfter  = 5 * time.Minute
	noRunnerCheckEvery = 10 * time.Second

	resultsDurable = "SCHED"
	resultsAckWait = 60 * time.Second
)

// Options configures New.
type Options struct {
	// Store is the server's amber store — the only shared CAS authority.
	Store *amber.Store
	// NC is a connection to the server's embedded NATS (JetStream enabled).
	NC *nats.Conn
	// Log receives scheduler logs; nil means slog.Default().
	Log *slog.Logger
	// Touch, when set, receives the ref names each runner result reports
	// as read (wire.Result.ReadRefs) — the GC access-tracking seam.
	Touch func(names []string)
}

// Sched is the scheduler. All exported methods are safe for concurrent use.
type Sched struct {
	store *amber.Store
	nc    *nats.Conn
	js    jetstream.JetStream
	jobs  jetstream.Stream // JOBS stream handle (queued-job purge on eviction)
	kv    jetstream.KeyValue
	log   *slog.Logger
	touch func(names []string)

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	subs   []*nats.Subscription
	iter   jetstream.MessagesContext

	// retryBase scales the retryable backoff (test hook; production 1s).
	retryBase time.Duration
	// noRunnerAfter/noRunnerCheck scale the no-runners watchdog (test
	// hooks; production noRunnerFailAfter / noRunnerCheckEvery).
	noRunnerAfter time.Duration
	noRunnerCheck time.Duration

	mu            sync.Mutex
	closed        bool
	version       uint64 // bumped on every transition; watch/KV coalescing cursor
	lastKVVersion uint64
	startedAt     time.Time
	nodes         map[nodeID]*node
	requests      map[string]*request
	watchers      map[*watcher]struct{}
	lanes         map[string]bool
	logs          map[string]*nodeLog
	logSubs       map[string]map[chan wire.LogChunk]struct{}
	fleet         map[string]wire.RunnerInfo
	lastReqKV     map[string]string

	// refCount caches Stats' ref count for refCountTTL: counting walks
	// every ref name, and an admin TUI polls stats every two seconds.
	refCountMu sync.Mutex
	refCount   int
	refCountAt time.Time
}

// refCountTTL bounds how stale Stats' ref count may be.
const refCountTTL = 10 * time.Second

// New creates/updates the JOBS work-queue, RESULTS and FAILURES streams and
// the status KV bucket, then starts the scheduler's background work: the results
// consumer, the logs.> fold (per-(node,gen) head+tail ring buffers), the
// runners.hello / runners.*.hb fleet fold, and the snapshot ticker.
//
// ctx bounds the setup calls AND parents the background work: cancelling it
// (or calling Close) stops the scheduler. New replays no state — see the
// package comment for the crash-recovery contract.
func New(ctx context.Context, o Options) (*Sched, error) {
	if o.Store == nil {
		return nil, errors.New("sched: Options.Store is required")
	}
	if o.NC == nil {
		return nil, errors.New("sched: Options.NC is required")
	}
	log := o.Log
	if log == nil {
		log = slog.Default()
	}

	js, err := jetstream.New(o.NC)
	if err != nil {
		return nil, fmt.Errorf("sched: jetstream: %w", err)
	}
	jobsStream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        wire.StreamJobs,
		Description: "jobs-iroh work queue (one msg per placeable node attempt)",
		Subjects:    []string{wire.SubjectJobsRoot + ".>"},
		Retention:   jetstream.WorkQueuePolicy,
		Duplicates:  10 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("sched: create %s stream: %w", wire.StreamJobs, err)
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        wire.StreamResults,
		Description: "jobs-iroh attempt results",
		Subjects:    []string{wire.SubjectResultsRoot + ".>"},
		Retention:   jetstream.LimitsPolicy,
		MaxAge:      7 * 24 * time.Hour,
		Duplicates:  15 * time.Minute,
	}); err != nil {
		return nil, fmt.Errorf("sched: create %s stream: %w", wire.StreamResults, err)
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:              wire.StreamFailures,
		Description:       "jobs-iroh per-attempt failure records (diagnostics, never load-bearing)",
		Subjects:          []string{wire.SubjectFailuresRoot + ".>"},
		Retention:         jetstream.LimitsPolicy,
		MaxAge:            7 * 24 * time.Hour,
		MaxMsgsPerSubject: failuresPerNode,
		MaxBytes:          failuresMaxBytes,
		Discard:           jetstream.DiscardOld,
		Duplicates:        15 * time.Minute,
	}); err != nil {
		return nil, fmt.Errorf("sched: create %s stream: %w", wire.StreamFailures, err)
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      wire.KVStatus,
		Description: "jobs-iroh node and request statuses (revision = watch cursor)",
	})
	if err != nil {
		return nil, fmt.Errorf("sched: create %s KV: %w", wire.KVStatus, err)
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, wire.StreamResults, jetstream.ConsumerConfig{
		Durable:       resultsDurable,
		FilterSubject: wire.SubjectResultsRoot + ".>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       resultsAckWait,
	})
	if err != nil {
		return nil, fmt.Errorf("sched: create results consumer: %w", err)
	}

	sctx, cancel := context.WithCancel(ctx)
	s := &Sched{
		store:         o.Store,
		nc:            o.NC,
		js:            js,
		jobs:          jobsStream,
		kv:            kv,
		log:           log,
		touch:         o.Touch,
		ctx:           sctx,
		cancel:        cancel,
		retryBase:     defaultRetryBase,
		noRunnerAfter: noRunnerFailAfter,
		noRunnerCheck: noRunnerCheckEvery,
		startedAt:     time.Now(),
		nodes:         map[nodeID]*node{},
		requests:      map[string]*request{},
		watchers:      map[*watcher]struct{}{},
		lanes:         map[string]bool{},
		logs:          map[string]*nodeLog{},
		logSubs:       map[string]map[chan wire.LogChunk]struct{}{},
		fleet:         map[string]wire.RunnerInfo{},
		lastReqKV:     map[string]string{},
	}

	iter, err := cons.Messages(jetstream.PullMaxMessages(1))
	if err != nil {
		cancel()
		return nil, fmt.Errorf("sched: results messages: %w", err)
	}
	s.iter = iter

	for _, sub := range []struct {
		subject string
		handler nats.MsgHandler
	}{
		{wire.SubjectLogsRoot + ".>", s.onLogChunk},
		{wire.SubjectRunnerHello, s.onRunnerHello},
		{"runners.*.hb", s.onRunnerHeartbeat},
	} {
		ns, err := o.NC.Subscribe(sub.subject, sub.handler)
		if err != nil {
			cancel()
			iter.Stop()
			s.drainSubs()
			return nil, fmt.Errorf("sched: subscribe %s: %w", sub.subject, err)
		}
		s.subs = append(s.subs, ns)
	}

	s.wg.Add(2)
	go s.runResults(iter)
	go s.runTicker()

	log.Info("scheduler started")
	return s, nil
}

// Close stops the background work and detaches every watcher and log
// follower. It does not close the store or the NATS connection — both are
// owned by the caller.
func (s *Sched) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	s.cancel()
	s.iter.Stop()
	s.drainSubs()
	s.wg.Wait()

	s.mu.Lock()
	for w := range s.watchers {
		if !w.closed {
			w.closed = true
			close(w.ch)
		}
	}
	s.watchers = map[*watcher]struct{}{}
	for node, set := range s.logSubs {
		for ch := range set {
			close(ch)
		}
		delete(s.logSubs, node)
	}
	s.mu.Unlock()
	return nil
}

func (s *Sched) drainSubs() {
	for _, sub := range s.subs {
		_ = sub.Unsubscribe()
	}
	s.subs = nil
}

// bumpLocked advances the transition version driving watcher and KV
// coalescing.
func (s *Sched) bumpLocked() { s.version++ }

// putNodeStatusLocked mirrors one node's state into the status KV (KV
// revision = watch cursor; the KV is observation-only, never re-read).
func (s *Sched) putNodeStatusLocked(n *node) {
	st := wire.NodeStatus{
		Phase:      n.phase,
		Gen:        n.gen,
		Runner:     n.runner,
		ErrSummary: n.errSummary,
	}
	if !n.startedAt.IsZero() {
		st.StartedNs = n.startedAt.UnixNano()
	}
	if _, err := s.kv.Put(s.ctx, n.name, wire.MustEncode(st)); err != nil && s.ctx.Err() == nil {
		s.log.Debug("node status write failed", "node", n.name, "error", err)
	}
}

// purgeNodeStatusLocked removes a dropped node's KV entry.
func (s *Sched) purgeNodeStatusLocked(name string) {
	if err := s.kv.Purge(s.ctx, name); err != nil && s.ctx.Err() == nil {
		s.log.Debug("node status purge failed", "node", name, "error", err)
	}
}
