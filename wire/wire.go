// Package wire is the shared vocabulary of the jobs-iroh scheduler: the NATS
// stream/subject/KV layout, the size-class ladder, the job and result payload
// schemas, and the node-name grammar. Both the server scheduler (sched) and
// the runner daemon (runnerd) speak exactly these types — change them only in
// lockstep.
//
// Encoding is plain (non-canonical) CBOR: wire payloads are never hashed.
package wire

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/amber-store/core/key"
	"github.com/fxamacker/cbor/v2"

	"github.com/jobs-build/jobs-iroh/resources"
)

// Stream, KV bucket and subject roots.
const (
	StreamJobs     = "JOBS"
	StreamResults  = "RESULTS"
	StreamFailures = "FAILURES"
	KVStatus       = "status"

	SubjectJobsRoot     = "jobs"     // jobs.<platform>.<class>
	SubjectResultsRoot  = "results"  // results.<node>
	SubjectLogsRoot     = "logs"     // logs.<node>
	SubjectFailuresRoot = "failures" // failures.<node>
	SubjectRunnerHello  = "runners.hello"
	subjectRunnerHBFmt  = "runners.%s.hb"
)

// Node kinds. buildvalue is server-internal (never placed on the queue).
const (
	KindImport        = "import"
	KindBuildFrom     = "buildfrom"
	KindPluginResolve = "pluginresolve"
	KindPin           = "pin"
	KindBuildRun      = "buildrun"
	KindBuildValue    = "buildvalue"
)

// Node phases (status KV + snapshots).
const (
	PhaseWaiting    = "waiting"
	PhaseQueued     = "queued"
	PhaseRunning    = "running"
	PhasePublishing = "publishing"
	PhaseDone       = "done"
	PhaseFailed     = "failed"
	PhaseUpstream   = "failed-upstream"
	PhaseCancelled  = "cancelled"
)

// Result classes.
const (
	ClassOK        = "ok"
	ClassHard      = "hard"
	ClassRetryable = "retryable"
	ClassControl   = "control"
	ClassCancelled = "cancelled"
)

// NodeName renders the queue/status identity of a node: <kind>_<hexkey>.
func NodeName(kind string, k key.Key) string { return kind + "_" + k.String() }

// ParseNodeName splits a node name, failing closed on anything but a kind
// from the fixed set and a canonical (lowercase, round-tripping) hex key.
func ParseNodeName(name string) (kind string, k key.Key, err error) {
	i := strings.IndexByte(name, '_')
	if i < 0 {
		return "", key.Key{}, fmt.Errorf("node name %q: no kind separator", name)
	}
	kind = name[:i]
	switch kind {
	case KindImport, KindBuildFrom, KindPluginResolve, KindPin, KindBuildRun, KindBuildValue:
	default:
		return "", key.Key{}, fmt.Errorf("node name %q: unknown kind", name)
	}
	hexPart := name[i+1:]
	raw, err := hex.DecodeString(hexPart)
	if err != nil {
		return "", key.Key{}, fmt.Errorf("node name %q: %w", name, err)
	}
	k, err = key.Parse(raw)
	if err != nil {
		return "", key.Key{}, fmt.Errorf("node name %q: %w", name, err)
	}
	if k.String() != hexPart {
		return "", key.Key{}, fmt.Errorf("node name %q: non-canonical key hex", name)
	}
	return kind, k, nil
}

// PlatformToken maps a jobs platform string (linux/arm64) onto a NATS
// subject token (linux-arm64).
func PlatformToken(platform string) string { return strings.ReplaceAll(platform, "/", "-") }

// JobsSubject is the work-queue subject for one (platform, class) lane.
func JobsSubject(platform string, c Class) string {
	return SubjectJobsRoot + "." + PlatformToken(platform) + "." + string(c)
}

// ResultsSubject carries one node's terminal results.
func ResultsSubject(node string) string { return SubjectResultsRoot + "." + node }

// FailuresSubject carries one node's durable failure records.
func FailuresSubject(node string) string { return SubjectFailuresRoot + "." + node }

// LogsSubject carries one node's live output chunks (core NATS, in-memory).
func LogsSubject(node string) string { return SubjectLogsRoot + "." + node }

// RunnerHBSubject carries one runner's heartbeats.
func RunnerHBSubject(runnerID string) string { return fmt.Sprintf(subjectRunnerHBFmt, runnerID) }

// ConsumerName is the durable consumer for one (platform, class) lane,
// shared by every runner that fits the class.
func ConsumerName(platform string, c Class) string {
	return "wq-" + PlatformToken(platform) + "-" + strings.ReplaceAll(string(c), ".", "_")
}

// JobMsgID / ResultMsgID / FailureMsgID are Nats-Msg-Id values (JetStream dedup).
func JobMsgID(node string, gen uint64) string     { return fmt.Sprintf("job-%s-%d", node, gen) }
func ResultMsgID(node string, gen uint64) string  { return fmt.Sprintf("result-%s-%d", node, gen) }
func FailureMsgID(node string, gen uint64) string { return fmt.Sprintf("failure-%s-%d", node, gen) }

// Job is one work-queue message: everything a runner needs to execute one
// stage attempt without asking the server anything over a side channel.
type Job struct {
	Node     string `cbor:"node"`
	Kind     string `cbor:"kind"`
	Key      []byte `cbor:"key"` // K or F (32-byte canonical)
	Gen      uint64 `cbor:"gen"`
	Platform string `cbor:"platform"`
	Class    string `cbor:"class"`

	// Def carries the canonical definition CBOR (the K-file content) for
	// kinds keyed by K (import, buildfrom); F-stage inputs ride in refs.
	Def []byte `cbor:"def,omitempty"`

	// PullRefs are the refs whose closures the runner must have locally
	// before running (server-computed).
	PullRefs []string `cbor:"pullRefs,omitempty"`

	CPUMilli int64 `cbor:"cpuMilli"`
	MemBytes int64 `cbor:"memBytes"`
}

// RefProposal is one runner-produced ref: the server gate-checks the name,
// verifies closure completeness, and publishes. Order is significant.
type RefProposal struct {
	Name string `cbor:"name"`
	Key  []byte `cbor:"key"`
	// Label is an optional display name riding a build-pinned proposal (the
	// recipe's `name =`); never identity, ignored by old servers.
	Label string `cbor:"label,omitempty"`
}

// Rusage is the attempt's resource report.
type Rusage struct {
	WallNs  int64 `cbor:"wallNs"`
	CPUNs   int64 `cbor:"cpuNs,omitempty"`
	MaxRSS  int64 `cbor:"maxRSS,omitempty"`
	Stored  int64 `cbor:"stored,omitempty"`  // CAS bytes newly stored by this attempt
	Deduped int64 `cbor:"deduped,omitempty"` // CAS objects deduped
}

// Result is the terminal report of one attempt. Published to
// results.<node> BEFORE the job message is acked; ResultMsgID dedups the
// crash-between window.
type Result struct {
	Node       string        `cbor:"node"`
	Gen        uint64        `cbor:"gen"`
	Runner     string        `cbor:"runner"`
	Class      string        `cbor:"class"` // ok | hard | retryable | control | cancelled
	Exit       int           `cbor:"exit,omitempty"`
	ErrSummary string        `cbor:"errSummary,omitempty"`
	Refs       []RefProposal `cbor:"refs,omitempty"` // in publish order
	Rusage     Rusage        `cbor:"rusage"`
	// ScratchRef is the runner-side push ref holding this attempt's output
	// closure; the server deletes it after committing the real refs.
	ScratchRef string `cbor:"scratchRef,omitempty"`
	// ReadRefs are the server ref names this attempt resolved as inputs
	// (pull or local cache hit), reported regardless of outcome so the
	// server's GC knows cached refs are in use. Additive: old runners
	// omit it — their warm-cache reads simply go unobserved.
	ReadRefs []string `cbor:"readRefs,omitempty"`
}

// FailureRecord origins: who produced the failing verdict.
const (
	FailOriginResult = "result" // runner-reported class (hard/retryable/control)
	FailOriginCommit = "commit" // ok result whose CheckComplete/ref-write failed
	FailOriginGate   = "gate"   // ok result whose ref batch the gate refused
	FailOriginServer = "server" // no runner attempt: unfold, pull-ref computation, …
)

// FailureRecord dispositions: what the scheduler did about it.
const (
	FailDispositionRetry  = "retry"  // re-enqueue scheduled
	FailDispositionFailed = "failed" // terminal
)

// FailureRecord is one failed (or budget-burning retried) attempt, folded
// durable on failures.<node> at the moment the scheduler decides the
// attempt's fate — the only moment the runner's Result, the node's retry
// counters, and the attempt's log ring coexist. Written and read by the
// server only (no runner lockstep); best-effort by contract — losing one
// costs observability, never correctness. One NATS message per record, so
// the log snapshot is trimmed to fit the 1MiB default max_payload.
type FailureRecord struct {
	Node     string `cbor:"node"`
	Gen      uint64 `cbor:"gen"`
	Platform string `cbor:"platform,omitempty"`
	// Label is the node's display-only name (recipe dep name, dir,
	// fetcher) at failure time; optional, never identity-bearing.
	Label string `cbor:"label,omitempty"`

	Origin      string `cbor:"origin"`
	Disposition string `cbor:"disposition"`

	ErrSummary  string   `cbor:"errSummary"` // the folded summary incl. prefixes ("commit: …")
	ConsecRetry int      `cbor:"consecRetry,omitempty"`
	ConsecCtrl  int      `cbor:"consecControl,omitempty"`
	BackoffMs   int64    `cbor:"backoffMs,omitempty"` // when Disposition == retry
	RequestIDs  []string `cbor:"requestIds,omitempty"`
	// ForF are the hex F keys of the buildvalues waiting on a KP-keyed
	// buildrun at failure time (sibling-sources design §10.3): the human
	// bridge from a buildrun_<KP> record back to the F-keyed pin/eval rows a
	// KP node can serve for MANY Fs. omitempty; empty for every other kind.
	ForF []string `cbor:"forF,omitempty"`

	// Result is the runner's verbatim report when Origin != server (runner
	// ID, class, exit, rusage; for commit/gate failures also the proposed
	// ref batch).
	Result *Result `cbor:"result,omitempty"`

	EnqueuedNs int64 `cbor:"enqueuedNs,omitempty"`
	StartedNs  int64 `cbor:"startedNs,omitempty"`
	FailedNs   int64 `cbor:"failedNs"`

	// Trimmed snapshot of the attempt's captured output (LogMissing when
	// the in-memory ring was evicted or the attempt never produced output).
	LogHead    []byte `cbor:"logHead,omitempty"`
	LogGap     int64  `cbor:"logGap,omitempty"`
	LogTail    []byte `cbor:"logTail,omitempty"`
	LogMissing bool   `cbor:"logMissing,omitempty"`
}

// LogChunk is one live output chunk on logs.<node> (core NATS).
type LogChunk struct {
	Gen    uint64 `cbor:"gen"`
	Stream string `cbor:"stream"` // stdout | stderr
	Seq    uint64 `cbor:"seq"`
	Data   []byte `cbor:"data"`
}

// NodeStatus is the status-KV value for one node.
type NodeStatus struct {
	Phase      string `cbor:"phase"`
	Gen        uint64 `cbor:"gen"`
	Runner     string `cbor:"runner,omitempty"`
	StartedNs  int64  `cbor:"startedNs,omitempty"`
	ErrSummary string `cbor:"errSummary,omitempty"`
}

// RequestStatus is the status-KV value for one request (key "req.<id>").
type RequestStatus struct {
	Phase     string `cbor:"phase"` // running | done | failed | cancelled
	TargetK   []byte `cbor:"targetK"`
	Platform  string `cbor:"platform"`
	Counts    Counts `cbor:"counts"`
	CreatedNs int64  `cbor:"createdNs"`
	// Error mirrors the request-level failure reason (api.Snapshot.Error);
	// additive + omitempty, absent for node-attributed failures.
	Error string `cbor:"error,omitempty"`
}

// Counts summarizes a request's closure.
type Counts struct {
	Total   int `cbor:"total"`
	Done    int `cbor:"done"`
	Running int `cbor:"running"`
	Failed  int `cbor:"failed"`
	Waiting int `cbor:"waiting"`
}

// RunnerInfo is the fleet view of one runner (hello ∪ latest heartbeat).
type RunnerInfo struct {
	ID       string `cbor:"id"`
	Name     string `cbor:"name"`
	Platform string `cbor:"platform"`
	Size     string `cbor:"size"`
	InFlight int    `cbor:"inFlight"`
	FreeCPU  int64  `cbor:"freeCPUMilli"`
	FreeMem  int64  `cbor:"freeMemBytes"`
	SeenNs   int64  `cbor:"seenNs"`
}

// Hello is the runner's connect announcement on runners.hello.
type Hello struct {
	ID       string `cbor:"id"`
	Name     string `cbor:"name"`
	Platform string `cbor:"platform"`
	Size     string `cbor:"size"`
	CPUMilli int64  `cbor:"cpuMilli"`
	MemBytes int64  `cbor:"memBytes"`
}

// Heartbeat is the runner's periodic liveness+capacity report.
type Heartbeat struct {
	ID       string `cbor:"id"`
	InFlight int    `cbor:"inFlight"`
	FreeCPU  int64  `cbor:"freeCPUMilli"`
	FreeMem  int64  `cbor:"freeMemBytes"`
}

// Encode/Decode wrap the package's CBOR profile.
func Encode(v any) ([]byte, error) { return cbor.Marshal(v) }
func Decode(b []byte, v any) error { return cbor.Unmarshal(b, v) }
func MustEncode(v any) []byte      { b, err := cbor.Marshal(v); must(err); return b }
func must(err error) {
	if err != nil {
		panic(err)
	}
}

// ParseKey parses a 32-byte canonical store key from wire bytes.
func ParseKey(b []byte) (key.Key, error) { return key.Parse(b) }

// ResourcesOf converts a job's requirement into the resources value type.
func (j Job) Resources() resources.Resources {
	return resources.Resources{CPUMilli: j.CPUMilli, MemBytes: j.MemBytes}
}
