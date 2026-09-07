# GC and Auto-Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** jobs-server tracks every reference's last access (server reads, remote pulls, runner job reports, registry pin-asserts), expires refs unused for a configurable retention (default 30 days), and drives amber-store-core's mark-and-sweep collector hourly, logging disk/ref/garbage stats — with registry-served images pinned forever and a full admin surface (stats block, refs columns, manual gc, pin/unpin).

**Architecture:** A new `reftrack.Tracker` (in-memory map + CBOR snapshot in the data dir) receives touches from three hooks (an `amber.Store` observer, `amberiroh.Server` pull/push callbacks, `wire.Result.ReadRefs` forwarded by sched) and pin-asserts from a new `TPin` amberiroh message the registry sends. A `gcRunner` in `serve/` owns the tracker plus a `gc.Collector`, runs the hourly sweep (reconcile → expire → Status → conditional Run → flush → log), and backs four new admin frames. Every ref PUT is guarded by the collector's `PrepareRef` via a nil-safe `RefGuard` seam in both `amber.PutRef` and `amberiroh.handlePush`.

**Tech Stack:** Go, amber-store-core `gc` package (mark-sweep, merged 2026-08-25), fxamacker/cbor, embedded NATS, iroh QUIC, urfave/cli, bubbletea.

**Design spec:** `docs/design/2026-08-25-gc-auto-cleanup.md` — read it first. Two deliberate deviations from the spec, both noted in Task 13's doc update: (a) runner `ReadRefs` records the `job.PullRefs` actually ensured, not the out-of-band `shell:` pull inside `buildRunCfg` — `shell:`/`fetcher:` are protected classes whose clock is irrelevant, and `build-cache:` rides PullRefs whenever it exists server-side; (b) the e2e test uses a tiny retention + manual sweep instead of an injected clock.

## Global Constraints

- Build/test ONLY via the Nix devShell: `nix develop -c go test ./...`, `nix develop -c go build ./...` (`GOPRIVATE=github.com/jobs-build/*` comes from `.envrc`).
- After touching anything with platform-split files, cross-vet macOS: `nix develop -c env CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go vet ./...`. Run it at least in Tasks 8 and 13 (the `unix.Statfs` code) and at the end of every task that adds imports.
- NEVER change the pinned chunker params in `amber/` (32Ki/128Ki/256Ki, ItemBits 7) or `amberiroh.ALPN` (`amber-store-iroh/1`).
- `wire/` and `api/` are frozen contracts: additive CBOR fields with `omitempty` only (the established pattern: `RefProposal.Label`, `NodeSnap.Deps`).
- No `jobs-runner-nats/3.0` ALPN bump: old runners produce missing-field results, never wrong ones.
- Every commit message body ends with:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01BD4piG8AjhmpJ8ZeCTMauu`
- Run `nix develop -c gofmt -l .` before each commit; it must print nothing.

## File Structure

| File | Responsibility |
|---|---|
| `reftrack/reftrack.go` (new) | Tracker: touch/pin/expire/reconcile + CBOR snapshot persistence |
| `reftrack/reftrack_test.go` (new) | Tracker unit tests |
| `amber/store.go`, `amber/ref.go` | `SetObserver` (touch on GetRef), `SetRefGuard` (PrepareRef around PutRef) |
| `amber/ref_test.go` | Guard/observer tests |
| `amberiroh/protocol.go` | `TPin` frame type + `Msg.Names` field |
| `amberiroh/server.go` | `SetOnAccess`/`SetOnPin`/`SetRefGuard`, `handlePin`, hooks in `handlePull`/`handlePush` |
| `amberiroh/server_gc_test.go` (new) | net.Pipe tests for pin/access/guard |
| `amberclient/client.go` | `Pin(ctx, names)` control-stream call |
| `wire/wire.go` | `Result.ReadRefs []string` |
| `runnerd/job.go` | Record ensured PullRefs into the result |
| `runnerd/result_test.go` (new) | `buildResult` carries ReadRefs on every class |
| `sched/sched.go`, `sched/results.go` | `Options.Touch` forwarded from `handleResult` |
| `serve/gc.go` (new) | `gcRunner`: collector+tracker wiring, sweep, hourly loop, stats, statfs |
| `serve/gc_test.go` (new) | End-to-end sweep test over a real server |
| `serve/serve.go` | GC options + wiring into Run |
| `serve/apihandler.go` | GC block in stats, refs columns, `gc`/`pin`/`unpin` frames |
| `serve/api_gc_test.go` (new) | Admin frame tests |
| `api/api.go` | `GCStats`, `GCRequest`, `PinRequest`, new frame consts, RefInfo/StatsReply extensions |
| `cmd/jobs-server/main.go` | `--gc-retention`, `--gc-interval`, `--gc-rate`, `--gc-min-free` |
| `clientcli/admin.go` | stats GC block, refs columns, `admin gc`/`pin`/`unpin` |
| `tui/format.go` | `statsLines` GC lines, `refRow`/`refHeader` columns |
| `registryd/pins.go` (new) | `pinAsserter` (coalesce + disable) and `assertPins` |
| `registryd/pins_test.go` (new) | pinAsserter unit tests |
| `registryd/sync.go`, `registryd/handler.go`, `registryd/registryd.go` | `reconnSync.Pin`, assert calls on manifest serves |
| `CLAUDE.md`, `CHANGELOG.md`, design doc | Documentation |

---

### Task 1: Bump amber-store-core to the GC merge

**Files:**
- Modify: `go.mod`, `go.sum`

**Interfaces:**
- Produces: importable `github.com/amber-store/core/gc` package: `gc.Open(dir string, objects *packstore.Store, refs *refstore.Store, opts gc.Options) (*gc.Collector, error)`; `(*Collector).PrepareRef(root key.Key) (commit, abort func(), err error)`; `(*Collector).Run(ctx, garbage float64) (gc.CycleStats, error)`; `(*Collector).Status(ctx) (gc.Status, error)`; `(*Collector).Close() error`; `gc.Options{Grace, Garbage, MinFree, Rate, Interval, Jobs}`; `gc.Status{Packs, LiveBytes, GarbageBytes, Refs, Marked, Last *CycleStats, LastError string}`; `gc.CycleStats{Start, Duration, MarkDuration, SweepDuration, Threshold, Marked, Scored, Reaped, CopiedRecords, CopiedBytes, FreedBytes}`.

- [ ] **Step 1: Bump the dependency**

The local clone at `~/jobs-build/amber-store-core` has the mark-sweep merge as commit `a2ff135` (2026-08-25). Fetch it as a module version:

```bash
cd /home/dragan/jobs-build/jobs-iroh
nix develop -c go get github.com/amber-store/core@a2ff135cd1c94bdd04c9eca4c5019062eb4dbe81
nix develop -c go mod tidy
```

Expected: `go.mod` line 13 changes to a `v0.0.0-20260825…-a2ff135…` pseudo-version. If the module proxy/GOPRIVATE fetch fails, check `.envrc` was loaded (`direnv allow`).

- [ ] **Step 2: Verify nothing broke**

```bash
nix develop -c go build ./...
nix develop -c go test ./amber/... ./amberiroh/...
nix develop -c env CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go vet ./...
```

Expected: all pass — the bump is additive (new `gc` package, new packstore methods `NewMarkSet`/`BeginBarrier`/`AbortBarrier`/`ObserveKeys`/`Compact`/`Liveness`/`Segments`; nothing existing changed).

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "deps: bump amber-store-core to the mark-sweep GC merge"
```

---

### Task 2: `reftrack` package

**Files:**
- Create: `reftrack/reftrack.go`
- Create: `reftrack/reftrack_test.go`

**Interfaces:**
- Produces:
  - `reftrack.New() *Tracker`
  - `(*Tracker).Touch(name string)` — family rule applied (build-output ↔ build-output-deps share the clock)
  - `(*Tracker).TouchAll(names []string)`
  - `(*Tracker).Pin(name string)` / `(*Tracker).Unpin(name string)`
  - `(*Tracker).Get(name string) (Entry, bool)`
  - `(*Tracker).Reconcile(existing []string, now time.Time)` — seed unknown names, drop vanished ones
  - `(*Tracker).Expired(retention time.Duration, now time.Time) []string` — skips protected/pinned; `build-output-deps:` names sorted after all others
  - `(*Tracker).Forget(name string)`
  - `(*Tracker).Counts() (total, pinned int)`
  - `(*Tracker).Load(path string) error` (missing file → nil; corrupt → error, tracker left empty)
  - `(*Tracker).Flush(path string) error` (atomic tmp+rename)
  - `reftrack.Entry{FirstSeen, LastAccess time.Time; Pinned bool}`
  - `reftrack.Protected(name string) bool` — true for `shell:`, `fetcher:`, `seed-src:` prefixes

- [ ] **Step 1: Write the failing tests**

Create `reftrack/reftrack_test.go`:

```go
package reftrack

import (
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestTouchAndExpire(t *testing.T) {
	tr := New()
	base := time.Now()
	tr.Reconcile([]string{"b"}, base.Add(-2*time.Hour)) // b: seeded two hours ago
	tr.Reconcile([]string{"a", "b"}, base)              // a: fresh; b keeps its old clock

	exp := tr.Expired(time.Hour, base)
	if !slices.Contains(exp, "b") {
		t.Fatalf("b should expire, got %v", exp)
	}
	if slices.Contains(exp, "a") {
		t.Fatalf("a is fresh, got %v", exp)
	}

	// A touch resets the clock (Touch stamps time.Now() ≈ base).
	tr.Touch("b")
	if exp := tr.Expired(time.Hour, base); len(exp) != 0 {
		t.Fatalf("nothing should expire after the touch, got %v", exp)
	}
}

func TestReconcileSeedsAndDrops(t *testing.T) {
	tr := New()
	now := time.Now()
	tr.Reconcile([]string{"x"}, now)
	e, ok := tr.Get("x")
	if !ok || !e.FirstSeen.Equal(now) || !e.LastAccess.Equal(now) {
		t.Fatalf("seed: got %+v ok=%v", e, ok)
	}
	// A later reconcile must not reset the clock of a known name…
	tr.Reconcile([]string{"x"}, now.Add(time.Hour))
	if e, _ := tr.Get("x"); !e.LastAccess.Equal(now) {
		t.Fatalf("reconcile reset the clock: %+v", e)
	}
	// …and drops vanished names.
	tr.Reconcile(nil, now.Add(2*time.Hour))
	if _, ok := tr.Get("x"); ok {
		t.Fatal("vanished name kept")
	}
}

func TestPinnedAndProtectedNeverExpire(t *testing.T) {
	tr := New()
	base := time.Now()
	tr.Reconcile([]string{"shell:linux/amd64", "fetcher:github:linux/amd64",
		"seed-src:shell:deadbeef", "keep", "doomed"}, base)
	tr.Pin("keep")
	exp := tr.Expired(time.Hour, base.Add(48*time.Hour))
	if !slices.Equal(exp, []string{"doomed"}) {
		t.Fatalf("want [doomed], got %v", exp)
	}
	tr.Unpin("keep")
	exp = tr.Expired(time.Hour, base.Add(48*time.Hour))
	slices.Sort(exp)
	if !slices.Equal(exp, []string{"doomed", "keep"}) {
		t.Fatalf("after unpin want [doomed keep], got %v", exp)
	}
}

func TestFamilyRule(t *testing.T) {
	tr := New()
	base := time.Now()
	out, deps := "build-output:abc", "build-output-deps:abc"
	tr.Reconcile([]string{out, deps}, base)

	// Touching either touches both.
	tr.Touch(deps)
	eo, _ := tr.Get(out)
	ed, _ := tr.Get(deps)
	if !eo.LastAccess.Equal(ed.LastAccess) || eo.LastAccess.Equal(base) {
		t.Fatalf("family clocks diverge: out=%v deps=%v", eo.LastAccess, ed.LastAccess)
	}

	// Expiry orders output strictly before deps.
	exp := tr.Expired(time.Nanosecond, base.Add(time.Hour))
	io, id := slices.Index(exp, out), slices.Index(exp, deps)
	if io == -1 || id == -1 || io > id {
		t.Fatalf("want output before deps, got %v", exp)
	}
}

func TestPinTouches(t *testing.T) {
	tr := New()
	tr.Pin("p")
	if e, ok := tr.Get("p"); !ok || !e.Pinned || e.LastAccess.IsZero() {
		t.Fatalf("pin must create+touch: %+v ok=%v", e, ok)
	}
}

func TestLoadFlushRoundTrip(t *testing.T) {
	tr := New()
	tr.Touch("a")
	tr.Pin("p")
	path := filepath.Join(t.TempDir(), "refaccess.cbor")
	if err := tr.Flush(path); err != nil {
		t.Fatal(err)
	}
	tr2 := New()
	if err := tr2.Load(path); err != nil {
		t.Fatal(err)
	}
	a1, _ := tr.Get("a")
	a2, ok := tr2.Get("a")
	if !ok || !a1.LastAccess.Equal(a2.LastAccess) {
		t.Fatalf("round trip lost a: %+v ok=%v", a2, ok)
	}
	if p, ok := tr2.Get("p"); !ok || !p.Pinned {
		t.Fatalf("round trip lost pin: %+v ok=%v", p, ok)
	}
}

func TestLoadMissingAndCorrupt(t *testing.T) {
	tr := New()
	if err := tr.Load(filepath.Join(t.TempDir(), "nope.cbor")); err != nil {
		t.Fatalf("missing snapshot must be a clean start: %v", err)
	}
	bad := filepath.Join(t.TempDir(), "bad.cbor")
	if err := writeFileAtomic(bad, []byte("not cbor")); err != nil {
		t.Fatal(err)
	}
	tr2 := New()
	if err := tr2.Load(bad); err == nil {
		t.Fatal("corrupt snapshot should report an error")
	}
	if total, _ := tr2.Counts(); total != 0 {
		t.Fatal("corrupt snapshot must leave the tracker empty")
	}
}
```

- [ ] **Step 2: Run to verify failure**

```bash
nix develop -c go test ./reftrack/
```
Expected: FAIL — package does not exist / undefined symbols.

- [ ] **Step 3: Implement**

Create `reftrack/reftrack.go`:

```go
// Package reftrack records when each server reference was last used, so the
// GC sweep can expire refs unused for the retention window. It is the
// server-side companion of docs/design/2026-08-25-gc-auto-cleanup.md §3:
// entries are seeded at first sight (never from CreatedAt — the safe-upgrade
// rule), touched on every read, and persisted as a CBOR snapshot whose loss
// is benign (a ref merely looks staler than it is; worst case an early
// rebuild, "wasteful but never wrong").
package reftrack

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"
)

// protectedPrefixes are the bootstrap-seed classes that never expire
// regardless of clock: runners and images depend on them existing.
var protectedPrefixes = []string{"shell:", "fetcher:", "seed-src:"}

// Family prefixes: build-output:X and build-output-deps:X share one clock
// and expire output-before-deps (mirror of the "deps strictly before
// output" write invariant).
const (
	outputPrefix = "build-output:"
	depsPrefix   = "build-output-deps:"
)

// Protected reports whether name belongs to a never-expire class.
func Protected(name string) bool {
	for _, p := range protectedPrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// Entry is one ref's tracked state.
type Entry struct {
	FirstSeen  time.Time
	LastAccess time.Time
	Pinned     bool
}

// Tracker is safe for concurrent use; Touch is cheap enough for read paths.
type Tracker struct {
	mu      sync.Mutex
	entries map[string]Entry
}

func New() *Tracker {
	return &Tracker{entries: map[string]Entry{}}
}

// Touch resets name's clock (seeding it if unknown), and mirrors the touch
// onto its build-output family sibling when that sibling is tracked.
func (t *Tracker) Touch(name string) {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.touchLocked(name, now)
	if sib, ok := familySibling(name); ok {
		if _, tracked := t.entries[sib]; tracked {
			t.touchLocked(sib, now)
		}
	}
}

func (t *Tracker) TouchAll(names []string) {
	for _, n := range names {
		t.Touch(n)
	}
}

func (t *Tracker) touchLocked(name string, now time.Time) {
	e, ok := t.entries[name]
	if !ok {
		e.FirstSeen = now
	}
	e.LastAccess = now
	t.entries[name] = e
}

// familySibling maps build-output:X ↔ build-output-deps:X.
func familySibling(name string) (string, bool) {
	if s, ok := strings.CutPrefix(name, depsPrefix); ok {
		return outputPrefix + s, true
	}
	if s, ok := strings.CutPrefix(name, outputPrefix); ok {
		return depsPrefix + s, true
	}
	return "", false
}

// Pin marks name kept-forever and touches it (a pin is an access).
func (t *Tracker) Pin(name string) {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.touchLocked(name, now)
	e := t.entries[name]
	e.Pinned = true
	t.entries[name] = e
}

// Unpin clears the flag; the ref then lives by its access clock.
func (t *Tracker) Unpin(name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e, ok := t.entries[name]; ok {
		e.Pinned = false
		t.entries[name] = e
	}
}

func (t *Tracker) Get(name string) (Entry, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.entries[name]
	return e, ok
}

func (t *Tracker) Forget(name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.entries, name)
}

// Counts reports tracked and pinned totals.
func (t *Tracker) Counts() (total, pinned int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, e := range t.entries {
		if e.Pinned {
			pinned++
		}
	}
	return len(t.entries), pinned
}

// Reconcile aligns the tracker with the store's ref listing: unknown names
// are seeded at now (the safe-upgrade rule — never CreatedAt), tracked names
// keep their clocks, entries for vanished refs are dropped.
func (t *Tracker) Reconcile(existing []string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	keep := make(map[string]bool, len(existing))
	for _, name := range existing {
		keep[name] = true
		if _, ok := t.entries[name]; !ok {
			t.entries[name] = Entry{FirstSeen: now, LastAccess: now}
		}
	}
	for name := range t.entries {
		if !keep[name] {
			delete(t.entries, name)
		}
	}
}

// Expired lists the names whose LastAccess is older than now−retention,
// skipping pinned entries and protected classes. build-output-deps: names
// sort after everything else so the caller deletes output before deps.
func (t *Tracker) Expired(retention time.Duration, now time.Time) []string {
	cutoff := now.Add(-retention)
	t.mu.Lock()
	var out []string
	for name, e := range t.entries {
		if e.Pinned || Protected(name) {
			continue
		}
		if e.LastAccess.Before(cutoff) {
			out = append(out, name)
		}
	}
	t.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		di := strings.HasPrefix(out[i], depsPrefix)
		dj := strings.HasPrefix(out[j], depsPrefix)
		if di != dj {
			return !di
		}
		return out[i] < out[j]
	})
	return out
}

// snapEntry is the persisted shape (ns since epoch keeps the file compact).
type snapEntry struct {
	First  int64 `cbor:"f"`
	Last   int64 `cbor:"l"`
	Pinned bool  `cbor:"p,omitempty"`
}

// Load replaces the tracker's state with the snapshot at path. A missing
// file is a clean start (nil); a corrupt one leaves the tracker empty and
// returns the decode error for logging.
func (t *Tracker) Load(path string) error {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var snap map[string]snapEntry
	if err := cbor.Unmarshal(b, &snap); err != nil {
		return fmt.Errorf("reftrack: decode %s: %w", path, err)
	}
	entries := make(map[string]Entry, len(snap))
	for name, s := range snap {
		entries[name] = Entry{
			FirstSeen:  time.Unix(0, s.First),
			LastAccess: time.Unix(0, s.Last),
			Pinned:     s.Pinned,
		}
	}
	t.mu.Lock()
	t.entries = entries
	t.mu.Unlock()
	return nil
}

// Flush writes the snapshot atomically (tmp + rename).
func (t *Tracker) Flush(path string) error {
	t.mu.Lock()
	snap := make(map[string]snapEntry, len(t.entries))
	for name, e := range t.entries {
		snap[name] = snapEntry{First: e.FirstSeen.UnixNano(), Last: e.LastAccess.UnixNano(), Pinned: e.Pinned}
	}
	t.mu.Unlock()
	b, err := cbor.Marshal(snap)
	if err != nil {
		return fmt.Errorf("reftrack: encode snapshot: %w", err)
	}
	return writeFileAtomic(path, b)
}

func writeFileAtomic(path string, b []byte) error {
	tmp := filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+".tmp")
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
```

- [ ] **Step 4: Run tests**

```bash
nix develop -c go test ./reftrack/
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add reftrack/
git commit -m "reftrack: access-time tracker for server refs (touch/pin/expire/snapshot)"
```

---

### Task 3: `amber` observer + ref guard

**Files:**
- Modify: `amber/store.go` (Store struct + setters), `amber/ref.go` (GetRef touch, PutRef guard)
- Test: `amber/ref_test.go` (append)

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `amber.RefGuard` interface: `PrepareRef(root key.Key) (commit, abort func(), err error)` — satisfied directly by `*gc.Collector`.
  - `(*amber.Store).SetObserver(f func(name string))` — called on every successful `GetRef`/`GetKey` (one hook site: `GetRef`). Call before serving traffic; not synchronized.
  - `(*amber.Store).SetRefGuard(g RefGuard)` — nil guard = current behavior.

- [ ] **Step 1: Write the failing tests** (append to `amber/ref_test.go`; match the file's existing test helpers for opening a store — it opens with `Open(t.TempDir())`):

```go
type fakeGuard struct {
	prepared []key.Key
	commits  int
	aborts   int
	err      error
}

func (g *fakeGuard) PrepareRef(root key.Key) (func(), func(), error) {
	g.prepared = append(g.prepared, root)
	if g.err != nil {
		return nil, nil, g.err
	}
	return func() { g.commits++ }, func() { g.aborts++ }, nil
}

func TestObserverFiresOnGetRef(t *testing.T) {
	ctx := context.Background()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	k, err := s.IngestFile(ctx, []byte("observed"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PutRef(ctx, "obs", k); err != nil {
		t.Fatal(err)
	}

	var touched []string
	s.SetObserver(func(name string) { touched = append(touched, name) })

	if _, _, err := s.GetKey(ctx, "obs"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.GetKey(ctx, "absent"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListRefs(ctx); err != nil {
		t.Fatal(err)
	}
	// Exactly one touch: the successful GetKey→GetRef. Absent names and
	// listings are not accesses.
	if len(touched) != 1 || touched[0] != "obs" {
		t.Fatalf("touched = %v, want [obs]", touched)
	}
}

func TestRefGuardCommitAndAbort(t *testing.T) {
	ctx := context.Background()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	k, err := s.IngestFile(ctx, []byte("guarded"))
	if err != nil {
		t.Fatal(err)
	}

	g := &fakeGuard{}
	s.SetRefGuard(g)
	if err := s.PutRef(ctx, "guarded", k); err != nil {
		t.Fatal(err)
	}
	if len(g.prepared) != 1 || g.prepared[0] != k || g.commits != 1 || g.aborts != 0 {
		t.Fatalf("guard = %+v", g)
	}

	g.err = errors.New("closure incomplete")
	if err := s.PutRef(ctx, "refused", k); err == nil {
		t.Fatal("guard error must refuse the put")
	}
	if _, ok, _ := s.GetKey(ctx, "refused"); ok {
		t.Fatal("refused ref must not exist")
	}
}
```

Add `"errors"` and `"context"` to the test file's imports if absent.

- [ ] **Step 2: Run to verify failure**

```bash
nix develop -c go test ./amber/ -run 'TestObserverFires|TestRefGuard'
```
Expected: FAIL — `SetObserver`/`SetRefGuard`/`fakeGuard` mismatch undefined.

- [ ] **Step 3: Implement**

In `amber/store.go`, extend the struct and add setters:

```go
// Store owns an open amber-store-core store: … (existing comment unchanged)
type Store struct {
	objects *packstore.Store
	refs    *refstore.Store

	// observe, when set, is called with the name of every reference read
	// that found its record (GetRef/GetKey) — the GC access-tracking seam.
	// Set once before the store serves traffic; not synchronized.
	observe func(name string)
	// guard, when set, brackets every PutRef with the collector's
	// PrepareRef so a reference published mid-mark keeps its closure.
	guard RefGuard
}

// RefGuard is the GC write-barrier seam around reference publication.
// *gc.Collector satisfies it directly. Exactly one of commit/abort must be
// called after PrepareRef returns nil.
type RefGuard interface {
	PrepareRef(root key.Key) (commit, abort func(), err error)
}

// SetObserver installs the ref-read hook. Call before serving traffic.
func (s *Store) SetObserver(f func(name string)) { s.observe = f }

// SetRefGuard installs the reference write barrier. Call before serving
// traffic; nil keeps the unguarded behavior (runner/registry private
// stores, tests).
func (s *Store) SetRefGuard(g RefGuard) { s.guard = g }
```

In `amber/ref.go`:

In `GetRef`, after `decodeRef` succeeds (replace the final `return decodeRef(name, b)`):

```go
	ri, err := decodeRef(name, b)
	if err == nil && s.observe != nil {
		s.observe(name)
	}
	return ri, err
```

In `PutRef`, replace the final `return s.refs.Put(name, b)`:

```go
	if s.guard != nil {
		commit, abort, gerr := s.guard.PrepareRef(k)
		if gerr != nil {
			return fmt.Errorf("gc guard for ref %q: %w", name, gerr)
		}
		if err := s.refs.Put(name, b); err != nil {
			abort()
			return err
		}
		commit()
		return nil
	}
	return s.refs.Put(name, b)
```

- [ ] **Step 4: Run tests**

```bash
nix develop -c go test ./amber/
```
Expected: PASS (whole package — nil-guard behavior unchanged).

- [ ] **Step 5: Commit**

```bash
git add amber/
git commit -m "amber: ref-read observer and PutRef guard (GC seams)"
```

---

### Task 4: `amberiroh` — access/pin hooks, push guard, TPin frame

**Files:**
- Modify: `amberiroh/protocol.go` (TPin const, Msg.Names)
- Modify: `amberiroh/server.go` (hooks, handlePin, guard in handlePush, touch in handlePull)
- Create: `amberiroh/server_gc_test.go`

**Interfaces:**
- Consumes: nothing from other tasks (declares its own RefGuard — amberiroh must not import `amber`).
- Produces:
  - `amberiroh.TPin = 13` frame type; `Msg.Names []string` (cbor key 17).
  - `(*Server).SetOnAccess(f func(name string))` — fired on successful pull resolution and on push commit.
  - `(*Server).SetOnPin(f func(name string))` — fired per existing name in a TPin.
  - `(*Server).SetRefGuard(g RefGuard)` with `amberiroh.RefGuard` structurally identical to `amber.RefGuard`.
  - Wire contract for Task 5: client sends `Msg{Type: TPin, Names: […]}`, server replies `Msg{Type: TOK}` (or TErr).

- [ ] **Step 1: Write the failing tests**

Create `amberiroh/server_gc_test.go`:

```go
package amberiroh

import (
	"log/slog"
	"net"
	"path/filepath"
	"slices"
	"testing"

	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/core/refstore"
)

// gcTestServer opens a Server over throwaway stores with one raw ref
// record named "present" (handlePin/handlePull only check existence).
func gcTestServer(t *testing.T) *Server {
	t.Helper()
	dir := t.TempDir()
	objects, err := packstore.Open(filepath.Join(dir, "objects"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { objects.Close() })
	refs, err := refstore.Open(filepath.Join(dir, "refs"), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { refs.Close() })
	if err := refs.Put("present", []byte{0xa0}); err != nil { // any bytes: existence is all these paths check
		t.Fatal(err)
	}
	return New(slog.Default(), objects, refs)
}

// exchange runs one HandleStream conversation over a pipe.
func exchange(t *testing.T, srv *Server, send Msg) Msg {
	t.Helper()
	c, s := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.HandleStream("test-peer", s)
	}()
	if err := WriteMsg(c, send); err != nil {
		t.Fatal(err)
	}
	m, err := ReadMsg(c)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	<-done
	return m
}

func TestPinAssert(t *testing.T) {
	srv := gcTestServer(t)
	var pinned []string
	srv.SetOnPin(func(name string) { pinned = append(pinned, name) })

	m := exchange(t, srv, Msg{Type: TPin, Names: []string{"present", "absent"}})
	if m.Type != TOK {
		t.Fatalf("reply type %d, want TOK", m.Type)
	}
	if !slices.Equal(pinned, []string{"present"}) {
		t.Fatalf("pinned = %v, want [present]", pinned)
	}
}

func TestPinWithoutHookStillOK(t *testing.T) {
	srv := gcTestServer(t)
	if m := exchange(t, srv, Msg{Type: TPin, Names: []string{"present"}}); m.Type != TOK {
		t.Fatalf("reply type %d, want TOK", m.Type)
	}
}

func TestPullFiresOnAccess(t *testing.T) {
	srv := gcTestServer(t)
	var accessed []string
	srv.SetOnAccess(func(name string) { accessed = append(accessed, name) })

	c, s := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.HandleStream("test-peer", s)
	}()
	if err := WriteMsg(c, Msg{Type: TPull, Name: "present"}); err != nil {
		t.Fatal(err)
	}
	m, err := ReadMsg(c)
	if err != nil || m.Type != TRef {
		t.Fatalf("m=%+v err=%v, want TRef", m, err)
	}
	// End the transfer: an empty TWants finishes the Send loop.
	if err := WriteMsg(c, Msg{Type: TWants}); err != nil {
		t.Fatal(err)
	}
	c.Close()
	<-done
	if !slices.Equal(accessed, []string{"present"}) {
		t.Fatalf("accessed = %v, want [present]", accessed)
	}
}

type recordingGuard struct {
	prepared int
	commits  int
}

func (g *recordingGuard) PrepareRef(root key.Key) (func(), func(), error) {
	g.prepared++
	return func() { g.commits++ }, func() {}, nil
}
```

(The push-guard path is exercised end-to-end by the existing push tests once the guard is nil-safe; `recordingGuard` compiles the interface. A full guarded-push exchange needs a real object closure — the serve e2e in Task 8 covers it against the real collector.)

- [ ] **Step 2: Run to verify failure**

```bash
nix develop -c go test ./amberiroh/ -run 'TestPin|TestPullFires'
```
Expected: FAIL — `TPin`, `Names`, `SetOnPin`, `SetOnAccess` undefined.

- [ ] **Step 3: Implement**

`amberiroh/protocol.go` — extend the frame-type const block:

```go
	TAttach  = 11 // client→server: attach this stream to a transfer (Token)
	TAccept  = 12 // server→client: sharded transfer accepted (Token)
	TPin     = 13 // client→server: keep these refs forever (Names) — GC pin-assert
```

and the Msg struct (after DataEndpoints):

```go
	// Names are the ref names of a TPin assert. Additive: old peers
	// ignore the field, old servers answer TPin itself with TErr.
	Names []string `cbor:"17,keyasint,omitempty"`
```

`amberiroh/server.go` — Server struct additions (after `dataEndpoints`):

```go
	// onAccess, when set, is called with every ref name a pull resolved or
	// a push committed — the GC access-tracking seam. onPin is called for
	// every existing name in a TPin assert. guard, when set, brackets the
	// push path's reference write with the collector's PrepareRef. All
	// three are set before Serve, like SetDataPorts.
	onAccess func(name string)
	onPin    func(name string)
	guard    RefGuard
```

New declarations (near `New`):

```go
// RefGuard is the GC write barrier around reference publication, identical
// in shape to amber.RefGuard (declared here so amberiroh keeps importing
// only amber-store-core). *gc.Collector satisfies it directly.
type RefGuard interface {
	PrepareRef(root key.Key) (commit, abort func(), err error)
}

// SetOnAccess installs the ref-access hook. Call before Serve.
func (s *Server) SetOnAccess(f func(name string)) { s.onAccess = f }

// SetOnPin installs the pin-assert hook. Call before Serve.
func (s *Server) SetOnPin(f func(name string)) { s.onPin = f }

// SetRefGuard installs the reference write barrier. Call before Serve.
func (s *Server) SetRefGuard(g RefGuard) { s.guard = g }
```

Dispatch in `HandleStream` — add a case:

```go
	case TPin:
		err = s.handlePin(rw, m)
```

Handler:

```go
// handlePin marks the named refs kept-forever. Nonexistent names are
// ignored, not an error — a registry may assert ahead of a re-resolve.
func (s *Server) handlePin(rw io.ReadWriter, m Msg) error {
	for _, name := range m.Names {
		if _, err := s.refs.Get(name); err != nil {
			continue
		}
		if s.onPin != nil {
			s.onPin(name)
		}
	}
	return WriteMsg(rw, Msg{Type: TOK})
}
```

`handlePull` — after the successful `s.refs.Get(m.Name)` (right before building the `ref := Msg{…}` reply):

```go
	if s.onAccess != nil {
		s.onAccess(m.Name)
	}
```

`handlePush` — wrap the ref write. Replace:

```go
	if err := s.refs.Put(m.Name, raw); err != nil {
		return s.fail(rw, CodeInternal, err)
	}
```

with:

```go
	if s.guard != nil {
		commit, abort, gerr := s.guard.PrepareRef(root)
		if gerr != nil {
			return s.fail(rw, CodeInternal, fmt.Errorf("gc guard: %w", gerr))
		}
		if err := s.refs.Put(m.Name, raw); err != nil {
			abort()
			return s.fail(rw, CodeInternal, err)
		}
		commit()
	} else if err := s.refs.Put(m.Name, raw); err != nil {
		return s.fail(rw, CodeInternal, err)
	}
	if s.onAccess != nil {
		s.onAccess(m.Name)
	}
```

- [ ] **Step 4: Run tests**

```bash
nix develop -c go test ./amberiroh/
```
Expected: PASS (existing protocol/pack/push tests confirm nothing regressed).

- [ ] **Step 5: Commit**

```bash
git add amberiroh/
git commit -m "amberiroh: TPin pin-asserts, access hooks, push ref guard"
```

---

### Task 5: `amberclient.Pin`

**Files:**
- Modify: `amberclient/client.go`

**Interfaces:**
- Consumes: `amberiroh.TPin`, `Msg.Names`, TOK reply (Task 4).
- Produces: `(*amberclient.Client).Pin(ctx context.Context, names []string) error` — one control-stream exchange; a `*amberiroh.RemoteError` reply surfaces as-is (callers detect old servers with `errors.As`).

- [ ] **Step 1: Implement** (mirror of `Refs` at `amberclient/client.go:570`; place directly below it):

```go
// Pin asks the server to keep the named refs forever (the GC pin-assert,
// amberiroh.TPin). Nonexistent names are ignored server-side. An old
// server answers with a bad-request RemoteError — callers should detect it
// with errors.As and stop asserting.
func (c *Client) Pin(ctx context.Context, names []string) error {
	stream, stop, err := c.openStream(ctx)
	if err != nil {
		return fmt.Errorf("amberclient: pin: %w", err)
	}
	defer stop()
	defer CloseStream(stream)

	if err := amberiroh.WriteMsg(stream, amberiroh.Msg{Type: amberiroh.TPin, Names: names}); err != nil {
		return fmt.Errorf("amberclient: pin: %w", err)
	}
	m, err := amberiroh.ReadMsg(stream)
	if err != nil {
		return fmt.Errorf("amberclient: pin: %w", err)
	}
	switch m.Type {
	case amberiroh.TOK:
		return nil
	case amberiroh.TErr:
		return fmt.Errorf("amberclient: pin: %w", amberiroh.RemoteFromMsg(m))
	default:
		return fmt.Errorf("amberclient: pin: %w: type %d, want TOK", amberiroh.ErrProtocol, m.Type)
	}
}
```

- [ ] **Step 2: Build + vet** (round-trip verification is Task 8's e2e, which dials a real server with amberclient and pins over the wire):

```bash
nix develop -c go build ./amberclient/
nix develop -c go vet ./amberclient/
```
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add amberclient/
git commit -m "amberclient: Pin control-stream call (GC pin-assert)"
```

---

### Task 6: `wire.Result.ReadRefs` + runner recording

**Files:**
- Modify: `wire/wire.go` (Result field)
- Modify: `runnerd/job.go` (attempt records, buildResult carries)
- Create: `runnerd/result_test.go`

**Interfaces:**
- Consumes: nothing new.
- Produces: `wire.Result.ReadRefs []string` (cbor `readRefs,omitempty`); `buildResult(job wire.Job, runnerID string, out runner.Outcome, refs []wire.RefProposal, scratch string, ru wire.Rusage, readRefs []string) wire.Result` (new final parameter); `(*daemon).attempt` returns `(runner.Outcome, []wire.RefProposal, string, []string)` — the fourth value is the pull-ref names successfully ensured, populated on every path including failures.

- [ ] **Step 1: Write the failing test**

Create `runnerd/result_test.go`:

```go
package runnerd

import (
	"slices"
	"testing"

	"github.com/jobs-build/jobs-iroh/runner"
	"github.com/jobs-build/jobs-iroh/wire"
)

// ReadRefs must ride the result for every class — the server needs to know
// cached refs were used regardless of the build outcome.
func TestBuildResultCarriesReadRefs(t *testing.T) {
	job := wire.Job{Node: "n", Gen: 3}
	reads := []string{"build-output:aa", "shell:linux/amd64"}

	ok := buildResult(job, "r1", runner.Outcome{}, nil, "", wire.Rusage{}, reads)
	failed := buildResult(job, "r1", runner.Outcome{Failed: true, Class: "hard"}, nil, "", wire.Rusage{}, reads)
	cancelled := buildResult(job, "r1", runner.Outcome{Cancelled: true}, nil, "", wire.Rusage{}, reads)

	for name, res := range map[string]wire.Result{"ok": ok, "failed": failed, "cancelled": cancelled} {
		if !slices.Equal(res.ReadRefs, reads) {
			t.Errorf("%s: ReadRefs = %v, want %v", name, res.ReadRefs, reads)
		}
	}
}
```

- [ ] **Step 2: Run to verify failure**

```bash
nix develop -c go test ./runnerd/ -run TestBuildResultCarriesReadRefs
```
Expected: FAIL — `buildResult` has no readRefs parameter / `ReadRefs` undefined.

- [ ] **Step 3: Implement**

`wire/wire.go` — add to `Result` (after `Rusage`):

```go
	// ReadRefs are the server ref names this attempt resolved as inputs
	// (pull or local cache hit), reported regardless of outcome so the
	// server's GC knows cached refs are in use. Additive: old runners
	// omit it — their warm-cache reads simply go unobserved.
	ReadRefs []string `cbor:"readRefs,omitempty"`
```

`runnerd/job.go`:

1. `attempt` signature and every return gain the read list. Change the signature to:

```go
func (d *daemon) attempt(ctx context.Context, job wire.Job, ev *events.Job) (runner.Outcome, []wire.RefProposal, string, []string) {
```

Declare `var readRefs []string` before the PullRefs loop and rewrite the loop:

```go
	ev.Phase("pulling")
	var readRefs []string
	for _, name := range job.PullRefs {
		if _, err := d.ensureRef(ctx, name); err != nil {
			if ctx.Err() != nil {
				return runner.Outcome{Cancelled: true}, nil, "", readRefs
			}
			class := "retryable"
			if errors.Is(err, errPullIncomplete) {
				class = "hard"
			}
			return runner.Outcome{Failed: true, Class: class, Phase: "pulling", Stderr: err.Error()}, nil, "", readRefs
		}
		readRefs = append(readRefs, name)
	}
```

Then append `, readRefs` to EVERY other `return` in `attempt` (there are seven more: bad job key, ingest def, def hash mismatch, local def ref, driver cancel, driver terminal, proposed-key/assemble/push failures, and the final success return). The out-of-band `shell:` ensure inside `buildRunCfg` is deliberately not recorded: `shell:`/`fetcher:` are protected classes that never expire, and `build-cache:` rides PullRefs whenever the server has it.

2. `handleJob` — update the call:

```go
	out, refs, scratch, readRefs := d.attempt(ctx, job, ev)
```
and the result build:

```go
	res := buildResult(job, d.id, out, refs, scratch, ru, readRefs)
```

3. `buildResult` — new final parameter, set unconditionally:

```go
func buildResult(job wire.Job, runnerID string, out runner.Outcome, refs []wire.RefProposal, scratch string, ru wire.Rusage, readRefs []string) wire.Result {
	res := wire.Result{
		Node:     job.Node,
		Gen:      job.Gen,
		Runner:   runnerID,
		Rusage:   ru,
		ReadRefs: readRefs,
	}
	// … rest unchanged …
```

- [ ] **Step 4: Run tests**

```bash
nix develop -c go test ./runnerd/ ./wire/
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add wire/wire.go runnerd/
git commit -m "runner: report per-job read refs in the result (GC access tracking)"
```

---

### Task 7: sched forwards ReadRefs

**Files:**
- Modify: `sched/sched.go` (Options + field), `sched/results.go` (forward)

**Interfaces:**
- Consumes: `wire.Result.ReadRefs` (Task 6).
- Produces: `sched.Options.Touch func(names []string)` — nil-safe; called from `handleResult` for every decodable result BEFORE the dedup gate (reads happened regardless of result disposition).

- [ ] **Step 1: Implement**

`sched/sched.go` — `Options` gains (after `Log`):

```go
	// Touch, when set, receives the ref names each runner result reports
	// as read (wire.Result.ReadRefs) — the GC access-tracking seam.
	Touch func(names []string)
```

`Sched` struct gains `touch func(names []string)` (next to `log`), and `New` copies it: find where `New` assigns `log:` into the Sched literal and add `touch: opts.Touch,`.

`sched/results.go` — in `handleResult`, immediately after the decode error return (before `defer s.deleteScratch(...)`):

```go
	if s.touch != nil && len(res.ReadRefs) > 0 {
		// Reads happened regardless of how the result is disposed of below
		// (stale, duplicate, unknown node) — touch before the dedup gate.
		s.touch(res.ReadRefs)
	}
```

- [ ] **Step 2: Build + existing tests** (end-to-end verification rides Task 8's serve test, which publishes a synthetic result and observes the tracker):

```bash
nix develop -c go test ./sched/
```
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add sched/
git commit -m "sched: forward runner-reported read refs to the GC tracker"
```

---

### Task 8: serve — gcRunner, hourly loop, wiring, e2e test

**Files:**
- Create: `serve/gc.go`
- Modify: `serve/serve.go` (Options + wiring), `cmd/jobs-server/main.go` (flags)
- Create: `serve/gc_test.go`

**Interfaces:**
- Consumes: `reftrack` (Task 2), `amber.SetObserver`/`SetRefGuard` (3), `amberiroh.SetOnAccess`/`SetOnPin`/`SetRefGuard` (4), `amberclient.Pin` (5), `wire.Result.ReadRefs` (6), `sched.Options.Touch` (7), `gc` package (1).
- Produces:
  - `serve.Options` gains `GCRetention, GCInterval time.Duration; GCRate int64; GCMinFree uint64`. `GCRetention == 0` disables GC entirely (no tracker, no collector, no loop) — the zero value keeps every existing test unchanged.
  - unexported `gcRunner` with `Sweep(ctx context.Context, garbage float64, force bool) (api.GCStats, error)`, `StatsSnapshot() api.GCStats`, `Entry(name string) (reftrack.Entry, bool)`, `Pin(ctx, name) (api.RefInfo, error)`, `Unpin(ctx, name) (api.RefInfo, error)`, `Close()`. Field `gc *gcRunner` on `serve.Server` mirror struct is NOT added; instead `apiService` gets `gc *gcRunner` (Task 9) and tests reach it via a package-internal handle (see wiring).
  - `api.GCStats` and the `StatsReply.GC`/`RefInfo` extensions are ALSO created in this task (serve/gc.go returns them), with the frame handlers following in Task 9.

- [ ] **Step 1: api types** (needed by gc.go's return shape) — in `api/api.go`:

Extend `StatsReply`:

```go
// StatsReply is the server stats snapshot (admin).
type StatsReply struct {
	StoreBytes   int64 `cbor:"storeBytes"`
	RefCount     int   `cbor:"refCount"`
	UptimeNs     int64 `cbor:"uptimeNs"`
	Requests     int   `cbor:"requests"`
	NodesTracked int   `cbor:"nodesTracked"`
	// GC is the auto-cleanup block — nil on servers without GC (additive).
	GC *GCStats `cbor:"gc,omitempty"`
}

// GCStats reports the GC/auto-cleanup state as of the last sweep — no mark
// walk runs on the stats path.
type GCStats struct {
	RetentionNs     int64  `cbor:"retentionNs"`
	LastSweepNs     int64  `cbor:"lastSweepNs,omitempty"` // 0 = no sweep yet
	ExpiredLast     int    `cbor:"expiredLast,omitempty"`
	ExpiredTotal    int    `cbor:"expiredTotal,omitempty"` // since boot
	Pinned          int    `cbor:"pinned,omitempty"`
	RefCount        int    `cbor:"refCount,omitempty"`
	DiskBytes       int64  `cbor:"diskBytes,omitempty"`
	LiveBytes       int64  `cbor:"liveBytes,omitempty"`
	GarbageBytes    int64  `cbor:"garbageBytes,omitempty"`
	LastCycleNs     int64  `cbor:"lastCycleNs,omitempty"` // start of the last cycle
	LastCycleReaped int    `cbor:"lastCycleReaped,omitempty"`
	LastCycleFreed  int64  `cbor:"lastCycleFreed,omitempty"`
	LastCycleWallNs int64  `cbor:"lastCycleWallNs,omitempty"`
	LastError       string `cbor:"lastError,omitempty"`
}
```

Extend `RefInfo`:

```go
// RefInfo is one ref row.
type RefInfo struct {
	Name      string `cbor:"name"`
	Key       []byte `cbor:"key"`
	CreatedNs int64  `cbor:"createdNs"`
	// LastAccessNs and Pinned come from the GC tracker — zero/false on
	// servers without GC (additive).
	LastAccessNs int64 `cbor:"lastAccessNs,omitempty"`
	Pinned       bool  `cbor:"pinned,omitempty"`
}
```

- [ ] **Step 2: serve/gc.go**

```go
package serve

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/amber-store/core/gc"

	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/api"
	"github.com/jobs-build/jobs-iroh/reftrack"
)

// gcRunner owns the GC feature: the access tracker, the mark-sweep
// collector, and the periodic sweep. One per server; nil when GC is
// disabled (Options.GCRetention == 0).
type gcRunner struct {
	log       *slog.Logger
	store     *amber.Store
	storeDir  string
	snapPath  string // <data-dir>/refaccess.cbor
	tracker   *reftrack.Tracker
	coll      *gc.Collector
	retention time.Duration
	interval  time.Duration
	minFree   uint64

	sweepMu sync.Mutex // serializes sweeps (hourly loop vs admin gc)

	mu    sync.Mutex // guards stats
	stats api.GCStats

	stop context.CancelFunc
	done chan struct{}
}

// newGCRunner opens the collector next to the already-open store, loads the
// tracker snapshot, and installs the store hooks. The caller wires the
// amberiroh and sched hooks and starts the loop.
func newGCRunner(log *slog.Logger, store *amber.Store, dataDir, storeDir string, opts Options) (*gcRunner, error) {
	tracker := reftrack.New()
	snapPath := filepath.Join(dataDir, "refaccess.cbor")
	if err := tracker.Load(snapPath); err != nil {
		// Corrupt snapshot: start empty (refs re-seed at the next sweep),
		// never fatal — access data is approximately reconstructible.
		log.Warn("gc: tracker snapshot unreadable; starting empty", "error", err)
	}
	coll, err := gc.Open(filepath.Join(storeDir, "closures"), store.Objects(), store.RefStore(), gc.Options{
		Rate:    opts.GCRate,
		MinFree: opts.GCMinFree,
	})
	if err != nil {
		return nil, fmt.Errorf("open gc collector: %w", err)
	}
	g := &gcRunner{
		log:       log,
		store:     store,
		storeDir:  storeDir,
		snapPath:  snapPath,
		tracker:   tracker,
		coll:      coll,
		retention: opts.GCRetention,
		interval:  opts.GCInterval,
		minFree:   opts.GCMinFree,
	}
	g.stats.RetentionNs = int64(opts.GCRetention)
	store.SetObserver(tracker.Touch)
	store.SetRefGuard(coll)
	return g, nil
}

// start launches the periodic sweep loop.
func (g *gcRunner) start(ctx context.Context) {
	loopCtx, cancel := context.WithCancel(ctx)
	g.stop = cancel
	g.done = make(chan struct{})
	go g.loop(loopCtx)
}

func (g *gcRunner) loop(ctx context.Context) {
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
func (g *gcRunner) Close() {
	if g.stop != nil {
		g.stop()
		<-g.done
	}
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
func (g *gcRunner) Sweep(ctx context.Context, garbage float64, force bool) (api.GCStats, error) {
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

	// 2. Expire (output before deps — Expired orders them).
	expired := g.tracker.Expired(g.retention, now)
	deleted := 0
	for _, name := range expired {
		e, _ := g.tracker.Get(name)
		if err := g.store.DeleteRef(ctx, name); err != nil {
			g.log.Warn("gc: delete expired ref", "ref", name, "error", err)
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

	// 4. Persist & report.
	if err := g.tracker.Flush(g.snapPath); err != nil {
		g.log.Warn("gc: tracker flush", "error", err)
	}
	total, pinned := g.tracker.Counts()

	g.mu.Lock()
	g.stats.LastSweepNs = now.UnixNano()
	g.stats.ExpiredLast = deleted
	g.stats.ExpiredTotal += deleted
	g.stats.Pinned = pinned
	g.stats.RefCount = total
	g.stats.DiskBytes = dirSize(g.storeDir)
	g.stats.LiveBytes = st.LiveBytes
	g.stats.GarbageBytes = st.GarbageBytes
	g.stats.LastError = ""
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
	if cycleErr != nil {
		args = append(args, "cycleError", cycleErr)
	}
	g.log.Info("gc sweep", args...)
	return out, cycleErr
}

// failSweep records a sweep-level failure in the stats and returns it.
func (g *gcRunner) failSweep(err error) (api.GCStats, error) {
	g.mu.Lock()
	g.stats.LastError = err.Error()
	out := g.stats
	g.mu.Unlock()
	return out, err
}

// StatsSnapshot returns the last-known stats — no walk, no lock beyond the
// stats mutex (the numbers are "as of the last sweep").
func (g *gcRunner) StatsSnapshot() api.GCStats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.stats
}

// Entry exposes one tracker entry for the refs listing.
func (g *gcRunner) Entry(name string) (reftrack.Entry, bool) {
	return g.tracker.Get(name)
}

// Pin marks an existing ref kept-forever and returns its row.
func (g *gcRunner) Pin(ctx context.Context, name string) (api.RefInfo, error) {
	ri, err := g.store.GetRef(ctx, name)
	if err != nil {
		return api.RefInfo{}, err
	}
	g.tracker.Pin(name)
	if err := g.tracker.Flush(g.snapPath); err != nil {
		g.log.Warn("gc: tracker flush after pin", "error", err)
	}
	return g.refRow(ri), nil
}

// Unpin clears the flag (always succeeds; the ref may already be gone).
func (g *gcRunner) Unpin(ctx context.Context, name string) (api.RefInfo, error) {
	g.tracker.Unpin(name)
	if err := g.tracker.Flush(g.snapPath); err != nil {
		g.log.Warn("gc: tracker flush after unpin", "error", err)
	}
	ri, err := g.store.GetRef(ctx, name)
	if err != nil {
		return api.RefInfo{Name: name}, nil
	}
	return g.refRow(ri), nil
}

func (g *gcRunner) refRow(ri amber.RefInfo) api.RefInfo {
	row := api.RefInfo{Name: ri.Name, Key: ri.Key[:], CreatedNs: ri.CreatedAt.UnixNano()}
	if e, ok := g.tracker.Get(ri.Name); ok {
		row.LastAccessNs = e.LastAccess.UnixNano()
		row.Pinned = e.Pinned
	}
	return row
}

// freeBelow reports whether the filesystem holding path has less than min
// bytes free; min 0 means 5% of the filesystem (the collector's own
// pressure line). Portable across linux and darwin.
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
```

Note: `GetRef` inside `Pin` fires the observer (a pin is an access) — intended.

- [ ] **Step 3: serve.Options + wiring** in `serve/serve.go`:

Options additions (after `DataEndpoints`):

```go
	// GCRetention enables auto-cleanup: refs unread for this long are
	// deleted and the mark-sweep collector reclaims the disk. 0 disables
	// GC entirely (tracker, collector, sweep loop).
	GCRetention time.Duration
	// GCInterval is the sweep period (default 1h when GC is enabled).
	GCInterval time.Duration
	// GCRate caps the GC copier in bytes/s (0 = unlimited).
	GCRate int64
	// GCMinFree is the free-space floor in bytes under which the collector
	// reaps more aggressively (0 = 5% of the filesystem).
	GCMinFree uint64
```

Wiring in `Run` — insert directly after the `defer store.Close()` line (`serve.go:137`), so the guard is installed before `bootstrap.Seed` publishes any ref and the runner closes (LIFO) before the store:

```go
	var gcr *gcRunner
	if opts.GCRetention > 0 {
		if opts.GCInterval <= 0 {
			opts.GCInterval = time.Hour
		}
		storeDir := filepath.Join(opts.DataDir, "store")
		gcr, err = newGCRunner(log.With("component", "gc"), store, opts.DataDir, storeDir, opts)
		if err != nil {
			return err
		}
		defer gcr.Close()
	}
```

sched.New call gains the touch seam — extend the existing `sched.Options{…}` literal:

```go
	schedOpts := sched.Options{
		Store: store,
		NC:    nc,
		Log:   log.With("component", "sched"),
	}
	if gcr != nil {
		schedOpts.Touch = gcr.tracker.TouchAll
	}
	sd, err := sched.New(ctx, schedOpts)
```

After `amberSrv := amberiroh.New(…)` (`serve.go:214`):

```go
	if gcr != nil {
		amberSrv.SetOnAccess(gcr.tracker.Touch)
		amberSrv.SetOnPin(gcr.tracker.Pin)
		amberSrv.SetRefGuard(gcr.coll)
	}
```

Start the loop right before the `<-ctx.Done()` wait (after `opts.Ready`):

```go
	if gcr != nil {
		gcr.start(ctx)
	}
```

Also pass `gcr` into both apiService literals (`serve.go:297-298`) — add field `gc *gcRunner` to `apiService` (`serve/apihandler.go:26`) now, used by Task 9:

```go
	buildSvc := &apiService{log: log.With("service", "build"), sd: sd, store: store, storeDir: storeDir, gc: gcr}
	adminSvc := &apiService{log: log.With("service", "admin"), sd: sd, store: store, storeDir: storeDir, gc: gcr, admin: true}
```

`cmd/jobs-server/main.go` — flags (append to the Flags slice):

```go
			&cli.DurationFlag{
				Name:    "gc-retention",
				Usage:   "delete refs unread for this long and run store GC (0 disables)",
				EnvVars: []string{"JOBS_GC_RETENTION"},
				Value:   720 * time.Hour, // 30 days
			},
			&cli.DurationFlag{
				Name:    "gc-interval",
				Usage:   "period of the GC sweep job",
				EnvVars: []string{"JOBS_GC_INTERVAL"},
				Value:   time.Hour,
			},
			&cli.Int64Flag{
				Name:    "gc-rate",
				Usage:   "GC copier bandwidth cap in bytes/s (0 = unlimited)",
				EnvVars: []string{"JOBS_GC_RATE"},
			},
			&cli.Uint64Flag{
				Name:    "gc-min-free",
				Usage:   "free-space floor in bytes for aggressive GC (0 = 5% of the filesystem)",
				EnvVars: []string{"JOBS_GC_MIN_FREE"},
			},
```

and the Options literal:

```go
				GCRetention: c.Duration("gc-retention"),
				GCInterval:  c.Duration("gc-interval"),
				GCRate:      c.Int64("gc-rate"),
				GCMinFree:   c.Uint64("gc-min-free"),
```

(add `"time"` to main.go's imports).

- [ ] **Step 4: Write the e2e test**

Create `serve/gc_test.go` (package `serve` — internal access to `gcr` is not available through `Server`; use a local Options-taking variant of the test harness plus a channel to capture the runner):

```go
package serve

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/tmc/go-iroh/iroh"

	"github.com/jobs-build/jobs-iroh/amberclient"
	"github.com/jobs-build/jobs-iroh/reftrack"
	"github.com/jobs-build/jobs-iroh/wire"
)

// startGCServer mirrors startServer (serve_test.go:20-68) with GC enabled
// at a tiny retention; the loop interval is far in the future so sweeps
// are manual and deterministic.
func startGCServer(t *testing.T, ctx context.Context) (*Server, *gcRunner, func(alpn string) *iroh.Conn) {
	t.Helper()

	ready := make(chan *Server, 1)
	captured := make(chan *gcRunner, 1)
	gcTestCapture = func(g *gcRunner) { captured <- g }
	t.Cleanup(func() { gcTestCapture = nil })
	done := make(chan error, 1)
	runCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("server run: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("server did not shut down")
		}
	})
	go func() {
		done <- Run(runCtx, Options{
			DataDir:     t.TempDir(),
			BindAddr:    netip.AddrPortFrom(netip.IPv6Loopback(), 0),
			GCRetention: 200 * time.Millisecond,
			GCInterval:  time.Hour, // the loop never fires in tests
			Ready:       func(s *Server) { ready <- s },
		})
	}()

	var srv *Server
	select {
	case srv = <-ready:
	case <-time.After(30 * time.Second):
		t.Fatal("server not ready")
	}
	var gcr *gcRunner
	select {
	case gcr = <-captured:
	case <-time.After(30 * time.Second):
		t.Fatal("gc runner not captured")
	}

	clientEP, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		t.Fatalf("bind client endpoint: %v", err)
	}
	t.Cleanup(func() { clientEP.Shutdown(ctx) })

	dial := func(alpn string) *iroh.Conn {
		addr := netaddr.NewEndpointAddr(srv.Endpoint.ID()).WithIP(srv.Endpoint.LocalAddr())
		conn, err := clientEP.Connect(ctx, addr, alpn)
		if err != nil {
			t.Fatalf("connect %s: %v", alpn, err)
		}
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	return srv, gcr, dial
}
```

(add `"github.com/tmc/go-iroh/netaddr"` to the test file's imports.)

The capture seam is two tiny additions. In `serve/gc.go`:

```go
// gcTestCapture, when set (tests only), receives the gcRunner Run wires.
var gcTestCapture func(*gcRunner)
```

and in `serve/serve.go`, inside the `if opts.GCRetention > 0` block after `defer gcr.Close()`:

```go
		if gcTestCapture != nil {
			gcTestCapture(gcr)
		}
```

The tests:

```go
func TestGCSweepExpiresAndSpares(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	srv, gcr, _ := startGCServer(t, ctx)

	// Three refs over real content.
	k, err := srv.Store.IngestFile(ctx, []byte("gc fixture"))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"gc-test:doomed", "gc-test:pinned", "gc-test:reader"} {
		if err := srv.Store.PutRef(ctx, name, k); err != nil {
			t.Fatal(err)
		}
	}

	// Pin one over the wire (verifies amberclient.Pin + TPin end to end).
	pc, err := amberclient.Dial(ctx, amberclient.Options{
		EndpointID: srv.Endpoint.ID().String(),
		Addrs:      []string{srv.Endpoint.LocalAddr().String()},
		ALPN:       ALPNAmberAdmin,
		BindAddr:   netip.AddrPortFrom(netip.IPv6Loopback(), 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	if err := pc.Pin(ctx, []string{"gc-test:pinned"}); err != nil {
		t.Fatal(err)
	}

	// Age everything past the 200ms retention.
	time.Sleep(300 * time.Millisecond)

	// A synthetic runner result touches gc-test:reader (verifies the
	// sched Touch forwarding of Task 7).
	nc, err := nats.Connect("", nats.InProcessServer(srv.NATS))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	res := wire.MustEncode(wire.Result{Node: "buildrun-ffff", Gen: 1, Runner: "fake",
		Class: wire.ClassOK, ReadRefs: []string{"gc-test:reader"}})
	if err := nc.Publish(wire.ResultsSubject("buildrun-ffff"), res); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool {
		e, ok := gcr.Entry("gc-test:reader")
		return ok && time.Since(e.LastAccess) < 250*time.Millisecond
	}, "runner-reported read never touched the tracker")

	stats, err := gcr.Sweep(ctx, -1, true)
	if err != nil {
		t.Fatalf("sweep: %v (stats %+v)", err, stats)
	}
	if stats.ExpiredLast < 1 {
		t.Fatalf("expected expiries, got %+v", stats)
	}

	assertRef := func(name string, want bool) {
		t.Helper()
		_, ok, err := srv.Store.GetKey(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if ok != want {
			t.Errorf("%s present=%v want %v", name, ok, want)
		}
	}
	assertRef("gc-test:doomed", false)
	assertRef("gc-test:pinned", true)
	assertRef("gc-test:reader", true)

	// Protected classes survive any clock.
	refs, err := srv.Store.ListRefs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	shell := false
	for _, r := range refs {
		if reftrack.Protected(r.Name) {
			shell = true
		}
	}
	if !shell {
		t.Error("bootstrap-seeded protected refs vanished")
	}

	if stats.DiskBytes <= 0 || stats.LiveBytes <= 0 {
		t.Errorf("stats missing sizes: %+v", stats)
	}
}

func waitFor(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
```

Delete the placeholder `gcHooks` var and `panic` stub once `startGCServer` is written for real.

Caveats the implementer must respect:
- `wire.Result` node names must parse for handleResult to proceed past `ParseNodeName` — BUT the touch happens before that only if Task 7 placed it before the parse. Task 7 places the touch immediately after decode, before `ParseNodeName` — use any node string (`"buildrun-ffff"` is fine either way).
- The touch resets `gc-test:reader`'s clock; the sweep must run AFTER the touch is observed (the `waitFor`) but soon enough that 200ms haven't elapsed again — hence the tight poll and the immediate sweep. If flaky on slow machines, raise retention to 500ms and the sleep to 700ms.
- The forced sweep runs a real GC cycle over the seeded store (a few MB) — sub-second.

- [ ] **Step 5: Run**

```bash
nix develop -c go test ./serve/ -run TestGCSweep -v
nix develop -c go test ./serve/
nix develop -c env CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go vet ./...
```
Expected: PASS, and the darwin vet is clean (the `unix.Statfs` conversions compile on both).

- [ ] **Step 6: Commit**

```bash
git add serve/ api/api.go cmd/jobs-server/main.go
git commit -m "serve: GC runner — access-tracked ref expiry, hourly mark-sweep, stats"
```

---

### Task 9: Admin frames — gc, pin, unpin, stats/refs extensions

**Files:**
- Modify: `api/api.go` (frame consts + request payloads)
- Modify: `serve/apihandler.go` (handlers)
- Create: `serve/api_gc_test.go`

**Interfaces:**
- Consumes: `gcRunner` methods (Task 8), api types (Task 8).
- Produces:
  - Frame consts: `api.TGC = "gc"`, `api.TPin = "pin"`, `api.TUnpin = "unpin"`; replies `api.TGCReply = "gc-reply"`, `api.TPinReply = "pin-reply"`.
  - `api.GCRequest{Garbage *float64}` (nil = policy line), `api.PinRequest{Name string}`.
  - `TGC` → runs `Sweep(ctx, garbage, force=true)`, replies `TGCReply` with `GCStats`; `TPin`/`TUnpin` reply `TPinReply` with `RefInfo`. All three answer `unavailable` when GC is disabled. `TStats` gains the GC block; `TRefs` rows gain LastAccessNs/Pinned.

- [ ] **Step 1: api additions** — `api/api.go`:

Client→server consts: append `TGC = "gc"`, `TPin = "pin"`, `TUnpin = "unpin"` to the first const block; `TGCReply = "gc-reply"`, `TPinReply = "pin-reply"` to the second. Payloads (near RefsRequest):

```go
// GCRequest triggers one immediate GC sweep+cycle (admin).
type GCRequest struct {
	// Garbage forces the pack selection line (0..1); nil uses policy
	// (0.5, or 0.1 under free-space pressure).
	Garbage *float64 `cbor:"garbage,omitempty"`
}

// PinRequest pins or unpins one ref (admin).
type PinRequest struct {
	Name string `cbor:"name"`
}
```

- [ ] **Step 2: Write the failing test**

Create `serve/api_gc_test.go` (package `serve`; reuse `startGCServer` from Task 8 and the `request` helper from `serve/api_test.go`):

```go
package serve

import (
	"context"
	"testing"
	"time"

	"github.com/jobs-build/jobs-iroh/api"
)

func TestAdminGCFrames(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	srv, _, dial := startGCServer(t, ctx)
	conn := dial(ALPNAdmin)

	k, err := srv.Store.IngestFile(ctx, []byte("admin gc fixture"))
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Store.PutRef(ctx, "gc-api:target", k); err != nil {
		t.Fatal(err)
	}

	// pin → pinned row
	var row api.RefInfo
	request(t, ctx, conn, api.TPin, api.PinRequest{Name: "gc-api:target"}, api.TPinReply, &row)
	if !row.Pinned || row.LastAccessNs == 0 {
		t.Fatalf("pin reply %+v", row)
	}

	// refs listing carries the columns
	var refs api.RefsReply
	request(t, ctx, conn, api.TRefs, api.RefsRequest{Prefix: "gc-api:"}, api.TRefsReply, &refs)
	if len(refs.Refs) != 1 || !refs.Refs[0].Pinned || refs.Refs[0].LastAccessNs == 0 {
		t.Fatalf("refs reply %+v", refs.Refs)
	}

	// unpin → cleared row
	request(t, ctx, conn, api.TUnpin, api.PinRequest{Name: "gc-api:target"}, api.TPinReply, &row)
	if row.Pinned {
		t.Fatalf("unpin reply still pinned: %+v", row)
	}

	// manual gc runs a sweep and reports stats
	var gcStats api.GCStats
	request(t, ctx, conn, api.TGC, api.GCRequest{}, api.TGCReply, &gcStats)
	if gcStats.LastSweepNs == 0 || gcStats.RefCount == 0 {
		t.Fatalf("gc reply %+v", gcStats)
	}

	// stats carries the GC block
	var stats api.StatsReply
	request(t, ctx, conn, api.TStats, nil, api.TStatsReply, &stats)
	if stats.GC == nil || stats.GC.RetentionNs != int64(200*time.Millisecond) {
		t.Fatalf("stats.GC = %+v", stats.GC)
	}
}

func TestAdminGCDisabled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, dial := startServer(t, ctx) // GC off (zero Options)
	conn := dial(ALPNAdmin)

	stream := openRequest(t, ctx, conn, api.TGC, api.GCRequest{})
	defer stream.Close()
	typ, body, err := api.ReadFrame(stream)
	if err != nil {
		t.Fatal(err)
	}
	if typ != api.TError {
		t.Fatalf("frame %q, want error", typ)
	}
	var e api.Error
	if err := api.DecodeBody(body, &e); err != nil {
		t.Fatal(err)
	}
	if e.Code != api.CodeUnavailable {
		t.Fatalf("code %q, want unavailable", e.Code)
	}

	var stats api.StatsReply
	request(t, ctx, conn, api.TStats, nil, api.TStatsReply, &stats)
	if stats.GC != nil {
		t.Fatalf("GC block on a GC-less server: %+v", stats.GC)
	}
}
```

(`openRequest` exists in `serve/api_test.go`; check its exact signature — it is `openRequest(t, ctx, conn, typ, body)` returning the stream — and adjust if it differs.)

- [ ] **Step 3: Run to verify failure**

```bash
nix develop -c go test ./serve/ -run TestAdminGC
```
Expected: FAIL — consts/handlers missing.

- [ ] **Step 4: Implement handlers** in `serve/apihandler.go`:

`TStats` case — extend:

```go
	case api.TStats:
		st := svc.sd.Stats()
		st.StoreBytes = dirSize(svc.storeDir)
		if svc.gc != nil {
			gcs := svc.gc.StatsSnapshot()
			st.GC = &gcs
		}
		_ = api.WriteFrame(stream, api.TStatsReply, st)
		return nil
```

`TRefs` case — extend the row build:

```go
			out = append(out, api.RefInfo{
				Name:      r.Name,
				Key:       k[:],
				CreatedNs: r.CreatedAt.UnixNano(),
			})
			if svc.gc != nil {
				if e, ok := svc.gc.Entry(r.Name); ok {
					out[len(out)-1].LastAccessNs = e.LastAccess.UnixNano()
					out[len(out)-1].Pinned = e.Pinned
				}
			}
```

New cases in the admin switch (before `default:`):

```go
	case api.TGC:
		if svc.gc == nil {
			return &sched.Error{Code: api.CodeUnavailable, Text: "GC is disabled on this server (--gc-retention 0)"}
		}
		var req api.GCRequest
		if len(body) > 0 {
			if err := api.DecodeBody(body, &req); err != nil {
				return badFrame(t, err)
			}
		}
		garbage := -1.0
		if req.Garbage != nil {
			garbage = *req.Garbage
		}
		svc.log.Info("admin gc", "remote", remote, "garbage", garbage)
		stats, err := svc.gc.Sweep(ctx, garbage, true)
		if err != nil {
			return err
		}
		_ = api.WriteFrame(stream, api.TGCReply, stats)
		return nil

	case api.TPin, api.TUnpin:
		if svc.gc == nil {
			return &sched.Error{Code: api.CodeUnavailable, Text: "GC is disabled on this server (--gc-retention 0)"}
		}
		var req api.PinRequest
		if err := api.DecodeBody(body, &req); err != nil {
			return badFrame(t, err)
		}
		if req.Name == "" {
			return badRequest("pin: name is required")
		}
		var row api.RefInfo
		var err error
		if t == api.TPin {
			row, err = svc.gc.Pin(ctx, req.Name)
		} else {
			row, err = svc.gc.Unpin(ctx, req.Name)
		}
		if errors.Is(err, amber.ErrRefNotFound) {
			return &sched.Error{Code: api.CodeNotFound, Text: err.Error()}
		}
		if err != nil {
			return err
		}
		svc.log.Info("admin "+t, "remote", remote, "ref", req.Name)
		_ = api.WriteFrame(stream, api.TPinReply, row)
		return nil
```

Add `"errors"` to the imports. A GC-cycle-in-flight error from `Run` (`gc.ErrCycleRunning`) simply propagates as the internal-error frame with its message — acceptable ("cycle already running").

- [ ] **Step 5: Run**

```bash
nix develop -c go test ./serve/
```
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add api/api.go serve/apihandler.go serve/api_gc_test.go
git commit -m "admin API: gc/pin/unpin frames, GC stats block, ref access columns"
```

---

### Task 10: clientcli — stats block, refs columns, gc/pin/unpin commands

**Files:**
- Modify: `clientcli/admin.go`

**Interfaces:**
- Consumes: api frames (Task 9).
- Produces: `jobs-client admin gc [--garbage F]`, `admin pin <ref>`, `admin unpin <ref>`; extended `admin stats` and `admin refs` output.

- [ ] **Step 1: Implement**

In `adminCmd()` register the new subcommands:

```go
		Subcommands: []*cli.Command{
			adminStatsCmd(),
			adminFleetCmd(),
			adminRequestsCmd(),
			adminRefsCmd(),
			adminGCCmd(),
			adminPinCmd(true),
			adminPinCmd(false),
		},
```

`adminStatsCmd` — after the uptime line:

```go
			if st.GC != nil {
				g := st.GC
				fmt.Fprintf(w, "gc retention:  %s\n", time.Duration(g.RetentionNs))
				if g.LastSweepNs == 0 {
					fmt.Fprintf(w, "gc sweep:      never\n")
				} else {
					pct := 0.0
					if tot := g.LiveBytes + g.GarbageBytes; tot > 0 {
						pct = 100 * float64(g.GarbageBytes) / float64(tot)
					}
					fmt.Fprintf(w, "gc sweep:      %s ago (expired %d, total %d)\n",
						time.Since(time.Unix(0, g.LastSweepNs)).Round(time.Second), g.ExpiredLast, g.ExpiredTotal)
					fmt.Fprintf(w, "gc store:      live %d, garbage %d (%.1f%%), pinned %d\n",
						g.LiveBytes, g.GarbageBytes, pct, g.Pinned)
				}
				if g.LastCycleNs != 0 {
					fmt.Fprintf(w, "gc last cycle: %s ago, reaped %d packs, freed %d bytes in %s\n",
						time.Since(time.Unix(0, g.LastCycleNs)).Round(time.Second),
						g.LastCycleReaped, g.LastCycleFreed, time.Duration(g.LastCycleWallNs).Round(time.Millisecond))
				}
				if g.LastError != "" {
					fmt.Fprintf(w, "gc last error: %s\n", g.LastError)
				}
			}
```

`adminRefsCmd` — replace the row print:

```go
			for _, r := range reply.Refs {
				access, pin := "-", ""
				if r.LastAccessNs > 0 {
					access = time.Since(time.Unix(0, r.LastAccessNs)).Round(time.Second).String() + " ago"
				}
				if r.Pinned {
					pin = "pin"
				}
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", r.Name, hex.EncodeToString(r.Key), access, pin)
			}
```

New commands:

```go
func adminGCCmd() *cli.Command {
	return &cli.Command{
		Name:  "gc",
		Usage: "run one GC sweep+cycle now and print the report",
		Flags: append(serverFlags(),
			&cli.Float64Flag{Name: "garbage", Value: -1,
				Usage: "force the pack selection line 0..1 (default: server policy)"},
		),
		Action: func(c *cli.Context) error {
			req := api.GCRequest{}
			if g := c.Float64("garbage"); g >= 0 {
				req.Garbage = &g
			}
			var st api.GCStats
			if err := adminCall(c, api.TGC, req, api.TGCReply, &st); err != nil {
				return err
			}
			w := c.App.Writer
			pct := 0.0
			if tot := st.LiveBytes + st.GarbageBytes; tot > 0 {
				pct = 100 * float64(st.GarbageBytes) / float64(tot)
			}
			fmt.Fprintf(w, "disk:      %d bytes\n", st.DiskBytes)
			fmt.Fprintf(w, "refs:      %d (%d pinned, %d expired this sweep)\n", st.RefCount, st.Pinned, st.ExpiredLast)
			fmt.Fprintf(w, "store:     live %d, garbage %d (%.1f%%)\n", st.LiveBytes, st.GarbageBytes, pct)
			if st.LastCycleNs != 0 {
				fmt.Fprintf(w, "cycle:     reaped %d packs, freed %d bytes in %s\n",
					st.LastCycleReaped, st.LastCycleFreed, time.Duration(st.LastCycleWallNs).Round(time.Millisecond))
			}
			if st.LastError != "" {
				return cli.Exit("gc cycle error: "+st.LastError, 1)
			}
			return nil
		},
	}
}

func adminPinCmd(pin bool) *cli.Command {
	name, usage, frame := "pin", "keep a ref forever (exempt from GC expiry)", api.TPin
	if !pin {
		name, usage, frame = "unpin", "clear a ref's pin; it then lives by its access clock", api.TUnpin
	}
	return &cli.Command{
		Name:      name,
		Usage:     usage,
		ArgsUsage: "<ref-name>",
		Flags:     serverFlags(),
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return cli.Exit("exactly one ref name required", 2)
			}
			var row api.RefInfo
			if err := adminCall(c, frame, api.PinRequest{Name: c.Args().First()}, api.TPinReply, &row); err != nil {
				return err
			}
			state := "unpinned"
			if row.Pinned {
				state = "pinned"
			}
			fmt.Fprintf(c.App.Writer, "%s\t%s\n", row.Name, state)
			return nil
		},
	}
}
```

- [ ] **Step 2: Build + manual smoke** (frame behavior is covered by Task 9's server tests; the CLI layer is presentational):

```bash
nix develop -c go build ./clientcli/ ./cmd/jobs-client/
nix develop -c go test ./clientcli/
```
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add clientcli/
git commit -m "jobs-client: admin gc/pin/unpin commands, GC stats and ref access output"
```

---

### Task 11: TUI — stats lines + refs columns

**Files:**
- Modify: `tui/format.go` (`statsLines` at :225, `refRow` at :204, and the `refHeader` var near it)

**Interfaces:**
- Consumes: api extensions (Task 9).

- [ ] **Step 1: Implement**

`statsLines` — append after the uptime line:

```go
func statsLines(st api.StatsReply) []string {
	lines := []string{
		fmt.Sprintf("store used     %s (%d bytes)", humanBytes(st.StoreBytes), st.StoreBytes),
		fmt.Sprintf("refs           %d", st.RefCount),
		fmt.Sprintf("requests       %d", st.Requests),
		fmt.Sprintf("nodes tracked  %d", st.NodesTracked),
		fmt.Sprintf("uptime         %s", humanAge(time.Duration(st.UptimeNs))),
	}
	if g := st.GC; g != nil {
		pct := 0.0
		if tot := g.LiveBytes + g.GarbageBytes; tot > 0 {
			pct = 100 * float64(g.GarbageBytes) / float64(tot)
		}
		sweep := "never"
		if g.LastSweepNs != 0 {
			sweep = humanAge(time.Since(time.Unix(0, g.LastSweepNs))) + " ago"
		}
		lines = append(lines,
			fmt.Sprintf("gc retention   %s", time.Duration(g.RetentionNs)),
			fmt.Sprintf("gc sweep       %s (expired %d, total %d)", sweep, g.ExpiredLast, g.ExpiredTotal),
			fmt.Sprintf("gc garbage     %s of %s (%.1f%%), pinned %d",
				humanBytes(g.GarbageBytes), humanBytes(g.LiveBytes+g.GarbageBytes), pct, g.Pinned),
		)
		if g.LastError != "" {
			lines = append(lines, "gc last error  "+g.LastError)
		}
	}
	return lines
}
```

`refRow` — add the two columns:

```go
func refRow(r api.RefInfo, now time.Time) []string {
	access, pin := "-", ""
	if r.LastAccessNs > 0 {
		access = ageAt(now, r.LastAccessNs)
	}
	if r.Pinned {
		pin = "pin"
	}
	return []string{
		r.Name,
		shortKey(r.Key),
		ageAt(now, r.CreatedNs),
		access,
		pin,
	}
}
```

Find the `refHeader` var (grep `refHeader` in `tui/`) and extend it with two entries, e.g. `"access"` and `""` (or `"pin"`), keeping length equal to the row slices — `joinCells`/`refWidths` size off the header, so check `refsModel.refWidths()` (`tui/refs.go:108`) still computes per-column widths generically; adjust the fixed width table if it is hardcoded.

- [ ] **Step 2: Build + tests**

```bash
nix develop -c go test ./tui/
```
Expected: PASS (fix any column-count assertion the package's tests carry).

- [ ] **Step 3: Commit**

```bash
git add tui/
git commit -m "tui: GC block in stats view, access/pin columns in refs view"
```

---

### Task 12: registryd pin-asserts

**Files:**
- Create: `registryd/pins.go`, `registryd/pins_test.go`
- Modify: `registryd/sync.go` (Pin), `registryd/registryd.go` (field init), `registryd/handler.go` (assert calls)

**Interfaces:**
- Consumes: `amberclient.Pin` (Task 5), `amberiroh.RemoteError`.
- Produces: `(*registry).assertPins(rec imageRecord)` — fire-and-forget, coalesced, self-disabling on old servers.

- [ ] **Step 1: Write the failing unit test**

Create `registryd/pins_test.go`:

```go
package registryd

import (
	"slices"
	"testing"
	"time"
)

func TestPinAsserterCoalesces(t *testing.T) {
	p := newPinAsserter()
	now := time.Now()

	due := p.due([]string{"a", "b"}, now)
	slices.Sort(due)
	if !slices.Equal(due, []string{"a", "b"}) {
		t.Fatalf("first due = %v", due)
	}
	if got := p.due([]string{"a", "b"}, now.Add(time.Minute)); len(got) != 0 {
		t.Fatalf("within the hour must coalesce, got %v", got)
	}
	if got := p.due([]string{"a"}, now.Add(2*time.Hour)); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("after an hour must re-assert, got %v", got)
	}
}

func TestPinAsserterRetryAndDisable(t *testing.T) {
	p := newPinAsserter()
	now := time.Now()
	_ = p.due([]string{"a"}, now)

	// A transport failure re-arms the names for the next serve.
	p.retry([]string{"a"})
	if got := p.due([]string{"a"}, now.Add(time.Second)); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("retry must re-arm, got %v", got)
	}

	// An old server disables asserting for good.
	p.disable()
	if got := p.due([]string{"a", "b"}, now.Add(3*time.Hour)); len(got) != 0 {
		t.Fatalf("disabled asserter returned %v", got)
	}
}

func TestPinNames(t *testing.T) {
	rec := imageRecord{K: "aa", F: "bb", Platform: "linux/amd64"}
	want := []string{"build-from:aa", "build:aa", "build-output:bb",
		"build-output-deps:bb", "shell:linux/amd64"}
	got := pinNames(rec)
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("pinNames = %v, want %v", got, want)
	}
}
```

- [ ] **Step 2: Run to verify failure**

```bash
nix develop -c go test ./registryd/ -run TestPin
```
Expected: FAIL.

- [ ] **Step 3: Implement**

Create `registryd/pins.go`:

```go
package registryd

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jobs-build/jobs-iroh/amberiroh"
)

// pinAssertEvery coalesces pin-asserts per ref: as long as an image keeps
// being served, its pins reappear on the server within this window even
// after a server access-DB loss (design §4).
const pinAssertEvery = time.Hour

// pinAsserter tracks which refs were recently asserted and whether the
// server supports TPin at all.
type pinAsserter struct {
	mu       sync.Mutex
	sent     map[string]time.Time
	disabled bool
}

func newPinAsserter() *pinAsserter {
	return &pinAsserter{sent: map[string]time.Time{}}
}

// due filters names down to the ones whose last assert is over an hour old,
// marking them sent. Empty when disabled.
func (p *pinAsserter) due(names []string, now time.Time) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.disabled {
		return nil
	}
	var out []string
	for _, n := range names {
		if last, ok := p.sent[n]; ok && now.Sub(last) < pinAssertEvery {
			continue
		}
		p.sent[n] = now
		out = append(out, n)
	}
	return out
}

// retry re-arms names after a transport failure so the next serve retries.
func (p *pinAsserter) retry(names []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, n := range names {
		delete(p.sent, n)
	}
}

// disable stops asserting for the life of the process (old server).
func (p *pinAsserter) disable() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.disabled = true
}

// pinNames lists the server refs backing one image — what must survive GC
// for the image to stay resolvable and reassemblable.
func pinNames(rec imageRecord) []string {
	names := []string{
		"build-from:" + rec.K,
		"build:" + rec.K,
		"build-output:" + rec.F,
		"build-output-deps:" + rec.F,
	}
	if rec.Platform != "" {
		names = append(names, "shell:"+rec.Platform)
	}
	return names
}

// assertPins fires a best-effort TPin for the refs backing rec: never on
// the request path (a goroutine under the daemon context), coalesced to
// once per ref per hour, self-disabling when the server predates TPin.
func (r *registry) assertPins(rec imageRecord) {
	due := r.pins.due(pinNames(rec), time.Now())
	if len(due) == 0 {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(r.runCtx, time.Minute)
		defer cancel()
		if err := r.sync.Pin(ctx, due); err != nil {
			var re *amberiroh.RemoteError
			if errors.As(err, &re) {
				r.pins.disable()
				r.log.Warn("jobs-server does not support pin-asserts; images may be GC'd there", "error", err)
				return
			}
			r.pins.retry(due)
			r.log.Debug("pin-assert failed; will retry on the next serve", "error", err)
		}
	}()
}
```

`registryd/sync.go` — add:

```go
func (r *reconnSync) Pin(ctx context.Context, names []string) error {
	return r.do(ctx, func(c *amberclient.Client) error {
		return c.Pin(ctx, names)
	})
}
```

BUT note `reconnSync.do` redials on RemoteError? Read the `do` doc comment (`sync.go:17-21`): server-answered errors (RemoteError) never redial — exactly what the disable path needs. ✔

`registryd/registryd.go` — `registry` struct gains `pins *pinAsserter` (after `layers`), and the literal in `Run` gains `pins: newPinAsserter(),`.

`registryd/handler.go` — two call sites:
1. In `serveManifest` (handler.go:81), after `manifestFor` succeeds (the `b, rec, assembled, err := r.manifestFor(…)`/error-check block, before writing the manifest): add `r.assertPins(rec)`.
2. In `serveManifestByDigest` (handler.go:152), after the record lookup succeeds and the manifest is servable: add `r.assertPins(rec)` (the variable holding the `imageRecord` there — check its name in the function body).

- [ ] **Step 4: Run tests, then the registryd e2e suite**

```bash
nix develop -c go test ./registryd/ -run TestPin
nix develop -c go test ./registryd/
```
Expected: PASS (the e2e suite runs against a GC-less server — pin-asserts are fire-and-forget and must not disturb it; the old-server disable path only triggers on TErr, and Task 4's server answers TOK).

Optional (recommended if quick): extend `TestRegistryServesServerBuild` — after the image pull, dial the server admin ALPN (`api.WriteFrame`/`ReadFrame` over an iroh stream, mirroring how the fixture is pushed) with `TRefs{Prefix: "build-output:"}` and assert the pulled build's row eventually shows `Pinned: true` — poll with a deadline since the assert is async. Requires `startServer` there to enable GC (`GCRetention: time.Hour`); skip if the harness resists, the serve-side path is already covered.

- [ ] **Step 5: Commit**

```bash
git add registryd/
git commit -m "registryd: assert keep-forever pins for served images (TPin, coalesced)"
```

---

### Task 13: Docs + full verification

**Files:**
- Modify: `CLAUDE.md`, `CHANGELOG.md`, `docs/design/2026-08-25-gc-auto-cleanup.md`

- [ ] **Step 1: CLAUDE.md**

- Package map: add row `reftrack/` — "Server-side ref access tracker: touch/pin/expire + CBOR snapshot (`<data-dir>/refaccess.cbor`); protected classes shell:/fetcher:/seed-src:; build-output(-deps) family shares one clock."
- `amber/` row: mention "SetObserver/SetRefGuard GC seams".
- `amberiroh/` row: mention "TPin pin-asserts + OnAccess/OnPin/RefGuard hooks".
- `serve/` row: mention "GC runner (hourly access-expiry sweep + mark-sweep cycles)".
- `registryd/` row: mention "pins served images on the server (TPin, hourly re-assert)".
- Binaries: `jobs-server` flags gain `[--gc-retention 720h] [--gc-interval 1h] [--gc-rate N] [--gc-min-free N]` with a one-line description ("refs unread for the retention are deleted and the store mark-sweeps; registry-served images are pinned forever; 0 disables"). `jobs-client admin` list gains `gc|pin|unpin`.
- Invariants: append one bullet — "**GC expiry is rebuild-safe**: deleting a cold output ref un-memoizes, never corrupts (doneness = ref existence); bootstrap seeds and pinned refs never expire; every ref PUT goes through the collector's PrepareRef guard (`amber.PutRef` + `amberiroh.handlePush` — a new PUT path MUST take the guard too)."

- [ ] **Step 2: CHANGELOG.md** — add an Unreleased/next-version entry summarizing: GC + auto-cleanup (30d default retention, hourly sweep, stats log line), runner read-ref reporting, registry keep-forever pins, admin gc/pin/unpin + stats/refs extensions, amber-store-core bump.

- [ ] **Step 3: Design-doc deviations** — in `docs/design/2026-08-25-gc-auto-cleanup.md`, amend §2 item 3 (runner reports the ensured PullRefs; the out-of-band shell ensure is not recorded because `shell:` is protected) and §9 (e2e uses tiny retention + forced sweep instead of a clock hook).

- [ ] **Step 4: Full verification**

```bash
nix develop -c gofmt -l .                    # must print nothing
nix develop -c go build ./...
nix develop -c go test ./...
nix develop -c env CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go vet ./...
nix develop -c go run ./cmd/jobs-client build   # JOBS self-build; rerun must print (cached)
```

The self-build exercises the recipe against the bumped go.mod — if the gomod import set changed, follow the recipe header's pin-bump instructions. If it fails for pin reasons unrelated to this feature, report it rather than force it.

- [ ] **Step 5: Commit**

```bash
git add CLAUDE.md CHANGELOG.md docs/design/2026-08-25-gc-auto-cleanup.md
git commit -m "docs: GC auto-cleanup — CLAUDE.md, changelog, design amendments"
```

---

## Plan Self-Review Notes

- **Spec coverage:** §2 access sources → Tasks 3 (server reads), 4 (pulls/pushes), 6+7 (runner reports), 4+12 (pins). §3 tracker → Task 2. §4 TPin → Tasks 4, 5, 12. §5 collector+sweep → Task 8. §6 admin → Tasks 9–11. §7 config → Task 8. §8 compat → constraints + Task 12 disable path. §9 testing → per-task tests + Task 8 e2e.
- **Known judgment calls for the implementer:** exact insertion points in `registryd/handler.go` are given by function (`serveManifest`, `serveManifestByDigest`) since the record variable names there must be read in place; `refHeader`/`refWidths` in tui may need a width-table tweak; `startGCServer` clones `serve_test.go:20-68` with the documented capture seam.
- **Type consistency:** `RefGuard` appears twice (amber, amberiroh) with identical shape by design — both satisfied by `*gc.Collector`. `api.GCStats` is produced by `gcRunner.Sweep`/`StatsSnapshot` and consumed verbatim by frames, CLI, TUI.
