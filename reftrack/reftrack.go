// Package reftrack records when each server reference was last used, so the
// GC sweep can expire refs unused for the retention window. It is the
// server-side companion of docs/design/2026-08-25-gc-auto-cleanup.md §3:
// entries are seeded at first sight (never from CreatedAt — the safe-upgrade
// rule), touched on every read, and persisted as a CBOR snapshot. Losing the
// clocks alone would be benign (a ref merely looks fresher or staler than it
// is), but the snapshot also holds the pins, so an unreadable snapshot is an
// error the caller must not paper over with an empty tracker (issue #22):
// gcsweep halts instead of re-seeding.
package reftrack

import (
	"errors"
	"fmt"
	"math"
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

// Pin marks name kept-forever and touches it (a pin is an access), and
// mirrors the pin onto its build-output family sibling when that sibling is
// tracked — pinning build-output:X alone would leave build-output-deps:X
// expirable, letting the sweep delete deps while output survives, which
// inverts the family invariant (deps strictly before output).
func (t *Tracker) Pin(name string) {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.touchLocked(name, now)
	e := t.entries[name]
	e.Pinned = true
	t.entries[name] = e
	if sib, ok := familySibling(name); ok {
		if _, tracked := t.entries[sib]; tracked {
			t.touchLocked(sib, now)
			se := t.entries[sib]
			se.Pinned = true
			t.entries[sib] = se
		}
	}
}

// Unpin clears the flag; the ref then lives by its access clock. Mirrors
// onto the build-output family sibling the same way Pin does, when tracked.
func (t *Tracker) Unpin(name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e, ok := t.entries[name]; ok {
		e.Pinned = false
		t.entries[name] = e
	}
	if sib, ok := familySibling(name); ok {
		if se, tracked := t.entries[sib]; tracked {
			se.Pinned = false
			t.entries[sib] = se
		}
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

// MaxEntries is the most entries a snapshot may hold and still load — the
// CBOR decoder's map limit, raised from the library's default of 131,072
// (which a 12-day-old server passed, losing every clock and pin on restart:
// issue #22) to the largest the library accepts. Flush refuses to write a
// snapshot past it, so a written snapshot always loads back.
const MaxEntries = math.MaxInt32

// snapDecMode decodes snapshots with the map limit at MaxEntries.
var snapDecMode = func() cbor.DecMode {
	m, err := cbor.DecOptions{MaxMapPairs: MaxEntries}.DecMode()
	if err != nil {
		panic(err)
	}
	return m
}()

// Load replaces the tracker's state with the snapshot at path. A missing
// file is a clean start (nil). A present file that cannot be read or decoded
// leaves the tracker empty and returns the error: the caller must treat that
// as "state unknown", not "no state" — gcsweep refuses to sweep and leaves
// the file untouched, since sweeping from an empty tracker re-seeds every
// clock and drops every pin.
func (t *Tracker) Load(path string) error {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reftrack: read %s: %w", path, err)
	}
	var snap map[string]snapEntry
	if err := snapDecMode.Unmarshal(b, &snap); err != nil {
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
	if len(snap) > MaxEntries {
		return fmt.Errorf("reftrack: %d entries exceed the snapshot limit of %d", len(snap), MaxEntries)
	}
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
