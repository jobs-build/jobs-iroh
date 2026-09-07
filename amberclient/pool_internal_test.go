package amberclient

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/amber-store/transport-iroh/amberiroh"
	"github.com/tmc/go-iroh/iroh"
	irohkey "github.com/tmc/go-iroh/key"
)

// stubConn is a fake shardConn whose liveness is its context; failOpen
// simulates a connection whose path died between transfers, pathsFn (when
// set) makes the conn report transport paths like a real *iroh.Conn.
type stubConn struct {
	ctx      context.Context
	cancel   context.CancelFunc
	failOpen bool
	pathsFn  func() []iroh.PathInfo
}

func newStubConn() *stubConn {
	ctx, cancel := context.WithCancel(context.Background())
	return &stubConn{ctx: ctx, cancel: cancel}
}

func (s *stubConn) Context() context.Context { return s.ctx }
func (s *stubConn) OpenStreamConn(ctx context.Context) (net.Conn, error) {
	if s.failOpen {
		return nil, context.DeadlineExceeded
	}
	c, _ := net.Pipe()
	return c, nil
}
func (s *stubConn) Close() error { s.cancel(); return nil }
func (s *stubConn) Paths() []iroh.PathInfo {
	if s.pathsFn == nil {
		return nil
	}
	return s.pathsFn()
}

// relayedPaths is a path snapshot for a connection stuck on a relay.
func relayedPaths() []iroh.PathInfo {
	return []iroh.PathInfo{{Selected: true, Validated: true, Relayed: true}}
}

// stubDialer counts dials and records closes; each dial's identity comes
// from the records exactly like the real dialer (shardTarget's i%len).
// failOpenNew makes every future dial's conn refuse stream opens;
// relayedNew makes every future dial's conn report a relayed path; delay
// makes every future dial take that long (a slow punch ramp).
type stubDialer struct {
	mu          sync.Mutex
	dials       int
	closed      int
	failOpenNew bool
	relayedNew  bool
	delay       time.Duration
	conns       []*stubConn
}

func (d *stubDialer) dial(ctx context.Context, i int, ports []uint16, eps []amberiroh.DataEndpointRec) (shardConn, func(), irohkey.EndpointID, error) {
	d.mu.Lock()
	delay := d.delay
	d.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, nil, irohkey.EndpointID{}, ctx.Err()
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dials++
	c := newStubConn()
	c.failOpen = d.failOpenNew
	if d.relayedNew {
		c.pathsFn = relayedPaths
	}
	d.conns = append(d.conns, c)
	var id irohkey.EndpointID
	if len(eps) > 0 {
		var err error
		id, err = irohkeyFromBytes(eps[i%len(eps)].ID)
		if err != nil {
			return nil, nil, irohkey.EndpointID{}, err
		}
	}
	return c, func() {
		c.cancel()
		d.mu.Lock()
		d.closed++
		d.mu.Unlock()
	}, id, nil
}

func (d *stubDialer) counts() (int, int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dials, d.closed
}

func testRecs(t *testing.T, n int) []amberiroh.DataEndpointRec {
	t.Helper()
	recs := make([]amberiroh.DataEndpointRec, n)
	for i := range recs {
		recs[i] = recWith(t, "ip:10.0.0.5:4001")
	}
	return recs
}

func testPool(d *stubDialer, baseline, max int, ttl time.Duration) *shardPool {
	return newShardPool(d.dial, slog.New(slog.NewTextHandler(io.Discard, nil)), baseline, max, ttl)
}

// waitCounts polls until the dialer reports the wanted dial/close counts —
// pool growth is asynchronous, so tests observe it by convergence.
func waitCounts(t *testing.T, d *stubDialer, dials, closed int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		gd, gc := d.counts()
		if gd == dials && gc == closed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("dials/closed = %d/%d, want %d/%d", gd, gc, dials, closed)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPoolReusesAcrossSequentialAcquires(t *testing.T) {
	d := &stubDialer{}
	p := testPool(d, 3, 11, time.Hour)
	recs := testRecs(t, 3)

	first, _, _, rel := p.acquire(context.Background(), 3, true, nil, recs)
	if len(first) != 3 {
		t.Fatalf("acquired %d, want 3", len(first))
	}
	rel()
	second, _, _, rel2 := p.acquire(context.Background(), 3, true, nil, recs)
	defer rel2()
	if dials, _ := d.counts(); dials != 3 {
		t.Fatalf("dials %d, want 3 (reuse)", dials)
	}
	seen := map[*poolEntry]bool{}
	for _, e := range first {
		seen[e] = true
	}
	for _, e := range second {
		if !seen[e] {
			t.Fatal("second acquire returned a fresh entry; want reuse")
		}
	}
}

func TestPoolGrowsInBackgroundUnderConcurrency(t *testing.T) {
	d := &stubDialer{}
	p := testPool(d, 3, 11, time.Hour)
	recs := testRecs(t, 3)

	a, _, _, relA := p.acquire(context.Background(), 3, true, nil, recs)
	defer relA()
	b, _, _, relB := p.acquire(context.Background(), 3, true, nil, recs)
	defer relB()
	// The concurrent acquire shares the live entries instead of waiting on
	// fresh dials; growth toward the doubled target lands asynchronously.
	if len(b) != 3 {
		t.Fatalf("second acquire got %d entries, want 3 shared", len(b))
	}
	waitCounts(t, d, 6, 0)
	// Once grown, a third transfer spreads across the fresh entries: the
	// least-loaded pick must not double up on a's and b's busy conns.
	c, _, _, relC := p.acquire(context.Background(), 3, true, nil, recs)
	defer relC()
	for _, e := range c {
		if e.streams > 1 {
			t.Fatalf("entry carries %d streams; least-loaded selection should pick the grown entries", e.streams)
		}
	}
	_ = a
}

func TestPoolGrowthDoesNotBlockAcquire(t *testing.T) {
	d := &stubDialer{}
	p := testPool(d, 3, 11, time.Hour)
	recs := testRecs(t, 3)

	_, _, _, relA := p.acquire(context.Background(), 3, true, nil, recs)
	defer relA()
	d.mu.Lock()
	d.delay = 300 * time.Millisecond // growth dials now ride a slow punch ramp
	d.mu.Unlock()

	start := time.Now()
	b, _, _, relB := p.acquire(context.Background(), 3, true, nil, recs)
	defer relB()
	if el := time.Since(start); el > 100*time.Millisecond {
		t.Fatalf("acquire blocked %s on growth dials; want immediate reuse", el)
	}
	if len(b) != 3 {
		t.Fatalf("second acquire got %d entries, want 3 reused", len(b))
	}
	waitCounts(t, d, 6, 0)
}

func TestPoolCapsAtMax(t *testing.T) {
	d := &stubDialer{}
	p := testPool(d, 3, 4, time.Hour)
	recs := testRecs(t, 3)

	a, _, _, relA := p.acquire(context.Background(), 3, true, nil, recs)
	defer relA()
	b, _, _, relB := p.acquire(context.Background(), 3, true, nil, recs)
	defer relB()
	if len(a) != 3 || len(b) != 3 {
		t.Fatalf("acquired %d/%d, want 3/3 (shared entries)", len(a), len(b))
	}
	waitCounts(t, d, 4, 0) // growth stops at max
	if dials, _ := d.counts(); dials > 4 {
		t.Fatalf("dials %d, want ≤ max 4", dials)
	}
}

func TestPoolEvictsDeadConns(t *testing.T) {
	d := &stubDialer{}
	p := testPool(d, 3, 11, time.Hour)
	recs := testRecs(t, 3)

	first, _, _, rel := p.acquire(context.Background(), 3, true, nil, recs)
	rel()
	first[0].conn.(*stubConn).cancel()

	got, _, _, rel2 := p.acquire(context.Background(), 3, true, nil, recs)
	defer rel2()
	if len(got) != 2 {
		t.Fatalf("acquired %d, want the 2 live entries (replacement dials in background)", len(got))
	}
	waitCounts(t, d, 4, 1)
}

func TestPoolEvictsRotatedIdentities(t *testing.T) {
	d := &stubDialer{}
	p := testPool(d, 3, 11, time.Hour)

	_, _, _, rel := p.acquire(context.Background(), 3, true, nil, testRecs(t, 3))
	rel()
	got, _, _, rel2 := p.acquire(context.Background(), 3, true, nil, testRecs(t, 3))
	defer rel2()
	// All prior identities rotated away: the pool is cold again, so this
	// acquire waits for the replacement dials.
	if len(got) != 3 {
		t.Fatalf("acquired %d, want 3 fresh after rotation", len(got))
	}
	waitCounts(t, d, 6, 3)
}

func TestPoolIdleScaleDown(t *testing.T) {
	d := &stubDialer{}
	p := testPool(d, 3, 11, 30*time.Millisecond)
	recs := testRecs(t, 3)

	_, _, _, relA := p.acquire(context.Background(), 3, true, nil, recs)
	_, _, _, relB := p.acquire(context.Background(), 3, true, nil, recs)
	waitCounts(t, d, 6, 0) // background growth landed
	relA()
	relB()
	deadline := time.Now().Add(2 * time.Second)
	for {
		p.mu.Lock()
		n := len(p.entries)
		p.mu.Unlock()
		if n == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pool still holds %d entries, want baseline 3", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, closed := d.counts(); closed != 3 {
		t.Fatalf("closed %d, want 3", closed)
	}
}

func TestPoolDrain(t *testing.T) {
	d := &stubDialer{}
	p := testPool(d, 3, 11, time.Hour)
	_, _, _, rel := p.acquire(context.Background(), 3, true, nil, testRecs(t, 3))
	rel()
	p.drain()
	if _, closed := d.counts(); closed != 3 {
		t.Fatalf("closed %d, want all 3 on drain", closed)
	}
	got, _, _, rel2 := p.acquire(context.Background(), 3, true, nil, testRecs(t, 3))
	rel2()
	if len(got) != 0 {
		t.Fatalf("closed pool returned %d entries, want 0", len(got))
	}
}

func TestPoolParksRelayedEntries(t *testing.T) {
	d := &stubDialer{}
	p := testPool(d, 3, 11, time.Hour)
	recs := testRecs(t, 3)

	_, _, _, rel := p.acquire(context.Background(), 3, true, nil, recs)
	rel()

	// One relayed, one direct, one path-unknown: a direct-only acquire may
	// pick only the direct and unknown ones. The relayed entry is parked —
	// held for its punch to land, not closed, never handed out.
	p.mu.Lock()
	p.entries[0].path = func() (Path, bool) { return Path{Relayed: true}, true }
	p.entries[1].path = func() (Path, bool) { return Path{Relayed: false}, true }
	p.entries[2].path = nil
	p.mu.Unlock()

	got, _, parked, rel2 := p.acquire(context.Background(), 3, true, nil, recs)
	defer rel2()
	if len(got) != 2 {
		t.Fatalf("acquired %d, want 2 (relayed entry parked)", len(got))
	}
	for _, e := range got {
		if e.path == nil {
			continue
		}
		if pth, ok := e.path(); ok && pth.Relayed {
			t.Fatal("direct-only acquire handed out a relayed entry")
		}
	}
	if parked != 1 {
		t.Fatalf("parked %d, want 1", parked)
	}
	if _, closed := d.counts(); closed != 0 {
		t.Fatalf("closed %d, want 0 (parked, not evicted)", closed)
	}
}

func TestPoolServesParkedEntriesWhenNotDirectOnly(t *testing.T) {
	d := &stubDialer{}
	p := testPool(d, 3, 11, time.Hour)
	recs := testRecs(t, 3)

	_, _, _, rel := p.acquire(context.Background(), 3, true, nil, recs)
	rel()
	p.mu.Lock()
	for _, e := range p.entries {
		e.path = func() (Path, bool) { return Path{Relayed: true}, true }
	}
	p.mu.Unlock()

	// The optimistic first transfer (no cached endpoints yet) promised the
	// server DataConns shards, so it attaches whatever is live — relayed
	// included — exactly like before parking existed.
	got, _, _, rel2 := p.acquire(context.Background(), 3, false, nil, recs)
	defer rel2()
	if len(got) != 3 {
		t.Fatalf("acquired %d, want 3 (relayed entries still usable when promised)", len(got))
	}
}

func TestPoolAbandonsRelayStuckEntriesEventually(t *testing.T) {
	d := &stubDialer{}
	p := testPool(d, 3, 11, time.Hour)
	recs := testRecs(t, 3)

	_, _, _, rel := p.acquire(context.Background(), 3, true, nil, recs)
	rel()
	p.mu.Lock()
	p.entries[0].dialed = time.Now().Add(-2 * punchAbandon)
	p.entries[0].path = func() (Path, bool) { return Path{Relayed: true}, true }
	p.mu.Unlock()

	got, _, _, rel2 := p.acquire(context.Background(), 3, true, nil, recs)
	defer rel2()
	if len(got) != 2 {
		t.Fatalf("acquired %d, want 2 (abandoned entry closed, replacement in background)", len(got))
	}
	waitCounts(t, d, 4, 1)
}

func TestPoolKeepsParkedEntriesInsidePatience(t *testing.T) {
	d := &stubDialer{}
	p := testPool(d, 3, 11, time.Hour)
	recs := testRecs(t, 3)

	_, _, _, rel := p.acquire(context.Background(), 3, true, nil, recs)
	rel()
	p.mu.Lock()
	for _, e := range p.entries {
		e.path = func() (Path, bool) { return Path{Relayed: true}, true } // punch still pending
	}
	p.mu.Unlock()

	got, _, parked, rel2 := p.acquire(context.Background(), 3, true, nil, recs)
	defer rel2()
	if len(got) != 0 || parked != 3 {
		t.Fatalf("got %d entries, parked %d; want 0/3 (all parked, none closed)", len(got), parked)
	}
	if _, closed := d.counts(); closed != 0 {
		t.Fatalf("closed %d, want 0 (patience not elapsed, punch may still land)", closed)
	}
}

func TestPoolNeverAbandonsBusyRelayedEntries(t *testing.T) {
	d := &stubDialer{}
	p := testPool(d, 3, 11, time.Hour)
	recs := testRecs(t, 3)

	held, _, _, relHold := p.acquire(context.Background(), 3, false, nil, recs)
	defer relHold()
	p.mu.Lock()
	for _, e := range p.entries {
		e.dialed = time.Now().Add(-2 * punchAbandon)
		e.path = func() (Path, bool) { return Path{Relayed: true}, true }
	}
	p.mu.Unlock()

	_, _, _, rel2 := p.acquire(context.Background(), 3, true, nil, recs)
	rel2()
	if _, closed := d.counts(); closed != 0 {
		t.Fatalf("closed %d, want 0 (entries carry another transfer's streams)", closed)
	}
	_ = held
}

func TestPoolColdDirectOnlyAcquireParksRelayedDials(t *testing.T) {
	d := &stubDialer{relayedNew: true}
	p := testPool(d, 3, 11, time.Hour)
	recs := testRecs(t, 3)

	// Cold pool over a punch-hostile path: the dials land but their conns
	// ride the relay. A direct-only acquire must report them parked and
	// hand out nothing — the transfer then runs single-channel on the
	// (direct) control stream instead of sharding across relays.
	got, _, parked, rel := p.acquire(context.Background(), 3, true, nil, recs)
	defer rel()
	if len(got) != 0 {
		t.Fatalf("acquired %d relayed entries, want 0", len(got))
	}
	if parked != 3 {
		t.Fatalf("parked %d, want 3", parked)
	}
}

func TestPoolDiscardRemovesAndCloses(t *testing.T) {
	d := &stubDialer{}
	p := testPool(d, 3, 11, time.Hour)
	recs := testRecs(t, 3)
	first, _, _, rel := p.acquire(context.Background(), 3, true, nil, recs)
	rel()
	p.discard(first[0])
	if _, closed := d.counts(); closed != 1 {
		t.Fatalf("closed %d, want 1", closed)
	}
	got, _, _, rel2 := p.acquire(context.Background(), 3, true, nil, recs)
	defer rel2()
	if len(got) != 2 {
		t.Fatalf("acquired %d, want 2 live (replacement dials in background)", len(got))
	}
	waitCounts(t, d, 4, 1)
}

// failOpen makes a stubConn refuse stream opens — the shape of a pooled
// connection whose path went dead between transfers.
func TestAttachZeroOnReusedEntriesDiscardsWithoutDemote(t *testing.T) {
	d := &stubDialer{}
	p := testPool(d, 3, 11, time.Hour)
	recs := testRecs(t, 3)
	c := &Client{conns: 4, log: slog.New(slog.NewTextHandler(io.Discard, nil)), pool: p}

	// Warm the pool, then break every conn's stream-open path.
	_, _, _, rel := p.acquire(context.Background(), 3, true, nil, recs)
	rel()
	d.mu.Lock()
	for _, sc := range d.conns {
		sc.failOpen = true
	}
	d.mu.Unlock()

	streams, closeAll := c.attachExtras(context.Background(), []byte("tok"), nil, recs, 3)
	closeAll()
	if len(streams) != 0 {
		t.Fatalf("attached %d, want 0", len(streams))
	}
	if c.demoted.Load() {
		t.Fatal("zero-attach on reused entries must not demote")
	}
	p.mu.Lock()
	n := len(p.entries)
	p.mu.Unlock()
	if n != 0 {
		t.Fatalf("pool holds %d entries, want 0 (zombies discarded)", n)
	}
}

func TestAttachZeroOnFreshDialsDemotes(t *testing.T) {
	d := &stubDialer{failOpenNew: true}
	p := testPool(d, 3, 11, time.Hour)
	recs := testRecs(t, 3)
	c := &Client{conns: 4, log: slog.New(slog.NewTextHandler(io.Discard, nil)), pool: p}

	streams, closeAll := c.attachExtras(context.Background(), []byte("tok"), nil, recs, 3)
	closeAll()
	if len(streams) != 0 {
		t.Fatalf("attached %d, want 0", len(streams))
	}
	if !c.demoted.Load() {
		t.Fatal("zero-attach on all-fresh dials is the topology verdict; must demote")
	}
}

func TestPoolRotatesEntriesBeforeStreamBudget(t *testing.T) {
	d := &stubDialer{}
	p := testPool(d, 3, 11, time.Hour)
	recs := testRecs(t, 3)
	for range poolEntryMaxUses {
		_, _, _, rel := p.acquire(context.Background(), 3, true, nil, recs)
		rel()
	}
	if dials, closed := d.counts(); dials != 3 || closed != 0 {
		t.Fatalf("dials %d closed %d before the cap, want 3/0", dials, closed)
	}
	got, _, _, rel := p.acquire(context.Background(), 3, true, nil, recs)
	defer rel()
	if len(got) != 3 {
		t.Fatalf("acquired %d, want 3 fresh (rotation empties the pool, acquire waits)", len(got))
	}
	waitCounts(t, d, 6, 3)
}
