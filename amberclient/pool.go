package amberclient

// Shard-connection pooling (2026-07-28-pooled-shard-connections.md): the
// punch ramp — relay connect, TAttach, riding the relay until QNT lands the
// direct path — is paid per connection, so connections must outlive
// transfers. TAttach binds a STREAM to a transfer token, not a connection:
// a pooled connection serves transfer after transfer by opening a fresh
// stream each time. The pool grows with concurrent transfers, clamps at a
// max, and an idle sweep shrinks it back to baseline; QUIC keepalive (on by
// default in go-iroh) keeps idle entries alive, and any entry found dead at
// acquire is evicted and redialed.
//
// Relayed entries are PARKED, not evicted (2026-07-30, after field evidence
// from the NAT'd-server fleet): punches there land in minutes, not the ~5s
// the old relayGrace assumed, so evicting a still-relayed connection killed
// every punch mid-ramp and locked shard traffic onto the relay forever. A
// parked entry stays pooled and invisible to direct-only acquires until its
// punch lands; only after punchAbandon does the pool give up and replace it
// (in the background) for a fresh punch attempt. Growth dials likewise moved
// off the acquire critical path: a transfer that can be served from live
// entries never waits on a dial — only a cold pool waits, bounded by ctx.

import (
	"context"
	"log/slog"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/amber-store/transport-iroh/amberiroh"
	"github.com/tmc/go-iroh/iroh"
	irohkey "github.com/tmc/go-iroh/key"
)

// shardConn is the slice of *iroh.Conn the pool needs — an interface so
// pool tests can fake dials.
type shardConn interface {
	Context() context.Context
	OpenStreamConn(ctx context.Context) (net.Conn, error)
	Close() error
}

// poolDialer dials shard slot i and returns the connection, its full
// teardown (connection close + endpoint shutdown) and the identity it
// authenticated.
type poolDialer func(ctx context.Context, i int, ports []uint16, eps []amberiroh.DataEndpointRec) (shardConn, func(), irohkey.EndpointID, error)

// poolEntryMaxUses rotates a pooled connection before quic-go's initial
// 100-stream budget can run dry against servers that do not retire attach
// streams (pre-closeStream servers; also a live safety margin while the
// transport-level retirement gap on migrated paths is open). The proactive
// redial costs ~1s once per 90 transfers instead of a starved 8s attach.
const poolEntryMaxUses = 90

// punchAbandon is how long a pooled entry may sit relayed before the pool
// closes it and grows a replacement in the background. Parked entries are
// invisible to direct-only acquires, so this is not about protecting
// transfers — it is about giving up on a punch that stalled for good and
// getting a fresh QNT attempt on a new connection. Field punches through
// hostile NATs land in minutes, so patience is generous.
const punchAbandon = 5 * time.Minute

// poolDialTimeout bounds one background pool dial. Growth dials run outside
// any transfer's context — the transfer that triggered them may finish
// first — so they carry their own budget, sized like a punching attach.
const poolDialTimeout = 8 * time.Second

type poolEntry struct {
	conn     shardConn
	close    func()
	id       irohkey.EndpointID
	streams  int
	uses     int // transfers served; rotation guard, see poolEntryMaxUses
	dialed   time.Time
	lastUsed time.Time
	// path reports the connection's current transport path; nil when the
	// conn cannot report one (test stubs). Wired from the conn itself at
	// entry creation.
	path func() (Path, bool)
}

// relayed reports whether the entry's connection currently rides a relay.
// Unknown paths count as direct: the pool cannot prove they are stuck, and
// treating them as usable matches the pre-parking behavior for test stubs
// and snapshots taken before the transport reports a path.
func (e *poolEntry) relayed() bool {
	if e.path == nil {
		return false
	}
	p, ok := e.path()
	return ok && p.Relayed
}

// pathReporter is the slice of *iroh.Conn that exposes path snapshots.
type pathReporter interface {
	Paths() []iroh.PathInfo
}

// shardPool owns shard-connection lifecycle for one Client. Entries are
// shared: concurrent transfers each open their own stream on an entry, so
// acquire never blocks on "busy" connections — it only spreads load by
// picking the least-loaded k.
type shardPool struct {
	dial     poolDialer
	log      *slog.Logger
	baseline int // entries kept through idle (Conns-1)
	max      int // entry cap (PoolMax-1)
	idleTTL  time.Duration

	mu      sync.Mutex
	entries []*poolEntry
	dialing int           // background dials in flight
	wake    chan struct{} // closed+replaced whenever entries/dialing change
	active  int           // acquire/release balance = concurrent transfers
	sweep   *time.Timer
	closed  bool
}

func newShardPool(dial poolDialer, log *slog.Logger, baseline, max int, idleTTL time.Duration) *shardPool {
	if baseline < 1 {
		baseline = 1
	}
	if max < baseline {
		max = baseline
	}
	return &shardPool{dial: dial, log: log, baseline: baseline, max: max, idleTTL: idleTTL}
}

// wakeLocked signals every acquire waiting for pool state to change.
func (p *shardPool) wakeLocked() {
	if p.wake != nil {
		close(p.wake)
		p.wake = nil
	}
}

// waitChLocked returns the channel the next state change will close.
func (p *shardPool) waitChLocked() chan struct{} {
	if p.wake == nil {
		p.wake = make(chan struct{})
	}
	return p.wake
}

// pickableLocked counts entries a transfer in the given mode may use.
func (p *shardPool) pickableLocked(directOnly bool) int {
	n := 0
	for _, e := range p.entries {
		if directOnly && e.relayed() {
			continue
		}
		n++
	}
	return n
}

// acquire returns up to k live entries for one transfer, a per-entry
// first-use map (true = this is the entry's first transfer — the
// distinction decides whether an attach failure is a topology verdict or
// just a stale entry), the count of live entries withheld because they are
// parked on a relay, and a release that MUST be called when the transfer
// ends. directOnly selects the reserve mode: only direct-path entries are
// handed out (parked ones wait for their punch). With directOnly false —
// the optimistic first transfer, whose DataConns promise is already on the
// wire — relayed entries are served too, exactly as before parking existed.
//
// Growth dials run in the background and never block an acquire that can
// be served from live entries; only a cold pool waits, bounded by ctx.
func (p *shardPool) acquire(ctx context.Context, k int, directOnly bool, ports []uint16, eps []amberiroh.DataEndpointRec) ([]*poolEntry, map[*poolEntry]bool, int, func()) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, nil, 0, func() {}
	}
	p.active++
	if p.sweep != nil {
		p.sweep.Stop()
	}

	// Evict dead connections; entries whose identity the server no longer
	// advertises (an identity rotation means a server restart, so those
	// connections are dead or dying); stream-budget rotations; and idle
	// entries whose punch has not landed within the abandon patience —
	// their replacement dials happen in the background, so closing here
	// costs the transfer nothing.
	live := p.entries[:0]
	for _, e := range p.entries {
		stale := e.conn.Context().Err() != nil
		if !stale && len(eps) > 0 && !e.id.IsZero() {
			stale = true
			for _, rec := range eps {
				if id, err := irohkeyFromBytes(rec.ID); err == nil && id == e.id {
					stale = false
					break
				}
			}
		}
		if !stale && e.streams == 0 && e.uses >= poolEntryMaxUses {
			stale = true
		}
		if !stale && e.streams == 0 && time.Since(e.dialed) > punchAbandon && e.relayed() {
			p.log.Info("amberclient: abandoning shard connection whose punch never landed; replacing",
				"endpoint", e.id.Short(), "age", time.Since(e.dialed).Round(time.Second).String())
			stale = true
		}
		if stale {
			e.close()
			continue
		}
		live = append(live, e)
	}
	p.entries = live

	// Grow toward the concurrency target: identities with the fewest
	// pooled connections first, so the spread across server sockets holds.
	target := min(p.baseline*p.active, p.max)
	// Never start more than this transfer could use — the next acquire
	// keeps growing if concurrency holds — and count dials already in
	// flight so concurrent acquires do not storm the server.
	need := min(target-len(p.entries)-p.dialing, k)
	perID := map[irohkey.EndpointID]int{}
	for _, e := range p.entries {
		perID[e.id]++
	}
	var slots []int
	if need > 0 && len(eps) > 0 {
		idx := make([]int, len(eps))
		for i := range idx {
			idx[i] = i
		}
		sort.SliceStable(idx, func(a, b int) bool {
			ca, cb := -1, -1
			if id, err := irohkeyFromBytes(eps[idx[a]].ID); err == nil {
				ca = perID[id]
			}
			if id, err := irohkeyFromBytes(eps[idx[b]].ID); err == nil {
				cb = perID[id]
			}
			return ca < cb
		})
		for len(slots) < need {
			slots = append(slots, idx[len(slots)%len(idx)])
		}
	} else {
		for i := range need {
			slots = append(slots, i)
		}
	}
	p.dialing += len(slots)
	cold := p.pickableLocked(directOnly) == 0 && p.dialing > 0
	p.mu.Unlock()

	// Dials run detached: each shard's ramp (relay connect, punch) is
	// independent of any one transfer, and a completed connection joins
	// the pool whether or not the acquire that wanted it is still around.
	for _, i := range slots {
		go p.dialSlot(i, ports, eps)
	}

	// A cold pool is the one case worth waiting for: nothing is pickable
	// yet and dials are in flight. Wait for enough entries (or the last
	// dial to settle, or the caller's budget) — a warm pool returns
	// immediately with whatever is live.
	if cold {
		for {
			p.mu.Lock()
			pickable := p.pickableLocked(directOnly)
			dialing := p.dialing
			if pickable >= k || dialing == 0 {
				p.mu.Unlock()
				break
			}
			w := p.waitChLocked()
			p.mu.Unlock()
			select {
			case <-w:
			case <-ctx.Done():
			}
			if ctx.Err() != nil {
				break
			}
		}
	}

	p.mu.Lock()
	sort.SliceStable(p.entries, func(a, b int) bool {
		if p.entries[a].streams != p.entries[b].streams {
			return p.entries[a].streams < p.entries[b].streams
		}
		return p.entries[a].lastUsed.After(p.entries[b].lastUsed)
	})
	picked := make([]*poolEntry, 0, k)
	parked := 0
	fresh := map[*poolEntry]bool{}
	for _, e := range p.entries {
		if directOnly && e.relayed() {
			parked++
			continue
		}
		if len(picked) == k {
			continue
		}
		e.streams++
		e.uses++
		e.lastUsed = time.Now()
		fresh[e] = e.uses == 1
		picked = append(picked, e)
	}
	p.mu.Unlock()

	var once sync.Once
	release := func() {
		once.Do(func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			for _, e := range picked {
				e.streams--
				e.lastUsed = time.Now()
			}
			p.active--
			if p.active == 0 && !p.closed {
				if p.sweep != nil {
					p.sweep.Stop()
				}
				p.sweep = time.AfterFunc(p.idleTTL, p.scaleDown)
			}
		})
	}
	return picked, fresh, parked, release
}

// dialSlot runs one background pool dial and folds the result in.
func (p *shardPool) dialSlot(i int, ports []uint16, eps []amberiroh.DataEndpointRec) {
	ctx, cancel := context.WithTimeout(context.Background(), poolDialTimeout)
	defer cancel()
	conn, closeFn, id, err := p.dial(ctx, i, ports, eps)
	p.mu.Lock()
	p.dialing--
	if err != nil {
		p.wakeLocked()
		p.mu.Unlock()
		p.log.Debug("amberclient: pool dial failed", "slot", i, "error", err)
		return
	}
	if p.closed {
		p.wakeLocked()
		p.mu.Unlock()
		closeFn()
		return
	}
	e := &poolEntry{conn: conn, close: closeFn, id: id, dialed: time.Now(), lastUsed: time.Now()}
	if pr, ok := conn.(pathReporter); ok {
		e.path = func() (Path, bool) { return pathOf(pr.Paths()) }
	}
	p.entries = append(p.entries, e)
	p.wakeLocked()
	p.mu.Unlock()
}

// discard removes entries from the pool and closes them — for connections
// that proved unusable at attach time (a hung or failed stream open on a
// pooled conn means it went bad between transfers; the next acquire then
// redials cleanly instead of tripping over it again).
func (p *shardPool) discard(entries ...*poolEntry) {
	if len(entries) == 0 {
		return
	}
	drop := map[*poolEntry]bool{}
	for _, e := range entries {
		drop[e] = true
	}
	p.mu.Lock()
	keep := p.entries[:0]
	var closing []*poolEntry
	for _, e := range p.entries {
		if drop[e] {
			closing = append(closing, e)
			continue
		}
		keep = append(keep, e)
	}
	p.entries = keep
	p.mu.Unlock()
	for _, e := range closing {
		e.close()
	}
}

// scaleDown closes idle entries back to baseline, keeping the most
// recently used, preferring direct paths over parked ones and distinct
// identities over doubled-up sockets.
func (p *shardPool) scaleDown() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.active > 0 || p.closed || len(p.entries) <= p.baseline {
		return
	}
	sort.SliceStable(p.entries, func(a, b int) bool {
		if ra, rb := p.entries[a].relayed(), p.entries[b].relayed(); ra != rb {
			return rb
		}
		return p.entries[a].lastUsed.After(p.entries[b].lastUsed)
	})
	seen := map[irohkey.EndpointID]bool{}
	var keep, rest []*poolEntry
	for _, e := range p.entries {
		if len(keep) < p.baseline && !seen[e.id] {
			seen[e.id] = true
			keep = append(keep, e)
			continue
		}
		rest = append(rest, e)
	}
	for _, e := range rest {
		if len(keep) == p.baseline {
			break
		}
		keep = append(keep, e)
	}
	for _, e := range p.entries {
		kept := false
		for _, ke := range keep {
			if ke == e {
				kept = true
				break
			}
		}
		if !kept {
			e.close()
		}
	}
	p.entries = keep
}

// drain closes every entry; the pool refuses further work.
func (p *shardPool) drain() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	if p.sweep != nil {
		p.sweep.Stop()
	}
	for _, e := range p.entries {
		e.close()
	}
	p.entries = nil
}
