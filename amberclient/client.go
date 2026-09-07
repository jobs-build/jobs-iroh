// Package amberclient is the importable client half of the amber sync
// protocol — the dial/push/pull/refs orchestration that upstream lives only
// in amber-store-iroh's cmd/amber mains, lifted into a library so jobs-iroh
// components can sync stores over any of the server's amber ALPNs
// (jobs-runner-amber/1.0, jobs-amber-admin/1.0). The wire protocol itself
// lives in the amberiroh package.
//
// Transfers are sharded: with Conns > 1 (DefaultConns is 4) each push and
// pull asks the server for extra data channels and attaches one stream per
// extra QUIC connection — each on its own endpoint and UDP socket, since one
// socket's loop caps throughput — and the want loop deals every round across
// all of them (the upstream TAttach path, see shard.go). A server without
// sharding support, or a failed attach, degrades to the single control
// stream. Unchanged is everything that matters for correctness: the
// resumable want-loop (an interrupted transfer resumes where it stopped,
// because wantsync prunes only CheckComplete subtrees), verified writes on
// pull (the peer is untrusted), and pack draining through TDataEnd so
// streams stay frame-aligned between rounds.
//
// Pushes are force-mode (no CAS, no remote-tracking refs): jobs-iroh pushes
// scratch/output refs it owns, so last-write-wins is the intended semantic
// and the upstream CLI's tracking-ref bookkeeping would be dead weight.
package amberclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/core/reference"
	"github.com/jobs-build/jobs-iroh/amberiroh"
	"github.com/tmc/go-iroh/iroh"
	irohkey "github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"

	"github.com/jobs-build/jobs-iroh/amber"
)

// ErrRefNotFound is returned (errors.Is-able) by Pull when the server does
// not have the requested ref.
var ErrRefNotFound = errors.New("amberclient: remote ref not found")

// Options configures Dial.
type Options struct {
	// EndpointID is the server's iroh endpoint ID (required).
	EndpointID string
	// Addrs are direct server addresses, "ip:port" or "host:port"
	// (hostnames may resolve to several candidates). When non-empty the
	// dial goes straight at them — no discovery, no relays — which is also
	// how loopback tests connect. When empty the endpoint ID is resolved
	// via mDNS (passive), pkarr and DNS, and every resolver's candidates
	// are raced.
	Addrs []string
	// ALPN selects the server's amber mount (e.g. serve.ALPNRunnerAmber or
	// serve.ALPNAmberAdmin). Empty means amber-store-iroh's own protocol
	// ALPN, for talking to a stock amber-serve.
	ALPN string
	// BindAddr optionally pins the client's UDP bind address (e.g.
	// loopback in tests). Zero value binds the default wildcard.
	BindAddr netip.AddrPort
	// Conns is the QUIC connections used per transfer: one control
	// connection plus Conns-1 data shards attached to it. 0 means
	// DefaultConns; other values are clamped to [1, 16], and 1 disables
	// sharding. A server without sharding support falls back to the
	// single control stream.
	Conns int
	// PoolMax caps the client's TOTAL connections (control included) as
	// concurrent transfers grow the shard pool. 0 means DefaultPoolMax;
	// values are clamped to [Conns, 17]. At rest the pool shrinks back to
	// Conns total.
	PoolMax int
	// Logger receives client logs; nil means slog.Default().
	Logger *slog.Logger
}

// DefaultConns is the connections per transfer when Options.Conns is 0.
const DefaultConns = 4

// DefaultPoolMax is the total-connection cap when Options.PoolMax is 0.
const DefaultPoolMax = 12

// poolIdleTTL is how long a client sits with no active transfer before
// the shard pool shrinks back to baseline.
const poolIdleTTL = 90 * time.Second

// Gate timing: on a connection younger than gateSettle a relayed control
// path is an unfinished punch (they land ~5s after the dial), so the gate
// waits up to gateWait for the upgrade — a transfer that starts moments
// after boot would otherwise be pinned single-channel to the relay for its
// whole (possibly very long) run. Past gateSettle, relayed is a settled
// verdict and the gate skips extras immediately, as before.
const (
	gateSettle      = 30 * time.Second
	defaultGateWait = 10 * time.Second
	defaultGatePoll = 250 * time.Millisecond
)

// clampPoolMax maps Options.PoolMax onto the usable range: at least the
// per-transfer width, at most control + maxDataConns-sized pool.
func clampPoolMax(n, conns int) int {
	switch {
	case n == 0:
		n = DefaultPoolMax
	case n < conns:
		n = conns
	}
	return min(n, 17)
}

// clampConns maps Options.Conns onto the usable range.
func clampConns(n int) int {
	switch {
	case n == 0:
		return DefaultConns
	case n < 1:
		return 1
	case n > 16:
		return 16
	}
	return n
}

// RefInfo is one reference in a Refs listing.
type RefInfo struct {
	Name      string
	Key       key.Key
	CreatedAt time.Time
	User      string
}

// Client is a connected amber sync client: one control QUIC connection on
// its own ephemeral endpoint, one stream per operation, plus short-lived
// per-transfer shard connections (torn down when the operation ends).
// Methods are safe for concurrent use — each opens its own stream and the
// protocol is stream-scoped. Close releases the control connection and the
// endpoint.
type Client struct {
	conn *iroh.Conn
	ep   *iroh.Endpoint
	log  *slog.Logger

	// Sharding needs to reach the same server again: identity, ALPN and
	// the candidate set of the control dial, plus the pinned bind (if any)
	// for shard endpoints.
	id       irohkey.EndpointID
	alpn     string
	cands    []netaddr.TransportAddr
	bindAddr netip.AddrPort
	conns    int
	// punchDials records that the control dial went through discovery —
	// shard endpoints then bind the same relay/net-report stack so their
	// relay-won connections punch to direct. Direct-addr dials stay bare.
	punchDials bool
	// pathFn reports the control path for the shard gate; nil means
	// Client.Path. A test seam — production always uses the default.
	pathFn func() (Path, bool)
	// dialedAt is when the control connection was established; while it is
	// younger than gateSettle a relayed path just means the punch has not
	// landed yet, so the gate waits for it instead of skipping extras.
	dialedAt time.Time
	// gatePoll/gateWait pace and bound that wait; zero values mean the
	// defaults (test seams).
	gatePoll time.Duration
	gateWait time.Duration
	// gateLogged keeps the relayed-path skip to one log line per
	// connection.
	gateLogged atomic.Bool

	// demoted latches when this connection proves unable to shard (no
	// attach landed in budget, or a sharded transfer failed that then
	// succeeded unsharded) — every later transfer skips the shard attempt.
	// A redial probes afresh.
	demoted atomic.Bool

	// dataEps caches the server's data-endpoint advertisement from the
	// last sharded reply. Once known, transfers reserve their shard
	// connections BEFORE promising DataConns, so the request is truthful
	// and the server never sits out its gather window waiting for shards
	// that are parked on a relay. nil until the first sharded reply.
	dataEps atomic.Pointer[dataEndpoints]

	// pool owns the shard connections: punched once, reused across
	// transfers, grown under concurrency, shrunk after idle.
	pool *shardPool
}

// dataEndpoints is one server data-endpoint advertisement (TAccept/TRef).
type dataEndpoints struct {
	ports []uint16
	eps   []amberiroh.DataEndpointRec
}

// cacheDataEndpoints remembers the server's advertisement for reserve-first
// transfers. Ports-only advertisements (pre-DataEndpoints servers) count
// too — the legacy dial path uses them the same way.
func (c *Client) cacheDataEndpoints(ports []uint16, eps []amberiroh.DataEndpointRec) {
	if len(ports) == 0 && len(eps) == 0 {
		return
	}
	c.dataEps.Store(&dataEndpoints{ports: ports, eps: eps})
}

// reservation holds shard-pool entries picked for one transfer before its
// request went out. release is once-guarded; every exit path may call it.
type reservation struct {
	entries []*poolEntry
	fresh   map[*poolEntry]bool
	release func()
}

// planTransfer decides the next transfer's channel count and, once the
// server's data endpoints are known, reserves the shard connections up
// front — only direct-path entries are reserved, so DataConns promises
// exactly the shards that will attach. Zero direct entries with parked
// ones pending means single-channel on the (direct) control stream: the
// punch is still in flight and sharding across relays moves no extra
// bytes. A nil reservation with conns > 1 is the optimistic first
// transfer, which attaches from the pool mid-transfer as before.
func (c *Client) planTransfer(ctx context.Context) (int, *reservation) {
	conns := c.transferConns()
	if conns <= 1 {
		return 1, nil
	}
	de := c.dataEps.Load()
	if de == nil {
		return conns, nil
	}
	budget := attachBudget
	if len(de.eps) > 0 && c.punchDials {
		budget = punchAttachBudget
	}
	actx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	entries, fresh, parked, release := c.pool.acquire(actx, conns-1, true, de.ports, de.eps)
	if len(entries) == 0 {
		release()
		if parked > 0 {
			c.log.Debug("amberclient: all shard connections relayed; running single-channel until a punch lands")
			return 1, nil
		}
		if ctx.Err() == nil {
			c.demote("no shard connections available")
		}
		return 1, nil
	}
	return len(entries) + 1, &reservation{entries: entries, fresh: fresh, release: release}
}

// transferConns is the connection count for the next transfer: the
// configured Conns until the connection demotes itself to 1 — and 1,
// without demoting, while the control path runs through a relay: extra
// relay connections move no additional bytes, and once hole punching lands
// (it commonly does moments after the dial) the next transfer shards again.
func (c *Client) transferConns() int {
	if c.demoted.Load() {
		return 1
	}
	if c.conns > 1 {
		pf := c.pathFn
		if pf == nil {
			pf = c.Path
		}
		p, ok := pf()
		if ok && p.Relayed && time.Since(c.dialedAt) < gateSettle {
			// Young connection: give the punch its window before deciding.
			wait, poll := c.gateWait, c.gatePoll
			if wait == 0 {
				wait = defaultGateWait
			}
			if poll == 0 {
				poll = defaultGatePoll
			}
			deadline := time.Now().Add(wait)
			for ok && p.Relayed && time.Now().Before(deadline) {
				time.Sleep(poll)
				p, ok = pf()
			}
		}
		if ok && p.Relayed {
			if c.gateLogged.CompareAndSwap(false, true) {
				c.log.Info("amberclient: extras skipped while the control path is relayed; sharding resumes when it goes direct")
			}
			return 1
		}
	}
	return c.conns
}

// demote permanently disables sharding on this connection. Logged once: the
// usual causes — relay-only reachability, filtered data ports, a server
// whose stream handler predates attach handoff — are properties of the
// path, not of one transfer.
func (c *Client) demote(reason string) {
	if c.demoted.CompareAndSwap(false, true) {
		c.log.Info("amberclient: sharded transfers disabled for this connection", "reason", reason)
	}
}

// retrySingle decides whether a failed sharded transfer deserves one
// immediate single-channel retry, demoting the connection first. A missing
// ref is a definitive verdict and a cancelled context will not improve;
// anything else — transport faults, but also a server-side TErr from a
// jobs-server release whose handler severs attached shard streams
// mid-transfer — is worth one unsharded attempt: push is force-mode and
// pull is resumable, so running twice is wasteful but never wrong.
func (c *Client) retrySingle(ctx context.Context, op, name string, err error) bool {
	if errors.Is(err, ErrRefNotFound) || ctx.Err() != nil {
		return false
	}
	c.demote(fmt.Sprintf("sharded %s %q failed: %v", op, name, err))
	return true
}

// Dial connects to the amber server described by o: direct addresses when
// given, discovery otherwise, always with an ephemeral client identity
// (access is open at this trust level; transport identity is the server's
// endpoint key). All candidate addresses are dialed concurrently and the
// first connection wins.
func Dial(ctx context.Context, o Options) (*Client, error) {
	log := o.Logger
	if log == nil {
		log = slog.Default()
	}
	alpn := o.ALPN
	if alpn == "" {
		alpn = amberiroh.ALPN
	}
	id, err := irohkey.ParseEndpointID(o.EndpointID)
	if err != nil {
		return nil, fmt.Errorf("amberclient: parse endpoint id: %w", err)
	}

	ep, cands, err := bindAndResolve(ctx, id, o.Addrs, o.BindAddr)
	if err != nil {
		return nil, err
	}
	conn, err := raceConnect(ctx, ep, id, alpn, cands)
	if err != nil {
		_ = ep.Shutdown(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("amberclient: connect %s: %w", id, err)
	}
	log.Debug("amberclient connected", "endpoint", id.String(), "alpn", alpn)
	c := &Client{
		conn: conn, ep: ep, log: log,
		id: id, alpn: alpn, cands: cands, bindAddr: o.BindAddr,
		conns:      clampConns(o.Conns),
		punchDials: len(o.Addrs) == 0,
		dialedAt:   time.Now(),
	}
	c.pool = newShardPool(c.dialShard, log, c.conns-1, clampPoolMax(o.PoolMax, c.conns)-1, poolIdleTTL)
	return c, nil
}

// Close tears down the shard pool, the control connection and the client
// endpoint.
func (c *Client) Close() error {
	c.pool.drain()
	err := c.conn.Close()
	if serr := c.ep.Shutdown(context.Background()); serr != nil {
		err = errors.Join(err, serr)
	}
	return err
}

// Push uploads the tree rooted at root from st and points the remote ref
// name at it. Force-mode: no CAS precondition, the remote ref is overwritten
// unconditionally (last-write-wins — jobs-iroh pushes refs it owns). Only
// objects the server is missing cross the wire; the server verifies
// completeness before committing the ref, so a reported success means the
// remote ref exists and its whole closure is on the server.
func (c *Client) Push(ctx context.Context, st *amber.Store, name string, root key.Key) error {
	_, err := c.PushWithProgress(ctx, st, name, root, nil)
	return err
}

// PushWithProgress is Push with a transfer observer: prog (nil is fine)
// receives objects done/total as want rounds are answered, and the returned
// stats report what actually moved — 0/0 when the server already had the
// whole closure.
func (c *Client) PushWithProgress(ctx context.Context, st *amber.Store, name string, root key.Key, prog ProgressFunc) (XferStats, error) {
	if err := reference.ValidateName(name); err != nil {
		return XferStats{}, fmt.Errorf("amberclient: push: %w", err)
	}
	// Fail with a clear message before any traffic: a missing local root
	// would otherwise surface mid-transfer as a get error inside Send.
	ok, err := st.Has(root)
	if err != nil {
		return XferStats{}, fmt.Errorf("amberclient: push %q: %w", name, err)
	}
	if !ok {
		return XferStats{}, fmt.Errorf("amberclient: push %q: root %s not in local store", name, root)
	}

	// One meter across both attempts: the want loop resumes where a failed
	// sharded attempt stopped, so counters stay monotonic for the callback
	// and the stats cover the whole operation.
	conns, rsv := c.planTransfer(ctx)
	mtr := &meter{cb: prog}
	err = c.pushOnce(ctx, st, name, root, mtr, conns, rsv)
	if err != nil && conns > 1 && c.retrySingle(ctx, "push", name, err) {
		err = c.pushOnce(ctx, st, name, root, mtr, 1, nil)
	}
	return mtr.stats(), err
}

// pushOnce runs one push attempt with the given connection count.
func (c *Client) pushOnce(ctx context.Context, st *amber.Store, name string, root key.Key, mtr *meter, conns int, rsv *reservation) error {
	if rsv != nil {
		defer rsv.release()
	}
	stream, stop, err := c.openStream(ctx)
	if err != nil {
		return fmt.Errorf("amberclient: push %q: %w", name, err)
	}
	defer stop()
	defer CloseStream(stream)

	// A QUIC stream is invisible to the server until data flows, so the
	// request goes out before any read. DataConns > 0 makes a sharding
	// server answer TAccept with a transfer token before its want rounds;
	// runSenders handles both that and the plain single-channel reply.
	req := amberiroh.Msg{Type: amberiroh.TPush, Name: name, Root: root[:]}
	if conns > 1 {
		req.DataConns = conns - 1
	}
	if err := amberiroh.WriteMsg(stream, req); err != nil {
		return fmt.Errorf("amberclient: push %q: %w", name, err)
	}
	if err := c.runSenders(ctx, stream, st, mtr, conns, rsv); err != nil {
		return fmt.Errorf("amberclient: push %q: %w", name, err)
	}
	m, err := amberiroh.ReadMsg(stream)
	if err != nil {
		return fmt.Errorf("amberclient: push %q: read commit: %w", name, err)
	}
	switch m.Type {
	case amberiroh.TOK:
	case amberiroh.TErr:
		return fmt.Errorf("amberclient: push %q: %w", name, amberiroh.RemoteFromMsg(m))
	default:
		return fmt.Errorf("amberclient: push %q: %w: type %d, want TOK", name, amberiroh.ErrProtocol, m.Type)
	}
	c.log.Debug("amberclient push committed", "ref", name, "root", root.String())
	return nil
}

// Pull fetches the remote ref name and every object below it that st is
// missing, then writes the plain local ref name → root via st.PutRef so
// local consumers (runner drivers' ensure* short-circuits) resolve the ref
// without touching the network again. Received objects are verified against
// their keys before being stored — the peer is untrusted — and the want loop
// only terminates once the whole closure is complete locally, so the local
// ref is written strictly after its objects (the objects-before-ref
// invariant). An absent remote ref returns an error satisfying
// errors.Is(err, ErrRefNotFound).
func (c *Client) Pull(ctx context.Context, st *amber.Store, name string) (key.Key, error) {
	root, _, err := c.PullWithProgress(ctx, st, name, nil)
	return root, err
}

// PullWithProgress is Pull with a transfer observer: prog (nil is fine)
// receives objects done/total as want rounds are exchanged, and the returned
// stats report what actually moved — 0/0 when the closure was already local.
func (c *Client) PullWithProgress(ctx context.Context, st *amber.Store, name string, prog ProgressFunc) (key.Key, XferStats, error) {
	if err := reference.ValidateName(name); err != nil {
		return key.Key{}, XferStats{}, fmt.Errorf("amberclient: pull: %w", err)
	}
	// One meter across both attempts: the want loop resumes where a failed
	// sharded attempt stopped, so counters stay monotonic for the callback
	// and the stats cover the whole operation.
	conns, rsv := c.planTransfer(ctx)
	mtr := &meter{cb: prog}
	root, err := c.pullOnce(ctx, st, name, mtr, conns, rsv)
	if err != nil && conns > 1 && c.retrySingle(ctx, "pull", name, err) {
		root, err = c.pullOnce(ctx, st, name, mtr, 1, nil)
	}
	return root, mtr.stats(), err
}

// pullOnce runs one pull attempt with the given connection count.
func (c *Client) pullOnce(ctx context.Context, st *amber.Store, name string, mtr *meter, conns int, rsv *reservation) (key.Key, error) {
	if rsv != nil {
		defer rsv.release()
	}
	stream, stop, err := c.openStream(ctx)
	if err != nil {
		return key.Key{}, fmt.Errorf("amberclient: pull %q: %w", name, err)
	}
	defer stop()
	defer CloseStream(stream)

	req := amberiroh.Msg{Type: amberiroh.TPull, Name: name}
	if conns > 1 {
		req.DataConns = conns - 1
	}
	if err := amberiroh.WriteMsg(stream, req); err != nil {
		return key.Key{}, fmt.Errorf("amberclient: pull %q: %w", name, err)
	}
	m, err := amberiroh.ReadMsg(stream)
	if err != nil {
		return key.Key{}, fmt.Errorf("amberclient: pull %q: read ref: %w", name, err)
	}
	switch m.Type {
	case amberiroh.TRef:
	case amberiroh.TErr:
		if m.Code == amberiroh.CodeUnknownRef {
			return key.Key{}, fmt.Errorf("amberclient: pull %q: %w", name, ErrRefNotFound)
		}
		return key.Key{}, fmt.Errorf("amberclient: pull %q: %w", name, amberiroh.RemoteFromMsg(m))
	default:
		return key.Key{}, fmt.Errorf("amberclient: pull %q: %w: type %d, want TRef", name, amberiroh.ErrProtocol, m.Type)
	}
	rec, err := reference.Decode(m.Record)
	if err != nil {
		return key.Key{}, fmt.Errorf("amberclient: pull %q: server ref record: %w", name, err)
	}
	root, err := key.Parse(rec.Key)
	if err != nil {
		return key.Key{}, fmt.Errorf("amberclient: pull %q: server ref record key: %w", name, err)
	}

	// A sharding-aware server put a transfer token in the ref frame; a
	// server without support omits it and the transfer stays
	// single-channel. Shard channels read under a watchdog — a shard the
	// server never gathered would otherwise park the want loop forever.
	if len(m.Token) > 0 {
		c.cacheDataEndpoints(m.DataPorts, m.DataEndpoints)
	}
	channels := []io.ReadWriter{stream}
	if len(m.Token) > 0 && conns > 1 {
		var extras []net.Conn
		var closeExtras func()
		if rsv != nil {
			extras, closeExtras = c.attachReserved(ctx, m.Token, rsv)
		} else {
			extras, closeExtras = c.attachExtras(ctx, m.Token, m.DataPorts, m.DataEndpoints, conns-1)
		}
		defer closeExtras()
		for _, ex := range extras {
			channels = append(channels, &watchdogConn{Conn: ex, ctx: ctx})
		}
	} else if rsv != nil {
		// The server did not shard this transfer; the reservation is
		// unused — return the entries to the pool now.
		rsv.release()
	}

	// Receive verifies every object against its key (WriteParallel with
	// Verify) and re-walks the frontier until CheckComplete prunes
	// everything — returning nil IS the completeness proof for root's
	// whole closure.
	if _, err := amberiroh.Receive(channels, st.Objects(), root, 0, mtr); err != nil {
		return key.Key{}, fmt.Errorf("amberclient: pull %q: %w", name, err)
	}
	if err := st.PutRef(ctx, name, root); err != nil {
		return key.Key{}, fmt.Errorf("amberclient: pull %q completed but writing local ref failed: %w", name, err)
	}
	c.log.Debug("amberclient pull complete", "ref", name, "root", root.String())
	return root, nil
}

// Refs lists every reference on the server.
func (c *Client) Refs(ctx context.Context) ([]RefInfo, error) {
	stream, stop, err := c.openStream(ctx)
	if err != nil {
		return nil, fmt.Errorf("amberclient: refs: %w", err)
	}
	defer stop()
	defer CloseStream(stream)

	if err := amberiroh.WriteMsg(stream, amberiroh.Msg{Type: amberiroh.TRefList}); err != nil {
		return nil, fmt.Errorf("amberclient: refs: %w", err)
	}
	m, err := amberiroh.ReadMsg(stream)
	if err != nil {
		return nil, fmt.Errorf("amberclient: refs: %w", err)
	}
	switch m.Type {
	case amberiroh.TRefs:
	case amberiroh.TErr:
		return nil, fmt.Errorf("amberclient: refs: %w", amberiroh.RemoteFromMsg(m))
	default:
		return nil, fmt.Errorf("amberclient: refs: %w: type %d, want TRefs", amberiroh.ErrProtocol, m.Type)
	}
	out := make([]RefInfo, 0, len(m.Refs))
	for _, r := range m.Refs {
		k, err := key.Parse(r.Key)
		if err != nil {
			return nil, fmt.Errorf("amberclient: refs: ref %q: key: %w", r.Name, err)
		}
		out = append(out, RefInfo{
			Name:      r.Name,
			Key:       k,
			CreatedAt: time.Unix(0, r.CreatedAt),
			User:      r.User,
		})
	}
	return out, nil
}

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

// openStream opens one operation stream and arms ctx cancellation: the
// protocol and wantsync loops speak plain io.ReadWriter, so cancellation is
// delivered by expiring the stream's deadline, which unblocks any pending
// read or write with an error. The returned stop must be called (deferred)
// to release the AfterFunc.
func (c *Client) openStream(ctx context.Context) (net.Conn, func(), error) {
	stream, err := c.conn.OpenStreamConn(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("open stream: %w", err)
	}
	stop := context.AfterFunc(ctx, func() { _ = stream.SetDeadline(time.Now()) })
	return stream, func() { stop() }, nil
}

// CloseStream fully terminates a request/response stream: Close finishes the
// send side (FIN), and CancelRead terminates the receive side, which these
// protocols never read to EOF — after the final expected frame both peers
// just stop reading. A QUIC stream is only retired (and its MAX_STREAMS
// credit returned by the peer) once BOTH directions terminate, so without
// the cancel every operation leaks a half-open stream and the peer's stream
// credit eventually runs dry — the 101st OpenStreamSync then blocks forever.
// Shared by every jobs-iroh client that speaks one-request-per-stream.
func CloseStream(stream net.Conn) {
	_ = stream.Close()
	if cr, ok := stream.(interface{ CancelRead(code uint64) }); ok {
		cr.CancelRead(0)
	}
}
