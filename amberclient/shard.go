package amberclient

// Sharded transfers, ported from amber-store-iroh cmd/amber (dial.go's
// attachExtras + push.go's runSenders): a transfer with Conns > 1 asks the
// server for extra data channels, attaches one stream per extra QUIC
// connection — each on its own endpoint, because an endpoint's single UDP
// socket loop caps throughput — and the want loop deals every round across
// all of them. A server without sharding support (no token in its reply)
// degrades to the single control stream, as do attach failures.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/amber-store/transport-iroh/amberiroh"
	"github.com/tmc/go-iroh/iroh"
	irohkey "github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"

	"github.com/jobs-build/jobs-iroh/amber"
)

// Attach timing. The server serves whatever shards arrived within its gather
// window (ambserver's attachWait, 10s; 5s on pre-DataEndpoints servers) —
// but a TAttach landing LATER is
// silently parked on a channel nothing ever reads. Handing such a dead
// channel to the want loop deadlocks a pull and fails a push whose ref the
// server already committed. So a shard's whole attach — dial, stream, TAttach
// frame — runs under a budget safely below that window, and a shard that
// misses it is closed, never used. (Residual risk needs one-way latency
// beyond seconds; links that slow rarely complete the dial inside the budget
// at all.)
const (
	// attachBudget bounds one shard's full attach.
	attachBudget = 3 * time.Second
	// dedicatedDialTimeout bounds the dial to an advertised data port. The
	// ports are kernel-assigned (rarely firewall-open) and a filtered UDP
	// port fails only by timeout, so the sub-budget keeps room for the
	// control-candidate fallback.
	dedicatedDialTimeout = 1500 * time.Millisecond
	// punchAttachBudget replaces attachBudget when the server advertised
	// DataEndpoints (which is also the signal that it gathers for 10s): a
	// punching attach pays a relay connect before its TAttach, and the
	// budget must still land safely inside the server's window.
	punchAttachBudget = 8 * time.Second
)

// dialShard opens one shard connection for pool slot i (the pool's
// dialer). A server that advertised DataEndpoints records gets a
// record-authenticated dial: the dedicated port (on the address the
// control connection reached) races the record's own candidates. On a
// discovery-dialed client the shard endpoint binds with the control dial's
// full stack — relay mode, net report, seeded local candidates — which is
// what feeds QNT punch coordination, so a relay-won shard upgrades to
// direct while pooled, exactly like the control connection did. A
// direct-addr client (Options.Addrs) stays bare, mirroring the control
// dial's own direct branch. Without records, the legacy path runs:
// dedicated ports under the server identity, then the control candidates.
// The returned teardown closes the connection AND its endpoint; the
// returned identity keys the pool entry.
func (c *Client) dialShard(ctx context.Context, i int, ports []uint16, eps []amberiroh.DataEndpointRec) (shardConn, func(), irohkey.EndpointID, error) {
	ctrlIP, haveCtrlIP := c.remoteIP()
	id, cands, ok, terr := shardTarget(i, ctrlIP, haveCtrlIP, ports, eps, !c.punchDials)
	if terr != nil {
		return nil, nil, irohkey.EndpointID{}, terr
	}
	teardown := func(conn *iroh.Conn, ep *iroh.Endpoint) func() {
		return func() {
			_ = conn.Close()
			_ = ep.Shutdown(context.Background())
		}
	}
	if ok {
		ep, err := c.bindShardEndpoint(ctx)
		if err != nil {
			return nil, nil, irohkey.EndpointID{}, err
		}
		conn, err := raceConnect(ctx, ep, id, c.alpn, cands)
		if err != nil {
			_ = ep.Shutdown(context.WithoutCancel(ctx))
			return nil, nil, irohkey.EndpointID{}, err
		}
		c.watchShardPath(conn, id)
		return conn, teardown(conn, ep), id, nil
	}

	var bindOpts []iroh.Option
	if c.bindAddr.IsValid() {
		// Same host as the pinned bind, port 0: every shard needs its own
		// socket.
		bindOpts = append(bindOpts, iroh.WithBindAddr(netip.AddrPortFrom(c.bindAddr.Addr(), 0)))
	}
	ep, err := iroh.Bind(ctx, bindOpts...)
	if err != nil {
		return nil, nil, irohkey.EndpointID{}, err
	}
	if len(ports) > 0 && haveCtrlIP {
		dctx, cancel := context.WithTimeout(ctx, dedicatedDialTimeout)
		cand := []netaddr.TransportAddr{netaddr.IPAddr{Addr: netip.AddrPortFrom(ctrlIP, ports[i%len(ports)])}}
		conn, derr := raceConnect(dctx, ep, c.id, c.alpn, cand)
		cancel()
		if derr == nil {
			c.watchShardPath(conn, c.id)
			return conn, teardown(conn, ep), c.id, nil
		}
	}
	conn, err := raceConnect(ctx, ep, c.id, c.alpn, c.cands)
	if err != nil {
		_ = ep.Shutdown(context.WithoutCancel(ctx))
		return nil, nil, irohkey.EndpointID{}, err
	}
	c.watchShardPath(conn, c.id)
	return conn, teardown(conn, ep), c.id, nil
}

// watchShardPath logs a shard connection's transport path and every later
// change — the per-shard analogue of the control links' reportPath. A shard
// commonly starts relayed and upgrades once hole punching lands; one that
// never does is what the pool's relayGrace eviction catches.
func (c *Client) watchShardPath(conn *iroh.Conn, id irohkey.EndpointID) {
	WatchPath(context.Background(), conn, func(p Path) {
		c.log.Info("amberclient: shard connection path",
			append([]any{"link", "store-shard", "endpoint", id.Short()}, p.LogAttrs()...)...)
	})
}

// bindShardEndpoint binds one record-authenticated shard's endpoint.
func (c *Client) bindShardEndpoint(ctx context.Context) (*iroh.Endpoint, error) {
	var opts []iroh.Option
	if c.bindAddr.IsValid() {
		opts = append(opts, iroh.WithBindAddr(netip.AddrPortFrom(c.bindAddr.Addr(), 0)))
	}
	if c.punchDials {
		// See bindAndResolve's discovery branch for why each piece exists;
		// mDNS/pkarr are omitted — the candidates are already provided.
		opts = append(opts, iroh.WithRelayMode(relay.ModeDefault()), iroh.WithNetReport())
	}
	ep, err := iroh.Bind(ctx, opts...)
	if err != nil {
		return nil, err
	}
	if c.punchDials {
		seedLocalCandidates(ep)
	}
	return ep, nil
}

// remoteIP is the address the control connection actually reached the
// server at.
func (c *Client) remoteIP() (netip.Addr, bool) {
	ua, ok := c.conn.RemoteAddr().(*net.UDPAddr)
	if !ok {
		return netip.Addr{}, false
	}
	ip, ok := netip.AddrFromSlice(ua.IP)
	if !ok {
		return netip.Addr{}, false
	}
	return ip.Unmap(), true
}

// attachExtras acquires up to n pooled shard connections, attaches one
// fresh stream per connection to the transfer token, and returns the
// attached streams with a closer. Connections belong to the pool and stay
// open across transfers — the closer closes only the streams, then
// releases the acquisition. Failures (or attaches that would miss the
// server's gather window — see attachBudget) reduce parallelism instead of
// failing the transfer. Every returned stream is armed with ctx
// cancellation exactly like the control stream, so a cancelled transfer
// unblocks all channels. When not a single shard attaches, the client
// demotes itself: this connection's topology evidently cannot shard
// (relay-only path, filtered ports), and paying the attach stall on every
// subsequent transfer would be pure waste.
//
// This is the OPTIMISTIC path — the first transfer on a connection, whose
// DataConns promise went out before the pool was consulted. Relayed pool
// entries are used too (directOnly=false): the server is waiting for
// exactly n shards, and a relayed shard beats a gather-window stall. Every
// later transfer reserves first (planTransfer) and attaches via
// attachReserved.
func (c *Client) attachExtras(ctx context.Context, token []byte, ports []uint16, eps []amberiroh.DataEndpointRec, n int) ([]net.Conn, func()) {
	if n <= 0 {
		return nil, func() {}
	}
	budget := attachBudget
	if len(eps) > 0 && c.punchDials {
		budget = punchAttachBudget
	}
	actx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	entries, fresh, _, release := c.pool.acquire(actx, n, false, ports, eps)
	streams, closers := c.attachTo(ctx, actx, token, entries, fresh)
	return streams, func() {
		for _, cl := range closers {
			cl()
		}
		release()
	}
}

// attachReserved attaches one fresh stream per already-reserved entry to
// the transfer token. The reservation was made before the request promised
// DataConns, so entries here are live, direct-path connections; a failure
// still only reduces parallelism. The reservation's release stays with the
// caller (pullOnce/pushOnce defer it) — the closer closes streams only.
func (c *Client) attachReserved(ctx context.Context, token []byte, rsv *reservation) ([]net.Conn, func()) {
	budget := attachBudget
	if c.punchDials {
		budget = punchAttachBudget
	}
	actx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	streams, closers := c.attachTo(ctx, actx, token, rsv.entries, rsv.fresh)
	return streams, func() {
		for _, cl := range closers {
			cl()
		}
	}
}

// attachTo opens one stream per entry and binds it to token under actx's
// budget, arming every surviving stream with ctx cancellation. Entries
// whose attach failed are discarded from the pool — a hung or failed
// stream open on a pooled conn means it went bad between transfers — and
// a zero-attach on a live context renders the sharding verdict: discard
// only when reused entries were involved (stale pool), sticky demote when
// even fresh dials could not attach (topology).
func (c *Client) attachTo(ctx, actx context.Context, token []byte, entries []*poolEntry, fresh map[*poolEntry]bool) ([]net.Conn, []func()) {
	slots := make([]net.Conn, len(entries))
	attachErrs := make([]error, len(entries))
	var wg sync.WaitGroup
	for ei, e := range entries {
		wg.Add(1)
		go func(ei int, e *poolEntry) {
			defer wg.Done()
			stream, err := e.conn.OpenStreamConn(actx)
			if err != nil {
				attachErrs[ei] = fmt.Errorf("open stream: %w", err)
				return
			}
			// The TAttach frame must land inside the budget too; once it is
			// out, the stream belongs to the whole transfer, so the write
			// deadline is lifted and the transfer ctx takes over.
			if dl, ok := actx.Deadline(); ok {
				_ = stream.SetWriteDeadline(dl)
			}
			err = amberiroh.WriteMsg(stream, amberiroh.Msg{Type: amberiroh.TAttach, Token: token})
			if err != nil || actx.Err() != nil {
				attachErrs[ei] = fmt.Errorf("write attach: %w", errors.Join(err, actx.Err()))
				CloseStream(stream)
				return
			}
			_ = stream.SetWriteDeadline(time.Time{})
			slots[ei] = stream
		}(ei, e)
	}
	wg.Wait()

	var failed []*poolEntry
	anyReused := false
	var streams []net.Conn
	var closers []func()
	for ei, e := range entries {
		if !fresh[e] {
			anyReused = true
		}
		if slots[ei] == nil {
			failed = append(failed, e)
			c.log.Warn("amberclient: shard attach failed",
				"shard", ei, "endpoint", e.id.Short(), "reused", !fresh[e], "error", attachErrs[ei])
			continue
		}
		stream := slots[ei]
		stop := context.AfterFunc(ctx, func() { _ = stream.SetDeadline(time.Now()) })
		streams = append(streams, stream)
		closers = append(closers, func() {
			stop()
			CloseStream(stream)
		})
	}
	c.pool.discard(failed...)
	if len(streams) > 0 {
		reusedOK := 0
		for ei, e := range entries {
			if slots[ei] != nil && !fresh[e] {
				reusedOK++
			}
		}
		c.log.Debug("amberclient: shards attached", "attached", len(streams), "reused", reusedOK, "failed", len(failed))
	}

	// A cancelled transfer proves nothing about the path (mirrors
	// retrySingle's guard) — only a live context's zero-attach does. And a
	// zero-attach involving REUSED entries is a stale-pool verdict, not a
	// topology one: the entries are discarded above and the next transfer
	// redials, so the sticky demote stays reserved for fresh dials failing.
	if len(entries) > 0 && len(streams) == 0 && ctx.Err() == nil {
		if anyReused {
			c.log.Warn("amberclient: pooled shard connections failed to attach; discarded for redial")
		} else {
			c.demote("no shard attached within budget")
		}
	}
	return streams, closers
}

// shardReadIdle bounds how long one Read on a pull's shard channel may sit
// without data. On pull the server answers each channel's TWants shard
// independently and promptly, so a healthy channel never idles long
// mid-round — but a shard whose TAttach landed just past the server's
// gather window (a transient delivery stall the local attach budget cannot
// see) is parked: nothing ever reads or answers it, and without a bound the
// want loop deadlocks. The watchdog turns that into a failed round, which
// retrySingle heals unsharded.
const shardReadIdle = 30 * time.Second

// watchdogConn arms a rolling read deadline on a pull shard channel.
type watchdogConn struct {
	net.Conn
	ctx context.Context
}

func (w *watchdogConn) Read(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	// Re-armed per read. This can overwrite the cancellation AfterFunc's
	// immediate deadline in a narrow race, delaying that one read's
	// unblock by at most shardReadIdle — bounded, and only on shards.
	_ = w.Conn.SetReadDeadline(time.Now().Add(shardReadIdle))
	return w.Conn.Read(p)
}

// runSenders drives a push's sending side. With one connection it is exactly
// the plain Send loop. With more, the first server frame decides: TAccept
// means a sharding-aware server — attach the extra connections and run one
// Send loop per channel; a TWants means a server that ignored the request's
// DataConns — replay the consumed frame in front of the stream and fall back
// to a single channel; a TErr surfaces as the usual remote error.
func (c *Client) runSenders(ctx context.Context, ctrl io.ReadWriter, st *amber.Store, prog amberiroh.Progress, conns int, rsv *reservation) error {
	if conns <= 1 {
		return amberiroh.Send(ctrl, st.Objects(), prog)
	}
	first, err := amberiroh.ReadMsg(ctrl)
	if err != nil {
		return err
	}
	switch first.Type {
	case amberiroh.TErr:
		return amberiroh.RemoteFromMsg(first)
	case amberiroh.TWants:
		var replay bytes.Buffer
		if err := amberiroh.WriteMsg(&replay, first); err != nil {
			return err
		}
		if rsv != nil {
			rsv.release() // server ignored DataConns; the reservation is unused
		}
		fallback := struct {
			io.Reader
			io.Writer
		}{io.MultiReader(&replay, ctrl), ctrl}
		return amberiroh.Send(fallback, st.Objects(), prog)
	case amberiroh.TAccept:
	default:
		return fmt.Errorf("%w: type %d, want TAccept or TWants", amberiroh.ErrProtocol, first.Type)
	}
	c.cacheDataEndpoints(first.DataPorts, first.DataEndpoints)

	var extras []net.Conn
	var closeExtras func()
	if rsv != nil {
		extras, closeExtras = c.attachReserved(ctx, first.Token, rsv)
	} else {
		extras, closeExtras = c.attachExtras(ctx, first.Token, first.DataPorts, first.DataEndpoints, conns-1)
	}
	defer closeExtras()
	// No read watchdog here, unlike pull: a push channel legitimately sits
	// in a read for as long as the server takes to verify and store a whole
	// round before its next TWants, which is unbounded. A parked channel's
	// Send unblocks at transfer end (the server closes ungathered streams)
	// and is tolerated below.
	channels := []io.ReadWriter{ctrl}
	for _, ex := range extras {
		channels = append(channels, ex)
	}
	errs := make([]error, len(channels))
	var wg sync.WaitGroup
	for i, ch := range channels {
		wg.Add(1)
		go func(i int, ch io.ReadWriter) {
			defer wg.Done()
			errs[i] = amberiroh.Send(ch, st.Objects(), prog)
		}(i, ch)
	}
	wg.Wait()
	if errs[0] != nil {
		return errors.Join(errs...)
	}
	if err := errors.Join(errs[1:]...); err != nil {
		// The control loop completed, so the server's commit verdict (read
		// next by the caller) is authoritative. A failed shard channel only
		// reduced parallelism: the server never depends on a channel it did
		// not gather, and a gathered channel that died fails the server's
		// OWN Receive — which surfaces as TErr on the control stream, not
		// here. (This is also what a server closing never-gathered late
		// shards at transfer end looks like: EOF after commit.)
		c.log.Debug("amberclient: shard channels errored after control completed", "error", err)
	}
	return nil
}
