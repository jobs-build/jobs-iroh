// Package serve wires the jobs-server process: one iroh endpoint whose ALPNs
// multiplex every service — the client build API, the runner NATS tunnel, the
// runner and admin CAS sync, and the admin API — over a single embedded
// amber store and a single embedded NATS server.
//
// The server is the only shared authority in jobs-iroh: runners and clients
// each own private stores and reach this one exclusively through the iroh
// protocols served here.
package serve

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/amber-store/transport-iroh/amberiroh"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
	"golang.org/x/sync/errgroup"

	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/bootstrap"
	"github.com/jobs-build/jobs-iroh/hostaddr"
	"github.com/jobs-build/jobs-iroh/natsiroh"
	"github.com/jobs-build/jobs-iroh/sched"
)

// The five service ALPNs of a jobs-server endpoint.
const (
	ALPNBuild = "jobs-build/1.0" // client: submit builds, watch progress
	// The runner-NATS ALPN is the FLEET FENCE (sibling-sources design §3.2
	// [INV]): the sibling-sources arc changed job semantics (KP-keyed
	// buildrun, ctx-widened defs) in ways an old runner would execute
	// silently WRONG (narrow F under a new K, F-keyed output refs the gate
	// now rejects). Runners pull work straight off JetStream queues, so a
	// polite version handshake could be ignored — bumping the ALPN makes an
	// old runner fail loudly at dial time instead. Bump it again whenever a
	// change would make an old runner produce wrong results rather than
	// clean errors. Bumped to 3.0 for source closures: an old runner's
	// recipe decoder silently ignores closure= and would fork
	// pin-cover/<v>:F content (source-closure design §7.2).
	ALPNRunnerNATS  = "jobs-runner-nats/3.0"
	ALPNRunnerAmber = "jobs-runner-amber/1.0" // runner: CAS object/ref sync
	ALPNAdmin       = "jobs-admin/1.0"        // admin TUI: observe builds, stats
	ALPNAmberAdmin  = "jobs-amber-admin/1.0"  // client: CAS ref sync (source up, outputs home)
)

// shutdownGrace bounds how long in-flight protocol handlers may run after
// shutdown begins.
const shutdownGrace = 10 * time.Second

// Options configures Run.
type Options struct {
	// DataDir holds everything the server owns: store/ (embedded amber
	// store), nats/ (JetStream state), server.key (endpoint identity).
	DataDir string
	// BindAddr optionally pins the UDP bind address (e.g. loopback in
	// tests). Zero value binds the default wildcard.
	BindAddr netip.AddrPort
	// Announce makes the endpoint discoverable (amber-serve's export
	// stack): connect to a relay (best-effort, bounded — an offline host
	// still starts), advertise direct addresses on every interface,
	// publish them over mDNS on the local link and via pkarr over the
	// internet. The jobs-server main sets it; loopback tests stay offline
	// with direct addresses only.
	Announce bool
	// AdvertiseAddrs are direct addresses to advertise, ip or ip:port
	// (bare IPs get the bound port). Non-empty replaces interface
	// auto-detection. Only meaningful with Announce.
	AdvertiseAddrs []string
	// RelayURL pins the relay fallback path; empty probes the built-in
	// relay map for the nearest one. Only meaningful with Announce.
	RelayURL string
	// DataEndpoints is how many extra UDP endpoints the amber mounts bind
	// for sharded transfers (0 = none, max 15): one socket's recv loop
	// caps well below a fast link, so shards get dedicated server sockets.
	// Each endpoint carries its own punchable identity; ports and identity
	// records are advertised in-band to sharding clients only — never
	// published. A client that cannot reach them falls back to the control
	// connection's addresses.
	DataEndpoints int
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
	// Logger receives server logs; nil means slog.Default().
	Logger *slog.Logger
	// Ready, when non-nil, is closed once the endpoint accepts connections.
	// The Server value is valid from that moment on.
	Ready func(*Server)
}

// Server exposes the running server's identity and internals to tests and to
// the admin surface.
type Server struct {
	Endpoint *iroh.Endpoint
	Store    *amber.Store
	NATS     *natsserver.Server
	Sched    *sched.Sched
}

// Run starts the server and blocks until ctx is cancelled, then shuts down
// gracefully: stop accepting, wait (bounded) for in-flight handlers, stop
// NATS, close the store.
func Run(ctx context.Context, opts Options) error {
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	if opts.DataDir == "" {
		return errors.New("serve: DataDir is required")
	}
	// Absolutize so logged paths and JetStream/store state never depend on
	// the launch cwd (mirrors runnerd/clientcli).
	if abs, err := filepath.Abs(opts.DataDir); err == nil {
		opts.DataDir = abs
	}
	if err := os.MkdirAll(opts.DataDir, 0o700); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	sk, err := loadOrCreateSecretKey(filepath.Join(opts.DataDir, "server.key"))
	if err != nil {
		return err
	}

	store, err := amber.Open(filepath.Join(opts.DataDir, "store"))
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

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
		if gcTestCapture != nil {
			gcTestCapture(gcr)
		}
	}

	// Self-seed the bootstrap floor (shell:<p>, fetcher:*:<p>) for every
	// embedded platform: runners own no seeds — they pull fetchers and the
	// shell from this store, so a fresh server must publish them before the
	// first job lands.
	if err := bootstrap.Seed(ctx, store, bootstrap.SeededPlatforms(), log.With("component", "bootstrap")); err != nil {
		return fmt.Errorf("seed bootstrap artifacts: %w", err)
	}

	ns, err := natsserver.NewServer(&natsserver.Options{
		ServerName: "jobs-server",
		DontListen: true,
		JetStream:  true,
		StoreDir:   filepath.Join(opts.DataDir, "nats"),
	})
	if err != nil {
		return fmt.Errorf("new nats server: %w", err)
	}
	ns.SetLoggerV2(newNATSLogger(log.With("component", "nats")), false, false, false)
	go ns.Start()
	defer ns.Shutdown()
	if !ns.ReadyForConnections(10 * time.Second) {
		return errors.New("embedded nats server not ready")
	}

	// The scheduler rides an in-process NATS connection — no TCP listener
	// exists; runners reach the same server through the iroh tunnel.
	nc, err := nats.Connect("", nats.InProcessServer(ns))
	if err != nil {
		return fmt.Errorf("in-process nats connect: %w", err)
	}
	defer nc.Close()

	schedOpts := sched.Options{
		Store: store,
		NC:    nc,
		Log:   log.With("component", "sched"),
	}
	if gcr != nil {
		schedOpts.Touch = gcr.sweeper.TouchAll
	}
	sd, err := sched.New(ctx, schedOpts)
	if err != nil {
		return fmt.Errorf("start scheduler: %w", err)
	}
	defer sd.Close()

	bindOpts := []iroh.Option{iroh.WithSecretKey(sk)}
	if opts.BindAddr.IsValid() {
		bindOpts = append(bindOpts, iroh.WithBindAddr(opts.BindAddr))
	}
	var relayMode relay.Mode
	if opts.Announce {
		relayMode, err = serverRelayMode(ctx, opts.RelayURL, log)
		if err != nil {
			return err
		}
		// Net reports are what discover this host's public mapping: a QAD probe
		// to a relay reports the address the outside world sees, which go-iroh
		// installs as an external candidate. Without it a NAT'd server knows
		// only its LAN addresses — it publishes a record no off-LAN peer can
		// dial, and advertises no QNT candidate for a peer to punch toward, so
		// every remote runner is stuck on the relay forever. Requires relays
		// (the probe target), hence inside the Announce branch.
		bindOpts = append(bindOpts, iroh.WithRelayMode(relayMode), iroh.WithNetReport())
	}
	ep, err := iroh.Bind(ctx, bindOpts...)
	if err != nil {
		return fmt.Errorf("bind endpoint: %w", err)
	}
	defer ep.Shutdown(context.WithoutCancel(ctx))

	if opts.Announce {
		pub, err := announce(ctx, ep, sk, opts.AdvertiseAddrs, log)
		if err != nil {
			return err
		}
		defer pub.Close()
	}

	amberSrv := amberiroh.New(log.With("component", "amber"), store.Objects(), store.RefStore())
	if gcr != nil {
		amberSrv.SetOnAccess(gcr.sweeper.Touch)
		amberSrv.SetOnPin(gcr.sweeper.MarkPinned)
		amberSrv.SetRefGuard(gcr.sweeper.Guard())
	}

	// Extra data endpoints for sharded transfers. Each carries only the two
	// amber ALPNs; TAttach reaches the one amberSrv (and its transfer
	// tokens) from any of them.
	var dataRouters []*iroh.Router
	if n := min(opts.DataEndpoints, 15); n > 0 {
		// Bind every endpoint and publish ports + records BEFORE any router
		// accepts: SetDataPorts/SetDataEndpoints are unsynchronized writes
		// the handlers read (ambserver documents "call before Serve").
		//
		// Each data endpoint carries its OWN identity: a punchable endpoint
		// needs its own relay home connection and QAD-discovered mapping,
		// and relays key sessions by endpoint ID — sharing the server's key
		// would clash with the main endpoint's session. Shard dials
		// authenticate the identity advertised on the control connection
		// (amberiroh.DataEndpointRec). One consequence for OLD clients:
		// their dedicated-port dial expects the server's identity, fails the
		// handshake against these endpoints, and falls back to the control
		// candidates — they still shard, onto the main socket.
		var deps []*iroh.Endpoint
		var seeded [][]netip.AddrPort
		var ports []uint16
		for range n {
			var depOpts []iroh.Option
			if opts.BindAddr.IsValid() {
				depOpts = append(depOpts, iroh.WithBindAddr(netip.AddrPortFrom(opts.BindAddr.Addr(), 0)))
			}
			if opts.Announce {
				// Same rationale as the main endpoint's branch above: the
				// relay is the punch coordination channel and the QAD probe
				// target; without Announce there are no relays to probe.
				depOpts = append(depOpts, iroh.WithRelayMode(relayMode), iroh.WithNetReport())
			}
			dep, err := iroh.Bind(ctx, depOpts...)
			if err != nil {
				return fmt.Errorf("bind data endpoint: %w", err)
			}
			defer dep.Shutdown(context.WithoutCancel(ctx))
			if opts.Announce {
				go func(dep *iroh.Endpoint) {
					octx, cancel := context.WithTimeout(ctx, onlineTimeout)
					defer cancel()
					if err := dep.Online(octx); err != nil {
						log.Warn("data endpoint relay connect failed; its punch candidates stay direct-only", "error", err)
					}
				}(dep)
			}
			deps = append(deps, dep)
			seeded = append(seeded, seedDataEndpointAddrs(dep))
			ports = append(ports, dep.LocalAddr().Port())
			// Log every address-set change: whether a data endpoint ever
			// learned its public QAD mapping decides whether off-LAN shards
			// can punch to it — this line is the observable for that.
			go func(port uint16, dep *iroh.Endpoint) {
				for a := range dep.WatchAddr().Stream(ctx) {
					log.Info("data endpoint advertised", "port", port, "addr", a)
				}
			}(dep.LocalAddr().Port(), dep)
		}
		amberSrv.SetDataPorts(ports)
		amberSrv.SetDataEndpoints(func() []amberiroh.DataEndpointRec {
			recs := make([]amberiroh.DataEndpointRec, len(deps))
			for i, dep := range deps {
				recs[i] = dataEndpointRec(dep, seeded[i])
			}
			return recs
		})
		amberOnly := map[string]iroh.ProtocolHandler{
			ALPNRunnerAmber: logConns(log, "runner-amber-data", amberConnHandler(amberSrv)),
			ALPNAmberAdmin:  logConns(log, "amber-admin-data", amberConnHandler(amberSrv)),
		}
		for _, dep := range deps {
			dr, err := iroh.NewRouter(dep, amberOnly, &iroh.RouterConfig{Logger: log})
			if err != nil {
				return fmt.Errorf("data endpoint router: %w", err)
			}
			dataRouters = append(dataRouters, dr)
		}
		log.Info("amber data endpoints", "ports", ports)
	}

	storeDir := filepath.Join(opts.DataDir, "store")
	buildSvc := &apiService{log: log.With("service", "build"), sd: sd, store: store, storeDir: storeDir, gc: gcr}
	adminSvc := &apiService{log: log.With("service", "admin"), sd: sd, store: store, storeDir: storeDir, gc: gcr, admin: true}

	handlers := map[string]iroh.ProtocolHandler{
		ALPNRunnerNATS:  logConns(log, "runner-nats", natsiroh.Handler(ns)),
		ALPNRunnerAmber: logConns(log, "runner-amber", amberConnHandler(amberSrv)),
		ALPNAmberAdmin:  logConns(log, "amber-admin", amberConnHandler(amberSrv)),
		ALPNBuild:       logConns(log, "build", buildSvc.handler()),
		ALPNAdmin:       logConns(log, "admin", adminSvc.handler()),
	}
	router, err := iroh.NewRouter(ep, handlers, &iroh.RouterConfig{Logger: log})
	if err != nil {
		return fmt.Errorf("new router: %w", err)
	}

	log.Info("jobs-server listening",
		"endpoint", ep.ID().String(),
		"addr", ep.LocalAddr(),
		"data-dir", opts.DataDir,
	)
	if opts.Ready != nil {
		opts.Ready(&Server{Endpoint: ep, Store: store, NATS: ns, Sched: sd})
	}
	if gcr != nil {
		gcr.start(ctx)
	}

	<-ctx.Done()
	log.Info("shutting down")
	shCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownGrace)
	defer cancel()
	if err := router.Shutdown(shCtx); err != nil {
		log.Warn("router shutdown", "error", err)
	}
	for _, dr := range dataRouters {
		if err := dr.Shutdown(shCtx); err != nil {
			log.Warn("data router shutdown", "error", err)
		}
	}
	return nil
}

// closeTracked wraps a stream so the handler can tell, after HandleStream
// returns, whether the operation finished with the stream (closed) or handed
// it to an in-progress sharded transfer (still open — the transfer owns it).
type closeTracked struct {
	net.Conn
	closed atomic.Bool
}

func (c *closeTracked) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

// amberConnHandler serves the amberiroh sync protocol on every stream of a
// connection. This is why amberiroh carries no accept loop of its own: the
// router owns connection dispatch, so only the per-connection half is needed.
func amberConnHandler(srv *amberiroh.Server) iroh.ProtocolHandler {
	return iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
		g, ctx := errgroup.WithContext(ctx)
		// Closing the connection on ctx cancel unblocks in-flight handlers.
		stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
		defer stop()
		for {
			stream, err := conn.AcceptStreamConn(ctx)
			if err != nil {
				break // peer closed the connection, or ctx cancelled
			}
			g.Go(func() error {
				tracked := &closeTracked{Conn: stream}
				srv.HandleStream(conn.RemoteID().String(), tracked)
				// HandleStream closes only the send side; the sync protocol
				// never reads the client's FIN after the final frame. Cancel
				// the receive side so the stream fully terminates — QUIC
				// returns MAX_STREAMS credit to the client only for fully
				// closed streams, and without this every operation leaks one
				// stream until the client's 101st open blocks forever.
				//
				// EXCEPT when HandleStream returned without closing: a
				// TAttach stream changed hands to an in-progress sharded
				// transfer, which owns and closes it — canceling here would
				// sever a live data channel mid-transfer. (Attach streams
				// leak no credit either way: the client tears down their
				// whole per-shard connection when the transfer ends.)
				if tracked.closed.Load() {
					if cr, ok := stream.(interface{ CancelRead(code uint64) }); ok {
						cr.CancelRead(0)
					}
				}
				return nil
			})
		}
		return g.Wait()
	})
}

// logConns wraps a handler with connect/disconnect logging.
func logConns(log *slog.Logger, service string, h iroh.ProtocolHandler) iroh.ProtocolHandler {
	return iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
		l := log.With("service", service, "remote", conn.RemoteID().String())
		l.Info("connected")
		err := h.Accept(ctx, conn)
		l.Info("disconnected", "error", err)
		return err
	})
}

// dataEndpointRec snapshots one data endpoint's identity and live dial
// candidates for a TAccept/TRef frame — read per frame, so the relay and
// QAD candidates appear as soon as the endpoint learns them. The seeded
// interface addresses are unioned in explicitly: they are what a LAN client
// races and what a punching peer aims at first.
func dataEndpointRec(dep *iroh.Endpoint, seeded []netip.AddrPort) amberiroh.DataEndpointRec {
	id := dep.ID().Bytes()
	rec := amberiroh.DataEndpointRec{ID: id[:]}
	seen := map[string]bool{}
	add := func(s string) {
		if !seen[s] {
			seen[s] = true
			rec.Addrs = append(rec.Addrs, s)
		}
	}
	for _, ta := range dep.Addr().Addrs() {
		add(ta.String())
	}
	for _, ap := range seeded {
		add(netaddr.IPAddr{Addr: ap}.String())
	}
	return rec
}

// seedDataEndpointAddrs mirrors announce's direct-address seeding for one
// data endpoint: interface addresses on its own port become dial and QNT
// punch candidates. Best-effort — a failed walk leaves the endpoint bare,
// like the client's seedLocalCandidates.
func seedDataEndpointAddrs(dep *iroh.Endpoint) []netip.AddrPort {
	addrs, err := hostaddr.LocalAddrPorts(dep.LocalAddr().Port())
	if err != nil {
		return nil
	}
	for _, ap := range addrs {
		dep.AddExternalAddr(ap)
	}
	return addrs
}
