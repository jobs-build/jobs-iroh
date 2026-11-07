// Package runnerd is the jobs-runner daemon loop (design 2026-07-23 §3): it
// owns a private amber store as materialization cache, reaches the server
// over two iroh ALPNs (the NATS tunnel for scheduling, the amber sync for
// objects), announces itself on runners.hello, and serves every work-queue
// class that fits inside its size with an admission-gated pull loop.
//
// Per job: pull the input closures, land the in-band def, run the real stage
// driver, push the outputs under ONE scratch ref, publish the wire.Result,
// ack. The server gates the proposed refs and owns all retry policy — a
// failed attempt here is reported and acked, never re-run locally.
//
// Scratch-push decision: the attempt pushes a single
// runner-push/<runner-id>/<node>-<gen> ref whose value is a BuildStoreTree
// union of every proposed root (entries named by key hex, shared subtrees
// dedup'd). One push therefore covers ALL proposed keys' closures — the
// server's per-key CheckComplete finds every object below the scratch root —
// and the runner never writes real result-ref names into the server store
// (the gate stays the only path to those). The server deletes the scratch
// ref after commit. The alternative (pushing each produced ref name
// directly) was rejected: it would bypass the gate's allow-table by design.
package runnerd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/tmc/go-iroh/iroh"
	"golang.org/x/sync/errgroup"

	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/amberclient"
	"github.com/jobs-build/jobs-iroh/events"
	"github.com/jobs-build/jobs-iroh/gcsweep"
	"github.com/jobs-build/jobs-iroh/natsiroh"
	"github.com/jobs-build/jobs-iroh/resources"
	"github.com/jobs-build/jobs-iroh/runner"
	"github.com/jobs-build/jobs-iroh/sandbox"
	"github.com/jobs-build/jobs-iroh/sched"
	"github.com/jobs-build/jobs-iroh/wire"
)

// The runner-facing server ALPNs (frozen contract — serve/ mounts these).
const (
	alpnRunnerNATS  = "jobs-runner-nats/4.0"
	alpnRunnerAmber = "jobs-runner-amber/1.0"
)

// Options configures Run.
type Options struct {
	// ServerID is the server's iroh endpoint ID (required).
	ServerID string
	// Addrs are optional direct server addresses (host:port, repeatable);
	// set, they skip discovery and relays entirely.
	Addrs []string
	// DataDir holds everything the runner owns: store/ (private cache
	// store), cache/ (fetcher cache), runner-id. Required.
	DataDir string
	// Size optionally caps the runner's capacity to a ladder rung
	// (wire.Classes); empty auto-detects the machine's capacity
	// (cgroup-aware, reserve-adjusted) and uses all of it.
	Size string
	// Name is the display name announced in hello; empty uses the hostname.
	Name string
	// Platform is the jobs platform string (linux/arm64 form); empty uses
	// the runtime's GOOS/GOARCH.
	Platform string
	// Slots optionally caps concurrent jobs regardless of resource fit
	// (0 = resource accounting only).
	Slots int
	// CPU and Memory optionally cap the advertised capacity ("4", "2000m" /
	// "28Gi", "16G"), applied verbatim over the auto-detected value (no
	// reserve is subtracted). Either may be empty. Ignored when Size is set.
	CPU    string
	Memory string
	// SkipSelfTest disables the boot self-test build that gates serving on a
	// working local sandbox (bootSelfTest). Escape hatch only — a runner that
	// cannot sandbox-exec hard-fails every job it is handed.
	SkipSelfTest bool
	// SyncConns is the QUIC connections per store transfer (control +
	// data shards; see amberclient.Options.Conns). 0 uses the amberclient
	// default, 1 disables sharding.
	SyncConns int
	// GCRetention enables local-store GC: refs unread for this long are
	// deleted and the store mark-sweeps; everything here is a cache of the
	// server, so a wrong expiry costs one re-pull. 0 disables GC.
	GCRetention time.Duration
	// GCInterval is the sweep period (default 1h when GC is enabled).
	GCInterval time.Duration
	// GCRate caps the GC copier in bytes/s (0 = unlimited). Compaction
	// holds local ref publication for its duration.
	GCRate int64
	// GCMinFree is the free-space floor in bytes under which the collector
	// reaps more aggressively (0 = 5% of the filesystem).
	GCMinFree uint64
	// Logger receives daemon logs; nil means slog.Default().
	Logger *slog.Logger
	// BindAddr optionally pins the UDP bind address (e.g. loopback in
	// tests). Zero value binds the default wildcard.
	BindAddr netip.AddrPort
}

// Tunables (spec §2/§3). Interval fields live on the daemon so tests can
// shrink them; the lane consumer config comes from sched.LaneConsumerConfig
// (shared with the server — config drift would break the shared durable).
const (
	msgHeartbeatEvery    = 5 * time.Second
	runnerHeartbeatEvery = 5 * time.Second
	defaultPollInterval  = 500 * time.Millisecond
	defaultPublishWait   = 30 * time.Second
	ensureRetryEvery     = 2 * time.Second
)

// Run starts the runner daemon and blocks until ctx is cancelled. In-flight
// jobs then publish cancelled results and nak their messages back to the
// queue before the daemon returns.
func Run(ctx context.Context, o Options) error {
	log := o.Logger
	if log == nil {
		log = slog.Default()
	}
	if o.ServerID == "" {
		return errors.New("runnerd: ServerID is required")
	}
	if o.DataDir == "" {
		return errors.New("runnerd: DataDir is required")
	}
	// Absolutize: cache paths derived from DataDir end up as sandbox exec
	// targets, and the sandbox child chdirs before exec'ing — a cwd-relative
	// command path would resolve against the job's working dir and ENOENT.
	if abs, err := filepath.Abs(o.DataDir); err == nil {
		o.DataDir = abs
	}
	if err := os.MkdirAll(o.DataDir, 0o700); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	cacheDir := filepath.Join(o.DataDir, "cache")
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		return fmt.Errorf("create cache dir: %w", err)
	}

	name := o.Name
	if name == "" {
		if h, err := os.Hostname(); err == nil {
			name = h
		} else {
			name = "jobs-runner"
		}
	}
	platform := o.Platform
	if platform == "" {
		platform = runner.Platform()
	}
	capacity, err := resolveCapacity(o.Size, o.CPU, o.Memory, log)
	if err != nil {
		return err
	}

	id, err := ensureRunnerID(o.DataDir)
	if err != nil {
		return err
	}
	log = log.With("runner", id)

	// Park in a cgroup leaf holder so job cgroups can get memory metering
	// and limits (cgroup v2's no-internal-process rule; jobs' runner.Run).
	if parked, err := sandbox.EnsureCgroupLeafHolder(); err != nil {
		log.Warn("cgroup leaf-holder setup failed; job memory metering and limits unavailable", "error", err)
	} else if parked {
		log.Info("parked in cgroup leaf holder; job cgroups get memory metering and limits")
	}

	st, err := amber.Open(filepath.Join(o.DataDir, "store"))
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer st.Close()

	// Work trees go under the data dir, not the OS temp dir — /tmp is a
	// RAM-backed tmpfs on many hosts (NixOS sizes it at 50% of RAM), where
	// one multi-GB build tree starves the builds' own memory and ENOSPCs
	// long before the disk fills. Placed after amber.Open: the store lock
	// guarantees one daemon per data dir, so the boot sweep can never
	// remove a live daemon's trees.
	work, err := initWorkDir(o.DataDir)
	if err != nil {
		return err
	}
	log.Info("work trees under data dir", "dir", work)

	// Prove the host can execute a sandboxed build before dialing anything —
	// a runner that would fail every job must never announce capacity.
	if o.SkipSelfTest {
		log.Warn("boot self-test skipped by configuration")
	} else if err := bootSelfTest(ctx, st, platform, cacheDir, log); err != nil {
		if ctx.Err() != nil {
			return nil // interrupted mid-test: a shutdown, not a verdict
		}
		return err
	}

	if o.GCRetention > 0 {
		if o.GCInterval <= 0 {
			o.GCInterval = time.Hour
		}
		gcw, err := gcsweep.New(log.With("component", "gc"), st, gcsweep.Options{
			StoreDir:     filepath.Join(o.DataDir, "store"),
			SnapshotPath: filepath.Join(o.DataDir, "refaccess.cbor"),
			CacheDir:     cacheDir,
			Retention:    o.GCRetention,
			Interval:     o.GCInterval,
			Rate:         o.GCRate,
			MinFree:      o.GCMinFree,
		})
		if err != nil {
			return fmt.Errorf("open gc: %w", err)
		}
		// Declared after st's defer, so LIFO closes it first — the sweeper
		// must stop touching the store before the store itself closes.
		defer gcw.Close()
		gcw.Start(ctx)
	}

	// Both connections dial the same server endpoint; each owns its own
	// client endpoint (amberclient's one-connection-one-endpoint model — a
	// single UDP socket loop would cap combined throughput anyway). A pinned
	// BindAddr must therefore use port 0 so the two binds don't collide.
	baseOpts := amberclient.Options{
		EndpointID: o.ServerID,
		Addrs:      o.Addrs,
		BindAddr:   o.BindAddr,
		Logger:     log.With("component", "amber-sync"),
	}

	amberOpts := baseOpts
	amberOpts.ALPN = alpnRunnerAmber
	amberOpts.Conns = o.SyncConns
	sync := newReconnSync(amberOpts, st)
	defer sync.Close()

	natsOpts := baseOpts
	natsOpts.ALPN = alpnRunnerNATS
	natsOpts.Logger = log.With("component", "nats-tunnel")
	src, closeSrc := natsConnSource(natsOpts)
	defer closeSrc()

	helloCh := make(chan struct{}, 1)
	signalHello := func(*nats.Conn) {
		select {
		case helloCh <- struct{}{}:
		default:
		}
	}
	nc, err := natsiroh.Connect(src,
		nats.Name("jobs-runner-"+id),
		nats.ConnectHandler(signalHello),
		nats.ReconnectHandler(signalHello),
	)
	if err != nil {
		return fmt.Errorf("nats connect: %w", err)
	}
	defer nc.Close()

	d, err := newDaemon(daemonConfig{
		Log:      log,
		Store:    st,
		NC:       nc,
		Sync:     sync,
		ID:       id,
		Name:     name,
		Platform: platform,
		Capacity: capacity,
		Slots:    o.Slots,
		CacheDir: cacheDir,
	})
	if err != nil {
		return err
	}
	d.helloCh = helloCh
	if nc.IsConnected() {
		signalHello(nc)
	}
	sync.Warm(ctx)

	log.Info("jobs-runner starting",
		"name", name, "platform", platform, "capacity", capacity.String(),
		"size", d.size, "classes", d.classes, "server", o.ServerID)

	return d.run(ctx)
}

// natsConnSource yields the iroh connection for the NATS tunnel, redialing
// (fresh endpoint + connection via amberclient.DialConn) whenever the cached
// connection has died — the NATS client's own reconnect loop calls it once
// per attempt. The returned closer tears down whatever is currently cached.
func natsConnSource(opts amberclient.Options) (natsiroh.ConnSource, func()) {
	var mu sync.Mutex
	var cur *iroh.Conn
	var curEP *iroh.Endpoint
	closeCurLocked := func() {
		if cur != nil {
			_ = cur.Close()
			cur = nil
		}
		if curEP != nil {
			_ = curEP.Shutdown(context.Background())
			curEP = nil
		}
	}
	src := func(ctx context.Context) (*iroh.Conn, error) {
		mu.Lock()
		defer mu.Unlock()
		if cur != nil && cur.Context().Err() == nil {
			return cur, nil
		}
		closeCurLocked()
		conn, ep, err := amberclient.DialConn(ctx, opts)
		if err != nil {
			return nil, err
		}
		// WithoutCancel: this ctx bounds one dial attempt (5s), while a
		// relay→direct upgrade lands after it — the watcher's real lifetime
		// bound is the connection, which closeCurLocked ends.
		amberclient.WatchPath(context.WithoutCancel(ctx), conn, reportPath(opts.Logger, "scheduling"))
		cur, curEP = conn, ep
		return conn, nil
	}
	return src, func() {
		mu.Lock()
		defer mu.Unlock()
		closeCurLocked()
	}
}

// reportPath builds the path reporter for one of the runner's two server
// connections (link is what it carries: "store" or "scheduling"). Whether
// the path is direct or relayed is the single biggest predictor of store
// throughput — a relayed connection funnels every CAS byte through a third
// party and is also what makes sharded transfers demote themselves — so a
// relay is reported as a warning, and every later change (hole punching
// landing, or a fall back to the relay) is reported too.
func reportPath(log *slog.Logger, link string) func(amberclient.Path) {
	if log == nil {
		log = slog.Default()
	}
	return func(p amberclient.Path) {
		attrs := append([]any{"link", link}, p.LogAttrs()...)
		if p.Relayed {
			log.Warn("server connection goes through a relay — no direct path; store transfers will be slower", attrs...)
			return
		}
		log.Info("server connection is direct", attrs...)
	}
}

// daemonConfig wires a daemon; split from Options so tests can inject an
// in-process NATS conn and a fake sync client (no iroh involved).
type daemonConfig struct {
	Log      *slog.Logger
	Store    *amber.Store
	NC       *nats.Conn
	Sync     syncClient
	ID       string
	Name     string
	Platform string
	Capacity resources.Resources
	Slots    int
	CacheDir string
}

type daemon struct {
	log      *slog.Logger
	st       *amber.Store
	nc       *nats.Conn
	js       jetstream.JetStream
	sync     syncClient
	id       string
	name     string
	platform string
	capacity resources.Resources
	size     wire.Class // display label: the biggest job class accepted
	classes  []wire.Class
	adm      *admission
	cacheDir string

	// execute runs one attempt's stage driver; tests stub it.
	execute func(ctx context.Context, job wire.Job, rw runner.RefWriter, ev *events.Job) runner.Outcome

	helloCh chan struct{}
	jobs    sync.WaitGroup

	msgHBInterval  time.Duration
	hbInterval     time.Duration
	pollInterval   time.Duration
	publishTimeout time.Duration
}

func newDaemon(cfg daemonConfig) (*daemon, error) {
	classes := wire.ClassesWithin(cfg.Capacity)
	if len(classes) == 0 {
		return nil, fmt.Errorf("capacity %s fits no ladder class (smallest is %s)",
			cfg.Capacity, wire.Classes[0])
	}
	js, err := jetstream.New(cfg.NC)
	if err != nil {
		return nil, fmt.Errorf("jetstream: %w", err)
	}
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}
	d := &daemon{
		log:            log,
		st:             cfg.Store,
		nc:             cfg.NC,
		js:             js,
		sync:           cfg.Sync,
		id:             cfg.ID,
		name:           cfg.Name,
		platform:       cfg.Platform,
		capacity:       cfg.Capacity,
		size:           classes[len(classes)-1],
		classes:        classes,
		adm:            newAdmission(cfg.Capacity, cfg.Slots),
		cacheDir:       cfg.CacheDir,
		helloCh:        make(chan struct{}, 1),
		msgHBInterval:  msgHeartbeatEvery,
		hbInterval:     runnerHeartbeatEvery,
		pollInterval:   defaultPollInterval,
		publishTimeout: defaultPublishWait,
	}
	d.execute = d.driveStage
	return d, nil
}

// run drives the lanes and the heartbeat until ctx ends, then waits for the
// in-flight jobs to report their cancelled results.
func (d *daemon) run(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return d.heartbeatLoop(gctx) })
	g.Go(func() error { return d.fetchLoop(gctx) })
	err := g.Wait()
	d.jobs.Wait()
	if errors.Is(err, context.Canceled) {
		err = nil
	}
	return err
}

// heartbeatLoop announces hello on every (re)connect and publishes the
// liveness+capacity heartbeat every 5s.
func (d *daemon) heartbeatLoop(ctx context.Context) error {
	t := time.NewTicker(d.hbInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-d.helloCh:
			d.publishHello()
		case <-t.C:
			d.publishHeartbeat()
		}
	}
}

func (d *daemon) publishHello() {
	b := wire.MustEncode(wire.Hello{
		ID:       d.id,
		Name:     d.name,
		Platform: d.platform,
		Size:     string(d.size),
		CPUMilli: d.capacity.CPUMilli,
		MemBytes: d.capacity.MemBytes,
	})
	if err := d.nc.Publish(wire.SubjectRunnerHello, b); err != nil {
		d.log.Warn("publish hello failed", "error", err)
	} else {
		d.log.Info("announced runner", "size", d.size, "platform", d.platform)
	}
}

func (d *daemon) publishHeartbeat() {
	free, inflight := d.adm.Snapshot()
	b := wire.MustEncode(wire.Heartbeat{
		ID:       d.id,
		InFlight: inflight,
		FreeCPU:  free.CPUMilli,
		FreeMem:  free.MemBytes,
	})
	if err := d.nc.Publish(wire.RunnerHBSubject(d.id), b); err != nil {
		d.log.Debug("publish heartbeat failed", "error", err)
	}
}

// fetchLoop drives every class lane from ONE sweeper goroutine: each sweep
// walks the classes largest-first, and for every class whose rung still fits
// into free capacity it reserves the rung FIRST and only then asks the
// lane's shared consumer for one message (no-wait) — a pulled message always
// has room to run, and an empty lane releases immediately. An idle sweep
// (nothing dispatched anywhere) sleeps one poll interval.
//
// Single-sweeper on purpose: concurrent blocking per-lane loops starve big
// rungs — a small rung's reserve→fetch→release cycle re-takes the freed
// capacity long before a bigger rung's waiter can win it. One sweeper visits
// every lane in a deterministic order instead; largest-first gives the jobs
// that need the most contiguous free capacity first claim, with the smaller
// rungs filling what remains.
func (d *daemon) fetchLoop(ctx context.Context) error {
	consumers := make(map[wire.Class]jetstream.Consumer, len(d.classes))
	for _, class := range d.classes {
		cons, err := d.ensureConsumer(ctx, class)
		if err != nil {
			return err
		}
		consumers[class] = cons
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		dispatched := false
		for i := len(d.classes) - 1; i >= 0; i-- {
			class := d.classes[i]
			laneID := "lane/" + string(class)
			if !d.adm.TryAcquire(laneID, class.Resources()) {
				continue
			}
			msgs, err := consumers[class].FetchNoWait(1)
			if err != nil {
				d.adm.Release(laneID)
				if ctx.Err() == nil {
					d.log.Debug("fetch failed", "class", class, "error", err)
				}
				continue
			}
			claimed := false
			for msg := range msgs.Messages() {
				if !claimed {
					claimed = d.dispatch(ctx, msg, laneID)
				} else {
					// Batch size is 1; anything more is unexpected — hand it back.
					_ = msg.Nak()
				}
			}
			if !claimed {
				d.adm.Release(laneID)
			} else {
				dispatched = true
			}
			if err := msgs.Error(); err != nil && ctx.Err() == nil {
				d.log.Debug("fetch batch error", "class", class, "error", err)
			}
		}
		if !dispatched {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(d.pollInterval):
			}
		}
	}
}

// dispatch decodes one fetched message and hands it to a job goroutine,
// transferring the lane's reservation onto the job. It reports whether the
// reservation was consumed.
func (d *daemon) dispatch(ctx context.Context, msg jetstream.Msg, laneID string) bool {
	var job wire.Job
	if err := wire.Decode(msg.Data(), &job); err != nil {
		d.log.Error("undecodable job message; terminating it", "subject", msg.Subject(), "error", err)
		_ = msg.Term()
		return false
	}
	// Keyed by (node, gen), NOT node alone: only a redelivery of the very
	// attempt already in flight here is a duplicate worth parking. A
	// DIFFERENT gen of the same node (the server re-enqueued after our own
	// cancelled/nak, and the nak'd old message redelivered too) must be
	// allowed to run — parking it in a nak loop burns one MaxDeliver
	// delivery per nak, and a long-running stale attempt would poison the
	// CURRENT gen's message right out of the work queue while its own
	// result gets dropped server-side as stale. Running both gens is merely
	// wasteful, never wrong: the server commits the current gen and drops
	// the other.
	jobID := fmt.Sprintf("%s%s-%d", jobIDPrefix, job.Node, job.Gen)
	if !d.adm.Rename(laneID, jobID) {
		// This exact attempt is already running here — a redelivery raced
		// our own in-flight execution (lapsed ack window). Delay it back
		// onto the queue; the in-flight attempt's result-then-ack settles it.
		d.log.Info("duplicate attempt delivery; nak with delay", "node", job.Node, "gen", job.Gen)
		_ = msg.NakWithDelay(10 * time.Second)
		return false
	}
	d.jobs.Add(1)
	go func() {
		defer d.jobs.Done()
		defer d.adm.Release(jobID)
		d.handleJob(ctx, msg, job)
	}()
	return true
}

// ensureConsumer binds (creating if needed) the shared durable consumer for
// one class lane, retrying until the server has created the JOBS stream —
// stream configuration is the server's job; runners only ensure consumers,
// with sched's exported lane config so every ensure (server or any runner)
// is byte-identical and therefore idempotent.
func (d *daemon) ensureConsumer(ctx context.Context, class wire.Class) (jetstream.Consumer, error) {
	cfg := sched.LaneConsumerConfig(d.platform, class)
	for {
		cons, err := d.js.CreateOrUpdateConsumer(ctx, wire.StreamJobs, cfg)
		if err == nil {
			return cons, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		d.log.Debug("ensure consumer failed; retrying", "class", class, "error", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(ensureRetryEvery):
		}
	}
}

// publishResult publishes one terminal result with its dedup message ID,
// retrying transient failures within the caller's deadline (the RESULTS
// stream may be a beat behind the runner at first boot).
func (d *daemon) publishResult(ctx context.Context, res wire.Result) error {
	msg := &nats.Msg{
		Subject: wire.ResultsSubject(res.Node),
		Data:    wire.MustEncode(res),
	}
	id := wire.ResultMsgID(res.Node, res.Gen)
	for {
		_, err := d.js.PublishMsg(ctx, msg, jetstream.WithMsgID(id))
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		d.log.Debug("publish result failed; retrying", "node", res.Node, "error", err)
		select {
		case <-ctx.Done():
			return err
		case <-time.After(time.Second):
		}
	}
}

// resolveCapacity resolves the runner's admission capacity: an explicit
// --size must be a ladder rung and caps the runner to exactly that rung's
// resources; otherwise capacity is auto-detected (runner.DetectCapacity —
// cgroup-aware, reserve-adjusted) and used in FULL, with --cpu / --memory
// (JOBS_RUNNER_CPU / JOBS_RUNNER_MEM) overriding either dimension verbatim.
// The ladder classifies jobs, not runners — flooring a 32-core box to the
// top rung would waste everything above it. DetectCapacity floors its result
// to one default build slot, so an auto-detected capacity always fits at
// least c1-m1.
func resolveCapacity(size, cpu, mem string, log *slog.Logger) (resources.Resources, error) {
	if size != "" {
		c, err := wire.ParseClass(size)
		if err != nil {
			return resources.Resources{}, err
		}
		return c.Resources(), nil
	}
	if cpu != "" {
		if _, err := resources.ParseCPU(cpu); err != nil {
			return resources.Resources{}, fmt.Errorf("--cpu %q: %w", cpu, err)
		}
	}
	if mem != "" {
		if _, err := resources.ParseMem(mem); err != nil {
			return resources.Resources{}, fmt.Errorf("--memory %q: %w", mem, err)
		}
	}
	return runner.DetectCapacity(cpu, mem, log), nil
}

// initWorkDir sweeps and recreates <dataDir>/work and points TMPDIR at it,
// so every anonymous MkdirTemp in the stage drivers (sandbox work trees,
// source extracts, output staging, plugin/pin scratch) lands on the data
// dir's filesystem. The sweep reclaims trees leaked by killed attempts —
// a sandbox child that dies with the runner never runs its cleanup.
func initWorkDir(dataDir string) (string, error) {
	work := filepath.Join(dataDir, "work")
	if err := os.RemoveAll(work); err != nil {
		return "", fmt.Errorf("sweep work dir: %w", err)
	}
	if err := os.MkdirAll(work, 0o700); err != nil {
		return "", fmt.Errorf("create work dir: %w", err)
	}
	if err := os.Setenv("TMPDIR", work); err != nil {
		return "", fmt.Errorf("set TMPDIR: %w", err)
	}
	return work, nil
}

// ensureRunnerID loads (or mints and persists) the runner's stable identity:
// 8 random bytes, hex-encoded, stored at <data-dir>/runner-id.
func ensureRunnerID(dataDir string) (string, error) {
	path := filepath.Join(dataDir, "runner-id")
	if b, err := os.ReadFile(path); err == nil {
		id := strings.TrimSpace(string(b))
		if id != "" {
			return id, nil
		}
	}
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("mint runner id: %w", err)
	}
	id := hex.EncodeToString(raw[:])
	if err := os.WriteFile(path, []byte(id+"\n"), 0o600); err != nil {
		return "", fmt.Errorf("persist runner id: %w", err)
	}
	return id, nil
}
