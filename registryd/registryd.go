// Package registryd implements jobs-registry: a read-only OCI Distribution
// registry that serves jobs build outputs as pullable container images.
//
// Images are named jobs:<K> — one repository ("jobs") whose tags are build
// keys (64 lowercase hex chars). On the first pull of a K the daemon resolves
// the build on the configured jobs-server (build-from:K → F via the server's
// ref listing, so the source closure is never synced), pulls build-output:F
// and build-output-deps:F into its own amber store over the jobs-amber-admin
// ALPN, and assembles a two-layer OCI image (runtime closure + artifact — see
// runner.AssembleOCIImage).
//
// Layers are uncompressed and never stored: their tars are a pure function of
// the store plus a runner.LayerSpec, so assembly streams each layer once to
// measure it and the record keeps the spec, not the bytes. A blob GET
// regenerates the tar from the store into the response. Only the manifest and
// the config land in <data-dir>/blobs; a blob file's mtime is bumped on every
// read and a background sweep deletes blobs not read for CacheTTL. The
// per-image records under <data-dir>/repos persist and carry the layer specs
// (rebuilt into the digest→spec index at startup), so together with the amber
// store they let an expired image be reassembled without the server. The amber
// store itself only grows (store GC is an upstream follow-up).
package registryd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/amberclient"
	"github.com/jobs-build/jobs-iroh/runner"
)

// alpnAmberAdmin is serve.ALPNAmberAdmin, declared locally so the registry
// binary does not link the server composition (the repo-wide convention for
// dial-side packages).
const alpnAmberAdmin = "jobs-amber-admin/1.0"

const shutdownGrace = 10 * time.Second

// Options configures the registry daemon.
type Options struct {
	// ServerID is the jobs-server endpoint ID whose store is synced on
	// demand. Required.
	ServerID string

	// Addrs are optional direct server addresses (host:port). Non-empty
	// skips discovery and relays (see amberclient.Options.Addrs).
	Addrs []string

	// DataDir holds the registry's private amber store, the blob cache and
	// the per-image records. The store dir is flocked — one process only.
	DataDir string

	// Listen is the HTTP listen address for the registry API.
	// Defaults to ":5000".
	Listen string

	// CacheTTL is how long an unread cached blob survives before the sweep
	// deletes it. Only manifests and configs are cached (layers stream from
	// the store), so this bounds a few KB per image. Defaults to 24h.
	CacheTTL time.Duration

	// DefaultPlatform (os/arch) is used for image configs when a build's
	// definition cannot be resolved to learn its platform. Defaults to the
	// registry host's platform.
	DefaultPlatform string

	// NoShell skips baking the platform shell (and its /bin/sh +
	// /jobs/shell symlinks) into images. Default off: script entrypoints
	// need the shell, exactly as `jobs-client run` and `image` provide it.
	NoShell bool

	// SyncConns is the QUIC connections per store transfer (control +
	// data shards; see amberclient.Options.Conns). 0 uses the amberclient
	// default, 1 disables sharding.
	SyncConns int

	// BindAddr optionally pins the iroh client UDP bind (tests use
	// loopback). The port should be 0.
	BindAddr netip.AddrPort

	// Logger receives daemon logs; nil means slog.Default().
	Logger *slog.Logger

	// Ready, when set, is called once with the bound HTTP listener address
	// (a test seam, like serve.Options.Ready).
	Ready func(addr net.Addr)
}

// registry is the daemon state shared by the HTTP handler, the assembler and
// the sweep.
type registry struct {
	log             *slog.Logger
	st              *amber.Store
	sync            *reconnSync
	adminOpts       amberclient.Options // jobs-admin/1.0 dial for the paged refs API (tags)
	blobs           *blobCache
	layers          *layerIndex
	pins            *pinAsserter
	reposDir        string
	ttl             time.Duration
	defaultPlatform string
	noShell         bool

	// runCtx outlives individual HTTP requests: assembly triggered by one
	// client keeps running (and lands in the cache) even if that client
	// disconnects, so concurrent waiters are not cancelled by proxy.
	runCtx     context.Context
	group      singleflight.Group
	assemblies sync.WaitGroup // in-flight assembleAndCache runs; awaited at shutdown
}

// Run starts the registry and blocks until ctx is cancelled (clean shutdown,
// returns nil) or startup fails.
func Run(ctx context.Context, o Options) error {
	if o.ServerID == "" {
		return errors.New("registryd: ServerID is required")
	}
	log := o.Logger
	if log == nil {
		log = slog.Default()
	}
	if o.Listen == "" {
		o.Listen = ":5000"
	}
	if o.CacheTTL <= 0 {
		o.CacheTTL = 24 * time.Hour
	}
	if o.DefaultPlatform == "" {
		o.DefaultPlatform = runner.Platform()
	}
	dataDir, err := filepath.Abs(o.DataDir)
	if err != nil {
		return fmt.Errorf("resolve data dir: %w", err)
	}
	for _, sub := range []string{"", "repos"} {
		if err := os.MkdirAll(filepath.Join(dataDir, sub), 0o700); err != nil {
			return err
		}
	}

	st, err := amber.Open(filepath.Join(dataDir, "store"))
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer st.Close()

	syncLog := log.With("component", "amber-sync")
	sc := newReconnSync(amberclient.Options{
		EndpointID: o.ServerID,
		Addrs:      o.Addrs,
		ALPN:       alpnAmberAdmin,
		BindAddr:   o.BindAddr,
		Conns:      o.SyncConns,
		Logger:     syncLog,
	}, st, syncLog)
	defer sc.Close()

	r := &registry{
		log:  log,
		st:   st,
		sync: sc,
		adminOpts: amberclient.Options{
			EndpointID: o.ServerID,
			Addrs:      o.Addrs,
			ALPN:       alpnAdmin,
			BindAddr:   o.BindAddr,
			Logger:     syncLog,
		},
		blobs:           &blobCache{dir: filepath.Join(dataDir, "blobs"), log: log},
		layers:          newLayerIndex(),
		pins:            newPinAsserter(),
		reposDir:        filepath.Join(dataDir, "repos"),
		ttl:             o.CacheTTL,
		defaultPlatform: o.DefaultPlatform,
		noShell:         o.NoShell,
		runCtx:          ctx,
	}
	// Layer bytes are never stored, so what makes a previously served image's
	// blobs answerable after a restart is the recipes in its record.
	for _, rec := range r.listRecords() {
		r.layers.put(rec.Layers...)
	}

	ln, err := net.Listen("tcp", o.Listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", o.Listen, err)
	}
	log.Info("jobs-registry listening",
		"addr", ln.Addr(), "server", o.ServerID,
		"data_dir", dataDir, "cache_ttl", o.CacheTTL)
	if o.Ready != nil {
		o.Ready(ln.Addr())
	}

	srv := &http.Server{
		Handler:           r.handler(),
		ReadHeaderTimeout: 10 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	sweepDone := make(chan struct{})
	go func() {
		defer close(sweepDone)
		r.sweepLoop(ctx)
	}()

	select {
	case err := <-serveErr:
		return fmt.Errorf("http serve: %w", err)
	case <-ctx.Done():
	}

	shCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownGrace)
	defer cancel()
	if err := srv.Shutdown(shCtx); err != nil {
		log.Warn("http shutdown", "error", err)
	}
	<-sweepDone
	// Let detached assemblies drain before the deferred store/sync closes:
	// their pulls die fast (runCtx is done) but their store reads must not
	// race Close.
	r.assemblies.Wait()
	return nil
}

// sweepLoop expires unread cache entries every sweepInterval until ctx dies.
// One sweep runs at startup so a restarted registry reclaims space promptly.
func (r *registry) sweepLoop(ctx context.Context) {
	interval := r.ttl / 4
	if interval > time.Hour {
		interval = time.Hour
	}
	if interval < time.Minute {
		interval = time.Minute
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		removed, bytes := r.blobs.sweep(r.ttl, time.Now())
		if removed > 0 {
			r.log.Info("blob cache sweep", "removed", removed, "bytes", bytes)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
