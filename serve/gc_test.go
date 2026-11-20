package serve

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"

	"github.com/jobs-build/jobs-iroh/amberclient"
	"github.com/jobs-build/jobs-iroh/reftrack"
	"github.com/jobs-build/jobs-iroh/wire"
)

// startGCServer mirrors startServer (serve_test.go:20-68) with GC enabled
// at a tiny retention; the loop interval is far in the future so sweeps
// are manual and deterministic.
func startGCServer(t *testing.T, ctx context.Context) (*Server, *gcRunner, func(alpn string) *iroh.Conn) {
	t.Helper()
	// dir must outlive the server shutdown: t.TempDir()'s RemoveAll cleanup
	// is registered here, before the cancel-and-wait t.Cleanup in
	// startGCServerIn, so it runs AFTER that cleanup (t.Cleanup runs LIFO)
	// and gcr's final tracker flush on Close doesn't hit ENOENT.
	return startGCServerIn(t, ctx, t.TempDir())
}

// startGCServerIn is startGCServer over a prepared data dir (whose
// TempDir cleanup the caller registered first).
func startGCServerIn(t *testing.T, ctx context.Context, dir string) (*Server, *gcRunner, func(alpn string) *iroh.Conn) {
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
			DataDir:     dir,
			BindAddr:    netip.AddrPortFrom(netip.IPv6Loopback(), 0),
			GCRetention: 500 * time.Millisecond,
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

	// Reconcile (inside Sweep) seeds a never-before-seen ref's clock at
	// "now" (the safe-upgrade rule in reftrack.Reconcile) rather than
	// backdating it — so a ref's very first sweep can never expire it in
	// the same call. Run one no-op sweep here to seed all three refs'
	// clocks BEFORE they age past the retention window below.
	if _, err := gcr.Sweep(ctx, -1, false); err != nil {
		t.Fatalf("seed sweep: %v", err)
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

	// Age everything past the 500ms retention.
	time.Sleep(700 * time.Millisecond)

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

	// LiveBytes/GarbageBytes only score SEALED packs (packstore.Status),
	// and the active segment seals at 256 MiB (packstore.DefaultSegmentSize)
	// — far above what this fixture ingests, so it stays 0 here by
	// construction. DiskBytes (a plain directory walk) is the meaningful
	// check at this scale.
	if stats.DiskBytes <= 0 {
		t.Errorf("stats missing disk size: %+v", stats)
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
