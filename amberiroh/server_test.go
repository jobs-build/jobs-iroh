package amberiroh

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/ingest"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/core/reference"
	"github.com/amber-store/core/refstore"
)

func testServer(t *testing.T) *Server {
	t.Helper()
	return testServerTo(t, os.Stderr)
}

// testServerTo builds a Server whose log lands on w.
func testServerTo(t *testing.T, w io.Writer) *Server {
	t.Helper()
	dir := t.TempDir()
	objects, err := packstore.Open(filepath.Join(dir, "packstore"), packstore.WithSync(true))
	if err != nil {
		t.Fatal(err)
	}
	refs, err := refstore.Open(filepath.Join(dir, "refs"), true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { refs.Close(); objects.Close() })
	return New(slog.New(slog.NewTextHandler(w, nil)), objects, refs)
}

func clientStore(t *testing.T) (*packstore.Store, key.Key) {
	t.Helper()
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "f.txt"), []byte("hello p2p"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(src, "d"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "d", "g.txt"), []byte("nested"), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := packstore.Open(filepath.Join(t.TempDir(), "packstore"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	root, _, err := ingest.Dir(st, src, ingest.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	return st, root
}

// doPush runs one full client-side push against srv over net.Pipe and
// returns the final frame (TOK or TErr) or the client-side error.
func doPush(t *testing.T, srv *Server, st *packstore.Store, name string, root key.Key, cas bool, expectedOld []byte) (Msg, error) {
	t.Helper()
	c, s := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); srv.HandleStream("test-client", s) }()
	defer func() { c.Close(); <-done }()
	req := Msg{Type: TPush, Name: name, Root: root[:], CAS: cas, ExpectedOld: expectedOld}
	if err := WriteMsg(c, req); err != nil {
		return Msg{}, err
	}
	if err := Send(c, st, nil); err != nil {
		return Msg{}, err
	}
	return ReadMsg(c)
}

func TestPushCreatesRefAndTransfersObjects(t *testing.T) {
	srv := testServer(t)
	st, root := clientStore(t)
	m, err := doPush(t, srv, st, "backups/home", root, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != TOK {
		t.Fatalf("want TOK, got %+v", m)
	}
	if _, err := fstree.CheckComplete(root, srv.objects.Get, srv.objects.Has, 0); err != nil {
		t.Fatalf("server store incomplete: %v", err)
	}
	raw, err := srv.refs.Get("backups/home")
	if err != nil {
		t.Fatal(err)
	}
	rec, err := reference.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	k, err := key.Parse(rec.Key)
	if err != nil || k != root {
		t.Fatalf("ref points at %v (err %v), want %v", k, err, root)
	}
}

func TestPushCASMismatchOnExistingRef(t *testing.T) {
	srv := testServer(t)
	st, root := clientStore(t)
	if m, err := doPush(t, srv, st, "r", root, true, nil); err != nil || m.Type != TOK {
		t.Fatalf("first push: %+v %v", m, err)
	}
	// Second create-push (ExpectedOld nil) must be rejected before any
	// transfer: Send surfaces the TErr as *RemoteError.
	_, err := doPush(t, srv, st, "r", root, true, nil)
	var re *RemoteError
	if !errors.As(err, &re) || re.Code != CodeCASMismatch {
		t.Fatalf("want cas-mismatch, got %v", err)
	}
	if !errorsIsKey(re.Current, root) {
		t.Fatalf("Current = %x, want the committed root", re.Current)
	}
}

func errorsIsKey(b []byte, k key.Key) bool {
	kk, err := key.Parse(b)
	return err == nil && kk == k
}

func TestPushCASMatchUpdatesRef(t *testing.T) {
	srv := testServer(t)
	st, root := clientStore(t)
	if m, err := doPush(t, srv, st, "r", root, true, nil); err != nil || m.Type != TOK {
		t.Fatalf("first push: %+v %v", m, err)
	}
	// Same root again, but with the correct expected-old: allowed.
	if m, err := doPush(t, srv, st, "r", root, true, root[:]); err != nil || m.Type != TOK {
		t.Fatalf("cas update push: %+v %v", m, err)
	}
	// Forced push (no CAS) is always allowed.
	if m, err := doPush(t, srv, st, "r", root, false, nil); err != nil || m.Type != TOK {
		t.Fatalf("forced push: %+v %v", m, err)
	}
}

func TestPushRejectsBadName(t *testing.T) {
	srv := testServer(t)
	st, root := clientStore(t)
	_, err := doPush(t, srv, st, "bad@name", root, true, nil)
	var re *RemoteError
	if !errors.As(err, &re) || re.Code != CodeBadRequest {
		t.Fatalf("want bad-request, got %v", err)
	}
}

// TestPushTransferFailureReportsErr pins the spec rule that every failure
// reaches the peer as a TErr frame: a sender that never delivers the
// wanted objects must be told why, not left with a bare EOF.
func TestPushTransferFailureReportsErr(t *testing.T) {
	srv := testServer(t)
	_, root := clientStore(t)
	c, s := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); srv.HandleStream("test-client", s) }()
	defer func() { c.Close(); <-done }()
	if err := WriteMsg(c, Msg{Type: TPush, Name: "r", Root: root[:]}); err != nil {
		t.Fatal(err)
	}
	m, err := ReadMsg(c)
	if err != nil || m.Type != TWants {
		t.Fatalf("want TWants, got %+v %v", m, err)
	}
	empty := func(yield func(fstree.Object, error) bool) {}
	if err := sendPack(c, empty); err != nil {
		t.Fatal(err)
	}
	m, err = ReadMsg(c)
	if err != nil {
		t.Fatalf("expected a TErr frame, got read error %v", err)
	}
	if m.Type != TErr || m.Code != CodeInternal {
		t.Fatalf("want internal TErr, got %+v", m)
	}
}

// TestPushDoesNotEchoPeerError checks the exception: an error the peer
// itself reported must not be sent back to it.
func TestPushDoesNotEchoPeerError(t *testing.T) {
	srv := testServer(t)
	_, root := clientStore(t)
	c, s := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); srv.HandleStream("test-client", s) }()
	defer func() { c.Close(); <-done }()
	if err := WriteMsg(c, Msg{Type: TPush, Name: "r", Root: root[:]}); err != nil {
		t.Fatal(err)
	}
	if m, err := ReadMsg(c); err != nil || m.Type != TWants {
		t.Fatalf("want TWants, got %+v %v", m, err)
	}
	sent := Msg{Type: TErr, Code: CodeInternal, Text: "client-side read failed"}
	if err := WriteMsg(c, sent); err != nil {
		t.Fatal(err)
	}
	if m, err := ReadMsg(c); err == nil {
		t.Fatalf("server echoed the peer's error back: %+v", m)
	}
}

// TestPullBadWantsReportsErr covers the same rule on the pull path, where
// the failure originates in Send's key decoding.
func TestPullBadWantsReportsErr(t *testing.T) {
	srv := testServer(t)
	st, root := clientStore(t)
	if m, err := doPush(t, srv, st, "r", root, true, nil); err != nil || m.Type != TOK {
		t.Fatalf("push: %+v %v", m, err)
	}
	c, s := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); srv.HandleStream("test-client", s) }()
	defer func() { c.Close(); <-done }()
	if err := WriteMsg(c, Msg{Type: TPull, Name: "r"}); err != nil {
		t.Fatal(err)
	}
	if m, err := ReadMsg(c); err != nil || m.Type != TRef {
		t.Fatalf("want TRef, got %+v %v", m, err)
	}
	if err := WriteMsg(c, Msg{Type: TWants, Keys: [][]byte{{1, 2, 3}}}); err != nil {
		t.Fatal(err)
	}
	m, err := ReadMsg(c)
	if err != nil {
		t.Fatalf("expected a TErr frame, got read error %v", err)
	}
	if m.Type != TErr || m.Code != CodeInternal {
		t.Fatalf("want internal TErr, got %+v", m)
	}
}

func TestPullUnknownRef(t *testing.T) {
	srv := testServer(t)
	c, s := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); srv.HandleStream("test-client", s) }()
	defer func() { c.Close(); <-done }()
	if err := WriteMsg(c, Msg{Type: TPull, Name: "nope"}); err != nil {
		t.Fatal(err)
	}
	m, err := ReadMsg(c)
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != TErr || m.Code != CodeUnknownRef {
		t.Fatalf("want unknown-ref, got %+v", m)
	}
}

func TestPullTransfersTree(t *testing.T) {
	srv := testServer(t)
	st, root := clientStore(t)
	if m, err := doPush(t, srv, st, "r", root, true, nil); err != nil || m.Type != TOK {
		t.Fatalf("push: %+v %v", m, err)
	}
	dest, err := packstore.Open(filepath.Join(t.TempDir(), "packstore"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dest.Close() })

	c, s := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); srv.HandleStream("test-client", s) }()
	defer func() { c.Close(); <-done }()
	if err := WriteMsg(c, Msg{Type: TPull, Name: "r"}); err != nil {
		t.Fatal(err)
	}
	m, err := ReadMsg(c)
	if err != nil || m.Type != TRef {
		t.Fatalf("want TRef, got %+v %v", m, err)
	}
	rec, err := reference.Decode(m.Record)
	if err != nil {
		t.Fatal(err)
	}
	k, err := key.Parse(rec.Key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Receive([]io.ReadWriter{c}, dest, k, 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := fstree.CheckComplete(k, dest.Get, dest.Has, 0); err != nil {
		t.Fatalf("pulled tree incomplete: %v", err)
	}
}

func TestRefList(t *testing.T) {
	srv := testServer(t)
	st, root := clientStore(t)
	if m, err := doPush(t, srv, st, "r1", root, true, nil); err != nil || m.Type != TOK {
		t.Fatalf("push: %+v %v", m, err)
	}
	c, s := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); srv.HandleStream("test-client", s) }()
	defer func() { c.Close(); <-done }()
	if err := WriteMsg(c, Msg{Type: TRefList}); err != nil {
		t.Fatal(err)
	}
	m, err := ReadMsg(c)
	if err != nil || m.Type != TRefs {
		t.Fatalf("want TRefs, got %+v %v", m, err)
	}
	if len(m.Refs) != 1 || m.Refs[0].Name != "r1" || m.Refs[0].CreatedAt == 0 {
		t.Fatalf("refs: %+v", m.Refs)
	}
	if time.Since(time.Unix(0, m.Refs[0].CreatedAt)) > time.Minute {
		t.Fatalf("CreatedAt implausible: %d", m.Refs[0].CreatedAt)
	}
}

// TestPushLogsTransferStats asserts the per-push server log: offered is
// the full tree size, transferred is what crossed the wire — equal on a
// fresh push, zero transferred on an idempotent re-push.
func TestPushLogsTransferStats(t *testing.T) {
	var buf bytes.Buffer
	srv := testServerTo(t, &buf)
	st, root := clientStore(t)
	total, err := fstree.ReachableKeys(root, st.Get)
	if err != nil {
		t.Fatal(err)
	}

	if m, err := doPush(t, srv, st, "logged/ref", root, true, nil); err != nil || m.Type != TOK {
		t.Fatalf("push: %+v %v", m, err)
	}
	out := buf.String()
	for _, want := range []string{
		"msg=push",
		"ref=logged/ref",
		"client=test-client",
		fmt.Sprintf("offered=%d", len(total)),
		fmt.Sprintf("transferred=%d", len(total)),
		"throughput=",
		"duration=",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("push log missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "bytes=0 ") {
		t.Fatalf("fresh push must log nonzero bytes:\n%s", out)
	}

	buf.Reset()
	if m, err := doPush(t, srv, st, "logged/ref", root, false, nil); err != nil || m.Type != TOK {
		t.Fatalf("re-push: %+v %v", m, err)
	}
	out = buf.String()
	for _, want := range []string{
		fmt.Sprintf("offered=%d", len(total)),
		"transferred=0",
		"bytes=0",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("re-push log missing %q:\n%s", want, out)
		}
	}
}

func TestUnknownOperation(t *testing.T) {
	srv := testServer(t)
	c, s := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); srv.HandleStream("test-client", s) }()
	defer func() { c.Close(); <-done }()
	if err := WriteMsg(c, Msg{Type: 99}); err != nil {
		t.Fatal(err)
	}
	m, err := ReadMsg(c)
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != TErr || m.Code != CodeBadRequest {
		t.Fatalf("want bad-request, got %+v", m)
	}
}

// shardedPush drives a full sharded push against srv: a control pipe plus
// extra attached data pipes, one Send loop per channel.
func shardedPush(t *testing.T, srv *Server, st *packstore.Store, name string, root key.Key, dataConns, attach int) (Msg, error) {
	t.Helper()
	ctrl, s1 := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); srv.HandleStream("test-client", s1) }()
	defer func() { ctrl.Close(); <-done }()

	req := Msg{Type: TPush, Name: name, Root: root[:], DataConns: dataConns}
	if err := WriteMsg(ctrl, req); err != nil {
		return Msg{}, err
	}
	acc, err := ReadMsg(ctrl)
	if err != nil {
		return Msg{}, err
	}
	if acc.Type != TAccept || len(acc.Token) == 0 {
		return Msg{}, fmt.Errorf("want TAccept with token, got %+v", acc)
	}

	channels := []io.ReadWriter{ctrl}
	for i := 0; i < attach; i++ {
		c, sN := net.Pipe()
		go srv.HandleStream("test-client", sN)
		if err := WriteMsg(c, Msg{Type: TAttach, Token: acc.Token}); err != nil {
			return Msg{}, err
		}
		defer c.Close()
		channels = append(channels, c)
	}

	errs := make([]error, len(channels))
	var wg sync.WaitGroup
	for i, ch := range channels {
		wg.Add(1)
		go func(i int, ch io.ReadWriter) {
			defer wg.Done()
			errs[i] = Send(ch, st, nil)
		}(i, ch)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			return Msg{}, fmt.Errorf("send on channel %d: %w", i, err)
		}
	}
	return ReadMsg(ctrl)
}

func TestPushShardedOverPipes(t *testing.T) {
	srv := testServer(t)
	st, root := clientStore(t)
	m, err := shardedPush(t, srv, st, "sharded/ref", root, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != TOK {
		t.Fatalf("want TOK, got %+v", m)
	}
	if _, err := fstree.CheckComplete(root, srv.objects.Get, srv.objects.Has, 0); err != nil {
		t.Fatalf("server store incomplete after sharded push: %v", err)
	}
}

// TestPushShardedLenientGather promises more data connections than it
// attaches; the transfer must proceed with what arrived instead of
// stalling or failing.
func TestPushShardedLenientGather(t *testing.T) {
	srv := testServer(t)
	srv.attachWait = 200 * time.Millisecond
	st, root := clientStore(t)
	m, err := shardedPush(t, srv, st, "lenient/ref", root, 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != TOK {
		t.Fatalf("want TOK, got %+v", m)
	}
	if _, err := fstree.CheckComplete(root, srv.objects.Get, srv.objects.Has, 0); err != nil {
		t.Fatalf("server store incomplete: %v", err)
	}
}

func TestAttachUnknownToken(t *testing.T) {
	srv := testServer(t)
	c, sN := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); srv.HandleStream("test-client", sN) }()
	defer func() { c.Close(); <-done }()
	if err := WriteMsg(c, Msg{Type: TAttach, Token: []byte("bogus")}); err != nil {
		t.Fatal(err)
	}
	m, err := ReadMsg(c)
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != TErr || m.Code != CodeBadRequest {
		t.Fatalf("want bad-request for unknown token, got %+v", m)
	}
}

func TestAttachWaitDefaultCoversPunching(t *testing.T) {
	s := New(slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil)
	if s.attachWait != 10*time.Second {
		t.Fatalf("attachWait %v, want 10s (punching attaches ride the relay first)", s.attachWait)
	}
}

func TestShardChannelsAdvertisesDataEndpoints(t *testing.T) {
	s := New(slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil)
	s.attachWait = 50 * time.Millisecond
	s.SetDataPorts([]uint16{4001})
	rec := DataEndpointRec{ID: bytes.Repeat([]byte{7}, 32), Addrs: []string{"ip:127.0.0.1:4001"}}
	s.SetDataEndpoints(func() []DataEndpointRec { return []DataEndpointRec{rec} })

	cli, srv := net.Pipe()
	got := make(chan Msg, 1)
	go func() {
		m, err := ReadMsg(cli)
		if err != nil {
			t.Error(err)
		}
		got <- m
		cli.Close()
	}()
	channels, release, err := s.shardChannels(srv, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if len(channels) != 1 {
		t.Fatalf("gathered %d channels, want control only", len(channels))
	}
	m := <-got
	if m.Type != TAccept {
		t.Fatalf("type %d, want TAccept", m.Type)
	}
	if !reflect.DeepEqual(m.DataEndpoints, []DataEndpointRec{rec}) {
		t.Fatalf("DataEndpoints %+v, want %+v", m.DataEndpoints, rec)
	}
	if !reflect.DeepEqual(m.DataPorts, []uint16{4001}) {
		t.Fatalf("DataPorts %+v", m.DataPorts)
	}
}
