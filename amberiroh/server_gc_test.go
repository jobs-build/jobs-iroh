package amberiroh

import (
	"log/slog"
	"net"
	"path/filepath"
	"slices"
	"testing"

	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/core/refstore"
)

// gcTestServer opens a Server over throwaway stores with one raw ref
// record named "present" (handlePin/handlePull only check existence).
func gcTestServer(t *testing.T) *Server {
	t.Helper()
	dir := t.TempDir()
	objects, err := packstore.Open(filepath.Join(dir, "objects"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { objects.Close() })
	refs, err := refstore.Open(filepath.Join(dir, "refs"), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { refs.Close() })
	if err := refs.Put("present", []byte{0xa0}); err != nil { // any bytes: existence is all these paths check
		t.Fatal(err)
	}
	return New(slog.Default(), objects, refs)
}

// exchange runs one HandleStream conversation over a pipe.
func exchange(t *testing.T, srv *Server, send Msg) Msg {
	t.Helper()
	c, s := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.HandleStream("test-peer", s)
	}()
	if err := WriteMsg(c, send); err != nil {
		t.Fatal(err)
	}
	m, err := ReadMsg(c)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	<-done
	return m
}

func TestPinAssert(t *testing.T) {
	srv := gcTestServer(t)
	var pinned []string
	srv.SetOnPin(func(name string) { pinned = append(pinned, name) })

	m := exchange(t, srv, Msg{Type: TPin, Names: []string{"present", "absent"}})
	if m.Type != TOK {
		t.Fatalf("reply type %d, want TOK", m.Type)
	}
	if !slices.Equal(pinned, []string{"present"}) {
		t.Fatalf("pinned = %v, want [present]", pinned)
	}
}

func TestPinWithoutHookStillOK(t *testing.T) {
	srv := gcTestServer(t)
	if m := exchange(t, srv, Msg{Type: TPin, Names: []string{"present"}}); m.Type != TOK {
		t.Fatalf("reply type %d, want TOK", m.Type)
	}
}

func TestPullFiresOnAccess(t *testing.T) {
	srv := gcTestServer(t)
	var accessed []string
	srv.SetOnAccess(func(name string) { accessed = append(accessed, name) })

	c, s := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.HandleStream("test-peer", s)
	}()
	if err := WriteMsg(c, Msg{Type: TPull, Name: "present"}); err != nil {
		t.Fatal(err)
	}
	m, err := ReadMsg(c)
	if err != nil || m.Type != TRef {
		t.Fatalf("m=%+v err=%v, want TRef", m, err)
	}
	// End the transfer: an empty TWants finishes the Send loop.
	if err := WriteMsg(c, Msg{Type: TWants}); err != nil {
		t.Fatal(err)
	}
	c.Close()
	<-done
	if !slices.Equal(accessed, []string{"present"}) {
		t.Fatalf("accessed = %v, want [present]", accessed)
	}
}

type recordingGuard struct {
	prepared int
	commits  int
}

func (g *recordingGuard) PrepareRef(root key.Key) (func(), func(), error) {
	g.prepared++
	return func() { g.commits++ }, func() {}, nil
}
