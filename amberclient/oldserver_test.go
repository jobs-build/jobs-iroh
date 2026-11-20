package amberclient_test

import (
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/transport-iroh/amberiroh"
	"github.com/tmc/go-iroh/iroh"

	"github.com/jobs-build/jobs-iroh/amberclient"
)

// startOldServer runs an amber server as transport-iroh before v0.5.0
// answered: TRefList lists every ref in one frame whatever paging was asked
// for, and TRefGet is an unknown operation (bad-request).
func startOldServer(t *testing.T, ctx context.Context, names []string) (string, string) {
	t.Helper()
	ep, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		t.Fatal(err)
	}
	var k key.Key
	k, err = key.NewFromHash(key.Blob, 1, [key.Size]byte{9})
	if err != nil {
		t.Fatal(err)
	}
	refs := make([]amberiroh.RefInfo, len(names))
	for i, n := range names {
		refs[i] = amberiroh.RefInfo{Name: n, Key: k[:], CreatedAt: 1}
	}
	handler := iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
		for {
			stream, err := conn.AcceptStreamConn(ctx)
			if err != nil {
				return nil
			}
			go func() {
				defer amberclient.CloseStream(stream)
				m, err := amberiroh.ReadMsg(stream)
				if err != nil {
					return
				}
				switch m.Type {
				case amberiroh.TRefList:
					_ = amberiroh.WriteMsg(stream, amberiroh.Msg{Type: amberiroh.TRefs, Refs: refs})
				default:
					_ = amberiroh.WriteMsg(stream, amberiroh.Msg{Type: amberiroh.TErr, Code: amberiroh.CodeBadRequest, Text: "unknown operation"})
				}
			}()
		}
	})
	const alpn = "old-amber"
	router, err := iroh.NewRouter(ep, map[string]iroh.ProtocolHandler{alpn: handler}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = router.Shutdown(sctx)
	})
	return ep.ID().String(), ep.LocalAddr().String()
}

// Against a server predating TRefGet and paging, GetRef reports
// ErrUnsupported (the registry then falls back to listing) and the paging
// helpers still return exactly the asked-for refs, ending after one round.
func TestOldServerCompat(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	id, addr := startOldServer(t, ctx, []string{"a/1", "b/1", "b/2", "b/3", "c/1"})
	c, err := amberclient.Dial(ctx, amberclient.Options{
		EndpointID: id,
		Addrs:      []string{addr},
		ALPN:       "old-amber",
		BindAddr:   netip.AddrPortFrom(netip.IPv6Loopback(), 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = c.GetRef(ctx, "b/1")
	var re *amberiroh.RemoteError
	if !errors.Is(err, amberclient.ErrUnsupported) || !errors.As(err, &re) {
		t.Fatalf("GetRef on an old server: %v, want ErrUnsupported wrapping the RemoteError", err)
	}

	page, more, err := c.RefsPage(ctx, "b/", "", 2)
	if err != nil || more || len(page) != 3 {
		t.Fatalf("RefsPage: %d refs, more=%v, %v — want all 3 b/ refs and no more (the old reply is complete)", len(page), more, err)
	}
	page, _, err = c.RefsPage(ctx, "b/", "b/1", 2)
	if err != nil || len(page) != 2 || page[0].Name != "b/2" {
		t.Fatalf("RefsPage after b/1: %+v %v", page, err)
	}
	var seen []string
	if err := c.EachRef(ctx, "b/", func(r amberclient.RefInfo) error {
		seen = append(seen, r.Name)
		return nil
	}); err != nil || len(seen) != 3 {
		t.Fatalf("EachRef: %v %v", seen, err)
	}
}
