package amberclient_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jobs-build/jobs-iroh/amberclient"
	"github.com/jobs-build/jobs-iroh/serve"
)

// GetRef is a pull that wants nothing: the ref's key comes back, no object
// moves, and the exchange ends cleanly on both sides — 150 lookups on one
// connection would stall at QUIC's 100-stream budget if a stream leaked.
func TestGetRef(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	srv := startServer(t, ctx)
	client := dialClient(t, ctx, srv, serve.ALPNAmberAdmin)
	src := layeredSource(t)
	k, err := srv.Store.IngestDir(ctx, src)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Store.PutRef(ctx, "build-from:abc", k); err != nil {
		t.Fatal(err)
	}
	for i := range 150 {
		ri, err := client.GetRef(ctx, "build-from:abc")
		if err != nil {
			t.Fatalf("lookup %d: %v", i, err)
		}
		if ri.Name != "build-from:abc" || ri.Key != k || ri.CreatedAt.IsZero() {
			t.Fatalf("got %+v, want build-from:abc → %s", ri, k)
		}
	}
	if _, err := client.GetRef(ctx, "build-from:nope"); !errors.Is(err, amberclient.ErrRefNotFound) {
		t.Fatalf("absent ref: %v, want ErrRefNotFound", err)
	}
	// The connection still serves a real pull afterwards.
	st := openStore(t)
	if got, err := client.Pull(ctx, st, "build-from:abc"); err != nil || got != k {
		t.Fatalf("pull after lookups: %s %v", got, err)
	}
	if _, err := client.GetRef(ctx, "bad@name"); err == nil {
		t.Fatal("an invalid name must be refused client-side")
	}
}
