package amberclient_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/core/reference"
	"github.com/amber-store/core/refstore"

	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/amberclient"
	"github.com/jobs-build/jobs-iroh/serve"
)

// seedRefs writes n refs named fmt.Sprintf(format, i) → k straight into a
// store's refstore, in one batch.
func seedRefs(t *testing.T, st *amber.Store, format string, n int, k key.Key) {
	t.Helper()
	recs := make([]refstore.Record, n)
	for i := range recs {
		name := fmt.Sprintf(format, i)
		raw, err := reference.Reference{Name: name, Key: k[:], CreatedAt: time.Now().UnixNano()}.Encode()
		if err != nil {
			t.Fatal(err)
		}
		recs[i] = refstore.Record{Name: name, Data: raw}
	}
	if err := st.RefStore().PutBatch(recs); err != nil {
		t.Fatal(err)
	}
}

func TestGetRef(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	srv := startServer(t, ctx)
	client := dialClient(t, ctx, srv, serve.ALPNAmberAdmin)
	k, err := srv.Store.IngestFile(ctx, []byte("looked up"))
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Store.PutRef(ctx, "build-from:abc", k); err != nil {
		t.Fatal(err)
	}
	ri, err := client.GetRef(ctx, "build-from:abc")
	if err != nil {
		t.Fatalf("get ref: %v", err)
	}
	if ri.Name != "build-from:abc" || ri.Key != k || ri.CreatedAt.IsZero() {
		t.Fatalf("got %+v, want build-from:abc → %s", ri, k)
	}
	if _, err := client.GetRef(ctx, "build-from:nope"); !errors.Is(err, amberclient.ErrRefNotFound) {
		t.Fatalf("absent ref: %v, want ErrRefNotFound", err)
	}
}

// More refs than one page (amberiroh.MaxRefPage) under a prefix, plus noise
// outside it: EachRef visits exactly the prefix's refs, in order, once.
func TestEachRefPagesPrefix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	srv := startServer(t, ctx)
	client := dialClient(t, ctx, srv, serve.ALPNAmberAdmin)
	k, err := srv.Store.IngestFile(ctx, []byte("paged"))
	if err != nil {
		t.Fatal(err)
	}
	const n = amberclient.RefPage + 2345
	seedRefs(t, srv.Store, "build-from:%06d", n, k)
	seedRefs(t, srv.Store, "build-output-deps:%06d", 500, k)

	page, more, err := client.RefsPage(ctx, "build-from:", "", 10)
	if err != nil || len(page) != 10 || !more || page[0].Name != "build-from:000000" {
		t.Fatalf("first page of 10: %d refs, more=%v, err=%v", len(page), more, err)
	}
	seen, last := 0, ""
	err = client.EachRef(ctx, "build-from:", func(ri amberclient.RefInfo) error {
		if ri.Name <= last {
			return fmt.Errorf("out of order: %q after %q", ri.Name, last)
		}
		if ri.Key != k {
			return fmt.Errorf("%s → %s, want %s", ri.Name, ri.Key, k)
		}
		last = ri.Name
		seen++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if seen != n {
		t.Fatalf("EachRef saw %d refs, want %d", seen, n)
	}
	all, err := client.Refs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) < n+500 {
		t.Fatalf("Refs returned %d, want at least %d", len(all), n+500)
	}
}
