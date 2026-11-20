package registryd_test

import (
	"context"
	"encoding/binary"
	"net/http"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/core/reference"
	"github.com/amber-store/core/refstore"

	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/amberclient"
	"github.com/jobs-build/jobs-iroh/api"
	"github.com/jobs-build/jobs-iroh/serve"
)

// fakeKey is a distinct, well-formed store key per i.
func fakeKey(t *testing.T, i int) key.Key {
	t.Helper()
	var h [key.Size]byte
	binary.BigEndian.PutUint64(h[:8], uint64(i)+1)
	k, err := key.NewFromHash(key.Blob, 1, h)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// bulkRefs writes the named refs → v straight into st's refstore, batched.
func bulkRefs(t *testing.T, st *amber.Store, names []string, v key.Key) {
	t.Helper()
	const batch = 50_000
	for lo := 0; lo < len(names); lo += batch {
		hi := min(lo+batch, len(names))
		recs := make([]refstore.Record, 0, hi-lo)
		for _, name := range names[lo:hi] {
			raw, err := reference.Reference{Name: name, Key: v[:], CreatedAt: time.Now().UnixNano()}.Encode()
			if err != nil {
				t.Fatal(err)
			}
			recs = append(recs, refstore.Record{Name: name, Data: raw})
		}
		if err := st.RefStore().PutBatch(recs); err != nil {
			t.Fatal(err)
		}
	}
}

// adminRefsAll walks every page of the admin refs listing under prefix (the
// After cursor jobs-client `admin refs` and the TUI use) over one connection.
func adminRefsAll(t *testing.T, ctx context.Context, srv *serve.Server, prefix string) (int, int) {
	t.Helper()
	conn, ep, err := amberclient.DialConn(ctx, amberclient.Options{
		EndpointID: srv.Endpoint.ID().String(),
		Addrs:      []string{srv.Endpoint.LocalAddr().String()},
		ALPN:       "jobs-admin/1.0",
		BindAddr:   netip.AddrPortFrom(netip.IPv6Loopback(), 0),
	})
	if err != nil {
		t.Fatalf("dial admin: %v", err)
	}
	defer conn.Close()
	defer ep.Shutdown(context.Background())
	total, pages, matched, after := 0, 0, 0, ""
	for {
		stream, err := conn.OpenStreamConn(ctx)
		if err != nil {
			t.Fatalf("open admin stream: %v", err)
		}
		if err := api.WriteFrame(stream, api.TRefs, api.RefsRequest{Prefix: prefix, After: after}); err != nil {
			t.Fatalf("send refs request: %v", err)
		}
		rt, rb, err := api.ReadFrame(stream)
		amberclient.CloseStream(stream)
		if err != nil || rt != api.TRefsReply {
			t.Fatalf("refs page after %q: %q %v", after, rt, err)
		}
		var reply api.RefsReply
		if err := api.DecodeBody(rb, &reply); err != nil {
			t.Fatal(err)
		}
		if len(reply.Refs) > api.MaxRefsPage {
			t.Fatalf("page of %d rows exceeds the %d clamp", len(reply.Refs), api.MaxRefsPage)
		}
		total += len(reply.Refs)
		matched = reply.Matched
		pages++
		if !reply.More {
			return total, matched
		}
		after = reply.Refs[len(reply.Refs)-1].Name
		if pages > 1000 {
			t.Fatal("refs paging does not terminate")
		}
	}
}

// TestRegistryLargeRefStore is issue #22's acceptance: a server holding
// 300k+ refs still serves a new image (the K→F resolve is two point lookups,
// not a listing that no longer decodes past 131,072 refs or 16 MiB), lists
// its tags (prefix-paged), and lists every ref to admin clients page by page.
func TestRegistryLargeRefStore(t *testing.T) {
	if testing.Short() {
		t.Skip("300k refs")
	}
	ctx := context.Background()
	srv, _ := startServer(t, ctx)
	st := fixtureStore(t)
	ac := dialAmber(t, ctx, srv)
	k, _ := pushServerBuild(t, ctx, st, ac, "large")

	const (
		builds = 150_000 // build-from:K_i — servable tags…
		tests  = 50_000  // …except these, marked test:K_i
		other  = 100_000 // unrelated namespace
	)
	val := fakeKey(t, 0)
	var froms, marks, deps []string
	for i := range builds {
		hex := fakeKey(t, i+1).String()
		froms = append(froms, "build-from:"+hex)
		if i < tests {
			marks = append(marks, "test:"+hex)
		}
	}
	for i := range other {
		deps = append(deps, "build-output-deps:"+fakeKey(t, builds+i+1).String())
	}
	bulkRefs(t, srv.Store, froms, val)
	bulkRefs(t, srv.Store, marks, val)
	bulkRefs(t, srv.Store, deps, val)
	n, err := srv.Store.CountRefs(ctx)
	if err != nil || n < 300_000 {
		t.Fatalf("seeded store holds %d refs (%v), want ≥ 300k", n, err)
	}

	host, _, _ := startRegistry(t, ctx, srv, nil)
	validateImage(t, pullImage(t, ctx, host, k))

	var tags struct {
		Tags []string `json:"tags"`
	}
	getJSON(t, "http://"+host+"/v2/jobs/tags/list", &tags)
	if want := builds - tests + 1; len(tags.Tags) != want {
		t.Fatalf("tags/list has %d tags, want %d (non-test build-from Ks + the fixture)", len(tags.Tags), want)
	}
	if !contains(tags.Tags, k.String()) || contains(tags.Tags, fakeKey(t, 1).String()) {
		t.Fatal("tags/list must hold the fixture and skip test Ks")
	}
	resp, err := http.Get("http://" + host + "/v2/jobs/tags/list?n=3")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(resp.Header.Get("Link"), "n=3") {
		t.Fatalf("tags page: status %d, Link %q", resp.StatusCode, resp.Header.Get("Link"))
	}

	total, matched := adminRefsAll(t, ctx, srv, "")
	if total != n || matched != n {
		t.Fatalf("admin refs paged %d refs (matched %d), store holds %d", total, matched, n)
	}
	if got, _ := adminRefsAll(t, ctx, srv, "test:"); got != tests {
		t.Fatalf("admin refs test: prefix paged %d, want %d", got, tests)
	}
}
