package serve

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jobs-build/jobs-iroh/api"
)

func TestAdminGCFrames(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	srv, _, dial := startGCServer(t, ctx)
	conn := dial(ALPNAdmin)

	k, err := srv.Store.IngestFile(ctx, []byte("admin gc fixture"))
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Store.PutRef(ctx, "gc-api:target", k); err != nil {
		t.Fatal(err)
	}

	// pin → pinned row
	var row api.RefInfo
	request(t, ctx, conn, api.TPin, api.PinRequest{Name: "gc-api:target"}, api.TPinReply, &row)
	if !row.Pinned || row.LastAccessNs == 0 {
		t.Fatalf("pin reply %+v", row)
	}

	// refs listing carries the columns
	var refs api.RefsReply
	request(t, ctx, conn, api.TRefs, api.RefsRequest{Prefix: "gc-api:"}, api.TRefsReply, &refs)
	if len(refs.Refs) != 1 || !refs.Refs[0].Pinned || refs.Refs[0].LastAccessNs == 0 {
		t.Fatalf("refs reply %+v", refs.Refs)
	}

	// unpin → cleared row. Fresh variable: cbor.Unmarshal into a reused
	// struct leaves omitempty-false fields (like Pinned) at their prior
	// decoded value, since the wire omits them rather than sending false.
	var unpinned api.RefInfo
	request(t, ctx, conn, api.TUnpin, api.PinRequest{Name: "gc-api:target"}, api.TPinReply, &unpinned)
	if unpinned.Pinned {
		t.Fatalf("unpin reply still pinned: %+v", unpinned)
	}

	// manual gc runs a sweep and reports stats
	var gcStats api.GCStats
	request(t, ctx, conn, api.TGC, api.GCRequest{}, api.TGCReply, &gcStats)
	if gcStats.LastSweepNs == 0 || gcStats.RefCount == 0 {
		t.Fatalf("gc reply %+v", gcStats)
	}

	// stats carries the GC block
	var stats api.StatsReply
	request(t, ctx, conn, api.TStats, nil, api.TStatsReply, &stats)
	if stats.GC == nil || stats.GC.RetentionNs != int64(500*time.Millisecond) {
		t.Fatalf("stats.GC = %+v", stats.GC)
	}
}

func TestAdminGCDisabled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, dial := startServer(t, ctx) // GC off (zero Options)
	conn := dial(ALPNAdmin)

	stream := openRequest(t, ctx, conn, api.TGC, api.GCRequest{})
	defer stream.Close()
	typ, body, err := api.ReadFrame(stream)
	if err != nil {
		t.Fatal(err)
	}
	if typ != api.TError {
		t.Fatalf("frame %q, want error", typ)
	}
	var e api.Error
	if err := api.DecodeBody(body, &e); err != nil {
		t.Fatal(err)
	}
	if e.Code != api.CodeUnavailable {
		t.Fatalf("code %q, want unavailable", e.Code)
	}

	var stats api.StatsReply
	request(t, ctx, conn, api.TStats, nil, api.TStatsReply, &stats)
	if stats.GC != nil {
		t.Fatalf("GC block on a GC-less server: %+v", stats.GC)
	}
}

// A server whose refaccess.cbor exists but will not load keeps serving with
// its GC halted (issue #22): admin gc, pin and unpin answer unavailable, and
// stats carries the halt in the GC block and as a warning — instead of a
// silent restart from an empty access record that drops every pin.
func TestAdminGCHaltedOnUnreadableSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "refaccess.cbor"), []byte("not a snapshot"), 0o600); err != nil {
		t.Fatal(err)
	}
	srv, _, dial := startGCServerIn(t, ctx, dir)
	conn := dial(ALPNAdmin)
	k, err := srv.Store.IngestFile(ctx, []byte("halted fixture"))
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Store.PutRef(ctx, "gc-api:halted", k); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		typ  string
		body any
	}{
		{api.TGC, api.GCRequest{}},
		{api.TPin, api.PinRequest{Name: "gc-api:halted"}},
		{api.TUnpin, api.PinRequest{Name: "gc-api:halted"}},
	} {
		var e api.Error
		request(t, ctx, conn, tc.typ, tc.body, api.TError, &e)
		if e.Code != api.CodeUnavailable || !strings.Contains(e.Text, "gc halted") {
			t.Fatalf("%s while halted: %+v, want unavailable/gc halted", tc.typ, e)
		}
	}
	var stats api.StatsReply
	request(t, ctx, conn, api.TStats, nil, api.TStatsReply, &stats)
	if stats.GC == nil || stats.GC.Halted == "" {
		t.Fatalf("stats.GC = %+v, want Halted set", stats.GC)
	}
	if len(stats.Warnings) == 0 || !strings.Contains(stats.Warnings[0], "GC halted") {
		t.Fatalf("stats.Warnings = %q", stats.Warnings)
	}
	if b, err := os.ReadFile(filepath.Join(dir, "refaccess.cbor")); err != nil || string(b) != "not a snapshot" {
		t.Fatalf("snapshot rewritten while halted: %q %v", b, err)
	}
}
