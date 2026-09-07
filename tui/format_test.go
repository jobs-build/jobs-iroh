package tui

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/amber-store/core/key"

	"github.com/jobs-build/jobs-iroh/api"
	"github.com/jobs-build/jobs-iroh/sandbox"
	"github.com/jobs-build/jobs-iroh/wire"
)

// testKey mints a canonical key from a seed (ParseNodeName rejects
// non-canonical hex, so fixtures must go through key.New).
func testKey(t *testing.T, seed byte) key.Key {
	t.Helper()
	k, err := key.New(0, 1, []byte{seed})
	if err != nil {
		t.Fatalf("key.New: %v", err)
	}
	return k
}

// TestMain calls sandbox.Init() first (CLAUDE.md sandbox re-exec rule) —
// a no-op for these pure tests, mandatory the moment any sandbox-driving
// test lands in this binary.
func TestMain(m *testing.M) {
	sandbox.Init()
	os.Exit(m.Run())
}

func TestPadCell(t *testing.T) {
	for _, tc := range []struct {
		in   string
		w    int
		want string
	}{
		{"abc", 5, "abc  "},
		{"abcdef", 5, "abcd…"},
		{"abcde", 5, "abcde"},
		{"", 3, "   "},
		{"abc", 0, ""},
		{"abc", 1, "…"},
		{"héllo!", 5, "héll…"}, // rune-, not byte-indexed
	} {
		if got := padCell(tc.in, tc.w); got != tc.want {
			t.Errorf("padCell(%q, %d) = %q, want %q", tc.in, tc.w, got, tc.want)
		}
	}
}

func TestHumanBytes(t *testing.T) {
	for _, tc := range []struct {
		n    int64
		want string
	}{
		{0, "0 B"},
		{999, "999 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{5 << 20, "5.0 MiB"},
		{3 << 30, "3.0 GiB"},
		{1 << 40, "1.0 TiB"},
	} {
		if got := humanBytes(tc.n); got != tc.want {
			t.Errorf("humanBytes(%d) = %q, want %q", tc.n, got, tc.want)
		}
	}
}

func TestHumanAge(t *testing.T) {
	for _, tc := range []struct {
		d    time.Duration
		want string
	}{
		{-5 * time.Second, "0s"},
		{42 * time.Second, "42s"},
		{3*time.Minute + 12*time.Second, "3m12s"},
		{4*time.Hour + 7*time.Minute, "4h07m"},
		{49*time.Hour + 30*time.Minute, "2d01h"},
	} {
		if got := humanAge(tc.d); got != tc.want {
			t.Errorf("humanAge(%v) = %q, want %q", tc.d, got, tc.want)
		}
	}
}

func TestShortKeyAndNode(t *testing.T) {
	k := []byte{0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89}
	if got := shortKey(k); got != "abcdef012345" {
		t.Errorf("shortKey = %q, want first 12 hex chars", got)
	}
	if got := shortKey([]byte{0xab}); got != "ab" {
		t.Errorf("shortKey(short) = %q", got)
	}
	nk := testKey(t, 7)
	name := wire.NodeName(wire.KindBuildRun, nk)
	if want := "buildrun:" + nk.String()[:8]; shortNode(name) != want {
		t.Errorf("shortNode(%q) = %q, want %q", name, shortNode(name), want)
	}
	// Unparseable names pass through untouched.
	if got := shortNode("garbage"); got != "garbage" {
		t.Errorf("shortNode(garbage) = %q", got)
	}
}

func TestWindow(t *testing.T) {
	for _, tc := range []struct {
		total, height, cursor, top, want int
	}{
		{10, 20, 5, 0, 0}, // fits: no scroll
		{10, 4, 0, 0, 0},  // top
		{10, 4, 5, 0, 2},  // cursor below window
		{10, 4, 5, 5, 5},  // cursor at top of window
		{10, 4, 2, 5, 2},  // cursor above window pulls it up
		{10, 4, 9, 0, 6},  // bottom clamps to total-height
		{10, 4, 9, 99, 6}, // absurd top clamps
		{3, 0, 1, 0, 0},   // degenerate height
		{0, 4, 0, 0, 0},   // empty
	} {
		if got := window(tc.total, tc.height, tc.cursor, tc.top); got != tc.want {
			t.Errorf("window(%d,%d,%d,%d) = %d, want %d",
				tc.total, tc.height, tc.cursor, tc.top, got, tc.want)
		}
	}
}

func TestClamp(t *testing.T) {
	for _, tc := range []struct{ v, n, want int }{
		{5, 3, 2}, {-1, 3, 0}, {1, 3, 1}, {0, 0, 0}, {5, 0, 0},
	} {
		if got := clamp(tc.v, tc.n); got != tc.want {
			t.Errorf("clamp(%d,%d) = %d, want %d", tc.v, tc.n, got, tc.want)
		}
	}
}

func TestRequestRow(t *testing.T) {
	now := time.Unix(1000, 0)
	r := api.RequestInfo{
		RequestID: "deadbeef01234567",
		K:         []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07},
		Phase:     "running",
		Counts:    wire.Counts{Total: 40, Done: 12, Running: 3, Failed: 1},
		CreatedNs: time.Unix(940, 0).UnixNano(),
	}
	got := requestRow(r, now)
	want := []string{"deadbeef01234567", "010203040506", "running", "12/40 run 3 fail 1", "1m00s"}
	if len(got) != len(want) {
		t.Fatalf("requestRow len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("requestRow[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	if len(got) != len(buildCols) {
		t.Errorf("row width %d != column count %d", len(got), len(buildCols))
	}
}

func TestRunnerRow(t *testing.T) {
	now := time.Unix(1000, 0)
	r := wire.RunnerInfo{
		Name: "runner-a", Platform: "linux/amd64", Size: "l",
		InFlight: 2, FreeCPU: 1500, FreeMem: 2 << 30,
		SeenNs: time.Unix(998, 0).UnixNano(),
	}
	got := runnerRow(r, now)
	want := []string{"runner-a", "linux/amd64", "l", "2", "1500m", "2.0 GiB", "2s"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("runnerRow[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	if len(got) != len(fleetCols) {
		t.Errorf("row width %d != column count %d", len(got), len(fleetCols))
	}
}

func TestRefRow(t *testing.T) {
	now := time.Unix(1000, 0)
	r := api.RefInfo{
		Name:      "build-output:abc",
		Key:       []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00},
		CreatedNs: time.Unix(700, 0).UnixNano(),
	}
	got := refRow(r, now)
	want := []string{"build-output:abc", "aabbccddeeff", "5m00s", "-", ""}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("refRow[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	if len(got) != len(refHeader) {
		t.Errorf("row width %d != header count %d", len(got), len(refHeader))
	}

	r.LastAccessNs = time.Unix(900, 0).UnixNano()
	r.Pinned = true
	got = refRow(r, now)
	want = []string{"build-output:abc", "aabbccddeeff", "5m00s", "1m40s", "pin"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("refRow[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestJoinCellsWidths(t *testing.T) {
	got := joinCells([]string{"ab", "cdefgh"}, []int{4, 3})
	if got != "ab    cd…" {
		t.Errorf("joinCells = %q", got)
	}
}

func TestStatsLines(t *testing.T) {
	lines := statsLines(api.StatsReply{
		StoreBytes:   3 << 30,
		RefCount:     42,
		Requests:     2,
		NodesTracked: 17,
		UptimeNs:     (90 * time.Minute).Nanoseconds(),
	})
	joined := strings.Join(lines, "\n")
	for _, want := range []string{"3.0 GiB", "42", "requests       2", "nodes tracked  17", "1h30m"} {
		if !strings.Contains(joined, want) {
			t.Errorf("statsLines missing %q in:\n%s", want, joined)
		}
	}
	if strings.Contains(joined, "gc ") {
		t.Errorf("statsLines rendered a GC block with nil GC:\n%s", joined)
	}
}

func TestStatsLinesGC(t *testing.T) {
	lines := statsLines(api.StatsReply{
		StoreBytes: 1 << 30,
		GC: &api.GCStats{
			RetentionNs:  int64(24 * time.Hour),
			LastSweepNs:  time.Now().Add(-5 * time.Minute).UnixNano(),
			ExpiredLast:  3,
			ExpiredTotal: 9,
			Pinned:       2,
			LiveBytes:    300,
			GarbageBytes: 100,
			LastError:    "boom",
		},
	})
	joined := strings.Join(lines, "\n")
	for _, want := range []string{
		"gc retention   24h0m0s",
		"ago",
		"expired 3, total 9",
		"gc garbage     100 B of 400 B (25.0%), pinned 2",
		"gc last error  boom",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("statsLines missing %q in:\n%s", want, joined)
		}
	}
}

func snapForDetail(t *testing.T) api.Snapshot {
	t.Helper()
	name := func(kind string, seed byte) string { return wire.NodeName(kind, testKey(t, seed)) }
	return api.Snapshot{
		RequestID: "r1",
		Phase:     "running",
		Counts:    wire.Counts{Total: 5, Done: 1, Running: 2, Failed: 1},
		Nodes: []api.NodeSnap{
			{Node: name(wire.KindBuildRun, 1), Phase: wire.PhaseDone},
			{Node: name(wire.KindBuildRun, 2), Phase: wire.PhaseRunning, ElapsedMs: 90_000, Runner: "r-a"},
			{Node: name(wire.KindImport, 3), Phase: wire.PhaseRunning, ElapsedMs: 1_000},
			{Node: name(wire.KindBuildFrom, 4), Phase: wire.PhaseFailed, ErrSummary: "exit status 2"},
			{Node: name(wire.KindPin, 5), Phase: wire.PhaseWaiting},
		},
	}
}

func TestDetailEntriesGroupingAndOrder(t *testing.T) {
	entries := detailEntries(snapForDetail(t), 0)

	// Expect groups in phaseOrder: failed, running, waiting, done — each a
	// header followed by its nodes.
	var headers []string
	for _, e := range entries {
		if e.header {
			headers = append(headers, e.phase)
		}
	}
	wantHeaders := []string{wire.PhaseFailed, wire.PhaseRunning, wire.PhaseWaiting, wire.PhaseDone}
	if len(headers) != len(wantHeaders) {
		t.Fatalf("headers = %v, want %v", headers, wantHeaders)
	}
	for i := range wantHeaders {
		if headers[i] != wantHeaders[i] {
			t.Fatalf("headers = %v, want %v", headers, wantHeaders)
		}
	}

	// Header text carries the group size.
	if entries[0].text != "failed (1)" {
		t.Errorf("failed header = %q", entries[0].text)
	}
	// The failed node line carries the error summary.
	if !strings.Contains(entries[1].text, "exit status 2") {
		t.Errorf("failed node line = %q", entries[1].text)
	}
	// Running nodes sort by name within the group (buildrun_ < import_).
	if want := "buildrun:" + testKey(t, 2).String()[:8]; !strings.HasPrefix(entries[3].text, want) {
		t.Errorf("first running node = %q, want prefix %q", entries[3].text, want)
	}
	// Elapsed is rendered with the extra tick-forward added.
	entries2 := detailEntries(snapForDetail(t), 30*time.Second)
	if !strings.Contains(entries2[3].text, "2m00s") { // 90s + 30s
		t.Errorf("running node elapsed = %q", entries2[3].text)
	}
	if !strings.Contains(entries2[3].text, "on r-a") {
		t.Errorf("running node runner = %q", entries2[3].text)
	}
}

func TestDetailCursorNavigation(t *testing.T) {
	entries := detailEntries(snapForDetail(t), 0)
	first := firstEntry(entries)
	if first != 1 { // 0 is the "failed" header
		t.Fatalf("firstEntry = %d", first)
	}
	// Moving down from the last selectable stays put.
	last := len(entries) - 1
	if entries[last].header {
		t.Fatalf("expected last entry selectable")
	}
	if got := nextEntry(entries, last, +1); got != last {
		t.Errorf("nextEntry past end = %d, want %d", got, last)
	}
	// Moving down from first skips the next header.
	down := nextEntry(entries, first, +1)
	if entries[down].header {
		t.Errorf("nextEntry landed on header at %d", down)
	}
	// Moving up from first stays.
	if got := nextEntry(entries, first, -1); got != first {
		t.Errorf("nextEntry before start = %d, want %d", got, first)
	}
	// selectedNode returns the full node name, never a header.
	if selectedNode(entries, 0) != "" {
		t.Errorf("selectedNode(header) != \"\"")
	}
	if n := selectedNode(entries, first); !strings.HasPrefix(n, "buildfrom_") {
		t.Errorf("selectedNode(first) = %q", n)
	}
	if selectedNode(nil, 0) != "" {
		t.Errorf("selectedNode(empty) != \"\"")
	}
}

func TestLogAssembly(t *testing.T) {
	view := api.LogView{
		Node: "n", Gen: 3,
		Head:    []byte("head\n"),
		GapSize: 2048,
		Tail:    []byte("tail\n"),
	}
	buf := logInitial(view)
	if !strings.Contains(buf, "head\n") || !strings.Contains(buf, "tail\n") {
		t.Fatalf("logInitial = %q", buf)
	}
	if !strings.Contains(buf, "2.0 KiB omitted") {
		t.Errorf("gap marker missing: %q", buf)
	}
	// No gap marker when nothing was omitted.
	if s := logInitial(api.LogView{Head: []byte("x")}); strings.Contains(s, "omitted") {
		t.Errorf("unexpected gap marker: %q", s)
	}

	buf, gen := logAppend(buf, view.Gen, wire.LogChunk{Gen: 3, Data: []byte("more\n")})
	if gen != 3 || !strings.HasSuffix(buf, "more\n") {
		t.Fatalf("append same gen: gen=%d buf=%q", gen, buf)
	}
	// A new generation inserts a retry marker.
	buf, gen = logAppend(buf, gen, wire.LogChunk{Gen: 4, Data: []byte("retry out\n")})
	if gen != 4 || !strings.Contains(buf, "retry: gen 4") {
		t.Fatalf("append new gen: gen=%d buf=%q", gen, buf)
	}
	// The buffer is capped from the front.
	big, _ := logAppend("", 1, wire.LogChunk{Gen: 1, Data: make([]byte, maxLogBuf+100)})
	if len(big) != maxLogBuf {
		t.Errorf("cap: len=%d want %d", len(big), maxLogBuf)
	}
}

func TestRouteKey(t *testing.T) {
	for _, tc := range []struct {
		key      string
		captures bool
		want     keyAction
		wantTab  int
	}{
		{"ctrl+c", false, actQuit, 0},
		{"ctrl+c", true, actQuit, 0}, // always quits
		{"q", false, actQuit, 0},
		{"q", true, actNone, 0}, // captured views own q (back/edit)
		{"tab", false, actNext, 0},
		{"tab", true, actNone, 0},
		{"shift+tab", false, actPrev, 0},
		{"1", false, actSelect, 0},
		{"4", false, actSelect, 3},
		{"4", true, actNone, 0},
		{"j", false, actNone, 0},
		{"enter", false, actNone, 0},
	} {
		got, tab := routeKey(tc.key, tc.captures)
		if got != tc.want || tab != tc.wantTab {
			t.Errorf("routeKey(%q, captures=%v) = (%v, %d), want (%v, %d)",
				tc.key, tc.captures, got, tab, tc.want, tc.wantTab)
		}
	}
}

func TestPhaseStyleTotal(t *testing.T) {
	// Every wire phase has a style; unknown phases fall back cleanly.
	for _, p := range phaseOrder {
		_ = phaseStyle(p)
	}
	_ = phaseStyle("no-such-phase")
}
