package clientcli

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jobs-build/jobs-iroh/api"
	"github.com/jobs-build/jobs-iroh/wire"
)

// nodeName fabricates a parseable node name: the repeated byte is chosen so
// the 32-byte key is canonical (type ≤ 4, reserved bit clear, minimal length
// field), which shortNode needs to render kind:hex8. i must be < 40.
func nodeName(kind string, i int) string {
	b := byte(i/8<<4 | i%8)
	return kind + "_" + strings.Repeat(fmt.Sprintf("%02x", b), 32)
}

// hex8 is the shortNode key prefix for nodeName(_, i).
func hex8(i int) string {
	return strings.Repeat(fmt.Sprintf("%02x", byte(i/8<<4|i%8)), 4)
}

// TestSnapshotBlockMapping: header counts, running rows (runner + server
// elapsed), queued marker, failed rows with summaries.
func TestSnapshotBlockMapping(t *testing.T) {
	snap := api.Snapshot{
		Phase:  "running",
		Counts: wire.Counts{Total: 9, Done: 3, Running: 2, Failed: 1, Waiting: 3},
		Nodes: []api.NodeSnap{
			{Node: nodeName("buildrun", 9), Phase: wire.PhaseRunning, Runner: "runner-1", ElapsedMs: 32_000},
			{Node: nodeName("import", 10), Phase: wire.PhaseQueued},
			{Node: nodeName("buildfrom", 11), Phase: wire.PhaseFailed, ErrSummary: "exit 2: compile error"},
			{Node: nodeName("pin", 12), Phase: wire.PhaseWaiting},
			{Node: nodeName("import", 13), Phase: wire.PhaseDone},
		},
	}
	lines := snapshotBlock(snap, 45*time.Second)

	if lines[0] != "[build] running · 3/9 done · 2 running · 1 failed · 45s" {
		t.Fatalf("header = %q", lines[0])
	}
	joined := strings.Join(lines, "\n")
	wantRun := "  ▶ buildrun:" + hex8(9) + " · runner-1 · 32s"
	if !strings.Contains(joined, wantRun) {
		t.Fatalf("running row missing %q:\n%s", wantRun, joined)
	}
	if !strings.Contains(joined, "▶ import:"+hex8(10)+" · queued") {
		t.Fatalf("queued row missing:\n%s", joined)
	}
	if !strings.Contains(joined, "  ✗ buildfrom:"+hex8(11)+" · exit 2: compile error") {
		t.Fatalf("failed row missing:\n%s", joined)
	}
	for _, absent := range []string{"pin:", "import:" + hex8(13)} {
		if strings.Contains(joined, absent) {
			t.Fatalf("waiting/done nodes must not get rows (%q):\n%s", absent, joined)
		}
	}
}

// TestSnapshotBlockCapsRows: fan-outs beyond the row caps collapse into
// "… and N more" lines instead of filling the screen.
func TestSnapshotBlockCapsRows(t *testing.T) {
	snap := api.Snapshot{Phase: "running"}
	for i := 0; i < maxRunningRows+3; i++ {
		snap.Nodes = append(snap.Nodes, api.NodeSnap{
			Node: nodeName("import", i), Phase: wire.PhaseRunning,
		})
	}
	for i := 0; i < maxFailedRows+2; i++ {
		snap.Nodes = append(snap.Nodes, api.NodeSnap{
			Node: nodeName("buildrun", 20+i), Phase: wire.PhaseFailed,
		})
	}
	lines := snapshotBlock(snap, time.Second)
	joined := strings.Join(lines, "\n")
	if !strings.Contains(joined, "  … and 3 more\n") {
		t.Fatalf("running overflow line missing:\n%s", joined)
	}
	if !strings.HasSuffix(joined, "  … and 2 more failed") {
		t.Fatalf("failed overflow line missing:\n%s", joined)
	}
	// header + 8 running + overflow + 4 failed + overflow
	if len(lines) != 1+maxRunningRows+1+maxFailedRows+1 {
		t.Fatalf("row count = %d:\n%s", len(lines), joined)
	}
}

// TestWatchSummaryVerdicts: DONE/FAILED verdicts, upper-cased, painted only
// on a color view.
func TestWatchSummaryVerdicts(t *testing.T) {
	plain := &liveView{}
	if got := watchSummary(plain, api.Snapshot{Phase: "done"}, 5*time.Second); got != "build: DONE · 5s" {
		t.Fatalf("done summary = %q", got)
	}
	if got := watchSummary(plain, api.Snapshot{Phase: "failed"}, time.Second); got != "build: FAILED · 1s" {
		t.Fatalf("failed summary = %q", got)
	}
	if got := watchSummary(plain, api.Snapshot{Phase: "cancelled"}, time.Second); got != "build: CANCELLED · 1s" {
		t.Fatalf("cancelled summary = %q", got)
	}
	color := &liveView{isTTY: true, color: true}
	if got := watchSummary(color, api.Snapshot{Phase: "failed"}, time.Second); !strings.Contains(got, "\x1b[31mFAILED\x1b[0m") {
		t.Fatalf("color verdict not painted: %q", got)
	}
}

// TestFailureSummaryFirstHardFailure: the first hard failure with a summary
// wins; failed-upstream derivations are skipped.
func TestFailureSummaryFirstHardFailure(t *testing.T) {
	snap := api.Snapshot{Nodes: []api.NodeSnap{
		{Node: nodeName("buildvalue", 1), Phase: wire.PhaseUpstream, ErrSummary: "upstream"},
		{Node: nodeName("buildrun", 2), Phase: wire.PhaseFailed, ErrSummary: "exit 1: boom"},
	}}
	want := "buildrun:" + hex8(2) + ": exit 1: boom"
	if got := failureSummary(snap); got != want {
		t.Fatalf("failureSummary = %q, want %q", got, want)
	}
	if got := failureSummary(api.Snapshot{}); got != "" {
		t.Fatalf("empty snapshot must yield no summary: %q", got)
	}
}

// fakeLogs backs printFailureLogs in tests.
type fakeLogs map[string]api.LogView

func (f fakeLogs) fetchLogs(_ context.Context, node string) (api.LogView, error) {
	v, ok := f[node]
	if !ok {
		return api.LogView{}, fmt.Errorf("no logs for %s", node)
	}
	return v, nil
}

// TestPrintFailureLogs: hard failures get a banner + head/gap/tail; nodes
// without stored output or with fetch errors degrade to a note; upstream
// failures are skipped entirely.
func TestPrintFailureLogs(t *testing.T) {
	hard := nodeName("buildrun", 3)
	bare := nodeName("import", 4)
	upstream := nodeName("buildvalue", 5)
	snap := api.Snapshot{Nodes: []api.NodeSnap{
		{Node: hard, Phase: wire.PhaseFailed, Gen: 2, ErrSummary: "exit 1: boom"},
		{Node: bare, Phase: wire.PhaseFailed, Gen: 1, ErrSummary: "fetch failed"},
		{Node: upstream, Phase: wire.PhaseUpstream, ErrSummary: "upstream"},
	}}
	logs := fakeLogs{
		hard: {Node: hard, Gen: 2, Head: []byte("compiling...\n"), GapSize: 512, Tail: []byte("error: boom\n")},
		bare: {Node: bare, Gen: 1},
	}

	var buf bytes.Buffer
	printFailureLogs(context.Background(), logs, snap, &buf, nil)
	out := buf.String()

	if !strings.Contains(out, "--- "+hard+" failed (gen 2): exit 1: boom") {
		t.Fatalf("hard failure banner missing:\n%s", out)
	}
	if !strings.Contains(out, "compiling...\n") ||
		!strings.Contains(out, "... [512 bytes omitted] ...") ||
		!strings.Contains(out, "error: boom\n") {
		t.Fatalf("head/gap/tail missing:\n%s", out)
	}
	if !strings.Contains(out, "(no captured output)") {
		t.Fatalf("empty-log note missing:\n%s", out)
	}
	if strings.Contains(out, upstream) {
		t.Fatalf("failed-upstream node must not be tailed:\n%s", out)
	}

	// A node whose output already streamed live (--logs) keeps its banner but
	// points at the scroll instead of re-printing the log body.
	buf.Reset()
	printFailureLogs(context.Background(), logs, snap, &buf, map[string]bool{hard: true})
	out = buf.String()
	if !strings.Contains(out, "--- "+hard+" failed (gen 2): exit 1: boom") {
		t.Fatalf("streamed node banner missing:\n%s", out)
	}
	if !strings.Contains(out, "(output streamed above)") {
		t.Fatalf("streamed-above note missing:\n%s", out)
	}
	if strings.Contains(out, "compiling...") {
		t.Fatalf("streamed node's log body must not repeat:\n%s", out)
	}
}

func TestWatchSummaryAndBlockNameTests(t *testing.T) {
	plain := &liveView{}
	snap := api.Snapshot{Phase: "done", Nodes: []api.NodeSnap{{Node: "testvalue_" + strings.Repeat("0", 64), Phase: wire.PhaseDone}}}
	if got := watchSummary(plain, snap, 5*time.Second); got != "test: DONE · 5s" {
		t.Fatalf("watchSummary(test) = %q", got)
	}
	if lines := snapshotBlock(snap, time.Second); !strings.HasPrefix(lines[0], "[test] done") {
		t.Fatalf("block header = %q", lines[0])
	}
}
