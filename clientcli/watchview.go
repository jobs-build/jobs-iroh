package clientcli

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jobs-build/jobs-iroh/api"
	"github.com/jobs-build/jobs-iroh/tui"
	"github.com/jobs-build/jobs-iroh/wire"
)

// The snapshot → terminal-view mapping for remote-build/watch: one pure
// function per mode (TTY block, non-TTY change-line) so rendering is unit-
// testable without a stream. The shapes mirror jobs' buildBlock /
// schedProgressLine.

// maxRunningRows caps the TTY block's running rows so a wide import fan-out
// can't fill the screen; the header count still tells the whole story.
const maxRunningRows = 8

// maxFailedRows caps the block's failure rows the same way.
const maxFailedRows = 4

// isActivePhase reports a node the user would call "in flight".
func isActivePhase(phase string) bool {
	switch phase {
	case wire.PhaseQueued, wire.PhaseRunning, wire.PhasePublishing:
		return true
	}
	return false
}

// snapshotBlock renders the TTY live block for one snapshot: a counts header
// (elapsed is the client's watch clock), the running rows with the SERVER-
// computed per-node elapsed (clock-skew safety), and any hard-failure rows
// with their error summaries.
func snapshotBlock(snap api.Snapshot, elapsed time.Duration) []string {
	c := snap.Counts
	head := fmt.Sprintf("[%s] %s · %d/%d done · %d running", tui.RequestKind(snap), snap.Phase, c.Done, c.Total, c.Running)
	if c.Failed > 0 {
		head += fmt.Sprintf(" · %d failed", c.Failed)
	}
	head += " · " + formatDur(elapsed)
	lines := []string{head}

	running := 0
	for _, n := range snap.Nodes {
		if !isActivePhase(n.Phase) {
			continue
		}
		running++
		if running > maxRunningRows {
			continue
		}
		l := "  ▶ " + labelNode(n.Node, n.Label)
		if n.Phase != wire.PhaseRunning {
			l += " · " + n.Phase
		}
		if n.Runner != "" {
			l += " · " + n.Runner
		}
		if n.ElapsedMs > 0 {
			l += " · " + formatDur(time.Duration(n.ElapsedMs)*time.Millisecond)
		}
		lines = append(lines, l)
	}
	if more := running - maxRunningRows; more > 0 {
		lines = append(lines, fmt.Sprintf("  … and %d more", more))
	}

	failed := 0
	for _, n := range snap.Nodes {
		if n.Phase != wire.PhaseFailed {
			continue
		}
		failed++
		if failed > maxFailedRows {
			continue
		}
		l := "  ✗ " + labelNode(n.Node, n.Label)
		if n.ErrSummary != "" {
			l += " · " + n.ErrSummary
		}
		lines = append(lines, l)
	}
	if more := failed - maxFailedRows; more > 0 {
		lines = append(lines, fmt.Sprintf("  … and %d more failed", more))
	}
	return lines
}

// snapshotChangeLine renders the non-TTY change-line for one snapshot:
// counts plus the sorted, capped in-flight node set. The caller dedupes
// against the previously printed line, so only actual changes append.
func snapshotChangeLine(snap api.Snapshot) string {
	c := snap.Counts
	line := fmt.Sprintf("progress: %d/%d done, %d running, %d waiting", c.Done, c.Total, c.Running, c.Waiting)
	if c.Failed > 0 {
		line += fmt.Sprintf(", %d failed", c.Failed)
	}
	var names []string
	for _, n := range snap.Nodes {
		if isActivePhase(n.Phase) {
			names = append(names, labelNode(n.Node, n.Label))
		}
	}
	sort.Strings(names)
	if len(names) > 4 {
		names = append(names[:4], "…")
	}
	if len(names) > 0 {
		line += " · " + strings.Join(names, ", ")
	}
	return line
}

// watchSummary renders the terminal snapshot's permanent one-line summary
// (the Collapse line), color-coding the verdict on a color TTY.
func watchSummary(lv *liveView, snap api.Snapshot, elapsed time.Duration) string {
	verdict := strings.ToUpper(snap.Phase)
	switch snap.Phase {
	case "done":
		verdict = lv.paint(sgrGreen, "DONE")
	case "failed":
		verdict = lv.paint(sgrRed, "FAILED")
	}
	return fmt.Sprintf("%s: %s · %s", tui.RequestKind(snap), verdict, formatDur(elapsed))
}

// failureSummary extracts the first hard failure's one-line summary
// (jobs' schedFailureSummary). A request-level failure (no-runners
// watchdog) has no failed node — Snapshot.Error carries it instead.
func failureSummary(snap api.Snapshot) string {
	for _, n := range snap.Nodes {
		if n.Phase == wire.PhaseFailed && n.ErrSummary != "" {
			return labelNode(n.Node, n.Label) + ": " + n.ErrSummary
		}
	}
	return snap.Error
}
