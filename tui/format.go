package tui

import (
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/jobs-build/jobs-iroh/api"
	"github.com/jobs-build/jobs-iroh/wire"
)

// --- styles ---

var (
	styleTabActive = lipgloss.NewStyle().Bold(true).Reverse(true).Padding(0, 1)
	styleTab       = lipgloss.NewStyle().Faint(true).Padding(0, 1)
	styleHeader    = lipgloss.NewStyle().Bold(true).Underline(true)
	styleSelected  = lipgloss.NewStyle().Reverse(true)
	styleFaint     = lipgloss.NewStyle().Faint(true)
	styleTitle     = lipgloss.NewStyle().Bold(true)
	styleError     = lipgloss.NewStyle().Foreground(lipgloss.Color("9")).Bold(true)
	styleFlash     = lipgloss.NewStyle().Foreground(lipgloss.Color("10"))

	phaseStyles = map[string]lipgloss.Style{
		wire.PhaseWaiting:    lipgloss.NewStyle().Faint(true),
		wire.PhaseQueued:     lipgloss.NewStyle().Foreground(lipgloss.Color("12")), // blue
		wire.PhaseRunning:    lipgloss.NewStyle().Foreground(lipgloss.Color("11")), // yellow
		wire.PhasePublishing: lipgloss.NewStyle().Foreground(lipgloss.Color("14")), // cyan
		wire.PhaseDone:       lipgloss.NewStyle().Foreground(lipgloss.Color("10")), // green
		wire.PhaseFailed:     lipgloss.NewStyle().Foreground(lipgloss.Color("9")),  // red
		wire.PhaseUpstream:   lipgloss.NewStyle().Foreground(lipgloss.Color("1")),  // dark red
		wire.PhaseCancelled:  lipgloss.NewStyle().Foreground(lipgloss.Color("13")), // magenta
	}
)

// phaseStyle colors one phase name (request phases share the node palette).
func phaseStyle(phase string) lipgloss.Style {
	if s, ok := phaseStyles[phase]; ok {
		return s
	}
	return lipgloss.NewStyle()
}

// --- plain-text cell helpers (styling happens after padding, so ANSI never
// breaks column widths) ---

// padCell truncates s to w cells (… marker) and right-pads to exactly w.
func padCell(s string, w int) string {
	if w <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) > w {
		if w == 1 {
			return "…"
		}
		return string(r[:w-1]) + "…"
	}
	return s + strings.Repeat(" ", w-len(r))
}

// humanBytes renders n in binary units, one decimal (0 B, 3.4 MiB, 1.2 GiB).
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// humanAge renders a duration compactly with its two leading units
// (42s, 3m12s, 4h07m, 2d13h). Negative (clock skew) clamps to 0s.
func humanAge(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
	default:
		return fmt.Sprintf("%dd%02dh", int(d.Hours())/24, int(d.Hours())%24)
	}
}

// ageAt renders the age of a UnixNano timestamp at now.
func ageAt(now time.Time, ns int64) string {
	return humanAge(now.Sub(time.Unix(0, ns)))
}

// shortKey renders a key's first 12 hex chars.
func shortKey(b []byte) string {
	s := hex.EncodeToString(b)
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

// shortNode renders a node name as kind:key8.
func shortNode(name string) string {
	kind, k, err := wire.ParseNodeName(name)
	if err != nil {
		return name
	}
	return kind + ":" + k.String()[:8]
}

// fmtCounts renders a request's closure counters.
func fmtCounts(c wire.Counts) string {
	return fmt.Sprintf("%d/%d run %d fail %d", c.Done, c.Total, c.Running, c.Failed)
}

// window returns the new top offset keeping cursor visible in height rows,
// clamped so the window never scrolls past the end.
func window(total, height, cursor, top int) int {
	if height <= 0 || total <= height {
		return 0
	}
	if top > cursor {
		top = cursor
	}
	if cursor >= top+height {
		top = cursor - height + 1
	}
	if top > total-height {
		top = total - height
	}
	if top < 0 {
		top = 0
	}
	return top
}

// clamp bounds v to [0, n-1] (cursor after a shrinking refresh).
func clamp(v, n int) int {
	if n <= 0 {
		return 0
	}
	if v < 0 {
		return 0
	}
	if v >= n {
		return n - 1
	}
	return v
}

// --- rows (pure; views apply styles on top) ---

// buildCols are the builds-table column widths (id, k, phase, counts, age).
var buildCols = []int{16, 12, 11, 24, 8}

var buildHeader = []string{"REQUEST", "K", "PHASE", "NODES", "AGE"}

// requestRow builds one builds-table row.
func requestRow(r api.RequestInfo, now time.Time) []string {
	return []string{
		r.RequestID,
		shortKey(r.K),
		r.Phase,
		fmtCounts(r.Counts),
		ageAt(now, r.CreatedNs),
	}
}

// fleetCols are the fleet-table column widths.
var fleetCols = []int{18, 14, 6, 8, 8, 9, 8}

var fleetHeader = []string{"RUNNER", "PLATFORM", "SIZE", "INFLIGHT", "FREECPU", "FREEMEM", "SEEN"}

// runnerRow builds one fleet-table row.
func runnerRow(r wire.RunnerInfo, now time.Time) []string {
	return []string{
		r.Name,
		r.Platform,
		r.Size,
		fmt.Sprintf("%d", r.InFlight),
		fmt.Sprintf("%dm", r.FreeCPU),
		humanBytes(r.FreeMem),
		ageAt(now, r.SeenNs),
	}
}

// refCols are the refs-table column widths (name gets the slack; see
// refsModel.view which widens column 0 to the pane).
var refCols = []int{48, 14, 8, 8, 4}

var refHeader = []string{"NAME", "KEY", "AGE", "ACCESS", "PIN"}

// refRow builds one refs-table row.
func refRow(r api.RefInfo, now time.Time) []string {
	access, pin := "-", ""
	if r.LastAccessNs > 0 {
		access = ageAt(now, r.LastAccessNs)
	}
	if r.Pinned {
		pin = "pin"
	}
	return []string{
		r.Name,
		shortKey(r.Key),
		ageAt(now, r.CreatedNs),
		access,
		pin,
	}
}

// lipglossWidth is the ANSI-aware cell width of s.
func lipglossWidth(s string) int { return lipgloss.Width(s) }

// joinCells pads each cell to its column width and joins with two spaces.
func joinCells(cells []string, widths []int) string {
	parts := make([]string, len(cells))
	for i, c := range cells {
		parts[i] = padCell(c, widths[i])
	}
	return strings.Join(parts, "  ")
}

// statsLines renders the stats view body.
func statsLines(st api.StatsReply) []string {
	lines := []string{
		fmt.Sprintf("store used     %s (%d bytes)", humanBytes(st.StoreBytes), st.StoreBytes),
		fmt.Sprintf("refs           %d", st.RefCount),
		fmt.Sprintf("requests       %d", st.Requests),
		fmt.Sprintf("nodes tracked  %d", st.NodesTracked),
		fmt.Sprintf("uptime         %s", humanAge(time.Duration(st.UptimeNs))),
	}
	if g := st.GC; g != nil {
		pct := 0.0
		if tot := g.LiveBytes + g.GarbageBytes; tot > 0 {
			pct = 100 * float64(g.GarbageBytes) / float64(tot)
		}
		sweep := "never"
		if g.LastSweepNs != 0 {
			sweep = humanAge(time.Since(time.Unix(0, g.LastSweepNs))) + " ago"
		}
		lines = append(lines,
			fmt.Sprintf("gc retention   %s", time.Duration(g.RetentionNs)),
			fmt.Sprintf("gc sweep       %s (expired %d, total %d)", sweep, g.ExpiredLast, g.ExpiredTotal),
			fmt.Sprintf("gc garbage     %s of %s (%.1f%%), pinned %d",
				humanBytes(g.GarbageBytes), humanBytes(g.LiveBytes+g.GarbageBytes), pct, g.Pinned),
		)
		if g.LastError != "" {
			lines = append(lines, "gc last error  "+g.LastError)
		}
	}
	for _, w := range st.Warnings {
		lines = append(lines, "WARNING        "+w)
	}
	return lines
}

// --- build detail entries ---

// phaseOrder is the detail grouping order: problems first, activity next,
// the waiting tail, finished last.
var phaseOrder = []string{
	wire.PhaseFailed,
	wire.PhaseUpstream,
	wire.PhaseRunning,
	wire.PhasePublishing,
	wire.PhaseQueued,
	wire.PhaseWaiting,
	wire.PhaseCancelled,
	wire.PhaseDone,
}

// detailEntry is one rendered line of the build-detail node list: either a
// phase group header or a selectable node line.
type detailEntry struct {
	header bool
	text   string
	node   string // full node name when !header (log fetch target)
	phase  string
}

// detailEntries flattens a snapshot into group headers + node lines, sorted
// by phase group then node name. extra is added to running/publishing
// elapsed times (time since the snapshot arrived — the server computes
// ElapsedMs at snapshot time, the client keeps it ticking between pushes).
func detailEntries(snap api.Snapshot, extra time.Duration) []detailEntry {
	groups := map[string][]api.NodeSnap{}
	for _, n := range snap.Nodes {
		groups[n.Phase] = append(groups[n.Phase], n)
	}
	var out []detailEntry
	appendGroup := func(phase string, nodes []api.NodeSnap) {
		sort.Slice(nodes, func(i, j int) bool { return nodes[i].Node < nodes[j].Node })
		out = append(out, detailEntry{
			header: true,
			phase:  phase,
			text:   fmt.Sprintf("%s (%d)", phase, len(nodes)),
		})
		for _, n := range nodes {
			out = append(out, detailEntry{
				node:  n.Node,
				phase: n.Phase,
				text:  nodeLine(n, extra),
			})
		}
	}
	for _, phase := range phaseOrder {
		if nodes := groups[phase]; len(nodes) > 0 {
			appendGroup(phase, nodes)
		}
	}
	// Unknown phases (forward compat) trail in name order.
	var rest []string
	for phase := range groups {
		if !knownPhase(phase) {
			rest = append(rest, phase)
		}
	}
	sort.Strings(rest)
	for _, phase := range rest {
		appendGroup(phase, groups[phase])
	}
	return out
}

func knownPhase(phase string) bool {
	for _, p := range phaseOrder {
		if p == phase {
			return true
		}
	}
	return false
}

// nodeLine renders one node: short name, live elapsed for in-flight nodes,
// runner placement, and the error summary on failures.
func nodeLine(n api.NodeSnap, extra time.Duration) string {
	s := padCell(shortNode(n.Node), 22)
	switch n.Phase {
	case wire.PhaseRunning, wire.PhasePublishing:
		s += "  " + padCell(humanAge(time.Duration(n.ElapsedMs)*time.Millisecond+extra), 7)
		if n.Runner != "" {
			s += "  on " + n.Runner
		}
	case wire.PhaseFailed:
		if n.ErrSummary != "" {
			s += "  " + n.ErrSummary
		}
	}
	return s
}

// nextEntry moves a detail cursor by delta, skipping headers; it returns
// cur unchanged when no selectable entry lies in that direction.
func nextEntry(entries []detailEntry, cur, delta int) int {
	for i := cur + delta; i >= 0 && i < len(entries); i += delta {
		if !entries[i].header {
			return i
		}
	}
	return cur
}

// firstEntry returns the first selectable index (-1 if none).
func firstEntry(entries []detailEntry) int {
	for i, e := range entries {
		if !e.header {
			return i
		}
	}
	return -1
}

// selectedNode returns the node name under the cursor ("" when none).
func selectedNode(entries []detailEntry, cur int) string {
	if cur < 0 || cur >= len(entries) || entries[cur].header {
		return ""
	}
	return entries[cur].node
}

// --- log assembly ---

// maxLogBuf caps the assembled log text; older output is trimmed from the
// front (the server already stores head/gap/tail, this only bounds follow
// growth).
const maxLogBuf = 1 << 20

// logInitial assembles the stored head/gap/tail of a LogView.
func logInitial(view api.LogView) string {
	var b strings.Builder
	b.Write(view.Head)
	if view.GapSize > 0 {
		fmt.Fprintf(&b, "\n··· %s omitted ···\n", humanBytes(view.GapSize))
	}
	b.Write(view.Tail)
	return b.String()
}

// logAppend appends one follow chunk, marking generation restarts, and
// trims the front past maxLogBuf.
func logAppend(buf string, gen uint64, chunk wire.LogChunk) (string, uint64) {
	if chunk.Gen != gen && gen != 0 {
		buf += fmt.Sprintf("\n··· retry: gen %d ···\n", chunk.Gen)
	}
	if chunk.Gen > gen || gen == 0 {
		gen = chunk.Gen
	}
	buf += string(chunk.Data)
	if len(buf) > maxLogBuf {
		buf = buf[len(buf)-maxLogBuf:]
	}
	return buf, gen
}

// --- key routing (root) ---

// keyAction classifies a top-level key press. Pure, so routing is testable
// without the event loop.
type keyAction int

const (
	actNone   keyAction = iota // forward to the active view
	actQuit                    // quit the program
	actNext                    // next tab
	actPrev                    // previous tab
	actSelect                  // jump to tab (keyRoute's second result)
)

// routeKey decides what a key press means at the root given whether the
// active view is capturing input (text entry, confirm prompt, detail/log
// sub-screens). ctrl+c always quits.
func routeKey(key string, captures bool) (keyAction, int) {
	if key == "ctrl+c" {
		return actQuit, 0
	}
	if captures {
		return actNone, 0
	}
	switch key {
	case "q":
		return actQuit, 0
	case "tab":
		return actNext, 0
	case "shift+tab":
		return actPrev, 0
	case "1", "2", "3", "4":
		return actSelect, int(key[0] - '1')
	}
	return actNone, 0
}
