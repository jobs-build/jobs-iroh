package tui

// The standalone build view (docs/design/2026-08-19-remote-build-tui.md):
// a full-screen bubbletea program over one request's snapshot watch — the
// folded build tree on the left (buildtree.go), the selected row's output
// on the right (stored head/gap/tail + live follow). remote-build and
// watch run it on a TTY; the admin TUI can adopt it for its detail view
// later. Network I/O never runs in Update — streams ride the cmds.go
// subscription pattern over the BuildStreams seam, so any transport
// (clientcli's apiConn, the admin client) can drive the view.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"

	"github.com/jobs-build/jobs-iroh/api"
	"github.com/jobs-build/jobs-iroh/wire"
)

// BuildStreams is the transport seam of the build view: how it (re)opens
// the request's snapshot watch, follows one node's output, and sends a
// cancel. Implementations must unblock a pending next() when stop is
// called.
type BuildStreams interface {
	OpenWatch(ctx context.Context) (next func() (api.Snapshot, error), stop func(), err error)
	OpenLogs(ctx context.Context, node string) (view api.LogView, next func() (wire.LogChunk, error), stop func(), err error)
	Cancel(ctx context.Context) error
}

// BuildWatchOptions configures one build view run.
type BuildWatchOptions struct {
	RequestID string
	Label     string // display-only target name for the header
}

// BuildOutcome is how the view ended.
type BuildOutcome struct {
	Final     api.Snapshot // the terminal snapshot when HaveFinal
	HaveFinal bool
	Detached  bool // the user quit while the build was still running
}

// RunBuildWatch drives the build view until a terminal outcome: auto-exit
// on done/cancelled, stay-for-inspection on failure (quit ends it), detach
// on q mid-build. Rendered to stderr — stdout stays machine-readable.
func RunBuildWatch(ctx context.Context, s BuildStreams, opts BuildWatchOptions) (BuildOutcome, error) {
	m := newBuildView(s, opts)
	p := tea.NewProgram(m, tea.WithAltScreen(), tea.WithContext(ctx), tea.WithOutput(os.Stderr))
	res, err := p.Run()
	if err != nil {
		if errors.Is(err, tea.ErrProgramKilled) && ctx.Err() != nil {
			return BuildOutcome{Detached: true}, nil // SIGTERM/ctx ≙ detach
		}
		return BuildOutcome{}, err
	}
	fm := res.(buildView)
	return fm.outcome, fm.fatal
}

// logSwitchDebounce delays the output-pane stream switch after a cursor
// move, so scrolling through rows doesn't open a stream per row transited.
const logSwitchDebounce = 250 * time.Millisecond

// bvTickMsg is the view's 1s clock: elapsed ticking + stream retries.
type bvTickMsg time.Time

func bvTick() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg { return bvTickMsg(t) })
}

// bvLogSelMsg fires the debounced log-stream switch (stale tokens drop).
type bvLogSelMsg struct{ token int }

// bvCancelDoneMsg reports the cancel frame's outcome.
type bvCancelDoneMsg struct{ err error }

// bvWatchCmd opens the watch stream and pumps snapshots into a channel
// (the cmds.go subscription pattern over the BuildStreams seam).
func bvWatchCmd(s BuildStreams, seq int) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithCancel(context.Background())
		next, stop, err := s.OpenWatch(ctx)
		if err != nil {
			cancel()
			return watchFailedMsg{seq: seq, err: err}
		}
		ch := make(chan watchEvent, 8)
		go func() {
			defer close(ch)
			defer stop()
			defer cancel()
			send := func(ev watchEvent) bool {
				select {
				case ch <- ev:
					return true
				case <-ctx.Done():
					return false
				}
			}
			for {
				snap, err := next()
				if err != nil {
					if ctx.Err() == nil {
						send(watchEvent{err: err})
					}
					return
				}
				if !send(watchEvent{snap: snap}) || snap.Terminal {
					return
				}
			}
		}()
		return watchStartedMsg{seq: seq, ch: ch, cancel: cancel}
	}
}

// bvLogsCmd opens one node's follow stream: stored view first, then chunks.
func bvLogsCmd(s BuildStreams, seq int, node string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithCancel(context.Background())
		view, next, stop, err := s.OpenLogs(ctx, node)
		if err != nil {
			cancel()
			return logFailedMsg{seq: seq, node: node, err: err}
		}
		ch := make(chan logEvent, 64)
		go func() {
			defer close(ch)
			defer stop()
			defer cancel()
			send := func(ev logEvent) bool {
				select {
				case ch <- ev:
					return true
				case <-ctx.Done():
					return false
				}
			}
			for {
				chunk, err := next()
				if err != nil {
					if ctx.Err() == nil {
						send(logEvent{err: err})
					}
					return
				}
				if !send(logEvent{chunk: chunk}) {
					return
				}
			}
		}()
		return logStartedMsg{seq: seq, node: node, view: view, ch: ch, cancel: cancel}
	}
}

// bvCancelCmd sends the request cancel.
func bvCancelCmd(s BuildStreams) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
		defer cancel()
		return bvCancelDoneMsg{err: s.Cancel(ctx)}
	}
}

// buildView is the model. Value semantics like the rest of the package.
type buildView struct {
	s     BuildStreams
	reqID string
	label string

	width, height int
	start         time.Time
	outputMax     bool

	// watch stream
	watchSeq     int
	watchCh      <-chan watchEvent
	watchCancel  func()
	watchErr     error
	watchPending bool

	snap     api.Snapshot
	snapAt   time.Time
	haveSnap bool
	graph    *BuildGraph

	// tree
	exp     map[string]bool
	visible []TreeRow
	cursor  int
	top     int

	// output pane
	logNode     string // node whose stream is open ("" = none)
	logSeq      int
	logCh       <-chan logEvent
	logCancel   func()
	logErr      error
	logPending  bool
	logBuf      string
	logGen      uint64
	follow      bool
	vp          viewport.Model
	logSelToken int

	confirm    bool // cancel confirm prompt showing
	cancelSent bool
	cancelErr  error
	terminal   bool // terminal snapshot received and staying (failure)

	outcome BuildOutcome
	fatal   error // terminal stream error (server-answered): Run returns it
}

func newBuildView(s BuildStreams, opts BuildWatchOptions) buildView {
	return buildView{
		s:      s,
		reqID:  opts.RequestID,
		label:  opts.Label,
		start:  time.Now(),
		exp:    map[string]bool{},
		follow: true,
		vp:     viewport.New(80, 20),
		// Init arms the first watch attempt for this seq.
		watchSeq:     1,
		watchPending: true,
	}
}

func (m buildView) Init() tea.Cmd {
	return tea.Batch(bvTick(), bvWatchCmd(m.s, m.watchSeq))
}

// retryWatch arms a fresh watch stream attempt.
func (m buildView) retryWatch() (buildView, tea.Cmd) {
	m.watchSeq++
	m.watchPending = true
	return m, bvWatchCmd(m.s, m.watchSeq)
}

func (m buildView) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.sizeViewport()
		return m, nil

	case tea.KeyMsg:
		return m.updateKey(msg)

	case bvTickMsg:
		var cmds []tea.Cmd
		if m.watchErr != nil && !m.watchPending && !m.terminal && !isServerError(m.watchErr) {
			var cmd tea.Cmd
			m, cmd = m.retryWatch()
			cmds = append(cmds, cmd)
		}
		if m.logErr != nil && !m.logPending && m.logNode != "" && !isServerError(m.logErr) {
			m.logSeq++
			m.logPending = true
			cmds = append(cmds, bvLogsCmd(m.s, m.logSeq, m.logNode))
		}
		cmds = append(cmds, bvTick())
		return m, tea.Batch(cmds...)

	// --- watch stream ---
	case watchStartedMsg:
		if msg.seq != m.watchSeq {
			msg.cancel()
			return m, nil
		}
		m.watchCh, m.watchCancel = msg.ch, msg.cancel
		m.watchErr, m.watchPending = nil, false
		return m, awaitWatchCmd(msg.seq, msg.ch)

	case watchFailedMsg:
		if msg.seq != m.watchSeq {
			return m, nil
		}
		m.watchPending = false
		return m.watchDown(msg.err)

	case watchEventMsg:
		if msg.seq != m.watchSeq {
			return m, nil
		}
		if msg.ev.err != nil {
			return m.watchDown(msg.ev.err)
		}
		next, cmd := m.applySnapshot(msg.ev.snap)
		if next.terminal {
			return next, cmd // no re-arm: the stream ends after terminal
		}
		return next, tea.Batch(cmd, awaitWatchCmd(msg.seq, next.watchCh))

	case watchClosedMsg:
		if msg.seq != m.watchSeq {
			return m, nil
		}
		if !m.haveSnap || !m.snap.Terminal {
			return m.watchDown(errors.New("watch stream closed"))
		}
		return m, nil

	// --- log stream ---
	case logStartedMsg:
		if msg.seq != m.logSeq || msg.node != m.logNode {
			msg.cancel()
			return m, nil
		}
		m.logCh, m.logCancel = msg.ch, msg.cancel
		m.logErr, m.logPending = nil, false
		m.logBuf, m.logGen = logInitial(msg.view), msg.view.Gen
		m.vp.SetContent(m.logBuf)
		if m.follow {
			m.vp.GotoBottom()
		}
		return m, awaitLogsCmd(msg.seq, msg.ch)

	case logFailedMsg:
		if msg.seq != m.logSeq || msg.node != m.logNode {
			return m, nil
		}
		m.logErr, m.logPending = msg.err, false
		return m, nil

	case logEventMsg:
		if msg.seq != m.logSeq {
			return m, nil
		}
		if msg.ev.err != nil {
			m.logErr = msg.ev.err
			return m, nil
		}
		if msg.ev.chunk.Gen >= m.logGen { // stale attempts drop
			m.logBuf, m.logGen = logAppend(m.logBuf, m.logGen, msg.ev.chunk)
			m.vp.SetContent(m.logBuf)
			if m.follow {
				m.vp.GotoBottom()
			}
		}
		return m, awaitLogsCmd(msg.seq, m.logCh)

	case logClosedMsg:
		return m, nil

	case bvLogSelMsg:
		if msg.token != m.logSelToken {
			return m, nil
		}
		return m.switchLogs()

	case bvCancelDoneMsg:
		m.cancelErr = msg.err
		return m, nil
	}
	return m, nil
}

// watchDown records a broken watch stream; the tick retries transport
// failures, server answers (not-found: request deleted) end the view.
func (m buildView) watchDown(err error) (tea.Model, tea.Cmd) {
	m.watchErr = err
	if m.watchCancel != nil {
		m.watchCancel()
		m.watchCancel = nil
	}
	if isServerError(err) && !m.terminal {
		m.fatal = err
		return m, tea.Quit
	}
	return m, nil
}

// applySnapshot folds a fresh snapshot into the tree, preserves the cursor
// by path, drives the terminal transitions, and re-targets the output pane
// when the selected row's log node changed (e.g. queued → running).
func (m buildView) applySnapshot(snap api.Snapshot) (buildView, tea.Cmd) {
	var curPath string
	if m.cursor < len(m.visible) {
		curPath = m.visible[m.cursor].Path
	}
	m.snap, m.snapAt, m.haveSnap = snap, time.Now(), true
	m.graph = FoldSnapshot(snap)
	m.visible = FlattenTree(m.graph, m.exp)
	m.cursor = clamp(indexOfPath(m.visible, curPath), len(m.visible))
	m.top = window(len(m.visible), m.treeHeight(), m.cursor, m.top)

	if snap.Terminal {
		m.terminal = true
		m.outcome.Final, m.outcome.HaveFinal = snap, true
		if snap.Phase != "failed" {
			return m, tea.Quit // done/cancelled auto-exit
		}
		// Failure: stay for inspection, cursor on the first failed row.
		for i, tr := range m.visible {
			if r := m.graph.Rows[tr.Node]; r != nil && r.Phase == wire.PhaseFailed {
				m.cursor = i
				m.top = window(len(m.visible), m.treeHeight(), m.cursor, m.top)
				break
			}
		}
	}
	return m.switchLogs()
}

// indexOfPath finds a path's row index (0 when gone: the root).
func indexOfPath(rows []TreeRow, path string) int {
	for i, r := range rows {
		if r.Path == path {
			return i
		}
	}
	return 0
}

// selectedLogNode is the output-pane target for the cursor row.
func (m buildView) selectedLogNode() string {
	if m.graph == nil || m.cursor >= len(m.visible) {
		return ""
	}
	if r := m.graph.Rows[m.visible[m.cursor].Node]; r != nil {
		return r.LogNode
	}
	return ""
}

// switchLogs re-targets the output pane at the selected row's log node,
// closing the previous stream. No-op when already on target.
func (m buildView) switchLogs() (buildView, tea.Cmd) {
	want := m.selectedLogNode()
	if want == m.logNode {
		return m, nil
	}
	if m.logCancel != nil {
		m.logCancel()
		m.logCancel = nil
	}
	m.logSeq++ // invalidate in-flight messages of the old stream
	m.logNode, m.logErr = want, nil
	m.logBuf, m.logGen = "", 0
	m.follow = true
	m.vp.SetContent("")
	if want == "" {
		m.logPending = false
		return m, nil
	}
	m.logPending = true
	return m, bvLogsCmd(m.s, m.logSeq, want)
}

func (m buildView) updateKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	key := msg.String()

	if m.confirm {
		m.confirm = false
		if key == "y" {
			m.cancelSent = true
			return m, bvCancelCmd(m.s)
		}
		return m, nil
	}

	switch key {
	case "up", "k":
		return m.moveCursor(-1)
	case "down", "j":
		return m.moveCursor(1)
	case "left", "h":
		return m.setExpanded(false), nil
	case "right", "l":
		return m.setExpanded(true), nil
	case "enter", " ", "space":
		if m.cursor < len(m.visible) && m.visible[m.cursor].HasKids {
			return m.setExpanded(!m.visible[m.cursor].Expanded), nil
		}
		return m, nil
	case "o":
		m.outputMax = !m.outputMax
		m.sizeViewport()
		return m, nil
	case "G", "end":
		m.follow = true
		m.vp.GotoBottom()
		return m, nil
	case "pgup", "pgdown":
		var cmd tea.Cmd
		m.vp, cmd = m.vp.Update(msg)
		// Scrolling away pauses follow; returning to the bottom resumes it.
		m.follow = m.vp.AtBottom()
		return m, cmd
	case "q":
		if !m.terminal {
			m.outcome.Detached = true
		}
		return m, tea.Quit
	case "c", "ctrl+c":
		if m.terminal {
			return m, tea.Quit
		}
		if !m.cancelSent {
			m.confirm = true
		}
		return m, nil
	}
	return m, nil
}

// moveCursor shifts the tree cursor and schedules the debounced log switch.
func (m buildView) moveCursor(delta int) (tea.Model, tea.Cmd) {
	m.cursor = clamp(m.cursor+delta, len(m.visible))
	m.top = window(len(m.visible), m.treeHeight(), m.cursor, m.top)
	m.logSelToken++
	token := m.logSelToken
	return m, tea.Tick(logSwitchDebounce, func(time.Time) tea.Msg { return bvLogSelMsg{token: token} })
}

// setExpanded records the cursor row's expansion override and reflattens.
func (m buildView) setExpanded(v bool) buildView {
	if m.cursor >= len(m.visible) || !m.visible[m.cursor].HasKids {
		return m
	}
	path := m.visible[m.cursor].Path
	m.exp[path] = v
	m.visible = FlattenTree(m.graph, m.exp)
	m.cursor = clamp(indexOfPath(m.visible, path), len(m.visible))
	m.top = window(len(m.visible), m.treeHeight(), m.cursor, m.top)
	return m
}

// --- layout ---

func (m buildView) bodyHeight() int { return max(1, m.height-2) }
func (m buildView) treeHeight() int { return m.bodyHeight() }

// treeWidth is the tree pane's width: ~2/5 of the screen, bounded.
func (m buildView) treeWidth() int {
	if m.outputMax {
		return 0
	}
	w := m.width * 2 / 5
	if w < 30 {
		w = 30
	}
	if w > 60 {
		w = 60
	}
	if w > m.width-20 { // degenerate terminal: the tree takes it all
		w = m.width
	}
	return w
}

func (m *buildView) sizeViewport() {
	ow := m.width - m.treeWidth() - 1
	if m.outputMax {
		ow = m.width
	}
	m.vp.Width = max(1, ow)
	m.vp.Height = max(1, m.bodyHeight()-1)
}

func (m buildView) View() string {
	if m.width == 0 {
		return "starting…"
	}
	var b strings.Builder
	b.WriteString(truncCell(m.headerLine(), m.width))
	b.WriteByte('\n')

	tw := m.treeWidth()
	ow := m.width - tw - 1
	tree := m.treeLines()
	out := m.outputLines()
	for i := 0; i < m.bodyHeight(); i++ {
		var t, o string
		if i < len(tree) {
			t = tree[i]
		}
		if i < len(out) {
			o = out[i]
		}
		switch {
		case m.outputMax:
			b.WriteString(padStyled(o, m.width))
		case ow <= 0:
			b.WriteString(padStyled(t, tw))
		default:
			b.WriteString(padStyled(t, tw))
			b.WriteString(styleFaint.Render("│"))
			b.WriteString(padStyled(o, ow))
		}
		b.WriteByte('\n')
	}
	b.WriteString(truncCell(m.footerLine(), m.width))
	return b.String()
}

// headerLine: request identity, phase, counts, watch clock.
func (m buildView) headerLine() string {
	name := m.reqID
	if m.label != "" {
		name = m.label + " · " + name
	}
	if !m.haveSnap {
		return styleTitle.Render(name) + styleFaint.Render(" · connecting…")
	}
	c := m.snap.Counts
	s := fmt.Sprintf(" · %d/%d done · %d running", c.Done, c.Total, c.Running)
	if c.Failed > 0 {
		s += fmt.Sprintf(" · %d failed", c.Failed)
	}
	s += " · " + humanAge(time.Since(m.start))
	return styleTitle.Render(name) + " · " + phaseStyle(m.snap.Phase).Render(m.snap.Phase) + styleFaint.Render(s)
}

// footerLine: confirm prompt > stream trouble > key help.
func (m buildView) footerLine() string {
	switch {
	case m.confirm:
		return styleError.Render("cancel the " + RequestKind(m.snap) + "? y = yes · any other key = no")
	case m.watchErr != nil:
		return styleError.Render(fmt.Sprintf("watch stream down: %v — retrying", m.watchErr))
	case m.cancelErr != nil:
		return styleError.Render(fmt.Sprintf("cancel failed: %v", m.cancelErr))
	case m.terminal:
		return phaseStyle(m.snap.Phase).Render(RequestKind(m.snap)+" "+strings.ToUpper(m.snap.Phase)) +
			styleFaint.Render(" · ↑↓ inspect · enter fold · o output · q quit")
	case m.cancelSent:
		return styleFaint.Render("cancel sent — waiting for the server…")
	default:
		return styleFaint.Render("↑↓ select · ←→ fold · o output · pgup/pgdn scroll · G follow · c cancel · q detach")
	}
}

// treeLines renders the visible tree window, styles applied per padded row
// so the selection highlight spans the pane.
func (m buildView) treeLines() []string {
	if m.graph == nil {
		return []string{styleFaint.Render("waiting for the first snapshot…")}
	}
	tw := m.treeWidth()
	extra := time.Since(m.snapAt)
	end := min(len(m.visible), m.top+m.treeHeight())
	lines := make([]string, 0, end-m.top)
	for i := m.top; i < end; i++ {
		tr := m.visible[i]
		row := m.graph.Rows[tr.Node]
		line := padCell(buildRowLine(tr, row, extra), tw)
		st := phaseStyle(row.Phase)
		if row.Phase == wire.PhaseDone && row.Cached {
			st = styleFaint
		}
		if i == m.cursor {
			st = st.Reverse(true)
		}
		lines = append(lines, st.Render(line))
	}
	return lines
}

// outputLines renders the output pane: a title line + the viewport.
func (m buildView) outputLines() []string {
	var title string
	switch {
	case m.graph != nil && m.cursor < len(m.visible):
		row := m.graph.Rows[m.visible[m.cursor].Node]
		title = "output: " + rowName(row)
		if m.logNode == "" {
			title += styleFaint.Render(" (none)")
		} else if m.follow {
			title += styleFaint.Render(" · following")
		} else {
			title += styleFaint.Render(" · G to follow")
		}
	default:
		title = styleFaint.Render("output")
	}
	lines := []string{truncCell(title, m.vp.Width)}
	switch {
	case m.logErr != nil:
		lines = append(lines, styleError.Render(truncCell(fmt.Sprintf("logs: %v — retrying", m.logErr), m.vp.Width)))
	case m.logNode == "":
		lines = append(lines, styleFaint.Render("(no output for this row)"))
	case m.logPending:
		lines = append(lines, styleFaint.Render("loading…"))
	default:
		lines = append(lines, strings.Split(m.vp.View(), "\n")...)
	}
	return lines
}

// buildRowLine renders one tree row, unstyled (styling wraps the whole
// padded line so truncation never cuts an ANSI sequence).
func buildRowLine(tr TreeRow, row *BuildRow, extra time.Duration) string {
	expander := " "
	if tr.HasKids {
		if tr.Expanded {
			expander = "▾"
		} else {
			expander = "▸"
		}
	}
	var b strings.Builder
	b.WriteString(strings.Repeat("  ", tr.Depth))
	b.WriteString(expander)
	b.WriteByte(' ')
	b.WriteString(phaseGlyph(row.Phase))
	b.WriteByte(' ')
	b.WriteString(rowName(row))
	switch row.Phase {
	case wire.PhaseRunning, wire.PhasePublishing:
		b.WriteString(" · " + row.Stage + " · " + humanAge(time.Duration(row.ElapsedMs)*time.Millisecond+extra))
		if row.Runner != "" {
			b.WriteString(" on " + row.Runner)
		}
	case wire.PhaseQueued:
		b.WriteString(" · " + row.Stage + " · queued")
	case wire.PhaseDone:
		if row.Cached {
			b.WriteString(" (cached)")
		} else if row.ElapsedMs > 0 {
			b.WriteString(" · " + humanAge(time.Duration(row.ElapsedMs)*time.Millisecond))
		}
	case wire.PhaseFailed:
		b.WriteString(" · " + row.Stage)
		if row.Err != "" {
			b.WriteString(": " + row.Err)
		}
	case wire.PhaseUpstream:
		b.WriteString(" · upstream failed")
	case wire.PhaseCancelled:
		b.WriteString(" · cancelled")
	}
	return b.String()
}

// rowName is a row's display name: its label, else kind:key8.
func rowName(row *BuildRow) string {
	if row == nil {
		return ""
	}
	if row.Label != "" {
		return row.Label
	}
	return shortNode(row.Node)
}

// phaseGlyph is the row's one-cell state marker.
func phaseGlyph(phase string) string {
	switch phase {
	case wire.PhaseDone:
		return "✓"
	case wire.PhaseFailed, wire.PhaseUpstream:
		return "✗"
	case wire.PhaseRunning, wire.PhasePublishing:
		return "▶"
	case wire.PhaseQueued:
		return "◦"
	}
	return "·"
}

// truncCell truncates one (possibly styled) line to w display cells.
func truncCell(s string, w int) string {
	if w <= 0 || lipglossWidth(s) <= w {
		return s
	}
	return ansi.Truncate(s, w-1, "…")
}

// padStyled pads a styled line to exactly w display cells (ANSI-aware).
func padStyled(s string, w int) string {
	gap := w - lipglossWidth(s)
	if gap > 0 {
		return s + strings.Repeat(" ", gap)
	}
	return s
}
