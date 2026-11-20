package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// Config selects the server the TUI observes.
type Config struct {
	Server string   // endpoint ID of the jobs-server
	Addrs  []string // optional direct addresses (skips discovery/relays)
}

// Run drives the admin TUI until quit or ctx cancellation.
func Run(ctx context.Context, cfg Config) error {
	c := newClient(cfg.Server, cfg.Addrs)
	defer c.close()
	p := tea.NewProgram(newRoot(c, cfg.Server), tea.WithAltScreen(), tea.WithContext(ctx))
	_, err := p.Run()
	if err == tea.ErrProgramKilled && ctx.Err() != nil {
		return nil // Ctrl-C via signal context is a normal exit
	}
	return err
}

// tab indices (also the 1-4 jump keys).
const (
	tabBuilds = iota
	tabFleet
	tabStats
	tabRefs
	tabCount
)

var tabNames = [tabCount]string{"1 builds", "2 fleet", "3 stats", "4 refs"}

// rootModel is the router: tab bar, one model per view, the shared poll
// tick, and the status line (last network error + retry note).
type rootModel struct {
	c      *client
	server string
	active int

	builds buildsModel
	fleet  fleetModel
	stats  statsModel
	refs   refsModel

	width, height int
	netErr        error  // last failed exchange (nil when healthy)
	netErrOp      string // which exchange failed
	flash         string // transient action feedback (cancel/delete)
}

func newRoot(c *client, server string) rootModel {
	return rootModel{
		c:      c,
		server: server,
		builds: newBuildsModel(c),
		fleet:  newFleetModel(c),
		stats:  newStatsModel(c),
		refs:   newRefsModel(c),
	}
}

func (m rootModel) Init() tea.Cmd {
	return tea.Batch(tick(), fetchRequestsCmd(m.c))
}

// captures reports whether the active view owns the keyboard.
func (m rootModel) captures() bool {
	switch m.active {
	case tabBuilds:
		return m.builds.captures()
	case tabFleet:
		return m.fleet.captures()
	case tabStats:
		return m.stats.captures()
	default:
		return m.refs.captures()
	}
}

func (m rootModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		w, h := msg.Width, m.contentHeight()
		m.builds = m.builds.setSize(w, h)
		m.fleet = m.fleet.setSize(w, h)
		m.refs = m.refs.setSize(w, h)
		return m, nil

	case tea.KeyMsg:
		switch act, n := routeKey(msg.String(), m.captures()); act {
		case actQuit:
			return m, tea.Quit
		case actNext:
			m.active = (m.active + 1) % tabCount
			return m.refreshActive()
		case actPrev:
			m.active = (m.active + tabCount - 1) % tabCount
			return m.refreshActive()
		case actSelect:
			if m.active != n {
				m.active = n
				return m.refreshActive()
			}
			return m, nil
		}
		return m.forwardActive(msg)

	case tickMsg:
		m.flash = ""
		// The active view polls; a dead watch/log stream retries here too.
		next, cmd := m.forwardActive(msg)
		return next, tea.Batch(tick(), cmd)

	case netErrMsg:
		m.netErr, m.netErrOp = msg.err, msg.op
		return m, nil

	case actionDoneMsg:
		if msg.err != nil {
			m.netErr, m.netErrOp = msg.err, msg.op
		} else {
			m.netErr = nil
			m.flash = fmt.Sprintf("%s %s: ok", msg.op, msg.requestID)
		}
		return m.forwardBuilds(msg)

	// Data arriving proves the server is reachable again.
	case requestsMsg:
		m.netErr = nil
		return m.forwardBuilds(msg)
	case watchStartedMsg, watchEventMsg, watchFailedMsg, watchClosedMsg,
		logStartedMsg, logEventMsg, logFailedMsg, logClosedMsg:
		return m.forwardBuilds(msg)
	case fleetMsg:
		m.netErr = nil
		var cmd tea.Cmd
		m.fleet, cmd = m.fleet.update(msg)
		return m, cmd
	case statsMsg:
		m.netErr = nil
		var cmd tea.Cmd
		m.stats, cmd = m.stats.update(msg)
		return m, cmd
	case refsMsg:
		m.netErr = nil
		var cmd tea.Cmd
		m.refs, cmd = m.refs.update(msg)
		return m, cmd
	}
	return m, nil
}

// refreshActive kicks the freshly selected view with an immediate tick so a
// tab switch never shows 2s-stale emptiness.
func (m rootModel) refreshActive() (tea.Model, tea.Cmd) {
	return m.forwardActive(tickMsg(time.Now()))
}

// forwardActive routes a message into the active view.
func (m rootModel) forwardActive(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch m.active {
	case tabBuilds:
		m.builds, cmd = m.builds.update(msg)
	case tabFleet:
		m.fleet, cmd = m.fleet.update(msg)
	case tabStats:
		m.stats, cmd = m.stats.update(msg)
	default:
		m.refs, cmd = m.refs.update(msg)
	}
	return m, cmd
}

// forwardBuilds routes builds-owned messages (streams arrive regardless of
// the active tab; the builds model seq-guards stale ones).
func (m rootModel) forwardBuilds(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	m.builds, cmd = m.builds.update(msg)
	return m, cmd
}

// contentHeight is the pane height between tab bar and status line.
func (m rootModel) contentHeight() int { return max(1, m.height-3) }

func (m rootModel) View() string {
	if m.width == 0 {
		return "starting…"
	}
	var b strings.Builder

	// Tab bar + server identity.
	var tabs []string
	for i, name := range tabNames {
		if i == m.active {
			tabs = append(tabs, styleTabActive.Render(name))
		} else {
			tabs = append(tabs, styleTab.Render(name))
		}
	}
	left := strings.Join(tabs, " ")
	right := styleFaint.Render("server " + shortEndpoint(m.server))
	b.WriteString(spread(left, right, m.width))
	b.WriteString("\n\n")

	// Active pane, fixed to the content height.
	var pane string
	switch m.active {
	case tabBuilds:
		pane = m.builds.view()
	case tabFleet:
		pane = m.fleet.view()
	case tabStats:
		pane = m.stats.view()
	default:
		pane = m.refs.view()
	}
	b.WriteString(fitHeight(pane, m.contentHeight()))
	b.WriteByte('\n')

	// Status line: connection state or action feedback, plus key help.
	var status string
	switch {
	case m.netErr != nil:
		status = styleError.Render(fmt.Sprintf("%s failed: %v — retrying", m.netErrOp, m.netErr))
	case m.flash != "":
		status = styleFlash.Render(m.flash)
	default:
		status = styleFaint.Render("connected")
	}
	b.WriteString(spread(status, styleFaint.Render(m.helpLine()), m.width))
	return b.String()
}

// helpLine is the per-screen key hint.
func (m rootModel) helpLine() string {
	if m.active == tabBuilds {
		switch m.builds.mode {
		case modeDetail:
			return "↑↓ node · l logs · c cancel · d delete · esc back"
		case modeLogs:
			return "↑↓/pgup/pgdn scroll · G follow · esc back"
		case modeConfirmCancel, modeConfirmDelete:
			return "y confirm · any other key aborts"
		default:
			return "↑↓ select · enter detail · tab/1-4 view · q quit"
		}
	}
	if m.active == tabRefs {
		if m.refs.captures() {
			return "enter apply · esc abort"
		}
		return "/ filter · ↑↓ scroll · n/p page · tab/1-4 view · q quit"
	}
	return "tab/1-4 view · q quit"
}

// spread left- and right-aligns two rendered fragments on one line.
func spread(left, right string, width int) string {
	gap := width - lipgloss.Width(left) - lipgloss.Width(right)
	if gap < 1 {
		return left + " " + right
	}
	return left + strings.Repeat(" ", gap) + right
}

// fitHeight pads or trims a pane to exactly h lines.
func fitHeight(s string, h int) string {
	lines := strings.Split(s, "\n")
	if len(lines) > h {
		lines = lines[:h]
	}
	for len(lines) < h {
		lines = append(lines, "")
	}
	return strings.Join(lines, "\n")
}

// shortEndpoint compresses an endpoint ID for the header.
func shortEndpoint(s string) string {
	if len(s) > 12 {
		return s[:12] + "…"
	}
	return s
}
