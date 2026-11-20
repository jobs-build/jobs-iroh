package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/jobs-build/jobs-iroh/api"
)

// refsModel is the ref browser: a prefix-filterable listing, one page of up
// to refsLimit refs at a time, cursor-scrolled. '/' edits the prefix; enter
// applies it (and refetches from the first page), esc abandons the edit.
// 'n' moves to the next page when the server says more remain, 'p' back —
// the server holds one page per call however large the store (issue #22).
type refsModel struct {
	c             *client
	width, height int

	input  textinput.Model
	prefix string // applied filter (input.Value() is the draft)

	// after is the current page's cursor ("" = first page); pages holds the
	// cursors of the pages before it, so 'p' can walk back.
	after string
	pages []string

	refs    []api.RefInfo
	more    bool
	matched int
	loaded  bool
	cursor  int
	top     int
}

func newRefsModel(c *client) refsModel {
	in := textinput.New()
	in.Prompt = "prefix: "
	in.Placeholder = "e.g. build-output:"
	in.CharLimit = 256
	return refsModel{c: c, input: in}
}

// captures: while the prefix input is focused every key belongs to it.
func (m refsModel) captures() bool { return m.input.Focused() }

func (m refsModel) setSize(w, h int) refsModel {
	m.width, m.height = w, h
	m.input.Width = max(16, w-12)
	return m
}

// gotoPage switches to the page after the given cursor and fetches it.
func (m refsModel) gotoPage(after string, pages []string) (refsModel, tea.Cmd) {
	m.after, m.pages = after, pages
	m.cursor, m.top = 0, 0
	m.loaded = false
	m.refs = nil
	return m, fetchRefsCmd(m.c, m.prefix, m.after)
}

func (m refsModel) update(msg tea.Msg) (refsModel, tea.Cmd) {
	switch msg := msg.(type) {
	case tickMsg:
		return m, fetchRefsCmd(m.c, m.prefix, m.after)
	case refsMsg:
		if msg.prefix != m.prefix || msg.after != m.after {
			return m, nil // stale fetch from before an edit or a page move
		}
		m.refs = msg.refs
		m.more = msg.more
		m.matched = msg.matched
		m.loaded = true
		m.cursor = clamp(m.cursor, len(m.refs))
		m.top = window(len(m.refs), m.rowsHeight(), m.cursor, m.top)
		return m, nil

	case tea.KeyMsg:
		if m.input.Focused() {
			switch msg.String() {
			case "enter":
				m.input.Blur()
				m.prefix = m.input.Value()
				return m.gotoPage("", nil)
			case "esc":
				m.input.Blur()
				m.input.SetValue(m.prefix)
				return m, nil
			}
			var cmd tea.Cmd
			m.input, cmd = m.input.Update(msg)
			return m, cmd
		}
		n := len(m.refs)
		switch msg.String() {
		case "/":
			return m, m.input.Focus()
		case "n":
			if m.loaded && m.more && n > 0 {
				return m.gotoPage(m.refs[n-1].Name, append(append([]string(nil), m.pages...), m.after))
			}
			return m, nil
		case "p":
			if len(m.pages) > 0 {
				last := len(m.pages) - 1
				return m.gotoPage(m.pages[last], m.pages[:last:last])
			}
			return m, nil
		case "up", "k":
			m.cursor = clamp(m.cursor-1, n)
		case "down", "j":
			m.cursor = clamp(m.cursor+1, n)
		case "pgup":
			m.cursor = clamp(m.cursor-m.rowsHeight(), n)
		case "pgdown":
			m.cursor = clamp(m.cursor+m.rowsHeight(), n)
		case "g", "home":
			m.cursor = 0
		case "G", "end":
			m.cursor = clamp(n-1, n)
		case "r":
			return m, fetchRefsCmd(m.c, m.prefix, m.after)
		}
		m.top = window(n, m.rowsHeight(), m.cursor, m.top)
		return m, nil
	}
	return m, nil
}

// rowsHeight leaves room for the filter line + table header.
func (m refsModel) rowsHeight() int { return max(1, m.height-2) }

// refWidths widens the name column into the pane's slack.
func (m refsModel) refWidths() []int {
	w := make([]int, len(refCols))
	copy(w, refCols)
	fixed := 0
	for _, c := range refCols {
		fixed += c
	}
	fixed += 2 * (len(refCols) - 1) // joinCells' two-space separators
	if slack := m.width - fixed; slack > 0 {
		w[0] += slack
	}
	return w
}

// countLine summarises which refs the page shows: "refs 1001–2000 of 31245"
// when paged, the plain count for a single page.
func (m refsModel) countLine() string {
	first := len(m.pages)*refsLimit + 1
	last := first + len(m.refs) - 1
	switch {
	case len(m.refs) == 0:
		return "  0 refs"
	case m.matched > 0 && (m.more || len(m.pages) > 0):
		return fmt.Sprintf("  refs %d–%d of %d (n/p page)", first, last, m.matched)
	case m.more || len(m.pages) > 0:
		return fmt.Sprintf("  refs %d–%d (n/p page)", first, last)
	}
	return fmt.Sprintf("  %d refs", len(m.refs))
}

func (m refsModel) view() string {
	var b strings.Builder
	filter := m.input.View()
	if !m.input.Focused() && m.input.Value() == "" {
		filter = styleFaint.Render("prefix: (all — press / to filter)")
	}
	b.WriteString(filter + styleFaint.Render(m.countLine()))
	b.WriteByte('\n')

	widths := m.refWidths()
	b.WriteString(styleHeader.Render(joinCells(refHeader, widths)))
	b.WriteByte('\n')
	if len(m.refs) == 0 {
		if m.loaded {
			b.WriteString(styleFaint.Render("no refs match"))
		} else {
			b.WriteString(styleFaint.Render("loading…"))
		}
		return b.String()
	}
	now := time.Now()
	end := min(m.top+m.rowsHeight(), len(m.refs))
	for i := m.top; i < end; i++ {
		line := joinCells(refRow(m.refs[i], now), widths)
		if i == m.cursor {
			line = styleSelected.Render(line)
		}
		b.WriteString(line)
		if i != end-1 {
			b.WriteByte('\n')
		}
	}
	return b.String()
}
