package tui

import (
	"slices"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/jobs-build/jobs-iroh/api"
)

func runeKey(s string) tea.KeyMsg { return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)} }

func refRows(names ...string) []api.RefInfo {
	out := make([]api.RefInfo, len(names))
	for i, n := range names {
		out[i] = api.RefInfo{Name: n, Key: make([]byte, 32)}
	}
	return out
}

// The refs browser pages with the server's After cursor: 'n' moves on only
// while the server says more remain, 'p' walks back, and a reply for a page
// the view already left is dropped.
func TestRefsBrowserPages(t *testing.T) {
	m := newRefsModel(nil).setSize(100, 20)
	m, _ = m.update(refsMsg{refs: refRows("a", "b"), more: true, matched: 4})
	if got := m.countLine(); !strings.Contains(got, "of 4") || !strings.Contains(got, "n/p") {
		t.Fatalf("first page count line = %q", got)
	}

	m, cmd := m.update(runeKey("n"))
	if cmd == nil || m.after != "b" || !slices.Equal(m.pages, []string{""}) || m.loaded {
		t.Fatalf("after n: after=%q pages=%q loaded=%v cmd=%v", m.after, m.pages, m.loaded, cmd != nil)
	}
	// A late reply for the first page must not overwrite the second.
	m, _ = m.update(refsMsg{refs: refRows("a", "b"), more: true, matched: 4})
	if m.loaded {
		t.Fatal("stale first-page reply was applied to the second page")
	}
	m, _ = m.update(refsMsg{after: "b", refs: refRows("c", "d"), matched: 4})
	if !m.loaded || m.refs[0].Name != "c" || m.more {
		t.Fatalf("second page: %+v more=%v", m.refs, m.more)
	}
	// Last page: no more to go to.
	if m2, cmd := m.update(runeKey("n")); cmd != nil || m2.after != "b" {
		t.Fatalf("n on the last page moved to %q", m2.after)
	}

	m, cmd = m.update(runeKey("p"))
	if cmd == nil || m.after != "" || len(m.pages) != 0 {
		t.Fatalf("after p: after=%q pages=%q", m.after, m.pages)
	}
	// p on the first page is a no-op.
	if _, cmd := m.update(runeKey("p")); cmd != nil {
		t.Fatal("p on the first page fetched something")
	}
}

func TestStatsLinesWarnings(t *testing.T) {
	lines := strings.Join(statsLines(api.StatsReply{Warnings: []string{"GC halted: snapshot unreadable"}}), "\n")
	if !strings.Contains(lines, "WARNING") || !strings.Contains(lines, "GC halted") {
		t.Fatalf("warnings not rendered:\n%s", lines)
	}
}
