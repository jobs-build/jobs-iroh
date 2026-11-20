package tui

import (
	"context"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/jobs-build/jobs-iroh/api"
	"github.com/jobs-build/jobs-iroh/wire"
)

const (
	// pollEvery is the refresh cadence of the polled views (requests,
	// fleet, stats, refs) and the retry cadence after a lost server.
	pollEvery = 2 * time.Second
	// callTimeout bounds one-shot exchanges (first call includes the dial).
	callTimeout = 10 * time.Second
	// refsLimit caps one refs page.
	refsLimit = 1000
)

// --- messages ---

type tickMsg time.Time

// tick arms the shared poll/retry tick.
func tick() tea.Cmd {
	return tea.Tick(pollEvery, func(t time.Time) tea.Msg { return tickMsg(t) })
}

type requestsMsg struct{ rows []api.RequestInfo }
type fleetMsg struct{ runners []wire.RunnerInfo }
type statsMsg struct{ stats api.StatsReply }
type refsMsg struct {
	prefix, after string // the page asked for (stale replies are dropped)
	refs          []api.RefInfo
	more          bool // refs remain past this page
	matched       int  // every ref matching the prefix (0 from older servers)
}

// netErrMsg is a failed one-shot exchange; the root shows it on the status
// line and the next tick retries.
type netErrMsg struct {
	op  string
	err error
}

// actionDoneMsg reports a cancel/delete outcome.
type actionDoneMsg struct {
	op        string // "cancel" | "delete"
	requestID string
	err       error
}

// watchEvent is one frame of a snapshot watch stream (snap or err, never
// both). A closed channel without a prior err means terminal snapshot
// delivered (or deliberate stop).
type watchEvent struct {
	snap api.Snapshot
	err  error
}

type watchStartedMsg struct {
	seq    int
	ch     <-chan watchEvent
	cancel func()
}
type watchFailedMsg struct {
	seq int
	err error
}
type watchEventMsg struct {
	seq int
	ev  watchEvent
}
type watchClosedMsg struct{ seq int }

// logEvent is one live follow chunk (or the stream's terminal error).
type logEvent struct {
	chunk wire.LogChunk
	err   error
}

type logStartedMsg struct {
	seq    int
	node   string
	view   api.LogView
	ch     <-chan logEvent
	cancel func()
}
type logFailedMsg struct {
	seq  int
	node string
	err  error
}
type logEventMsg struct {
	seq int
	ev  logEvent
}
type logClosedMsg struct{ seq int }

// --- one-shot commands ---

// oneShot runs a single request/reply exchange under callTimeout.
func oneShot(c *client, t string, body any, wantType string, reply any) error {
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	return c.call(ctx, t, body, wantType, reply)
}

func fetchRequestsCmd(c *client) tea.Cmd {
	return func() tea.Msg {
		var r api.RequestsReply
		if err := oneShot(c, api.TRequests, nil, api.TRequestsReply, &r); err != nil {
			return netErrMsg{op: "requests", err: err}
		}
		return requestsMsg{rows: r.Requests}
	}
}

func fetchFleetCmd(c *client) tea.Cmd {
	return func() tea.Msg {
		var r api.FleetReply
		if err := oneShot(c, api.TFleet, nil, api.TFleetReply, &r); err != nil {
			return netErrMsg{op: "fleet", err: err}
		}
		return fleetMsg{runners: r.Runners}
	}
}

func fetchStatsCmd(c *client) tea.Cmd {
	return func() tea.Msg {
		var r api.StatsReply
		if err := oneShot(c, api.TStats, nil, api.TStatsReply, &r); err != nil {
			return netErrMsg{op: "stats", err: err}
		}
		return statsMsg{stats: r}
	}
}

// fetchRefsCmd fetches the refs page that starts after `after` ("" = the
// first page). An old server ignores After and answers its first page again:
// rows at or before the cursor are dropped so the view never repeats them.
func fetchRefsCmd(c *client, prefix, after string) tea.Cmd {
	return func() tea.Msg {
		var r api.RefsReply
		req := api.RefsRequest{Prefix: prefix, Limit: refsLimit, After: after}
		if err := oneShot(c, api.TRefs, req, api.TRefsReply, &r); err != nil {
			return netErrMsg{op: "refs", err: err}
		}
		refs := r.Refs
		if after != "" {
			refs = refs[:0:0]
			for _, ri := range r.Refs {
				if ri.Name > after {
					refs = append(refs, ri)
				}
			}
		}
		return refsMsg{prefix: prefix, after: after, refs: refs, more: r.More, matched: r.Matched}
	}
}

func cancelRequestCmd(c *client, id string) tea.Cmd {
	return func() tea.Msg {
		err := oneShot(c, api.TCancel, api.CancelRequest{RequestID: id}, api.TOK, nil)
		return actionDoneMsg{op: "cancel", requestID: id, err: err}
	}
}

func deleteRequestCmd(c *client, id string) tea.Cmd {
	return func() tea.Msg {
		err := oneShot(c, api.TDelete, api.CancelRequest{RequestID: id}, api.TOK, nil)
		return actionDoneMsg{op: "delete", requestID: id, err: err}
	}
}

// --- streams (bubbletea subscription pattern) ---
//
// A start cmd opens the stream and hands back a channel + cancel; an await
// cmd blocks (in its own goroutine) for the next event and is re-armed by
// Update after every delivery. seq guards against stale streams after the
// view moved on: messages whose seq is not current get dropped.

func startWatchCmd(c *client, seq int, requestID string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithCancel(context.Background())
		stream, closer, err := c.openStream(ctx, api.TWatch, api.WatchRequest{RequestID: requestID})
		if err != nil {
			cancel()
			return watchFailedMsg{seq: seq, err: err}
		}
		ch := make(chan watchEvent, 8)
		go func() {
			defer close(ch)
			defer closer()
			defer cancel()
			// send never blocks past cancellation: a full buffer at
			// teardown must not strand this goroutine (and the stream).
			send := func(ev watchEvent) bool {
				select {
				case ch <- ev:
					return true
				case <-ctx.Done():
					return false
				}
			}
			for {
				rt, rb, err := api.ReadFrame(stream)
				if err != nil {
					if ctx.Err() == nil {
						send(watchEvent{err: err})
					}
					return
				}
				var snap api.Snapshot
				if err := decodeReply(rt, rb, api.TSnapshot, &snap); err != nil {
					send(watchEvent{err: err})
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

func awaitWatchCmd(seq int, ch <-chan watchEvent) tea.Cmd {
	return func() tea.Msg {
		ev, ok := <-ch
		if !ok {
			return watchClosedMsg{seq: seq}
		}
		return watchEventMsg{seq: seq, ev: ev}
	}
}

func startLogsCmd(c *client, seq int, node string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithCancel(context.Background())
		stream, closer, err := c.openStream(ctx, api.TLogs, api.LogsRequest{Node: node, Follow: true})
		if err != nil {
			cancel()
			return logFailedMsg{seq: seq, node: node, err: err}
		}
		fail := func(err error) tea.Msg {
			closer()
			cancel()
			return logFailedMsg{seq: seq, node: node, err: err}
		}
		// The stored view arrives first, synchronously (we are already in a
		// cmd goroutine, not in Update).
		rt, rb, err := api.ReadFrame(stream)
		if err != nil {
			return fail(err)
		}
		var view api.LogView
		if err := decodeReply(rt, rb, api.TLogView, &view); err != nil {
			return fail(err)
		}
		ch := make(chan logEvent, 64)
		go func() {
			defer close(ch)
			defer closer()
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
				rt, rb, err := api.ReadFrame(stream)
				if err != nil {
					if ctx.Err() == nil {
						send(logEvent{err: err})
					}
					return
				}
				var chunk wire.LogChunk
				if err := decodeReply(rt, rb, api.TLogChunk, &chunk); err != nil {
					send(logEvent{err: err})
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

func awaitLogsCmd(seq int, ch <-chan logEvent) tea.Cmd {
	return func() tea.Msg {
		ev, ok := <-ch
		if !ok {
			return logClosedMsg{seq: seq}
		}
		return logEventMsg{seq: seq, ev: ev}
	}
}
