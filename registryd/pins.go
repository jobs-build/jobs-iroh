package registryd

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/amber-store/transport-iroh/amberiroh"
)

// pinAssertEvery coalesces pin-asserts per ref: as long as an image keeps
// being served, its pins reappear on the server within this window even
// after a server access-DB loss (design §4).
const pinAssertEvery = time.Hour

// pinAsserter tracks which refs were recently asserted and whether the
// server supports TPin at all.
type pinAsserter struct {
	mu       sync.Mutex
	sent     map[string]time.Time
	disabled bool
}

func newPinAsserter() *pinAsserter {
	return &pinAsserter{sent: map[string]time.Time{}}
}

// due filters names down to the ones whose last assert is over an hour old,
// marking them sent. Empty when disabled.
func (p *pinAsserter) due(names []string, now time.Time) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.disabled {
		return nil
	}
	var out []string
	for _, n := range names {
		if last, ok := p.sent[n]; ok && now.Sub(last) < pinAssertEvery {
			continue
		}
		p.sent[n] = now
		out = append(out, n)
	}
	return out
}

// retry re-arms names after a transport failure so the next serve retries.
func (p *pinAsserter) retry(names []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, n := range names {
		delete(p.sent, n)
	}
}

// disable stops asserting for the life of the process (old server).
func (p *pinAsserter) disable() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.disabled = true
}

// pinNames lists the server refs backing one image — what must survive GC
// for the image to stay resolvable and reassemblable.
func pinNames(rec imageRecord) []string {
	names := []string{
		"build-from:" + rec.K,
		"build:" + rec.K,
		"build-output:" + rec.F,
		"build-output-deps:" + rec.F,
	}
	if rec.Platform != "" {
		names = append(names, "shell:"+rec.Platform)
	}
	return names
}

// assertPins fires a best-effort TPin for the refs backing rec: never on
// the request path (a goroutine under the daemon context), coalesced to
// once per ref per hour, self-disabling when the server predates TPin. The
// goroutine is tracked like a detached assembly (assemble.go) and drained by
// Run before the deferred store/sync close.
func (r *registry) assertPins(rec imageRecord) {
	if r.runCtx.Err() != nil {
		return
	}
	due := r.pins.due(pinNames(rec), time.Now())
	if len(due) == 0 {
		return
	}
	r.assemblies.Add(1)
	go func() {
		defer r.assemblies.Done()
		ctx, cancel := context.WithTimeout(r.runCtx, time.Minute)
		defer cancel()
		if err := r.sync.Pin(ctx, due); err != nil {
			if shouldDisable(err) {
				r.pins.disable()
				r.log.Warn("jobs-server does not support pin-asserts; images may be GC'd there", "error", err)
				return
			}
			r.pins.retry(due)
			r.log.Debug("pin-assert failed; will retry on the next serve", "error", err)
		}
	}()
}

// shouldDisable reports whether err is the specific signal an old server
// sends for an unrecognized TPin operation (a bad-request RemoteError) —
// the only case that should permanently disable pin-asserting. Any other
// *amberiroh.RemoteError (e.g. a transient store error on the server) must
// not disable pinning; it goes down the retry path instead. Factored out as
// a pure function so it's testable without a network round trip.
func shouldDisable(err error) bool {
	var re *amberiroh.RemoteError
	if !errors.As(err, &re) {
		return false
	}
	return re.Code == amberiroh.CodeBadRequest
}
