package registryd

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amberiroh"

	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/amberclient"
)

// reconnSync wraps amberclient with redial-on-failure (the runnerd pattern):
// a Client is one connection, so when an operation fails on transport (server
// restart, link drop) the client is discarded and the operation retried once
// on a fresh dial. Errors the SERVER answered with (amberiroh.RemoteError,
// ErrRefNotFound) never redial — the connection just proved itself.
type reconnSync struct {
	opts amberclient.Options
	st   *amber.Store
	log  *slog.Logger

	mu  sync.Mutex
	cur *amberclient.Client
}

func newReconnSync(opts amberclient.Options, st *amber.Store, log *slog.Logger) *reconnSync {
	return &reconnSync{opts: opts, st: st, log: log}
}

func (r *reconnSync) Pull(ctx context.Context, name string) (key.Key, error) {
	start := time.Now()
	var root key.Key
	var stats amberclient.XferStats
	err := r.do(ctx, func(c *amberclient.Client) error {
		var err error
		var st amberclient.XferStats
		root, st, err = c.PullWithProgress(ctx, r.st, name, nil)
		// Accumulate across a redial retry: the want loop resumes where the
		// dropped attempt stopped, so the session's transfer is the sum and
		// stays consistent with elapsed, which spans both attempts.
		stats.Objects += st.Objects
		stats.Bytes += st.Bytes
		return err
	})
	elapsed := time.Since(start).Round(time.Millisecond)
	switch {
	case err == nil:
		r.log.Info("amber pull", "ref", name,
			"objects", stats.Objects, "bytes", stats.Bytes, "elapsed", elapsed)
	case errors.Is(err, amberclient.ErrRefNotFound):
		// An absent ref is a normal answer (optional deps/shell/definition
		// refs), not a transfer failure.
		r.log.Debug("amber pull: ref not on server", "ref", name, "elapsed", elapsed)
	case ctx.Err() != nil:
		// Cancellation surfaces as a stream-deadline error, not ctx.Err —
		// classify by context state so shutdown does not read as failure.
		r.log.Debug("amber pull cancelled", "ref", name, "elapsed", elapsed)
	default:
		r.log.Warn("amber pull failed", "ref", name, "elapsed", elapsed, "error", err)
	}
	return root, err
}

// Pin asserts that names must survive GC on the server (amberiroh.TPin).
// An old server answers with a bad-request RemoteError, which do's
// isRemoteVerdict treats as a server-answered verdict — no redial — so the
// caller sees the RemoteError straight through and can disable asserting.
func (r *reconnSync) Pin(ctx context.Context, names []string) error {
	return r.do(ctx, func(c *amberclient.Client) error {
		return c.Pin(ctx, names)
	})
}

func (r *reconnSync) Refs(ctx context.Context) ([]amberclient.RefInfo, error) {
	var refs []amberclient.RefInfo
	err := r.do(ctx, func(c *amberclient.Client) error {
		var err error
		refs, err = c.Refs(ctx)
		return err
	})
	return refs, err
}

// do runs op on the current client, dialing lazily and retrying once on a
// fresh connection after a transport failure.
//
// An op that failed because its OWN context expired must not drop the client:
// unlike runnerd, registryd feeds short-lived HTTP request contexts in here
// (tags listings) alongside long assembly pulls on the same shared
// connection, and a cancelled listing says nothing about the connection's
// health — closing it would abort every concurrent pull.
func (r *reconnSync) do(ctx context.Context, op func(*amberclient.Client) error) error {
	for attempt := 0; ; attempt++ {
		c, err := r.client(ctx)
		if err != nil {
			return err
		}
		err = op(c)
		if err == nil || isRemoteVerdict(err) || ctx.Err() != nil {
			return err
		}
		r.drop(c)
		if attempt >= 1 {
			return err
		}
		r.log.Warn("amber sync connection dropped; redialing", "error", err)
	}
}

// isRemoteVerdict reports whether the server itself answered err — proof the
// connection works, so redialing cannot change the outcome.
func isRemoteVerdict(err error) bool {
	var re *amberiroh.RemoteError
	return errors.As(err, &re) || errors.Is(err, amberclient.ErrRefNotFound)
}

func (r *reconnSync) client(ctx context.Context) (*amberclient.Client, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cur != nil {
		return r.cur, nil
	}
	c, err := amberclient.Dial(ctx, r.opts)
	if err != nil {
		return nil, err
	}
	r.cur = c
	return c, nil
}

// drop closes c and forgets it (if still current).
func (r *reconnSync) drop(c *amberclient.Client) {
	r.mu.Lock()
	if r.cur == c {
		r.cur = nil
	}
	r.mu.Unlock()
	_ = c.Close()
}

// Close releases the current connection, if any.
func (r *reconnSync) Close() {
	r.mu.Lock()
	c := r.cur
	r.cur = nil
	r.mu.Unlock()
	if c != nil {
		_ = c.Close()
	}
}
