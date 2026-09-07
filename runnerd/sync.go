package runnerd

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/amber-store/core/key"
	"github.com/amber-store/transport-iroh/amberiroh"

	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/amberclient"
)

// reconnSync adapts amberclient onto the daemon's syncClient seam with
// redial-on-failure: a Client is one connection, so when an operation fails
// on transport (server restart, link drop) the client is discarded and the
// operation retried once on a fresh dial. Errors the SERVER answered with
// (amberiroh.RemoteError, ErrRefNotFound) never redial — the connection just
// proved itself.
type reconnSync struct {
	opts amberclient.Options
	st   *amber.Store

	mu  sync.Mutex
	cur *amberclient.Client
}

var _ syncClient = (*reconnSync)(nil)

func newReconnSync(opts amberclient.Options, st *amber.Store) *reconnSync {
	return &reconnSync{opts: opts, st: st}
}

func (r *reconnSync) Pull(ctx context.Context, name string) (key.Key, error) {
	var root key.Key
	err := r.do(ctx, func(c *amberclient.Client) error {
		var err error
		root, err = c.Pull(ctx, r.st, name)
		return err
	})
	return root, err
}

func (r *reconnSync) Push(ctx context.Context, name string, root key.Key) error {
	return r.do(ctx, func(c *amberclient.Client) error {
		return c.Push(ctx, r.st, name, root)
	})
}

// do runs op on the current client, dialing lazily and retrying once on a
// fresh connection after a transport failure.
func (r *reconnSync) do(ctx context.Context, op func(*amberclient.Client) error) error {
	for attempt := 0; ; attempt++ {
		c, err := r.client(ctx)
		if err != nil {
			return err
		}
		err = op(c)
		if err == nil || isRemoteVerdict(err) {
			return err
		}
		r.drop(c)
		if attempt >= 1 || ctx.Err() != nil {
			return err
		}
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
	c.WatchPath(context.WithoutCancel(ctx), reportPath(r.opts.Logger, "store"))
	r.cur = c
	return c, nil
}

// Warm dials the store connection ahead of the first transfer, so the runner
// reports how it reaches the store (see reportPath) at startup instead of at
// the first job. Failure is not fatal — the connection is dialed lazily
// anyway and do retries it on every operation — so it is only logged.
func (r *reconnSync) Warm(ctx context.Context) {
	if _, err := r.client(ctx); err != nil {
		log := r.opts.Logger
		if log == nil {
			log = slog.Default()
		}
		log.Warn("store connection not established yet; retrying at first transfer", "error", err)
	}
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
