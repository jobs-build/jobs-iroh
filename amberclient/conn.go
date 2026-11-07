package amberclient

import (
	"context"
	"fmt"

	"github.com/tmc/go-iroh/iroh"
	irohkey "github.com/tmc/go-iroh/key"
)

// DialConn establishes one raw iroh connection on o.ALPN, returning the
// connection together with the endpoint that carries it (the caller owns
// both: close the connection, then shut the endpoint down). It reuses the
// package's dial plumbing — direct addresses or the discovery stack, all
// candidates raced — but speaks no amber protocol at all: it is the seam for
// the OTHER protocols multiplexed on a jobs-server endpoint, e.g. the
// runner's NATS tunnel (jobs-runner-nats/4.0), whose redial loop needs a
// fresh connection per attempt.
func DialConn(ctx context.Context, o Options) (*iroh.Conn, *iroh.Endpoint, error) {
	if o.ALPN == "" {
		return nil, nil, fmt.Errorf("amberclient: DialConn requires an ALPN")
	}
	id, err := irohkey.ParseEndpointID(o.EndpointID)
	if err != nil {
		return nil, nil, fmt.Errorf("amberclient: parse endpoint id: %w", err)
	}
	ep, cands, err := bindAndResolve(ctx, id, o.Addrs, o.BindAddr)
	if err != nil {
		return nil, nil, err
	}
	conn, err := raceConnect(ctx, ep, id, o.ALPN, cands)
	if err != nil {
		_ = ep.Shutdown(context.WithoutCancel(ctx))
		return nil, nil, fmt.Errorf("amberclient: connect %s: %w", id, err)
	}
	return conn, ep, nil
}
