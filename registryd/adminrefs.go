package registryd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tmc/go-iroh/iroh"

	"github.com/jobs-build/jobs-iroh/amberclient"
	"github.com/jobs-build/jobs-iroh/api"
)

// alpnAdmin is serve.ALPNAdmin, declared locally like alpnAmberAdmin.
const alpnAdmin = "jobs-admin/1.0"

// eachServerRef calls fn with the name of every jobs-server ref starting
// with prefix, in name order, through the server's paged admin `refs` API
// (one api.MaxRefsPage page per call, the After cursor between them). The
// amber protocol's own listing (TRefList) has no paging — one frame of
// every ref, unreadable past 131,072 refs (issue #22) — so the tag list
// walks this instead. One connection per walk: tag listings are rare. A
// server that ignores After (older than this registry) answers its first
// page again: names at or before the cursor are skipped, and its reply
// carries no More, which ends the walk.
func (r *registry) eachServerRef(ctx context.Context, prefix string, fn func(name string) error) error {
	conn, ep, err := amberclient.DialConn(ctx, r.adminOpts)
	if err != nil {
		return fmt.Errorf("dial admin: %w", err)
	}
	defer ep.Shutdown(context.Background())
	defer conn.Close()
	after := ""
	for {
		reply, err := refsPage(ctx, conn, api.RefsRequest{Prefix: prefix, After: after, Limit: api.MaxRefsPage})
		if err != nil {
			return err
		}
		for _, ri := range reply.Refs {
			if after != "" && ri.Name <= after {
				continue
			}
			if err := fn(ri.Name); err != nil {
				return err
			}
		}
		if !reply.More || len(reply.Refs) == 0 {
			return nil
		}
		after = reply.Refs[len(reply.Refs)-1].Name
	}
}

// refsPage performs one admin refs exchange on its own stream, cancelled
// with ctx (deadline expiry, as clientcli's API calls do).
func refsPage(ctx context.Context, conn *iroh.Conn, req api.RefsRequest) (api.RefsReply, error) {
	stream, err := conn.OpenStreamConn(ctx)
	if err != nil {
		return api.RefsReply{}, fmt.Errorf("admin refs: open stream: %w", err)
	}
	stop := context.AfterFunc(ctx, func() { _ = stream.SetDeadline(time.Now()) })
	defer stop()
	defer amberclient.CloseStream(stream)
	if err := api.WriteFrame(stream, api.TRefs, req); err != nil {
		return api.RefsReply{}, fmt.Errorf("admin refs: send: %w", err)
	}
	rt, rb, err := api.ReadFrame(stream)
	if err != nil {
		return api.RefsReply{}, fmt.Errorf("admin refs: read reply: %w", err)
	}
	switch rt {
	case api.TRefsReply:
		var reply api.RefsReply
		if len(rb) == 0 {
			return reply, nil // an empty listing is an omitempty-empty body
		}
		if err := api.DecodeBody(rb, &reply); err != nil {
			return api.RefsReply{}, fmt.Errorf("admin refs: decode reply: %w", err)
		}
		return reply, nil
	case api.TError:
		var e api.Error
		if err := api.DecodeBody(rb, &e); err != nil {
			return api.RefsReply{}, fmt.Errorf("admin refs: decode error: %w", err)
		}
		return api.RefsReply{}, fmt.Errorf("admin refs: server: %s: %s", e.Code, e.Text)
	default:
		return api.RefsReply{}, errors.New("admin refs: unexpected reply " + rt)
	}
}
