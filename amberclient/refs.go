package amberclient

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/core/reference"
	"github.com/amber-store/transport-iroh/amberiroh"
)

// ErrUnsupported is returned (errors.Is-able, wrapping the server's
// *amberiroh.RemoteError) when the server predates an operation — an
// older transport-iroh answers an unknown op with bad-request. Callers fall
// back to what such a server does support.
var ErrUnsupported = errors.New("amberclient: operation not supported by server")

// RefPage is the page size the listing helpers ask for; the server clamps
// anything larger to the same bound.
const RefPage = amberiroh.MaxRefPage

// GetRef looks up one remote ref by name (TRefGet): its key, creation time
// and creator, without transferring anything — the point lookup that spares
// a caller listing every ref to find one. An absent ref is ErrRefNotFound;
// a server without TRefGet is ErrUnsupported. The server counts the lookup
// as a read of the ref (its GC access clock), as it does a pull.
func (c *Client) GetRef(ctx context.Context, name string) (RefInfo, error) {
	stream, stop, err := c.openStream(ctx)
	if err != nil {
		return RefInfo{}, fmt.Errorf("amberclient: get ref %q: %w", name, err)
	}
	defer stop()
	defer CloseStream(stream)

	if err := amberiroh.WriteMsg(stream, amberiroh.Msg{Type: amberiroh.TRefGet, Name: name}); err != nil {
		return RefInfo{}, fmt.Errorf("amberclient: get ref %q: %w", name, err)
	}
	m, err := amberiroh.ReadMsg(stream)
	if err != nil {
		return RefInfo{}, fmt.Errorf("amberclient: get ref %q: %w", name, err)
	}
	switch m.Type {
	case amberiroh.TRef:
	case amberiroh.TErr:
		switch m.Code {
		case amberiroh.CodeUnknownRef:
			return RefInfo{}, fmt.Errorf("amberclient: get ref %q: %w", name, ErrRefNotFound)
		case amberiroh.CodeBadRequest:
			return RefInfo{}, fmt.Errorf("amberclient: get ref %q: %w (%w)", name, ErrUnsupported, amberiroh.RemoteFromMsg(m))
		}
		return RefInfo{}, fmt.Errorf("amberclient: get ref %q: %w", name, amberiroh.RemoteFromMsg(m))
	default:
		return RefInfo{}, fmt.Errorf("amberclient: get ref %q: %w: type %d, want TRef", name, amberiroh.ErrProtocol, m.Type)
	}
	rec, err := reference.Decode(m.Record)
	if err != nil {
		return RefInfo{}, fmt.Errorf("amberclient: get ref %q: server ref record: %w", name, err)
	}
	k, err := key.Parse(rec.Key)
	if err != nil {
		return RefInfo{}, fmt.Errorf("amberclient: get ref %q: server ref record key: %w", name, err)
	}
	return RefInfo{Name: rec.Name, Key: k, CreatedAt: time.Unix(0, rec.CreatedAt), User: rec.User}, nil
}

// RefsPage lists one page of remote refs: names starting with prefix and
// sorting strictly after after, in name order, at most limit of them (the
// server clamps to amberiroh.MaxRefPage). more reports that matching refs
// remain past the last one returned. An old server ignores the paging and
// answers every ref at once: the reply is filtered here and more is false,
// so a paging loop over either kind of server sees the same refs.
func (c *Client) RefsPage(ctx context.Context, prefix, after string, limit int) ([]RefInfo, bool, error) {
	if limit <= 0 {
		limit = RefPage
	}
	stream, stop, err := c.openStream(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("amberclient: refs: %w", err)
	}
	defer stop()
	defer CloseStream(stream)

	req := amberiroh.Msg{Type: amberiroh.TRefList, Prefix: prefix, After: after, Limit: limit}
	if err := amberiroh.WriteMsg(stream, req); err != nil {
		return nil, false, fmt.Errorf("amberclient: refs: %w", err)
	}
	m, err := amberiroh.ReadMsg(stream)
	if err != nil {
		return nil, false, fmt.Errorf("amberclient: refs: %w", err)
	}
	switch m.Type {
	case amberiroh.TRefs:
	case amberiroh.TErr:
		return nil, false, fmt.Errorf("amberclient: refs: %w", amberiroh.RemoteFromMsg(m))
	default:
		return nil, false, fmt.Errorf("amberclient: refs: %w: type %d, want TRefs", amberiroh.ErrProtocol, m.Type)
	}
	out := make([]RefInfo, 0, len(m.Refs))
	for _, r := range m.Refs {
		if !strings.HasPrefix(r.Name, prefix) || (after != "" && r.Name <= after) {
			continue // an old server's unfiltered answer
		}
		k, err := key.Parse(r.Key)
		if err != nil {
			return nil, false, fmt.Errorf("amberclient: refs: ref %q: key: %w", r.Name, err)
		}
		out = append(out, RefInfo{
			Name:      r.Name,
			Key:       k,
			CreatedAt: time.Unix(0, r.CreatedAt),
			User:      r.User,
		})
	}
	return out, m.More && len(m.Refs) > 0, nil
}

// EachRef calls fn for every remote ref whose name starts with prefix, in
// name order, one RefsPage at a time — memory and frames stay bounded by a
// page whatever the server holds. fn returning an error stops the walk and
// returns it.
func (c *Client) EachRef(ctx context.Context, prefix string, fn func(RefInfo) error) error {
	after := ""
	for {
		refs, more, err := c.RefsPage(ctx, prefix, after, RefPage)
		if err != nil {
			return err
		}
		for _, r := range refs {
			if err := fn(r); err != nil {
				return err
			}
		}
		if !more || len(refs) == 0 {
			return nil
		}
		after = refs[len(refs)-1].Name
	}
}

// Refs lists every reference on the server (paged underneath, see
// EachRef). Prefer GetRef for one name and EachRef with a prefix for a
// namespace: on a large store the full list is large.
func (c *Client) Refs(ctx context.Context) ([]RefInfo, error) {
	var out []RefInfo
	err := c.EachRef(ctx, "", func(r RefInfo) error {
		out = append(out, r)
		return nil
	})
	return out, err
}
