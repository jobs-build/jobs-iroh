package amberclient

import (
	"context"
	"fmt"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/core/reference"
	"github.com/amber-store/transport-iroh/amberiroh"
)

// GetRef looks up one remote ref by name: its key, creation time and
// creator, transferring nothing — the point lookup that spares a caller
// listing every ref to find one (issue #22: the listing is one frame, which
// stops decoding past 131,072 refs). It is a pull that wants nothing: TPull
// (no data connections), the server's TRef answer, then an empty TWants,
// which ends the server's send loop cleanly before any object moves. Every
// transport-iroh server speaks it. Like a pull, the server counts it as a
// read of the ref (its GC access clock). An absent ref is ErrRefNotFound.
func (c *Client) GetRef(ctx context.Context, name string) (RefInfo, error) {
	if err := reference.ValidateName(name); err != nil {
		return RefInfo{}, fmt.Errorf("amberclient: get ref: %w", err)
	}
	stream, stop, err := c.openStream(ctx)
	if err != nil {
		return RefInfo{}, fmt.Errorf("amberclient: get ref %q: %w", name, err)
	}
	defer stop()
	defer CloseStream(stream)

	if err := amberiroh.WriteMsg(stream, amberiroh.Msg{Type: amberiroh.TPull, Name: name}); err != nil {
		return RefInfo{}, fmt.Errorf("amberclient: get ref %q: %w", name, err)
	}
	m, err := amberiroh.ReadMsg(stream)
	if err != nil {
		return RefInfo{}, fmt.Errorf("amberclient: get ref %q: %w", name, err)
	}
	switch m.Type {
	case amberiroh.TRef:
	case amberiroh.TErr:
		if m.Code == amberiroh.CodeUnknownRef {
			return RefInfo{}, fmt.Errorf("amberclient: get ref %q: %w", name, ErrRefNotFound)
		}
		return RefInfo{}, fmt.Errorf("amberclient: get ref %q: %w", name, amberiroh.RemoteFromMsg(m))
	default:
		return RefInfo{}, fmt.Errorf("amberclient: get ref %q: %w: type %d, want TRef", name, amberiroh.ErrProtocol, m.Type)
	}
	// Want nothing: the server's send loop ends on an empty want list.
	if err := amberiroh.WriteMsg(stream, amberiroh.Msg{Type: amberiroh.TWants}); err != nil {
		return RefInfo{}, fmt.Errorf("amberclient: get ref %q: end pull: %w", name, err)
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

// Refs lists every reference on the server in ONE frame (the amber
// protocol's TRefList has no paging): past 131,072 refs, or 16 MiB, the
// reply no longer decodes. Use GetRef for one name, and jobs-iroh's paged
// admin `refs` API (jobs-admin/1.0) to browse a large store.
func (c *Client) Refs(ctx context.Context) ([]RefInfo, error) {
	stream, stop, err := c.openStream(ctx)
	if err != nil {
		return nil, fmt.Errorf("amberclient: refs: %w", err)
	}
	defer stop()
	defer CloseStream(stream)

	if err := amberiroh.WriteMsg(stream, amberiroh.Msg{Type: amberiroh.TRefList}); err != nil {
		return nil, fmt.Errorf("amberclient: refs: %w", err)
	}
	m, err := amberiroh.ReadMsg(stream)
	if err != nil {
		return nil, fmt.Errorf("amberclient: refs: %w", err)
	}
	switch m.Type {
	case amberiroh.TRefs:
	case amberiroh.TErr:
		return nil, fmt.Errorf("amberclient: refs: %w", amberiroh.RemoteFromMsg(m))
	default:
		return nil, fmt.Errorf("amberclient: refs: %w: type %d, want TRefs", amberiroh.ErrProtocol, m.Type)
	}
	out := make([]RefInfo, 0, len(m.Refs))
	for _, r := range m.Refs {
		k, err := key.Parse(r.Key)
		if err != nil {
			return nil, fmt.Errorf("amberclient: refs: ref %q: key: %w", r.Name, err)
		}
		out = append(out, RefInfo{
			Name:      r.Name,
			Key:       k,
			CreatedAt: time.Unix(0, r.CreatedAt),
			User:      r.User,
		})
	}
	return out, nil
}
