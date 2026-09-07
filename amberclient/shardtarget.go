package amberclient

import (
	"fmt"
	"net/netip"

	"github.com/amber-store/transport-iroh/amberiroh"
	irohkey "github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

// irohkeyFromBytes parses a wire-carried 32-byte endpoint ID.
func irohkeyFromBytes(b []byte) (irohkey.EndpointID, error) {
	return irohkey.EndpointIDFromSlice(b)
}

// shardTarget picks shard i's dial target from a TAccept/TRef. With
// DataEndpoints records the shard authenticates the record's own identity
// and races the dedicated port (on the address the control connection
// reached — spreading load across the server's sockets, as before) together
// with every advertised candidate; ok=false with a nil error means no
// records — the caller runs the legacy path. bare drops relay candidates:
// an endpoint bound without relays can never dial them. A malformed
// candidate is skipped (the rest still race); no dialable candidate at all
// is an error.
func shardTarget(i int, ctrlIP netip.Addr, haveCtrlIP bool, ports []uint16, eps []amberiroh.DataEndpointRec, bare bool) (irohkey.EndpointID, []netaddr.TransportAddr, bool, error) {
	if len(eps) == 0 {
		return irohkey.EndpointID{}, nil, false, nil
	}
	rec := eps[i%len(eps)]
	id, err := irohkeyFromBytes(rec.ID)
	if err != nil {
		return irohkey.EndpointID{}, nil, false, fmt.Errorf("data endpoint %d: bad id: %w", i%len(eps), err)
	}
	seen := make(map[string]bool, len(rec.Addrs)+1)
	var cands []netaddr.TransportAddr
	add := func(ta netaddr.TransportAddr) {
		if s := ta.String(); !seen[s] {
			seen[s] = true
			cands = append(cands, ta)
		}
	}
	if haveCtrlIP && len(ports) > 0 {
		add(netaddr.IPAddr{Addr: netip.AddrPortFrom(ctrlIP, ports[i%len(ports)])})
	}
	for _, s := range rec.Addrs {
		ta, err := netaddr.ParseTransportAddr(s)
		if err != nil {
			continue
		}
		if _, isRelay := ta.(netaddr.RelayAddr); isRelay && bare {
			continue
		}
		add(ta)
	}
	if len(cands) == 0 {
		return irohkey.EndpointID{}, nil, false, fmt.Errorf("data endpoint %d: no dialable candidates", i%len(eps))
	}
	return id, cands, true, nil
}
