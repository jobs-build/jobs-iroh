package amberclient

import (
	"net/netip"
	"testing"

	"github.com/amber-store/transport-iroh/amberiroh"
	irohkey "github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

// recWith builds a record with a real (parseable) endpoint identity — the
// wire ID is a validated Ed25519 point, not opaque bytes.
func recWith(t *testing.T, addrs ...string) amberiroh.DataEndpointRec {
	t.Helper()
	sk, err := irohkey.GenerateSecretKey()
	if err != nil {
		t.Fatal(err)
	}
	id := irohkey.EndpointID(sk.Public()).Bytes()
	return amberiroh.DataEndpointRec{ID: id[:], Addrs: addrs}
}

func addrStrings(cands []netaddr.TransportAddr) []string {
	out := make([]string, len(cands))
	for i, c := range cands {
		out[i] = c.String()
	}
	return out
}

func TestShardTargetNoRecordsMeansLegacy(t *testing.T) {
	_, _, ok, err := shardTarget(0, netip.Addr{}, false, []uint16{4001}, nil, true)
	if ok || err != nil {
		t.Fatalf("ok=%v err=%v, want legacy fallthrough", ok, err)
	}
}

func TestShardTargetUnionsAndDedupes(t *testing.T) {
	ctrl := netip.MustParseAddr("192.0.2.1")
	eps := []amberiroh.DataEndpointRec{recWith(t, "ip:192.0.2.1:4001", "ip:10.0.0.5:4001", "relay:https://euc1-1.relay.example./")}
	id, cands, ok, err := shardTarget(0, ctrl, true, []uint16{4001}, eps, false)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if want, _ := irohkeyFromBytes(eps[0].ID); id != want {
		t.Fatalf("id %s, want record's", id)
	}
	got := addrStrings(cands)
	want := []string{"ip:192.0.2.1:4001", "ip:10.0.0.5:4001", "relay:https://euc1-1.relay.example./"}
	if len(got) != len(want) {
		t.Fatalf("candidates %v, want %v (dedup of the dedicated addr)", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("candidates %v, want %v", got, want)
		}
	}
}

func TestShardTargetBareFiltersRelay(t *testing.T) {
	eps := []amberiroh.DataEndpointRec{recWith(t, "relay:https://euc1-1.relay.example./", "ip:10.0.0.5:4001")}
	_, cands, ok, err := shardTarget(0, netip.Addr{}, false, nil, eps, true)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	got := addrStrings(cands)
	if len(got) != 1 || got[0] != "ip:10.0.0.5:4001" {
		t.Fatalf("bare candidates %v, want the ip candidate only", got)
	}
}

func TestShardTargetCyclesRecords(t *testing.T) {
	eps := []amberiroh.DataEndpointRec{recWith(t, "ip:10.0.0.5:4001"), recWith(t, "ip:10.0.0.5:4002")}
	id, _, ok, err := shardTarget(3, netip.Addr{}, false, nil, eps, false)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if want, _ := irohkeyFromBytes(eps[1].ID); id != want {
		t.Fatalf("shard 3 of 2 records: id %s, want eps[1]'s", id)
	}
}

func TestShardTargetSkipsMalformedAddrs(t *testing.T) {
	eps := []amberiroh.DataEndpointRec{recWith(t, "not-a-transport-addr", "ip:10.0.0.5:4001")}
	_, cands, ok, err := shardTarget(0, netip.Addr{}, false, nil, eps, false)
	if err != nil || !ok || len(cands) != 1 {
		t.Fatalf("ok=%v err=%v cands=%v", ok, err, cands)
	}
}

func TestShardTargetAllFilteredIsError(t *testing.T) {
	eps := []amberiroh.DataEndpointRec{recWith(t, "relay:https://euc1-1.relay.example./")}
	_, _, _, err := shardTarget(0, netip.Addr{}, false, nil, eps, true)
	if err == nil {
		t.Fatal("no dialable candidates must error, not dial nothing")
	}
}

func TestShardTargetBadIDIsError(t *testing.T) {
	eps := []amberiroh.DataEndpointRec{{ID: []byte{1, 2, 3}, Addrs: []string{"ip:10.0.0.5:4001"}}}
	_, _, _, err := shardTarget(0, netip.Addr{}, false, nil, eps, false)
	if err == nil {
		t.Fatal("truncated ID must error")
	}
}
