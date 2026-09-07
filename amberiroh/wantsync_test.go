package amberiroh

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/amber-store/core/ingest"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
)

// buildTree ingests a small directory tree into a fresh packstore and
// returns the store and root key.
func buildTree(t *testing.T) (*packstore.Store, key.Key) {
	t.Helper()
	src := t.TempDir()
	for p, content := range map[string]string{
		"a.txt":         "alpha",
		"sub/b.txt":     "beta",
		"sub/deep/c.go": "gamma content that is a bit longer",
	} {
		full := filepath.Join(src, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	st := openStore(t)
	root, _, err := ingest.Dir(st, src, ingest.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	return st, root
}

func openStore(t *testing.T) *packstore.Store {
	t.Helper()
	st, err := packstore.Open(filepath.Join(t.TempDir(), "packstore"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func TestWantsEmptyStoreWantsRoot(t *testing.T) {
	_, root := buildTree(t)
	dest := openStore(t)
	wants, err := Wants(dest, []key.Key{root}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(wants) != 1 || wants[0] != root {
		t.Fatalf("want [root], got %v", wants)
	}
}

func TestWantsCompleteStorePrunes(t *testing.T) {
	st, root := buildTree(t)
	wants, err := Wants(st, []key.Key{root}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(wants) != 0 {
		t.Fatalf("complete store must prune, got wants %v", wants)
	}
}

// TestWantsPartialSubtreeDescends is the resume-correctness case from the
// spec: the root object is present but its children are missing (an
// interrupted transfer stores parents before children), so presence alone
// must NOT prune.
func TestWantsPartialSubtreeDescends(t *testing.T) {
	st, root := buildTree(t)
	dest := openStore(t)
	rootBytes, err := st.Get(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := dest.Put(root, rootBytes); err != nil {
		t.Fatal(err)
	}
	wants, err := Wants(dest, []key.Key{root}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(wants) != 1 || wants[0] != root {
		t.Fatalf("partial subtree must still be wanted, got %v", wants)
	}
}

func TestWantsDedupes(t *testing.T) {
	_, root := buildTree(t)
	dest := openStore(t)
	wants, err := Wants(dest, []key.Key{root, root, root}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(wants) != 1 {
		t.Fatalf("want deduped [root], got %v", wants)
	}
}

// mkKeys returns n distinct keys, cheaply and without any store.
func mkKeys(n int) []key.Key {
	out := make([]key.Key, n)
	for i := range out {
		out[i][0] = byte(i)
		out[i][1] = byte(i >> 8)
		out[i][2] = byte(i >> 16)
	}
	return out
}

func TestSplitWantsUnderCap(t *testing.T) {
	wants := mkKeys(5)
	send, carry := splitWants(wants, 8)
	if len(send) != 5 || carry != nil {
		t.Fatalf("send=%d carry=%d, want all sent", len(send), len(carry))
	}
}

func TestSplitWantsOverCapCarriesRemainder(t *testing.T) {
	wants := mkKeys(20)
	send, carry := splitWants(wants, 8)
	if len(send) != 8 || len(carry) != 12 {
		t.Fatalf("send=%d carry=%d, want 8/12", len(send), len(carry))
	}
	// Every want must appear exactly once across the split, in order.
	joined := append(append([]key.Key{}, send...), carry...)
	for i := range wants {
		if joined[i] != wants[i] {
			t.Fatalf("split reordered at %d", i)
		}
	}
}

// TestSplitWantsAtCap pins the boundary: exactly max keys ship in one
// round with nothing carried, so an empty want list can never be produced
// by the split itself (which would end the loop early).
func TestSplitWantsAtCap(t *testing.T) {
	send, carry := splitWants(mkKeys(8), 8)
	if len(send) != 8 || carry != nil {
		t.Fatalf("send=%d carry=%d", len(send), len(carry))
	}
}

func TestSplitWantsRespectsFrameLimit(t *testing.T) {
	// A full round must encode inside one frame with room to spare for
	// the message's other fields.
	if maxWantsPerRound*(key.Size+2) > MaxFrame/2 {
		t.Fatalf("maxWantsPerRound %d is too large for MaxFrame %d", maxWantsPerRound, MaxFrame)
	}
}

func TestDedupeKeys(t *testing.T) {
	a, b := mkKeys(2)[0], mkKeys(2)[1]
	got := dedupeKeys([]key.Key{a, b, a, a, b})
	if len(got) != 2 || got[0] != a || got[1] != b {
		t.Fatalf("dedupe got %d keys, want [a b]", len(got))
	}
}

func TestCheckDeliveredReportsMissing(t *testing.T) {
	ks := mkKeys(2)
	got, absent := ks[0], ks[1]
	received := map[key.Key]bool{got: true}
	if err := checkDelivered(received, []key.Key{got}); err != nil {
		t.Fatalf("received key: %v", err)
	}
	err := checkDelivered(received, []key.Key{got, absent})
	if err == nil {
		t.Fatal("undelivered key must be reported")
	}
	if !strings.Contains(err.Error(), "1 of 2") || !strings.Contains(err.Error(), absent.String()) {
		t.Fatalf("error must name the count and an example: %v", err)
	}
}

func TestKeyCodec(t *testing.T) {
	_, root := buildTree(t)
	ks, err := decodeKeys(encodeKeys([]key.Key{root}))
	if err != nil {
		t.Fatal(err)
	}
	if len(ks) != 1 || ks[0] != root {
		t.Fatalf("round trip: %v", ks)
	}
	if _, err := decodeKeys([][]byte{{1, 2, 3}}); err == nil {
		t.Fatal("short key must fail")
	}
}

var lengthKeySeq uint32

// lengthKey builds a distinct key whose embedded logical length is n.
func lengthKey(t *testing.T, n uint64) key.Key {
	t.Helper()
	lengthKeySeq++
	var h [key.Size]byte
	binary.BigEndian.PutUint32(h[:4], lengthKeySeq)
	k, err := key.NewFromHash(key.Blob, n, h)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestDealWantsProportionalToWeights(t *testing.T) {
	var wants []key.Key
	for range 100 {
		wants = append(wants, lengthKey(t, 1000))
	}
	shards := dealWants(wants, []int64{3000, 1000})
	if len(shards) != 2 {
		t.Fatalf("shards %d, want 2", len(shards))
	}
	total := len(shards[0]) + len(shards[1])
	if total != 100 {
		t.Fatalf("dealt %d keys, want 100", total)
	}
	// 3:1 weights over uniform keys: the fast channel gets ~75, slow ~25.
	if len(shards[0]) < 65 || len(shards[0]) > 85 {
		t.Fatalf("fast shard got %d of 100, want ~75", len(shards[0]))
	}
}

func TestDealWantsZeroWeightsFallBackToEven(t *testing.T) {
	var wants []key.Key
	for range 10 {
		wants = append(wants, lengthKey(t, 100))
	}
	shards := dealWants(wants, []int64{0, 0})
	if len(shards[0]) != 5 || len(shards[1]) != 5 {
		t.Fatalf("zero weights: %d/%d, want 5/5", len(shards[0]), len(shards[1]))
	}
}

func TestDealWantsSingleChannel(t *testing.T) {
	wants := []key.Key{lengthKey(t, 1), lengthKey(t, 2)}
	shards := dealWants(wants, []int64{7})
	if len(shards) != 1 || len(shards[0]) != 2 {
		t.Fatalf("single channel must carry everything")
	}
}

func TestDealWantsCoversAllKeysOnce(t *testing.T) {
	var wants []key.Key
	for i := range 31 {
		wants = append(wants, lengthKey(t, uint64(i+1)*17))
	}
	shards := dealWants(wants, []int64{5, 0, 2})
	seen := map[key.Key]int{}
	n := 0
	for _, sh := range shards {
		for _, k := range sh {
			seen[k]++
			n++
		}
	}
	if n != 31 {
		t.Fatalf("dealt %d keys, want 31", n)
	}
	for k, c := range seen {
		if c != 1 {
			t.Fatalf("key %s dealt %d times", k, c)
		}
	}
}
