package amberiroh

import (
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
)

// duplex joins one side's reader with its writer.
type duplex struct {
	io.Reader
	io.Writer
}

// pipePair returns two connected in-memory duplex streams.
func pipePair() (duplex, duplex) {
	ar, aw := io.Pipe()
	br, bw := io.Pipe()
	return duplex{ar, bw}, duplex{br, aw}
}

// runLoop drives Send on src and Receive on dest concurrently.
func runLoop(t *testing.T, src, dest *packstore.Store, root key.Key) (stats Stats, sendErr, recvErr error) {
	t.Helper()
	a, b := pipePair()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sendErr = Send(a, src, nil)
		// Unblock the peer if the sender bailed early.
		if c, ok := a.Writer.(io.Closer); ok && sendErr != nil {
			c.Close()
		}
	}()
	stats, recvErr = Receive([]io.ReadWriter{b}, dest, root, 0, nil)
	wg.Wait()
	return stats, sendErr, recvErr
}

func TestLoopSyncsIntoEmptyStore(t *testing.T) {
	src, root := buildTree(t)
	dest := openStore(t)
	_, sendErr, recvErr := runLoop(t, src, dest, root)
	if sendErr != nil || recvErr != nil {
		t.Fatalf("send=%v recv=%v", sendErr, recvErr)
	}
	if _, err := fstree.CheckComplete(root, dest.Get, dest.Has, 0); err != nil {
		t.Fatalf("dest incomplete after sync: %v", err)
	}
}

func TestLoopIsIdempotent(t *testing.T) {
	src, root := buildTree(t)
	dest := openStore(t)
	if _, se, re := runLoop(t, src, dest, root); se != nil || re != nil {
		t.Fatalf("first sync: send=%v recv=%v", se, re)
	}
	if _, se, re := runLoop(t, src, dest, root); se != nil || re != nil {
		t.Fatalf("second sync: send=%v recv=%v", se, re)
	}
	if _, err := fstree.CheckComplete(root, dest.Get, dest.Has, 0); err != nil {
		t.Fatal(err)
	}
}

// TestLoopResumesPartialTransfer plants only the root object in dest —
// the on-disk state an interrupted push leaves behind — and verifies the
// loop completes the tree.
func TestLoopResumesPartialTransfer(t *testing.T) {
	src, root := buildTree(t)
	dest := openStore(t)
	rootBytes, err := src.Get(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := dest.Put(root, rootBytes); err != nil {
		t.Fatal(err)
	}
	if _, se, re := runLoop(t, src, dest, root); se != nil || re != nil {
		t.Fatalf("send=%v recv=%v", se, re)
	}
	if _, err := fstree.CheckComplete(root, dest.Get, dest.Has, 0); err != nil {
		t.Fatalf("dest incomplete after resume: %v", err)
	}
}

// TestLoopSenderOmitsWantedObject drives Receive against a sender that
// answers every round with a well-formed but empty pack. The frontier
// advances only through received objects, so without delivery
// verification the loop would terminate as success over an empty store.
func TestLoopSenderOmitsWantedObject(t *testing.T) {
	_, root := buildTree(t)
	dest := openStore(t)
	err := receiveFromEmptyPackSender(t, dest, root)
	if err == nil {
		t.Fatal("undelivered wants must fail the loop, not succeed")
	}
	if !strings.Contains(err.Error(), "omitted 1 of 1") {
		t.Fatalf("error must name the missing wants: %v", err)
	}
}

// TestLoopSenderOmitsIncompleteWantedObject is the resume-path variant: the
// root is already present but incomplete, so it is requested again even
// though the store would answer Has for it. Delivery must be judged by what
// the pack actually carried, or a sender could skip exactly these rounds
// and still have the loop report success over an incomplete tree.
func TestLoopSenderOmitsIncompleteWantedObject(t *testing.T) {
	src, root := buildTree(t)
	dest := openStore(t)
	rootBytes, err := src.Get(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := dest.Put(root, rootBytes); err != nil {
		t.Fatal(err)
	}
	err = receiveFromEmptyPackSender(t, dest, root)
	if err == nil {
		t.Fatal("a present-but-incomplete want left undelivered must fail the loop")
	}
	if !strings.Contains(err.Error(), "omitted 1 of 1") {
		t.Fatalf("error must name the missing wants: %v", err)
	}
}

// receiveFromEmptyPackSender runs Receive against a sender that answers
// every want round with a well-formed but empty pack.
func receiveFromEmptyPackSender(t *testing.T, dest *packstore.Store, root key.Key) error {
	t.Helper()
	a, b := pipePair()
	go func() {
		for {
			m, err := ReadMsg(a)
			if err != nil || m.Type != TWants || len(m.Keys) == 0 {
				if c, ok := a.Writer.(io.Closer); ok {
					c.Close()
				}
				return
			}
			empty := func(yield func(fstree.Object, error) bool) {}
			if err := sendPack(a, empty); err != nil {
				return
			}
		}
	}()
	_, err := Receive([]io.ReadWriter{b}, dest, root, 0, nil)
	return err
}

// recordingProgress sums observer callbacks; safe for the loop's
// single-threaded use.
type recordingProgress struct {
	reqObjs, xferObjs              int
	reqBytes, xferBytes, wireBytes int64
}

func (r *recordingProgress) Requested(objects int, bytes int64) {
	r.reqObjs += objects
	r.reqBytes += bytes
}

func (r *recordingProgress) Transferred(objects int, bytes int64) {
	r.xferObjs += objects
	r.xferBytes += bytes
}

func (r *recordingProgress) Wire(bytes int64) {
	r.wireBytes += bytes
}

// TestLoopReportsProgress drives a fresh sync with observers on both
// halves: each side must see every object of the tree requested and
// transferred, and requested bytes (derived from key lengths) must match
// the bytes actually moved.
func TestLoopReportsProgress(t *testing.T) {
	src, root := buildTree(t)
	dest := openStore(t)
	total, err := fstree.ReachableKeys(root, src.Get)
	if err != nil {
		t.Fatal(err)
	}

	var sendRec, recvRec recordingProgress
	a, b := pipePair()
	var wg sync.WaitGroup
	wg.Add(1)
	var sendErr error
	go func() {
		defer wg.Done()
		sendErr = Send(a, src, &sendRec)
	}()
	_, recvErr := Receive([]io.ReadWriter{b}, dest, root, 0, &recvRec)
	wg.Wait()
	if sendErr != nil || recvErr != nil {
		t.Fatalf("send=%v recv=%v", sendErr, recvErr)
	}

	for name, rec := range map[string]*recordingProgress{"send": &sendRec, "recv": &recvRec} {
		if rec.reqObjs != len(total) || rec.xferObjs != len(total) {
			t.Fatalf("%s: requested=%d transferred=%d objects, want both %d", name, rec.reqObjs, rec.xferObjs, len(total))
		}
		// Key lengths are logical sizes: subtree footprints for
		// directory types, so requested bytes bound transferred
		// payload bytes from above.
		if rec.xferBytes == 0 || rec.reqBytes < rec.xferBytes {
			t.Fatalf("%s: requested %d bytes must be >= transferred %d", name, rec.reqBytes, rec.xferBytes)
		}
		if rec.wireBytes == 0 {
			t.Fatalf("%s: wire bytes must be observed", name)
		}
	}
}

// TestReceiveStats checks the transfer accounting a fresh sync and an
// idempotent re-sync report: a fresh sync requests and receives exactly
// the tree's objects and counts wire bytes; a re-sync moves nothing and
// ends after the single empty want round.
func TestReceiveStats(t *testing.T) {
	src, root := buildTree(t)
	dest := openStore(t)
	total, err := fstree.ReachableKeys(root, src.Get)
	if err != nil {
		t.Fatal(err)
	}

	stats, se, re := runLoop(t, src, dest, root)
	if se != nil || re != nil {
		t.Fatalf("send=%v recv=%v", se, re)
	}
	if stats.Received != len(total) || stats.Requested != len(total) {
		t.Fatalf("fresh sync: requested=%d received=%d, want both %d", stats.Requested, stats.Received, len(total))
	}
	if stats.Bytes == 0 {
		t.Fatal("fresh sync must count wire bytes")
	}
	if stats.Rounds < 2 {
		t.Fatalf("fresh sync of a multi-level tree took %d rounds", stats.Rounds)
	}

	stats, se, re = runLoop(t, src, dest, root)
	if se != nil || re != nil {
		t.Fatalf("re-sync: send=%v recv=%v", se, re)
	}
	if stats.Received != 0 || stats.Requested != 0 || stats.Bytes != 0 {
		t.Fatalf("re-sync must transfer nothing: %+v", stats)
	}
	if stats.Rounds != 1 {
		t.Fatalf("re-sync must end after the empty round, took %d", stats.Rounds)
	}
}

// TestLoopSenderMissingObject syncs from a sender that lacks the tree:
// the sender must report a remote error and the receiver must fail, not
// hang or succeed.
func TestLoopSenderMissingObject(t *testing.T) {
	_, root := buildTree(t)
	emptySrc := openStore(t)
	dest := openStore(t)
	_, sendErr, recvErr := runLoop(t, emptySrc, dest, root)
	if !errors.Is(sendErr, packstore.ErrNotFound) {
		t.Fatalf("sender error: %v", sendErr)
	}
	var re *RemoteError
	if !errors.As(recvErr, &re) || re.Code != CodeInternal {
		t.Fatalf("receiver error: %v", recvErr)
	}
}

// TestLoopShardedAcrossChannels deals each round's wants across three
// channels, each served by an independent Send loop, and the destination
// must still assemble the complete tree.
func TestLoopShardedAcrossChannels(t *testing.T) {
	src, root := buildTree(t)
	dest := openStore(t)
	total, err := fstree.ReachableKeys(root, src.Get)
	if err != nil {
		t.Fatal(err)
	}

	const n = 3
	senders := make([]duplex, n)
	receivers := make([]io.ReadWriter, n)
	for i := 0; i < n; i++ {
		a, b := pipePair()
		senders[i], receivers[i] = a, b
	}
	recs := make([]recordingProgress, n)
	var wg sync.WaitGroup
	sendErrs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sendErrs[i] = Send(senders[i], src, &recs[i])
		}(i)
	}
	stats, recvErr := Receive(receivers, dest, root, 0, nil)
	wg.Wait()
	for i, err := range sendErrs {
		if err != nil {
			t.Fatalf("sender %d: %v", i, err)
		}
	}
	if recvErr != nil {
		t.Fatalf("receive: %v", recvErr)
	}
	if _, err := fstree.CheckComplete(root, dest.Get, dest.Has, 0); err != nil {
		t.Fatalf("dest incomplete after sharded sync: %v", err)
	}
	if stats.Received != len(total) {
		t.Fatalf("received %d objects, want %d", stats.Received, len(total))
	}
	var sumObjs int
	channelsUsed := 0
	for i := range recs {
		sumObjs += recs[i].xferObjs
		if recs[i].xferObjs > 0 {
			channelsUsed++
		}
	}
	if sumObjs != len(total) {
		t.Fatalf("senders moved %d objects total, want %d", sumObjs, len(total))
	}
	if channelsUsed < 2 {
		t.Fatalf("sharding must spread work: only %d of %d channels used", channelsUsed, n)
	}
}

// TestLoopShardedSenderOmits pins the failure mode of the concurrent
// merge: one of three channels answers its shard with an empty pack.
// The loop must fail with the omission error — not hang and not commit.
func TestLoopShardedSenderOmits(t *testing.T) {
	src, root := buildTree(t)
	dest := openStore(t)

	const n = 3
	senders := make([]duplex, n)
	receivers := make([]io.ReadWriter, n)
	for i := 0; i < n; i++ {
		a, b := pipePair()
		senders[i], receivers[i] = a, b
	}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i != 1 {
				_ = Send(senders[i], src, nil)
				return
			}
			// Channel 1 answers every round with a well-formed but
			// empty pack, then keeps listening.
			for {
				m, err := ReadMsg(senders[i])
				if err != nil || m.Type != TWants || len(m.Keys) == 0 {
					return
				}
				empty := func(yield func(fstree.Object, error) bool) {}
				if err := sendPack(senders[i], empty); err != nil {
					return
				}
			}
		}(i)
	}
	done := make(chan error, 1)
	go func() {
		_, err := Receive(receivers, dest, root, 0, nil)
		done <- err
		for i := 0; i < n; i++ {
			if c, ok := receivers[i].(duplex); ok {
				if cl, ok := c.Writer.(io.Closer); ok {
					cl.Close()
				}
			}
		}
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "omitted") {
			t.Fatalf("want omission error, got %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("sharded receive hung on an omitting sender")
	}
	wg.Wait()
}

func TestShardWants(t *testing.T) {
	_, root := buildTree(t)
	keys := []key.Key{root, root, root, root, root}
	shards := shardWants(keys, 3)
	if len(shards) != 3 {
		t.Fatalf("want 3 shards, got %d", len(shards))
	}
	var total int
	for i, sh := range shards {
		total += len(sh)
		if len(sh) < 1 || len(sh) > 2 {
			t.Fatalf("shard %d has %d keys; round-robin must balance within 1", i, len(sh))
		}
	}
	if total != len(keys) {
		t.Fatalf("shards carry %d keys, want %d", total, len(keys))
	}
}
