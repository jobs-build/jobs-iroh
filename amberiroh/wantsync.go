package amberiroh

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/amber-store/core/amberpack"
	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
)

// maxWantsPerRound caps how many keys one TWants frame may carry. A very
// wide frontier would otherwise encode past MaxFrame, which is a
// permanent failure: re-running recomputes the same oversized round. The
// remainder is carried into the next round instead.
const maxWantsPerRound = 32 << 10

// splitWants divides wants into the keys to request this round and the
// remainder to defer to the next one.
func splitWants(wants []key.Key, max int) (send, carry []key.Key) {
	if len(wants) <= max {
		return wants, nil
	}
	return wants[:max], wants[max:]
}

// Wants partitions frontier into the keys that must be transferred. A key
// is pruned only when its object is present AND the whole subtree below it
// is complete — presence alone is not enough, because an interrupted
// transfer stores parents before their children. A present-but-incomplete
// key is re-requested whole; its redundant bytes are trivial and the
// receiver re-walks its children from the fresh copy.
func Wants(st *packstore.Store, frontier []key.Key, jobs int) ([]key.Key, error) {
	var wants []key.Key
	seen := make(map[key.Key]bool, len(frontier))
	for _, k := range frontier {
		if seen[k] {
			continue
		}
		seen[k] = true
		ok, err := st.Has(k)
		if err != nil {
			return nil, err
		}
		if !ok {
			wants = append(wants, k)
			continue
		}
		_, err = fstree.CheckComplete(k, st.Get, st.Has, jobs)
		switch {
		case err == nil: // complete subtree: prune
		case isMissing(err):
			wants = append(wants, k)
		default:
			return nil, err
		}
	}
	return wants, nil
}

// isMissing reports whether a CheckComplete failure means "object absent"
// (leaves surface as *fstree.MissingObjectError, interior nodes as the
// store's not-found error) rather than a real store fault.
func isMissing(err error) bool {
	var m *fstree.MissingObjectError
	return errors.As(err, &m) || errors.Is(err, packstore.ErrNotFound)
}

// encodeKeys flattens keys for a TWants frame.
func encodeKeys(keys []key.Key) [][]byte {
	out := make([][]byte, len(keys))
	for i, k := range keys {
		kk := k
		out[i] = kk[:]
	}
	return out
}

// decodeKeys parses and validates TWants keys.
func decodeKeys(bs [][]byte) ([]key.Key, error) {
	out := make([]key.Key, len(bs))
	for i, b := range bs {
		k, err := key.Parse(b)
		if err != nil {
			return nil, err
		}
		out[i] = k
	}
	return out, nil
}

// Progress observes a transfer as it happens. Requested grows the known
// work set when a want round is exchanged; its bytes come from the keys'
// embedded lengths, which are logical sizes (subtree footprints for
// directory types), so they bound the payload bytes from above rather
// than equal them. Transferred records objects that actually moved, with
// exact payload bytes. Callbacks arrive from the transfer goroutines;
// implementations must be safe for concurrent use. A nil Progress is
// legal and ignored.
type Progress interface {
	Requested(objects int, bytes int64)
	Transferred(objects int, bytes int64)
	// Wire reports bytes as they cross the stream (compressed records
	// plus framing), called from the transfer's read/write path.
	Wire(bytes int64)
}

// keyBytes sums the logical lengths embedded in keys — an upper bound
// on the uncompressed bytes of transferring those objects and, for
// aggregate types, everything below them.
func keyBytes(keys []key.Key) int64 {
	var n int64
	for _, k := range keys {
		n += int64(k.Length())
	}
	return n
}

// Stats summarizes one Receive run for transfer accounting.
type Stats struct {
	Rounds    int   // want rounds sent, including the final empty one
	Requested int   // keys requested across all rounds
	Received  int   // objects delivered in packs
	Bytes     int64 // wire bytes read: pack frames and payloads
}

// countingReader counts the bytes read through it, reporting them to an
// optional Progress as they arrive.
type countingReader struct {
	r    io.Reader
	n    int64
	prog Progress
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	if c.prog != nil && n > 0 {
		c.prog.Wire(int64(n))
	}
	return n, err
}

// wireWriter reports bytes written to the stream to a Progress.
type wireWriter struct {
	w    io.Writer
	prog Progress
}

func (w *wireWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	if w.prog != nil && n > 0 {
		w.prog.Wire(int64(n))
	}
	return n, err
}

// Receive runs the receiving half of the want loop: rounds of TWants →
// amberpack until nothing below root is missing. channels[0] is the
// control channel; any further channels are extra data connections and
// each round's wants are dealt across all of them, with the packs decoded
// concurrently into one verified write stream. A channel whose shard is
// empty gets no frame that round (an empty TWants would terminate its
// sender); the final, empty TWants goes to every channel to end the
// loop. Received objects are verified against their keys before being
// stored — the peer is untrusted.
func Receive(channels []io.ReadWriter, st *packstore.Store, root key.Key, jobs int, prog Progress) (Stats, error) {
	var stats Stats
	crs := make([]*countingReader, len(channels))
	for i, ch := range channels {
		crs[i] = &countingReader{r: ch, prog: prog}
	}
	wireTotal := func() int64 {
		var n int64
		for _, cr := range crs {
			n += cr.n
		}
		return n
	}
	frontier := []key.Key{root}
	// Per-channel throughput sampling for dealWants: each round's byte
	// deltas weight the next round's shards, so channel-speed asymmetry
	// (a relayed straggler among punched siblings) stops dictating the
	// whole round's pace.
	prevWire := make([]int64, len(channels))
	weights := make([]int64, len(channels))
	for {
		wants, err := Wants(st, frontier, jobs)
		if err != nil {
			return stats, err
		}
		wants, carry := splitWants(wants, maxWantsPerRound)
		stats.Rounds++
		stats.Requested += len(wants)
		if prog != nil && len(wants) > 0 {
			prog.Requested(len(wants), keyBytes(wants))
		}
		if len(wants) == 0 {
			for _, ch := range channels {
				if err := WriteMsg(ch, Msg{Type: TWants}); err != nil {
					return stats, err
				}
			}
			stats.Bytes = wireTotal()
			return stats, nil
		}

		for i, cr := range crs {
			weights[i] = cr.n - prevWire[i]
			prevWire[i] = cr.n
		}
		shards := dealWants(wants, weights)
		type active struct {
			idx     int
			packSrc io.Reader
			tracked *errTrackingReader
		}
		var actives []*active
		for i, shard := range shards {
			if len(shard) == 0 {
				continue
			}
			if err := WriteMsg(channels[i], Msg{Type: TWants, Keys: encodeKeys(shard)}); err != nil {
				return stats, err
			}
			actives = append(actives, &active{idx: i})
		}

		// Decode every active channel's pack concurrently into one
		// stream of objects; a single consumer records receipt and
		// children, and WriteParallel verifies and stores. done stops
		// the decoders if the consumer aborts early, so nobody blocks
		// on the merge channel forever.
		type item struct {
			obj  packstore.Object
			kids []key.Key
		}
		merged := make(chan item, 64)
		done := make(chan struct{})
		closeDone := sync.OnceFunc(func() { close(done) })
		decodeErrs := make([]error, len(actives))
		var wg sync.WaitGroup
		for ai, a := range actives {
			packSrc := NewPackReader(crs[a.idx])
			a.packSrc = packSrc
			a.tracked = &errTrackingReader{Reader: packSrc}
			wg.Add(1)
			go func(ai int, a *active) {
				defer wg.Done()
				for o, err := range amberpack.NewReader(a.tracked).All() {
					if err != nil {
						decodeErrs[ai] = err
						return
					}
					kids, err := fstree.ChildKeys(o.Key, o.Bytes)
					if err != nil {
						decodeErrs[ai] = err
						return
					}
					select {
					case merged <- item{obj: packstore.Object{Key: o.Key, Data: o.Bytes}, kids: kids}:
					case <-done:
						return
					}
				}
				// amberpack stops at its own end marker; drain through
				// our TDataEnd frame so the channel is frame-aligned
				// for the next round.
				if _, err := io.Copy(io.Discard, a.packSrc); err != nil {
					decodeErrs[ai] = err
				}
			}(ai, a)
		}
		go func() {
			wg.Wait()
			close(merged)
		}()

		var next []key.Key
		received := make(map[key.Key]bool, len(wants))
		seq := func(yield func(packstore.Object, error) bool) {
			defer closeDone()
			for it := range merged {
				received[it.obj.Key] = true
				next = append(next, it.kids...)
				if prog != nil {
					prog.Transferred(1, int64(len(it.obj.Data)))
				}
				if !yield(it.obj, nil) {
					return
				}
			}
		}
		_, werr := st.WriteParallel(seq, packstore.WriteOpts{Verify: true})
		closeDone()
		wg.Wait()
		stats.Received += len(received)

		// A TErr the sender wrote inside a pack surfaces through the
		// per-channel tracked readers; amberpack's own wrapping loses
		// the error type, so prefer the tracked errors, then decode
		// errors, then whatever the store reported.
		var chanErrs []error
		for ai, a := range actives {
			if a.tracked != nil && a.tracked.err != nil {
				if re := new(RemoteError); errors.As(a.tracked.err, &re) {
					return stats, a.tracked.err
				}
			}
			if decodeErrs[ai] != nil {
				chanErrs = append(chanErrs, decodeErrs[ai])
			}
		}
		if len(chanErrs) > 0 {
			return stats, errors.Join(chanErrs...)
		}
		if werr != nil {
			return stats, werr
		}
		if err := checkDelivered(received, wants); err != nil {
			return stats, err
		}
		// Carried-over wants rejoin the frontier; Wants dedupes them
		// against the children just discovered and re-verifies presence.
		frontier = append(next, carry...)
	}
}

// shardWants deals wants round-robin across n shards. A shard can come
// back empty; its channel then simply gets no frame that round.
func shardWants(wants []key.Key, n int) [][]key.Key {
	shards := make([][]key.Key, n)
	for i, k := range wants {
		shards[i%n] = append(shards[i%n], k)
	}
	return shards
}

// dealWants distributes wants across len(weights) shards proportionally to
// the weights — observed per-channel throughput, so a slow channel (say, a
// shard stuck on the relay while its siblings punched) stops being the
// per-round straggler every other channel barriers on. Keys are weighed by
// their embedded logical lengths; each key goes to the channel with the
// lowest assigned-bytes-to-weight ratio (ties to the lower index).
// Non-positive weights are floored to 1 so no channel is locked out — it
// keeps a probe-sized share and earns its weight back.
func dealWants(wants []key.Key, weights []int64) [][]key.Key {
	n := len(weights)
	shards := make([][]key.Key, n)
	if n == 1 {
		shards[0] = wants
		return shards
	}
	w := make([]float64, n)
	for i, wt := range weights {
		if wt < 1 {
			wt = 1
		}
		w[i] = float64(wt)
	}
	assigned := make([]float64, n)
	for _, k := range wants {
		best := 0
		for i := 1; i < n; i++ {
			if assigned[i]/w[i] < assigned[best]/w[best] {
				best = i
			}
		}
		bytes := float64(k.Length())
		if bytes < 1 {
			bytes = 1
		}
		assigned[best] += bytes
		shards[best] = append(shards[best], k)
	}
	return shards
}

// checkDelivered fails when the sender did not deliver every key this
// round asked for. The frontier only advances to children of received
// objects, so an omitted want would otherwise vanish silently and let an
// incomplete tree end the loop as success. Receipt is judged by what
// arrived in the pack, not by store presence: a key requested because it
// was present but incomplete already satisfies Has, so presence would let
// a sender skip exactly the resume rounds. An honest sender always resends
// every requested key.
func checkDelivered(received map[key.Key]bool, wants []key.Key) error {
	missing := 0
	var example key.Key
	for _, k := range wants {
		if !received[k] {
			if missing == 0 {
				example = k
			}
			missing++
		}
	}
	if missing > 0 {
		return fmt.Errorf("sender omitted %d of %d requested objects (e.g. %s)", missing, len(wants), example)
	}
	return nil
}

// Send runs the sending half: answer each TWants round with a pack of
// exactly the requested objects, until an empty TWants ends the loop. A
// local read failure is reported to the peer as a TErr frame and returned.
func Send(rw io.ReadWriter, st *packstore.Store, prog Progress) error {
	for {
		m, err := ReadMsg(rw)
		if err != nil {
			return err
		}
		switch m.Type {
		case TErr:
			return RemoteFromMsg(m)
		case TWants:
		default:
			return fmt.Errorf("%w: type %d, want TWants", ErrProtocol, m.Type)
		}
		if len(m.Keys) == 0 {
			return nil
		}
		keys, err := decodeKeys(m.Keys)
		if err != nil {
			return err
		}
		keys = dedupeKeys(keys)
		if prog != nil {
			prog.Requested(len(keys), keyBytes(keys))
		}
		st.SortByLocation(keys)
		// Zero-copy push path: stored records are wire-format identical
		// (header + still-compressed payload), so they go out verbatim —
		// no decompress/re-encode. ParseRecord validates framing and CRC
		// and yields the uncompressed length for progress accounting.
		seq := func(yield func([]byte, error) bool) {
			for _, k := range keys {
				rec, err := st.GetRecord(k)
				if err != nil {
					yield(nil, fmt.Errorf("object %s: %w", k, err))
					return
				}
				hdr, err := amberpack.ParseRecord(rec)
				if err != nil {
					yield(nil, fmt.Errorf("object %s: stored record: %w", k, err))
					return
				}
				if prog != nil {
					prog.Transferred(1, int64(hdr.Ulen))
				}
				if !yield(rec, nil) {
					return
				}
			}
		}
		if err := SendPackRecords(&wireWriter{w: rw, prog: prog}, seq); err != nil {
			// Best effort: tell the peer why the pack stopped short.
			_ = WriteMsg(rw, Msg{Type: TErr, Code: CodeInternal, Text: err.Error()})
			return err
		}
	}
}

// dedupeKeys drops repeats, keeping first-seen order: a peer that asks
// for one key many times must not get its bytes many times.
func dedupeKeys(keys []key.Key) []key.Key {
	seen := make(map[key.Key]bool, len(keys))
	out := keys[:0]
	for _, k := range keys {
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, k)
	}
	return out
}

// errTrackingReader remembers the last non-EOF error a Read returned,
// unmodified. amberpack.Reader.All folds read errors into its own
// ErrMalformed-wrapped message with %v instead of %w, which erases the
// type of errors like *RemoteError; Receive consults the tracked
// error to recover it.
type errTrackingReader struct {
	io.Reader
	err error
}

func (t *errTrackingReader) Read(p []byte) (int, error) {
	n, err := t.Reader.Read(p)
	if err != nil && err != io.EOF {
		t.err = err
	}
	return n, err
}
