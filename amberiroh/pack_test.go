package amberiroh

import (
	"bytes"
	"errors"
	"io"
	"iter"
	"testing"

	"github.com/amber-store/core/amberpack"
	"github.com/amber-store/core/fstree"
)

// sendPack serializes objs as one amberpack embedded in TData frames and
// terminates it with TDataEnd. An error from objs aborts the pack without the
// terminator and is returned; the caller decides how to tell the peer
// (typically a TErr frame, which NewPackReader surfaces as *RemoteError).
//
// Test-only: the production push path is SendPackRecords, which streams
// pre-encoded records without a decode/re-encode round trip. This object-level
// variant survives solely to build wire payloads in tests.
func sendPack(w io.Writer, objs iter.Seq2[fstree.Object, error]) error {
	cw := &chunkWriter{w: w}
	pw := amberpack.NewWriter(cw)
	for o, err := range objs {
		if err != nil {
			return err
		}
		if err := pw.Add(o); err != nil {
			return err
		}
	}
	if err := pw.Close(); err != nil {
		return err
	}
	return cw.finish()
}

// testObjects builds n distinct valid blobs.
func testObjects(t *testing.T, n int) []fstree.Object {
	t.Helper()
	objs := make([]fstree.Object, n)
	for i := range objs {
		o, err := fstree.EncodeBlob(bytes.Repeat([]byte{byte(i + 1)}, 100+i))
		if err != nil {
			t.Fatal(err)
		}
		objs[i] = o
	}
	return objs
}

func seqOf(objs []fstree.Object, failAfter int, failErr error) iter.Seq2[fstree.Object, error] {
	return func(yield func(fstree.Object, error) bool) {
		for i, o := range objs {
			if failErr != nil && i == failAfter {
				yield(fstree.Object{}, failErr)
				return
			}
			if !yield(o, nil) {
				return
			}
		}
	}
}

func TestPackRoundTrip(t *testing.T) {
	objs := testObjects(t, 5)
	var buf bytes.Buffer
	if err := sendPack(&buf, seqOf(objs, -1, nil)); err != nil {
		t.Fatalf("SendPack: %v", err)
	}
	pr := NewPackReader(&buf)
	var got []fstree.Object
	for o, err := range amberpack.NewReader(pr).All() {
		if err != nil {
			t.Fatalf("read pack: %v", err)
		}
		got = append(got, o)
	}
	if len(got) != len(objs) {
		t.Fatalf("got %d objects, want %d", len(got), len(objs))
	}
	for i := range objs {
		if got[i].Key != objs[i].Key || !bytes.Equal(got[i].Bytes, objs[i].Bytes) {
			t.Fatalf("object %d differs", i)
		}
	}
	// amberpack stops at its own end marker; draining consumes TDataEnd.
	if _, err := io.Copy(io.Discard, pr); err != nil {
		t.Fatalf("drain: %v", err)
	}
	// The stream must be positioned exactly after TDataEnd.
	if _, err := ReadMsg(&buf); !errors.Is(err, io.EOF) {
		t.Fatalf("stream not fully consumed: %v", err)
	}
}

// TestSendPackRecordsRoundTrip proves stored records are wire-format
// identical: pre-encoded records pass through untouched and decode to
// the original objects.
func TestSendPackRecordsRoundTrip(t *testing.T) {
	objs := testObjects(t, 4)
	recs := make([][]byte, len(objs))
	for i, o := range objs {
		rec, err := amberpack.EncodeRecord(o.Key, o.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		recs[i] = rec
	}
	var buf bytes.Buffer
	seq := func(yield func([]byte, error) bool) {
		for _, r := range recs {
			if !yield(r, nil) {
				return
			}
		}
	}
	if err := SendPackRecords(&buf, seq); err != nil {
		t.Fatalf("SendPackRecords: %v", err)
	}
	pr := NewPackReader(&buf)
	var got []fstree.Object
	for o, err := range amberpack.NewReader(pr).All() {
		if err != nil {
			t.Fatalf("read pack: %v", err)
		}
		got = append(got, o)
	}
	if _, err := io.Copy(io.Discard, pr); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(objs) {
		t.Fatalf("got %d objects, want %d", len(got), len(objs))
	}
	for i := range objs {
		if got[i].Key != objs[i].Key || !bytes.Equal(got[i].Bytes, objs[i].Bytes) {
			t.Fatalf("object %d differs after record pass-through", i)
		}
	}
}

func TestSendPackRecordsPropagatesSourceError(t *testing.T) {
	boom := errors.New("boom")
	var buf bytes.Buffer
	seq := func(yield func([]byte, error) bool) {
		yield(nil, boom)
	}
	if err := SendPackRecords(&buf, seq); !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
}

func TestSendPackPropagatesSourceError(t *testing.T) {
	objs := testObjects(t, 3)
	boom := errors.New("boom")
	var buf bytes.Buffer
	if err := sendPack(&buf, seqOf(objs, 2, boom)); !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
}

func TestPackReaderSurfacesRemoteError(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteMsg(&buf, Msg{Type: TData, Data: []byte("junk")}); err != nil {
		t.Fatal(err)
	}
	if err := WriteMsg(&buf, Msg{Type: TErr, Code: CodeInternal, Text: "sender died"}); err != nil {
		t.Fatal(err)
	}
	_, err := io.ReadAll(NewPackReader(&buf))
	var re *RemoteError
	if !errors.As(err, &re) || re.Code != CodeInternal {
		t.Fatalf("want RemoteError{internal}, got %v", err)
	}
}

func TestPackReaderRejectsUnexpectedFrame(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteMsg(&buf, Msg{Type: TWants}); err != nil {
		t.Fatal(err)
	}
	_, err := io.ReadAll(NewPackReader(&buf))
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("want ErrProtocol, got %v", err)
	}
}
