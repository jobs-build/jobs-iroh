package api

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"runtime"
	"testing"
)

// frameOf wraps a raw CBOR payload in the 4-byte length header.
func frameOf(payload []byte) []byte {
	out := binary.BigEndian.AppendUint32(nil, uint32(len(payload)))
	return append(out, payload...)
}

// A refs reply past the CBOR library's 131,072-element default round-trips
// through the client-side reader (issue #22); the server-side request reader
// refuses the same element count, since requests never need it and a 1-byte
// element can decode into a large struct.
func TestReplyAndRequestLimits(t *testing.T) {
	const n = 140_000
	reply := RefsReply{Refs: make([]RefInfo, n)}
	for i := range reply.Refs {
		reply.Refs[i] = RefInfo{Name: fmt.Sprintf("r/%d", i), Key: []byte{1}}
	}
	var buf bytes.Buffer
	if err := WriteFrame(&buf, TRefsReply, reply); err != nil {
		t.Fatal(err)
	}
	raw := buf.Bytes()
	typ, body, err := ReadFrame(bytes.NewReader(raw))
	if err != nil || typ != TRefsReply {
		t.Fatalf("ReadFrame: %q %v", typ, err)
	}
	var got RefsReply
	if err := DecodeBody(body, &got); err != nil || len(got.Refs) != n {
		t.Fatalf("DecodeBody: %d refs, %v", len(got.Refs), err)
	}
	if err := DecodeRequest(body, &got); err == nil {
		t.Fatal("DecodeRequest accepted an array past the default element limit")
	}

	// Envelope with an oversized junk array: the request reader refuses it.
	var junk bytes.Buffer
	junk.Write([]byte{0xa3, 0x61, 't', 0x61, 'x', 0x61, 'b', 0xa0, 0x61, 'j', 0x9a})
	_ = binary.Write(&junk, binary.BigEndian, uint32(n))
	junk.Write(bytes.Repeat([]byte{0xa0}, n))
	if _, _, err := ReadRequest(bytes.NewReader(frameOf(junk.Bytes()))); err == nil {
		t.Fatal("ReadRequest accepted an array past the default element limit")
	}
	if _, _, err := ReadFrame(bytes.NewReader(frameOf(junk.Bytes()))); err != nil {
		t.Fatalf("ReadFrame must accept it: %v", err)
	}

	big := binary.BigEndian.AppendUint32(nil, MaxRequestFrame+1)
	if _, _, err := ReadRequest(bytes.NewReader(append(big, 0))); err == nil {
		t.Fatal("ReadRequest accepted a frame past MaxRequestFrame")
	}
}

// A header may claim MaxFrame; the reader must not allocate it before the
// bytes arrive, and a stream that ends early is an unexpected EOF.
func TestReadFrameClaimedSizeNotTrusted(t *testing.T) {
	claim := append(binary.BigEndian.AppendUint32(nil, MaxFrame), "tiny"...)
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	before := ms.TotalAlloc
	if _, _, err := ReadFrame(bytes.NewReader(claim)); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("want ErrUnexpectedEOF, got %v", err)
	}
	runtime.ReadMemStats(&ms)
	if grew := ms.TotalAlloc - before; grew > 4*frameStep {
		t.Fatalf("a 4-byte body of a %d-byte claim allocated %d bytes", MaxFrame, grew)
	}
}
