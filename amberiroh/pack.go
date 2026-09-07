package amberiroh

import (
	"fmt"
	"io"
	"iter"

	"github.com/amber-store/core/amberpack"
)

// chunkWriter buffers pack bytes and emits them as ChunkSize TData frames.
type chunkWriter struct {
	w   io.Writer
	buf []byte
}

func (c *chunkWriter) Write(p []byte) (int, error) {
	n := len(p)
	for len(p) > 0 {
		take := min(ChunkSize-len(c.buf), len(p))
		c.buf = append(c.buf, p[:take]...)
		p = p[take:]
		if len(c.buf) == ChunkSize {
			if err := c.flush(); err != nil {
				return 0, err
			}
		}
	}
	return n, nil
}

func (c *chunkWriter) flush() error {
	if len(c.buf) == 0 {
		return nil
	}
	err := WriteMsg(c.w, Msg{Type: TData, Data: c.buf})
	c.buf = c.buf[:0]
	return err
}

func (c *chunkWriter) finish() error {
	if err := c.flush(); err != nil {
		return err
	}
	return WriteMsg(c.w, Msg{Type: TDataEnd})
}

// SendPackRecords serializes pre-encoded records (packstore.GetRecord /
// amberpack.EncodeRecord output) as one amberpack embedded in TData
// frames, terminated by TDataEnd — the zero-copy push path: stored
// records are wire-format-identical, so no decompress/re-encode happens
// here. Abort semantics match SendPack: an error from recs aborts the
// pack without the terminator and is returned.
func SendPackRecords(w io.Writer, recs iter.Seq2[[]byte, error]) error {
	cw := &chunkWriter{w: w}
	pw := amberpack.NewWriter(cw)
	for rec, err := range recs {
		if err != nil {
			return err
		}
		if err := pw.AddRecord(rec); err != nil {
			return err
		}
	}
	if err := pw.Close(); err != nil {
		return err
	}
	return cw.finish()
}

// NewPackReader returns a reader over the pack bytes of a TData…TDataEnd
// frame sequence on r. It reads exactly through the TDataEnd frame, so the
// underlying stream is positioned for the next frame afterwards.
//
// The TDataEnd frame is only consumed by reading to EOF; amberpack's decoder
// stops at its own end marker without triggering that read, so consumers must
// drain the reader (io.Copy(io.Discard, pr)) before reading further frames
// from the stream.
func NewPackReader(r io.Reader) io.Reader {
	return &packReader{r: r}
}

type packReader struct {
	r    io.Reader
	cur  []byte
	done bool
	err  error
}

func (p *packReader) Read(b []byte) (int, error) {
	for len(p.cur) == 0 {
		if p.err != nil {
			return 0, p.err
		}
		if p.done {
			return 0, io.EOF
		}
		m, err := ReadMsg(p.r)
		if err != nil {
			p.err = err
			return 0, err
		}
		switch m.Type {
		case TData:
			p.cur = m.Data
		case TDataEnd:
			p.done = true
		case TErr:
			p.err = RemoteFromMsg(m)
			return 0, p.err
		default:
			p.err = fmt.Errorf("%w: type %d during pack transfer", ErrProtocol, m.Type)
			return 0, p.err
		}
	}
	n := copy(b, p.cur)
	p.cur = p.cur[n:]
	return n, nil
}
