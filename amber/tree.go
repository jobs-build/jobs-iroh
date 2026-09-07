package amber

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/tarexport"
	"github.com/amber-store/core/tarextract"
)

// Entry is one directory entry as returned by Ls: the fstree entry's name,
// raw POSIX mode, and payload, with Size derived from the content key's
// embedded logical length (a file's total content bytes for S_IFREG, the
// subtree footprint for S_IFDIR — no object read needed). Key is the zero
// key and Size 0 for keyless entries (symlinks, fifos, devices); LinkTarget
// is set only for symlinks.
type Entry struct {
	Name       string
	Mode       uint64
	Size       uint64
	Key        key.Key
	LinkTarget string
}

// Ls lists the directory at the slash-separated subpath of the tree rooted at
// root ("" or "." lists root itself), in name order. Path components must be
// directories; ".." is rejected.
func (s *Store) Ls(ctx context.Context, root key.Key, path string) ([]Entry, error) {
	get := s.getFunc(ctx)
	dk, err := fstree.ResolvePath(root, path, get)
	if err != nil {
		return nil, err
	}
	fents, err := fstree.CollectEntries(dk, get)
	if err != nil {
		return nil, err
	}
	out := make([]Entry, 0, len(fents))
	for _, fe := range fents {
		e := Entry{Name: string(fe.Name), Mode: fe.Mode, LinkTarget: string(fe.LinkTarget)}
		if len(fe.ContentKey) > 0 {
			ck, err := key.Parse(fe.ContentKey)
			if err != nil {
				return nil, fmt.Errorf("amber: entry %q: content key: %w", fe.Name, err)
			}
			e.Key = ck
			e.Size = ck.Length()
		}
		out = append(out, e)
	}
	return out, nil
}

// File streams the content of the file addressed by ck (a Blob or FileNode
// key) — fstree.WriteContent behind an in-process pipe. The caller must Close
// the returned ReadCloser; the producer goroutine exits when the stream is
// fully read, the reader is closed, or ctx is cancelled (a watcher closes the
// read side with the cancel cause, and the per-object get fails fast), so it
// cannot leak past whichever of those happens first.
func (s *Store) File(ctx context.Context, ck key.Key) (io.ReadCloser, error) {
	if t := ck.Type(); t != key.Blob && t != key.FileNode {
		return nil, fmt.Errorf("amber: %s is not a file-content key (type %v)", ck, t)
	}
	get := s.getFunc(ctx)
	pr, pw := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		pw.CloseWithError(fstree.WriteContent(pw, ck, get))
	}()
	go func() {
		select {
		case <-ctx.Done():
			pr.CloseWithError(context.Cause(ctx))
		case <-done:
		}
	}()
	return pr, nil
}

// ReadFile returns the whole content of the file addressed by ck (a Blob or
// FileNode key) in memory — the convenience form of File for small reads.
func (s *Store) ReadFile(ctx context.Context, ck key.Key) ([]byte, error) {
	if t := ck.Type(); t != key.Blob && t != key.FileNode {
		return nil, fmt.Errorf("amber: %s is not a file-content key (type %v)", ck, t)
	}
	var buf bytes.Buffer
	// Pre-size from the key's logical length, as a hint only and bounded: the
	// length field is decoded from stored (possibly synced-in) bytes, and
	// bytes.Buffer.Grow panics on absurd values rather than erroring. Larger
	// files still read fine — the buffer just grows as it fills.
	if n := ck.Length(); n > 0 && n <= 1<<30 {
		buf.Grow(int(n))
	}
	if err := fstree.WriteContent(&buf, ck, s.getFunc(ctx)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Tar streams a PAX tar of the directory at the slash-separated subpath of
// the tree rooted at root ("" = the whole tree) — tarexport.Write behind an
// in-process pipe. Lifecycle contract as File: Close the reader; ctx
// cancellation closes the pipe with the cancel cause, so the producer cannot
// leak.
func (s *Store) Tar(ctx context.Context, root key.Key, path string) (io.ReadCloser, error) {
	get := s.getFunc(ctx)
	dk, err := fstree.ResolvePath(root, path, get)
	if err != nil {
		return nil, err
	}
	pr, pw := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		pw.CloseWithError(tarexport.Write(pw, dk, get))
	}()
	go func() {
		select {
		case <-ctx.Done():
			pr.CloseWithError(context.Cause(ctx))
		case <-done:
		}
	}()
	return pr, nil
}

// Materialize extracts the directory tree rooted at root into dest (created
// if necessary): tarexport piped straight into tarextract, all in-process.
// This is the store-mount restore path (materialize-to-disk; jobs-iroh ships
// no FUSE). Synchronous and leak-free: the export goroutine is always drained
// before returning — an extract failure closes the read side, unblocking the
// exporter.
func (s *Store) Materialize(ctx context.Context, root key.Key, dest string) error {
	get := s.getFunc(ctx)
	pr, pw := io.Pipe()
	exportErr := make(chan error, 1)
	go func() {
		err := tarexport.Write(pw, root, get)
		pw.CloseWithError(err)
		exportErr <- err
	}()
	extractErr := tarextract.Extract(pr, dest)
	pr.CloseWithError(extractErr)
	wErr := <-exportErr
	if extractErr != nil {
		// An export failure surfaces here too (the pipe hands it to the
		// extractor's read), so this is the primary error path.
		return extractErr
	}
	if wErr != nil && !errors.Is(wErr, io.ErrClosedPipe) {
		// The extractor stops at the end-of-archive marker without draining
		// trailing padding; the exporter's resulting closed-pipe write is the
		// expected shutdown, not a failure.
		return wErr
	}
	return nil
}
