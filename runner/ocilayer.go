package runner

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"github.com/amber-store/core/key"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/jobs-build/jobs-iroh/amber"
)

// LayerKind names one of the two shapes a registry image layer has.
type LayerKind string

const (
	// LayerDeps is the runtime-closure layer (writeDepsTar).
	LayerDeps LayerKind = "deps"
	// LayerArtifact is the build-output layer (writeArtifactTar).
	LayerArtifact LayerKind = "artifact"
)

// LayerSpec names the store content of one image layer — the whole recipe for
// (re)producing its tar via WriteLayerTar. Because the tar is a pure function
// of the spec and the content-addressed store, a spec is worth exactly as much
// as the layer bytes: jobs-registry records specs instead of blobs and streams
// a layer from the CAS on every pull, so multi-GB layers are never
// materialised on disk.
type LayerSpec struct {
	Kind     LayerKind
	Deps     []key.Key // LayerDeps: the runtime closure
	Shell    key.Key   // LayerDeps: the baked shell; zero = none
	Artifact key.Key   // LayerArtifact: the build's c/ tree
}

// ImageLayer is one assembled layer: the descriptor fields its manifest
// carries, plus the spec that re-streams its bytes. Digest is the digest of
// the tar itself — layers are uncompressed, so it is also the diffID.
type ImageLayer struct {
	Digest v1.Hash
	Size   int64
	Spec   LayerSpec
}

// WriteLayerTar writes the layer named by spec to w. It is the single
// generator of layer bytes: assembly hashes what it writes and serving streams
// what it writes, so the two can never disagree.
func WriteLayerTar(ctx context.Context, st *amber.Store, w io.Writer, spec LayerSpec) error {
	switch spec.Kind {
	case LayerDeps:
		return writeDepsTar(ctx, st, w, spec.Deps, spec.Shell)
	case LayerArtifact:
		return writeArtifactTar(ctx, st, w, spec.Artifact)
	default:
		return fmt.Errorf("unknown image layer kind %q", spec.Kind)
	}
}

// tarLayer is an image layer whose blob IS the tar:
// application/vnd.oci.image.layer.v1.tar, uncompressed, so its digest equals
// its diffID and nothing sits between the store and the wire. Compression was
// pure cost here — the bytes come from an already-deduplicated CAS over a LAN,
// and gzip bought a slower assembly (a whole extra pass to compress) and a
// slower pull (the client decompressing every layer it fetches).
type tarLayer struct {
	// ctx belongs to the assembly that built this layer: a v1.Layer is read
	// lazily (ggcr re-opens the stream when it writes an image out), so the
	// opener needs the caller's cancellation to reach the store.
	ctx    context.Context
	open   func(ctx context.Context) (io.ReadCloser, error)
	digest v1.Hash
	size   int64
}

func (l *tarLayer) Digest() (v1.Hash, error)             { return l.digest, nil }
func (l *tarLayer) DiffID() (v1.Hash, error)             { return l.digest, nil }
func (l *tarLayer) Size() (int64, error)                 { return l.size, nil }
func (l *tarLayer) MediaType() (types.MediaType, error)  { return types.OCIUncompressedLayer, nil }
func (l *tarLayer) Compressed() (io.ReadCloser, error)   { return l.open(l.ctx) }
func (l *tarLayer) Uncompressed() (io.ReadCloser, error) { return l.open(l.ctx) }

// newTarLayer measures a layer by streaming it once: the tar is generated,
// hashed and counted, and thrown away. That single pass is the whole cost of
// assembling a layer now — the bytes themselves are re-generated on demand by
// the same opener.
func newTarLayer(ctx context.Context, open func(ctx context.Context) (io.ReadCloser, error)) (*tarLayer, error) {
	rc, err := open(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	digest, size, err := v1.SHA256(rc)
	if err != nil {
		return nil, err
	}
	return &tarLayer{ctx: ctx, open: open, digest: digest, size: size}, nil
}

// layerPipeBuf is how much of the tar the producer batches before handing it
// to the consumer. An io.Pipe write is a rendezvous — the producer blocks
// until the reader takes the bytes — so this is really the handoff
// granularity, and at the tar writer's natural 32KiB a multi-GB layer pays a
// scheduler round-trip every 32KiB. Batching here measured ~1.3GB/s against
// ~0.9GB/s unbatched on a 256MiB layer; 1MiB batches bought nothing more, so
// this is the cheap end of the curve (one buffer per in-flight pull).
const layerPipeBuf = 1 << 18

// LayerTarOpener returns an opener that streams spec's tar out of st. The tar
// is produced by a goroutine writing into a pipe, so closing the returned
// reader (or cancelling ctx) stops the producer.
func LayerTarOpener(st *amber.Store, spec LayerSpec) func(ctx context.Context) (io.ReadCloser, error) {
	return func(ctx context.Context) (io.ReadCloser, error) {
		pr, pw := io.Pipe()
		go func() {
			bw := bufio.NewWriterSize(pw, layerPipeBuf)
			err := WriteLayerTar(ctx, st, bw, spec)
			if err == nil {
				err = bw.Flush()
			}
			pw.CloseWithError(err)
		}()
		return pr, nil
	}
}
