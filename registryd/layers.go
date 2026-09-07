package registryd

import (
	"fmt"
	"sync"

	"github.com/amber-store/core/key"
	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/jobs-build/jobs-iroh/runner"
)

// layerRecord is one image layer as the registry remembers it: the descriptor
// fields the manifest promises (digest, size) plus the store recipe that
// re-streams the bytes. Layer blobs are never written to disk — the tar is a
// pure function of this recipe and the content-addressed store, so the recipe
// IS the blob, at a few hundred bytes instead of gigabytes.
type layerRecord struct {
	Digest   string   `json:"digest"` // "sha256:<hex>" of the tar (uncompressed: also the diffID)
	Size     int64    `json:"size"`
	Kind     string   `json:"kind"`               // runner.LayerKind
	Deps     []string `json:"deps,omitempty"`     // deps layer: the runtime closure BOKs
	Shell    string   `json:"shell,omitempty"`    // deps layer: baked shell BOK, "" = none
	Artifact string   `json:"artifact,omitempty"` // artifact layer: the c/ tree BOK
}

// newLayerRecord captures an assembled layer.
func newLayerRecord(l runner.ImageLayer) layerRecord {
	rec := layerRecord{
		Digest: l.Digest.String(),
		Size:   l.Size,
		Kind:   string(l.Spec.Kind),
	}
	switch l.Spec.Kind {
	case runner.LayerDeps:
		for _, d := range l.Spec.Deps {
			rec.Deps = append(rec.Deps, d.String())
		}
		if l.Spec.Shell != (key.Key{}) {
			rec.Shell = l.Spec.Shell.String()
		}
	case runner.LayerArtifact:
		rec.Artifact = l.Spec.Artifact.String()
	}
	return rec
}

// spec rebuilds the runner spec that regenerates this layer's tar. It fails
// closed on anything unparseable — a record the current code cannot stream is
// treated as absent, so the image reassembles instead of serving wrong bytes.
func (l layerRecord) spec() (runner.LayerSpec, error) {
	spec := runner.LayerSpec{Kind: runner.LayerKind(l.Kind)}
	switch spec.Kind {
	case runner.LayerDeps:
		for _, s := range l.Deps {
			k, ok := parseHexKey(s)
			if !ok {
				return runner.LayerSpec{}, fmt.Errorf("layer %s: bad dep key %q", l.Digest, s)
			}
			spec.Deps = append(spec.Deps, k)
		}
		if l.Shell != "" {
			k, ok := parseHexKey(l.Shell)
			if !ok {
				return runner.LayerSpec{}, fmt.Errorf("layer %s: bad shell key %q", l.Digest, l.Shell)
			}
			spec.Shell = k
		}
	case runner.LayerArtifact:
		k, ok := parseHexKey(l.Artifact)
		if !ok {
			return runner.LayerSpec{}, fmt.Errorf("layer %s: bad artifact key %q", l.Digest, l.Artifact)
		}
		spec.Artifact = k
	default:
		return runner.LayerSpec{}, fmt.Errorf("layer %s: unknown kind %q", l.Digest, l.Kind)
	}
	return spec, nil
}

// layerIndex answers "what streams this digest?" for the blobs route. Layer
// bytes live nowhere, so this map is what makes a blob GET answerable: it is
// built from the durable image records at startup and extended by every
// assembly. Entries are immutable (a digest names its content), so a
// re-assembly that yields the same digests is a no-op here.
type layerIndex struct {
	mu sync.RWMutex
	m  map[string]layerRecord
}

func newLayerIndex() *layerIndex {
	return &layerIndex{m: map[string]layerRecord{}}
}

func (x *layerIndex) get(d v1.Hash) (layerRecord, bool) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	l, ok := x.m[d.String()]
	return l, ok
}

func (x *layerIndex) put(recs ...layerRecord) {
	x.mu.Lock()
	defer x.mu.Unlock()
	for _, l := range recs {
		if l.Digest != "" {
			x.m[l.Digest] = l
		}
	}
}

func (x *layerIndex) len() int {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return len(x.m)
}
