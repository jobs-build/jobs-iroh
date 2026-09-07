package registryd

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/amber-store/core/key"
	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/jobs-build/jobs-iroh/runner"
)

// imageRecord is the tiny durable record of an assembled image, one JSON file
// per K under <data-dir>/repos. It maps the repository to its manifest,
// remembers F (so a cache-expired image reassembles without asking the server
// for the K→F ref again) and carries each layer's descriptor plus the store
// recipe that streams it. Records are not swept — with layer bytes never
// materialised, the records and the two small JSON blobs (manifest, config)
// are all the registry keeps outside its store.
type imageRecord struct {
	K         string        `json:"k"`
	F         string        `json:"f"`
	Platform  string        `json:"platform"`
	Manifest  string        `json:"manifest"`  // manifest digest, "sha256:<hex>"
	MediaType string        `json:"mediaType"` // manifest media type
	Config    string        `json:"config"`    // image config digest (a cached blob)
	Layers    []layerRecord `json:"layers"`    // streamed from the store, never cached
	CreatedNs int64         `json:"createdNs"`
}

// imageFor returns a servable record for K, plus whether it had to
// (re)assemble it — false means the intact cache answered. Assembly is
// singleflighted per K and runs under the daemon context, so a departing HTTP
// client neither duplicates nor cancels the work its neighbours are waiting
// on.
func (r *registry) imageFor(reqCtx context.Context, k key.Key) (imageRecord, bool, error) {
	if rec, ok := r.readRecord(k.String()); ok && r.recordComplete(rec) {
		return rec, false, nil
	}
	if r.runCtx.Err() != nil {
		return imageRecord{}, false, fmt.Errorf("%w: registry shutting down", errUpstream)
	}
	ch := r.group.DoChan("assemble:"+k.String(), func() (any, error) {
		// Tracked so Run can wait assemblies out before closing the store.
		// (An assembly scheduled but not yet registered when Wait runs dies
		// at its first store/ctx use — the guard above keeps that window to
		// goroutine-startup scale.)
		r.assemblies.Add(1)
		defer r.assemblies.Done()
		start := time.Now()
		rec, err := r.assembleAndCache(r.runCtx, k)
		if err != nil {
			// Detail line only: request-level outcome (and Warn severity,
			// where deserved — an unknown K is an ordinary 404) is logged
			// once per waiter in serveManifest.
			r.log.Debug("image assembly failed", "k", k.String(),
				"elapsed", time.Since(start).Round(time.Millisecond), "error", err)
		}
		return rec, err
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			return imageRecord{}, true, res.Err
		}
		return res.Val.(imageRecord), true, nil
	case <-reqCtx.Done():
		return imageRecord{}, true, reqCtx.Err()
	}
}

// assembleAndCache resolves build K (syncing from the jobs-server as needed),
// assembles its two-layer OCI image and writes the manifest and config blobs
// plus the record. The layers themselves are only measured — their bytes are
// streamed straight from the store when a client asks for them.
func (r *registry) assembleAndCache(ctx context.Context, k key.Key) (imageRecord, error) {
	start := time.Now()
	var fHint key.Key
	if rec, ok := r.readRecord(k.String()); ok {
		fHint, _ = parseHexKey(rec.F)
	}
	res, err := r.resolveBuild(ctx, k, fHint)
	if err != nil {
		return imageRecord{}, err
	}

	img, layers, err := runner.AssembleOCIImage(ctx, r.st, res.artifact, res.deps, res.shell, res.ep, res.platform)
	if err != nil {
		return imageRecord{}, fmt.Errorf("assemble image for %s: %w", k.String(), err)
	}

	var layerBytes int64
	layerRecs := make([]layerRecord, 0, len(layers))
	for _, l := range layers {
		layerRecs = append(layerRecs, newLayerRecord(l))
		layerBytes += l.Size
	}

	cfgDigest, err := img.ConfigName()
	if err != nil {
		return imageRecord{}, err
	}
	rawCfg, err := img.RawConfigFile()
	if err != nil {
		return imageRecord{}, err
	}
	if err := r.blobs.put(cfgDigest, bytes.NewReader(rawCfg)); err != nil {
		return imageRecord{}, fmt.Errorf("cache config: %w", err)
	}

	manDigest, err := img.Digest()
	if err != nil {
		return imageRecord{}, err
	}
	rawMan, err := img.RawManifest()
	if err != nil {
		return imageRecord{}, err
	}
	if err := r.blobs.put(manDigest, bytes.NewReader(rawMan)); err != nil {
		return imageRecord{}, fmt.Errorf("cache manifest: %w", err)
	}
	mt, err := img.MediaType()
	if err != nil {
		return imageRecord{}, err
	}

	rec := imageRecord{
		K:         k.String(),
		F:         res.f.String(),
		Platform:  res.platform,
		Manifest:  manDigest.String(),
		MediaType: string(mt),
		Config:    cfgDigest.String(),
		Layers:    layerRecs,
		CreatedNs: time.Now().UnixNano(),
	}
	// Index before recording: a manifest is only served once this returns, and
	// the blobs it names must be streamable by then. A crash in between leaves
	// no record, so the image simply reassembles.
	r.layers.put(layerRecs...)
	if err := r.writeRecord(rec); err != nil {
		return imageRecord{}, err
	}
	r.log.Info("image assembled", "k", k.String(), "f", res.f.String(),
		"manifest", rec.Manifest, "deps", len(res.deps), "platform", res.platform,
		"layer_bytes", layerBytes,
		"elapsed", time.Since(start).Round(time.Millisecond))
	return rec, nil
}

// recordComplete reports whether the record can still be served: its two
// cached blobs (manifest and config) are present and every layer it names is
// streamable. A record written by an older registry — one that cached
// compressed layer blobs and knew no layer recipes — reads as incomplete and
// is reassembled into the current shape.
func (r *registry) recordComplete(rec imageRecord) bool {
	if rec.Config == "" || len(rec.Layers) == 0 {
		return false
	}
	for _, ds := range []string{rec.Manifest, rec.Config} {
		d, err := v1.NewHash(ds)
		if err != nil || !r.blobs.has(d) {
			return false
		}
	}
	for _, l := range rec.Layers {
		if _, err := v1.NewHash(l.Digest); err != nil {
			return false
		}
		if _, err := l.spec(); err != nil {
			return false
		}
	}
	return true
}

// touchRecord bumps the record's cached blobs as read: serving a manifest is
// proof the image is alive, so neither it nor its config should expire out
// from under the client about to fetch them. (Layers have nothing to touch —
// they are streamed from the store, which does not expire.)
func (r *registry) touchRecord(rec imageRecord) {
	for _, ds := range []string{rec.Manifest, rec.Config} {
		if d, err := v1.NewHash(ds); err == nil {
			r.blobs.touch(d)
		}
	}
}

func (r *registry) recordPath(kHex string) string {
	return filepath.Join(r.reposDir, kHex+".json")
}

func (r *registry) readRecord(kHex string) (imageRecord, bool) {
	b, err := os.ReadFile(r.recordPath(kHex))
	if err != nil {
		return imageRecord{}, false
	}
	var rec imageRecord
	if err := json.Unmarshal(b, &rec); err != nil || rec.K != kHex {
		return imageRecord{}, false
	}
	return rec, true
}

// writeRecord persists the record atomically (temp + rename in-dir).
func (r *registry) writeRecord(rec imageRecord) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(r.reposDir, "tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), r.recordPath(rec.K))
}

// listRecords returns every stored image record, K-sorted (ReadDir order).
func (r *registry) listRecords() []imageRecord {
	entries, err := os.ReadDir(r.reposDir)
	if err != nil {
		return nil
	}
	out := make([]imageRecord, 0, len(entries))
	for _, e := range entries {
		kHex, ok := strings.CutSuffix(e.Name(), ".json")
		if !ok {
			continue
		}
		if rec, ok := r.readRecord(kHex); ok {
			out = append(out, rec)
		}
	}
	return out
}

// findRecordByDigest locates the image whose manifest or config is d — the
// recovery path when a client asks for one of the two cached blobs after the
// sweep deleted it. Layer digests are not searched here: they are answered
// from the layer index, which never expires.
func (r *registry) findRecordByDigest(d v1.Hash) (imageRecord, bool) {
	ds := d.String()
	for _, rec := range r.listRecords() {
		if rec.Manifest == ds || rec.Config == ds {
			return rec, true
		}
	}
	return imageRecord{}, false
}

// findRecordByManifest locates the image whose MANIFEST digest is d — the
// gate for the manifests-by-digest route, so only actual manifests (small
// JSON, safe to buffer) are served there and layer digests 404.
func (r *registry) findRecordByManifest(d v1.Hash) (imageRecord, bool) {
	ds := d.String()
	for _, rec := range r.listRecords() {
		if rec.Manifest == ds {
			return rec, true
		}
	}
	return imageRecord{}, false
}

// parseHexKey parses a 64-char lowercase-hex store key (the repository-name
// form of K).
func parseHexKey(s string) (key.Key, bool) {
	if len(s) != 64 || strings.ToLower(s) != s {
		return key.Key{}, false
	}
	raw, err := hex.DecodeString(s)
	if err != nil {
		return key.Key{}, false
	}
	k, err := key.Parse(raw)
	if err != nil {
		return key.Key{}, false
	}
	return k, true
}
