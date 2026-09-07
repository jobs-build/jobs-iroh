package registryd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/amber-store/core/key"
	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// repoName is the single repository the registry serves; its tags are build
// keys K, so an image is named jobs:<K>.
const repoName = "jobs"

// handler serves the pull side of the OCI Distribution API:
//
//	GET  /v2/                          version check
//	GET  /v2/_catalog                  ["jobs"]
//	GET  /v2/jobs/tags/list            pullable build keys (server + cache)
//	GET  /v2/jobs/manifests/<ref>      ref = a build key K or a manifest digest
//	GET  /v2/jobs/blobs/<digest>       config and layer blobs (Range supported)
//
// (HEAD everywhere GET is.) Everything is read-only — push endpoints answer
// 405.
func (r *registry) handler() http.Handler {
	return http.HandlerFunc(r.route)
}

func (r *registry) route(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		r.writeError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "this registry is pull-only")
		return
	}
	p, ok := strings.CutPrefix(req.URL.Path, "/v2")
	if !ok {
		r.writeError(w, http.StatusNotFound, "UNSUPPORTED", "not an OCI registry path (want /v2/...)")
		return
	}
	p = strings.TrimPrefix(p, "/")
	switch p {
	case "":
		w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, "{}")
		return
	case "_catalog":
		r.writeJSON(w, req, map[string]any{"repositories": []string{repoName}})
		return
	}
	segs := strings.Split(p, "/")
	if len(segs) != 3 {
		r.writeError(w, http.StatusNotFound, "UNSUPPORTED", "unknown registry path")
		return
	}
	if segs[0] != repoName {
		r.writeError(w, http.StatusNotFound, "NAME_UNKNOWN",
			fmt.Sprintf("unknown repository %q — images are named %s:<build-K>", segs[0], repoName))
		return
	}
	switch {
	case segs[1] == "tags" && segs[2] == "list":
		r.serveTags(w, req)
	case segs[1] == "manifests":
		r.serveManifest(w, req, segs[2])
	case segs[1] == "blobs":
		r.serveBlob(w, req, segs[2])
	default:
		r.writeError(w, http.StatusNotFound, "UNSUPPORTED", "unknown registry path")
	}
}

func (r *registry) serveManifest(w http.ResponseWriter, req *http.Request, ref string) {
	if strings.Contains(ref, ":") {
		r.serveManifestByDigest(w, req, ref)
		return
	}
	k, ok := parseHexKey(ref)
	if !ok {
		r.writeError(w, http.StatusNotFound, "MANIFEST_UNKNOWN",
			fmt.Sprintf("tag %q is not a build key (want 64 lowercase hex chars)", ref))
		return
	}

	r.log.Info("image requested", "k", k.String(), "method", req.Method, "remote", req.RemoteAddr)
	start := time.Now()
	b, rec, assembled, err := r.manifestFor(req.Context(), k)
	elapsed := time.Since(start).Round(time.Millisecond)
	if err != nil {
		switch {
		case req.Context().Err() != nil || r.runCtx.Err() != nil ||
			errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
			// The client departed (or the daemon is shutting down) while the
			// singleflighted assembly keeps running — nothing failed here.
			// Classified by context state, not error identity: a cancelled
			// pull surfaces as a stream-deadline error, and ensureRef's %v
			// wrapping strips errors.Is-ability either way.
			r.log.Debug("image request abandoned", "k", k.String(), "elapsed", elapsed, "error", err)
		case errors.Is(err, errNotFound):
			// An unknown build key is an ordinary 404, not a daemon fault.
			r.log.Info("image request failed", "k", k.String(), "elapsed", elapsed, "error", err)
		default:
			r.log.Warn("image request failed", "k", k.String(), "elapsed", elapsed, "error", err)
		}
		r.mapError(w, req, err)
		return
	}
	r.touchRecord(rec)
	r.assertPins(rec)
	r.writeManifest(w, req, b, rec.MediaType, rec.Manifest)
	r.log.Info("image served", "k", k.String(), "cached", !assembled, "elapsed", elapsed)
}

// manifestFor resolves K to its manifest bytes, in two rounds: a torn first
// round means the sweep expired a blob between imageFor's completeness check
// and the read — the second imageFor sees the incomplete record and
// reassembles. assembled reports whether serving took a (re)assembly rather
// than the intact cache.
func (r *registry) manifestFor(ctx context.Context, k key.Key) (b []byte, rec imageRecord, assembled bool, err error) {
	var lastErr error
	for range 2 {
		rec, assembled, err = r.imageFor(ctx, k)
		if err != nil {
			return nil, imageRecord{}, assembled, err
		}
		d, err := v1.NewHash(rec.Manifest)
		if err != nil {
			return nil, imageRecord{}, assembled, err
		}
		b, err = r.blobs.get(d)
		if err != nil {
			lastErr = fmt.Errorf("manifest blob vanished: %w", err)
			continue
		}
		return b, rec, assembled, nil
	}
	return nil, imageRecord{}, true, lastErr
}

// serveManifestByDigest serves a manifest by digest — the second request of
// every containerd/docker pull, after the tag resolved. Gated on the digest
// being some record's MANIFEST: layer/config digests answer 404 here (they
// are multi-GB streams served by the blobs route, not JSON to buffer). An
// expired manifest is reassembled from its record.
func (r *registry) serveManifestByDigest(w http.ResponseWriter, req *http.Request, ref string) {
	start := time.Now()
	d, err := v1.NewHash(ref)
	if err != nil {
		r.writeError(w, http.StatusBadRequest, "DIGEST_INVALID", err.Error())
		return
	}
	rec, ok := r.findRecordByManifest(d)
	if !ok {
		r.writeError(w, http.StatusNotFound, "MANIFEST_UNKNOWN", "no manifest with digest "+ref)
		return
	}
	b, err := r.blobs.get(d)
	if err != nil {
		if !r.recoverDigest(req.Context(), d) {
			r.writeError(w, http.StatusNotFound, "MANIFEST_UNKNOWN", "no manifest with digest "+ref)
			return
		}
		if b, err = r.blobs.get(d); err != nil {
			r.mapError(w, req, err)
			return
		}
	}
	r.touchRecord(rec)
	r.assertPins(rec)
	r.writeManifest(w, req, b, rec.MediaType, d.String())
	r.log.Info("manifest served", "k", rec.K, "digest", d.String(), "method", req.Method,
		"elapsed", time.Since(start).Round(time.Millisecond))
}

func (r *registry) writeManifest(w http.ResponseWriter, req *http.Request, b []byte, mediaType, digest string) {
	w.Header().Set("Content-Type", mediaType)
	w.Header().Set("Docker-Content-Digest", digest)
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	if req.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(b)
}

func (r *registry) serveBlob(w http.ResponseWriter, req *http.Request, digest string) {
	start := time.Now()
	d, err := v1.NewHash(digest)
	if err != nil {
		r.writeError(w, http.StatusBadRequest, "DIGEST_INVALID", err.Error())
		return
	}
	// Layers are the big blobs and the registry keeps none of them: a layer
	// digest is answered by re-generating its tar from the store.
	if lay, ok := r.layers.get(d); ok {
		r.streamLayer(w, req, d, lay, start)
		return
	}
	f, size, err := r.blobs.open(d)
	if err != nil {
		if !r.recoverDigest(req.Context(), d) {
			r.writeError(w, http.StatusNotFound, "BLOB_UNKNOWN", "no blob with digest "+digest)
			return
		}
		if f, size, err = r.blobs.open(d); err != nil {
			r.mapError(w, req, err)
			return
		}
	}
	defer f.Close()
	r.blobs.touch(d)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Docker-Content-Digest", d.String())
	// Zero modtime: mtime is our last-read clock, not a content time, so it
	// must not drive Last-Modified/If-Modified-Since. ServeContent still
	// handles HEAD, Content-Length and Range.
	sw := &statusWriter{ResponseWriter: w}
	http.ServeContent(sw, req, "", time.Time{}, f)
	// status and sent make the line honest: ServeContent reports failures
	// (416, a client that dropped mid-layer) only through the writer, and
	// elapsed spans the client-paced body transfer.
	r.log.Info("blob served", "digest", d.String(), "status", sw.status, "sent", sw.bytes,
		"size", size, "method", req.Method, "elapsed", time.Since(start).Round(time.Millisecond))
}

// streamLayer serves a layer blob straight out of the content-addressed store:
// the tar is generated into the response as the client reads it, so a
// multi-GB layer costs no disk and no compression. ServeContent still handles
// HEAD, Range and Content-Length over the (never materialised) stream.
func (r *registry) streamLayer(w http.ResponseWriter, req *http.Request, d v1.Hash, lay layerRecord, start time.Time) {
	ls, err := newLayerStream(req.Context(), r.st, lay)
	if err != nil {
		r.log.Error("layer stream unavailable", "digest", d.String(), "error", err)
		r.writeError(w, http.StatusNotFound, "BLOB_UNKNOWN", "no blob with digest "+d.String())
		return
	}
	defer ls.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Docker-Content-Digest", d.String())
	sw := &statusWriter{ResponseWriter: w}
	// Zero modtime: a layer's content is fixed by its digest, so there is no
	// content time to expose and nothing for If-Modified-Since to decide.
	http.ServeContent(sw, singleRange(req), "", time.Time{}, ls)

	elapsed := time.Since(start).Round(time.Millisecond)
	if err := ls.failure(); err != nil {
		if req.Context().Err() != nil || r.runCtx.Err() != nil {
			// A client that walked away mid-layer (or a shutting-down daemon)
			// tears the generator down by design — classified by context
			// state, since the tear-down surfaces as a pipe/stream error.
			r.log.Debug("layer stream abandoned", "digest", d.String(),
				"sent", sw.bytes, "size", lay.Size, "elapsed", elapsed)
			return
		}
		// The body is already committed with a Content-Length, so a generator
		// failure can only truncate the response — the client sees a short
		// read and retries. Log it as the fault it is.
		r.log.Error("layer stream failed", "digest", d.String(), "kind", lay.Kind,
			"sent", sw.bytes, "size", lay.Size, "elapsed", elapsed, "error", err)
		return
	}
	r.log.Info("layer served", "digest", d.String(), "kind", lay.Kind, "status", sw.status,
		"sent", sw.bytes, "size", lay.Size, "method", req.Method, "elapsed", elapsed)
}

// singleRange drops a Range header asking for more than one range, so
// http.ServeContent answers the request whole (200) instead of taking its
// multipart path. Two reasons, both specific to a layer that is generated
// rather than stored: that path reads the stream from a goroutine it never
// joins — which would outlive the handler's Close — and it seeks per range,
// where each backwards seek costs a full regeneration of the layer. RFC 9110
// lets a server ignore Range, no registry client asks for more than one (a
// resume is a single `bytes=N-`), and refusing here keeps a request bounded to
// one pass over the content.
func singleRange(req *http.Request) *http.Request {
	if !strings.Contains(req.Header.Get("Range"), ",") {
		return req
	}
	trimmed := req.Clone(req.Context())
	trimmed.Header.Del("Range")
	return trimmed
}

// statusWriter records the status code and body bytes actually sent, for
// completion logs. ReadFrom forwards to the underlying writer, so a cached
// blob served from its file still takes ServeContent's sendfile path.
type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += int64(n)
	return n, err
}

func (w *statusWriter) ReadFrom(src io.Reader) (int64, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	var n int64
	var err error
	if rf, ok := w.ResponseWriter.(io.ReaderFrom); ok {
		n, err = rf.ReadFrom(src)
	} else {
		n, err = io.Copy(w.ResponseWriter, src)
	}
	w.bytes += n
	return n, err
}

// recoverDigest reassembles the image whose manifest references d after the
// sweep expired the blob — the content is still in the amber store, so an
// old manifest held by a client stays servable. Returns false when no record
// references d, or when reassembly did not reproduce d (a changed upstream
// output: the digest is genuinely gone, 404 is the honest answer).
func (r *registry) recoverDigest(reqCtx context.Context, d v1.Hash) bool {
	rec, ok := r.findRecordByDigest(d)
	if !ok {
		return false
	}
	k, ok := parseHexKey(rec.K)
	if !ok {
		return false
	}
	if _, _, err := r.imageFor(reqCtx, k); err != nil {
		r.log.Warn("blob recovery failed", "digest", d.String(), "k", rec.K, "error", err)
		return false
	}
	return r.blobs.has(d)
}

// serveTags lists the pullable build keys: every build-from:K on the
// jobs-server plus every locally assembled image. Server unreachable → the
// local set alone, so a cached registry keeps answering. Supports the
// standard n/last paging.
func (r *registry) serveTags(w http.ResponseWriter, req *http.Request) {
	seen := map[string]bool{}
	for _, rec := range r.listRecords() {
		seen[rec.K] = true
	}
	if refs, err := r.sync.Refs(req.Context()); err == nil {
		for _, ref := range refs {
			if kHex, ok := strings.CutPrefix(ref.Name, "build-from:"); ok {
				if _, valid := parseHexKey(kHex); valid {
					seen[kHex] = true
				}
			}
		}
	} else {
		r.log.Warn("tags list: server refs unavailable", "error", err)
	}
	tags := make([]string, 0, len(seen))
	for t := range seen {
		tags = append(tags, t)
	}
	sort.Strings(tags)

	if last := req.URL.Query().Get("last"); last != "" {
		i := sort.SearchStrings(tags, last)
		if i < len(tags) && tags[i] == last {
			i++
		}
		tags = tags[i:]
	}
	if nStr := req.URL.Query().Get("n"); nStr != "" {
		if n, err := strconv.Atoi(nStr); err == nil && n >= 0 && n < len(tags) {
			tags = tags[:n]
			if n > 0 {
				w.Header().Set("Link", fmt.Sprintf("</v2/%s/tags/list?n=%d&last=%s>; rel=\"next\"", repoName, n, tags[n-1]))
			}
		}
	}
	r.writeJSON(w, req, map[string]any{"name": repoName, "tags": tags})
}

func (r *registry) writeJSON(w http.ResponseWriter, req *http.Request, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		r.mapError(w, req, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	if req.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(b)
}

// mapError translates resolution/assembly failures onto the registry error
// contract: unknown build → 404, jobs-server unreachable → 503, cancellation
// → 503 (a departed client never reads it; a client caught in the daemon's
// shutdown must not get an implicit empty 200), anything else → 500.
func (r *registry) mapError(w http.ResponseWriter, req *http.Request, err error) {
	switch {
	case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		r.writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "request cancelled: "+err.Error())
	case errors.Is(err, errNotFound):
		r.writeError(w, http.StatusNotFound, "MANIFEST_UNKNOWN", err.Error())
	case errors.Is(err, errUpstream):
		r.writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", err.Error())
	default:
		r.log.Error("registry request failed", "path", req.URL.Path, "error", err)
		r.writeError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
	}
}

// writeError emits the OCI error envelope {"errors":[{code,message}]}.
func (r *registry) writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"errors": []map[string]string{{"code": code, "message": message}},
	})
}
