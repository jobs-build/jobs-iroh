package bootstrap

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/klauspost/compress/zstd"
)

//go:embed seed
var seedFS embed.FS

// seedArtifact maps an embedded blob basename to the ref it publishes.
type seedArtifact struct {
	file string
	ref  func(platform string) string
}

var seedArtifacts = []seedArtifact{
	{file: "tarball-https.tar.zst", ref: func(p string) string { return "fetcher:tarball+https:" + p }},
	{file: "shell.tar.zst", ref: func(p string) string { return "shell:" + p }},
	{file: "hostmusl.tar.zst", ref: func(p string) string { return "fetcher:hostmusl:" + p }},
	{file: "github.tar.zst", ref: func(p string) string { return "fetcher:github:" + p }},
}

// SeedFetcherNames lists the import-capable seed fetchers — the only names a
// string-valued imp(fetcher=...) or a plugin-emitted import may use without a
// FetcherDef (recipe-declared-fetchers design §3). Derived from seedArtifacts
// (single source of truth): every seed publishing a fetcher:<name>:<platform>
// ref qualifies; shell publishes shell:<platform> and so is not an import
// fetcher. github is a seed because submit-time source imports are constructed
// outside any recipe (design §8).
func SeedFetcherNames() []string {
	var out []string
	for _, a := range seedArtifacts {
		ref := a.ref("") // "fetcher:<name>:" or "shell:"
		if rest, ok := strings.CutPrefix(ref, "fetcher:"); ok {
			out = append(out, strings.TrimSuffix(rest, ":"))
		}
	}
	return out
}

// platformDir maps a platform ("linux/amd64") to its seed subdirectory name
// ("linux-amd64"), since "/" is not a clean path component.
func platformDir(platform string) string { return strings.ReplaceAll(platform, "/", "-") }

// dirPlatform is the inverse of platformDir.
func dirPlatform(dir string) string { return strings.Replace(dir, "-", "/", 1) }

// SeededPlatforms lists the platforms for which seed artifacts are embedded.
func SeededPlatforms() []string {
	entries, err := seedFS.ReadDir("seed")
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			out = append(out, dirPlatform(e.Name()))
		}
	}
	return out
}

// Seed ingests every embedded seed artifact for the given platforms and
// publishes its ref (fetcher:tarball+https:<p>, shell:<p>), idempotently.
// Idempotence is tracked per embedded *blob*, not per ref: alongside each ref a
// marker ref seed-src:<ref>:<blobhash> records which blob produced it, and an
// artifact is skipped only when the current blob's marker is present. A ref
// seeded from an older binary's blob (marker absent) is re-ingested and
// re-published, so a changed seed fetcher actually rolls out on restart —
// while unchanged blobs never re-ingest (amber keys are metadata-sensitive, so
// a re-ingest would churn the key on every boot). Seeding is publish-only — a
// host ingests and publishes a platform's artifact even when it cannot execute
// it, so a shared store is made ready for every platform in a heterogeneous
// fleet. Missing blobs are skipped (logged), so a partially-populated seed
// never blocks startup.
func Seed(ctx context.Context, st *amber.Store, platforms []string, log *slog.Logger) error {
	for _, p := range platforms {
		base := "seed/" + platformDir(p)
		for _, a := range seedArtifacts {
			ref := a.ref(p)
			blob, err := seedFS.ReadFile(base + "/" + a.file)
			if err != nil {
				log.Warn("seed: artifact not embedded for platform", "platform", p, "file", a.file)
				continue
			}
			marker := seedMarkerRef(ref, blob)
			if _, ok, err := st.GetKey(ctx, marker); err == nil && ok {
				log.Debug("seed: blob already seeded, skipping", "ref", ref)
				continue
			}
			k, err := ingestSeedBlob(ctx, st, blob)
			if err != nil {
				return fmt.Errorf("seed %s: ingest: %w", ref, err)
			}
			if err := st.PutRef(ctx, ref, k); err != nil {
				return fmt.Errorf("seed %s: publish: %w", ref, err)
			}
			if err := st.PutRef(ctx, marker, k); err != nil {
				return fmt.Errorf("seed %s: publish marker: %w", ref, err)
			}
			log.Info("seeded artifact", "ref", ref, "key", k.String())
		}
	}
	return nil
}

// ErrNoSeed reports that no seed blob is embedded for the requested
// artifact/platform (errors.Is-able, so callers can treat it as a skip).
var ErrNoSeed = errors.New("seed artifact not embedded")

// SeedShellAs ingests the embedded shell artifact for platform and publishes
// it under ref — NOT under shell:<platform>. The runner's boot self-test
// needs a locally runnable shell without publishing the real ref: on runners
// shell:<platform> must keep resolving via pull-from-server (a local publish
// would shadow the server's shell for every subsequent job). Idempotence is
// the same blob-marker scheme as Seed; a marker hit re-publishes ref (cheap)
// in case it was deleted. Returns the shell's content key.
func SeedShellAs(ctx context.Context, st *amber.Store, platform, ref string) (key.Key, error) {
	blob, err := seedFS.ReadFile("seed/" + platformDir(platform) + "/shell.tar.zst")
	if err != nil {
		return key.Key{}, fmt.Errorf("%w: shell for %s", ErrNoSeed, platform)
	}
	marker := seedMarkerRef(ref, blob)
	if k, ok, err := st.GetKey(ctx, marker); err == nil && ok {
		if err := st.PutRef(ctx, ref, k); err != nil {
			return key.Key{}, fmt.Errorf("seed %s: republish: %w", ref, err)
		}
		return k, nil
	}
	k, err := ingestSeedBlob(ctx, st, blob)
	if err != nil {
		return key.Key{}, fmt.Errorf("seed %s: ingest: %w", ref, err)
	}
	if err := st.PutRef(ctx, ref, k); err != nil {
		return key.Key{}, fmt.Errorf("seed %s: publish: %w", ref, err)
	}
	if err := st.PutRef(ctx, marker, k); err != nil {
		return key.Key{}, fmt.Errorf("seed %s: publish marker: %w", ref, err)
	}
	return k, nil
}

// seedMarkerRef names the marker recording that ref was seeded from this exact
// embedded blob. The blob hash in the name is what makes re-seeding blob-aware:
// a new binary with a changed blob sees no marker and refreshes the ref; the
// same blob sees its marker and skips. Stale markers from older blobs linger
// harmlessly (a store wipe clears them).
func seedMarkerRef(ref string, blob []byte) string {
	sum := sha256.Sum256(blob)
	return "seed-src:" + ref + ":" + hex.EncodeToString(sum[:8])
}

// ingestSeedBlob decompresses a .tar.zst seed blob into a temp dir and ingests
// it into amber, returning the artifact's content key.
func ingestSeedBlob(ctx context.Context, st *amber.Store, blob []byte) (key.Key, error) {
	zr, err := zstd.NewReader(bytes.NewReader(blob), zstd.WithDecoderMaxWindow(512<<20))
	if err != nil {
		return key.Key{}, fmt.Errorf("zstd reader: %w", err)
	}
	defer zr.Close()
	tmp, err := os.MkdirTemp("", "jobs-seed")
	if err != nil {
		return key.Key{}, err
	}
	defer os.RemoveAll(tmp)
	if err := extractTar(zr, tmp); err != nil {
		return key.Key{}, fmt.Errorf("extract: %w", err)
	}
	return st.IngestDir(ctx, tmp)
}

// extractTar unpacks a tar stream into dest, rejecting path traversal.
func extractTar(r io.Reader, dest string) error {
	cleanDest := filepath.Clean(dest)
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		name := filepath.Clean(hdr.Name)
		if name == "." {
			continue
		}
		if name == ".." || strings.HasPrefix(name, ".."+string(os.PathSeparator)) {
			return fmt.Errorf("unsafe tar path %q", hdr.Name)
		}
		target := filepath.Join(cleanDest, name)
		if target != cleanDest && !strings.HasPrefix(target, cleanDest+string(os.PathSeparator)) {
			return fmt.Errorf("unsafe tar path %q", hdr.Name)
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode)&0o777)
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			_ = os.Remove(target)
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return err
			}
		case tar.TypeLink:
			// Seed artifacts are vendored scripts + a static userland with no
			// hardlinks; a hardlink would ingest as an incomplete tree (wrong key).
			// Fail loud rather than silently drop it.
			return fmt.Errorf("unexpected hardlink in seed tar: %q -> %q", hdr.Name, hdr.Linkname)
		default:
			// skip device nodes, fifos — seed artifacts have none
		}
	}
	return nil
}
