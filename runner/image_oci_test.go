package runner

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/amber-store/core/key"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

// readLayerBlob reads the bytes a registry would serve for this layer.
func readLayerBlob(t *testing.T, l v1.Layer) []byte {
	t.Helper()
	rc, err := l.Compressed()
	if err != nil {
		t.Fatalf("open layer blob: %v", err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read layer blob: %v", err)
	}
	return b
}

func TestAssembleOCIImage(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)

	self := ingestTestDir(t, ctx, st, map[string]string{"bin/app": "APP"})
	other := ingestTestDir(t, ctx, st, map[string]string{"bin/other": "OTHER"})
	depA := ingestTestDir(t, ctx, st, map[string]string{"lib/liba": "A"})
	depB := ingestTestDir(t, ctx, st, map[string]string{"lib/libb": "B"})
	deps := []key.Key{depA, depB}

	ep := Entrypoint{Command: "bin/app", Args: []string{"--addr", ":8080"}, Env: map[string]string{"LOG": "info"}}

	img, imgLayers, err := AssembleOCIImage(ctx, st, self, deps, key.Key{}, &ep, "linux/amd64")
	if err != nil {
		t.Fatalf("AssembleOCIImage: %v", err)
	}

	t.Run("OCI media types", func(t *testing.T) {
		mt, err := img.MediaType()
		if err != nil {
			t.Fatal(err)
		}
		if mt != types.OCIManifestSchema1 {
			t.Errorf("manifest media type = %q, want %q", mt, types.OCIManifestSchema1)
		}
		man, err := img.Manifest()
		if err != nil {
			t.Fatal(err)
		}
		if man.Config.MediaType != types.OCIConfigJSON {
			t.Errorf("config media type = %q, want %q", man.Config.MediaType, types.OCIConfigJSON)
		}
		for i, l := range man.Layers {
			if l.MediaType != types.OCIUncompressedLayer {
				t.Errorf("layer %d media type = %q, want %q", i, l.MediaType, types.OCIUncompressedLayer)
			}
		}
	})

	t.Run("layer blobs are plain uncompressed tars", func(t *testing.T) {
		layers, err := img.Layers()
		if err != nil {
			t.Fatal(err)
		}
		for i, l := range layers {
			blob := readLayerBlob(t, l)
			if len(blob) >= 2 && blob[0] == 0x1f && blob[1] == 0x8b {
				t.Errorf("layer %d blob is gzip-compressed", i)
			}
			if _, err := tar.NewReader(bytes.NewReader(blob)).Next(); err != nil {
				t.Errorf("layer %d blob does not read as a tar: %v", i, err)
			}
			// Uncompressed: the blob digest IS the diffID, and the manifest's
			// size is the tar's size.
			d, err := l.Digest()
			if err != nil {
				t.Fatal(err)
			}
			diffID, err := l.DiffID()
			if err != nil {
				t.Fatal(err)
			}
			if d != diffID {
				t.Errorf("layer %d digest %s != diffID %s", i, d, diffID)
			}
			if got, _, err := v1.SHA256(bytes.NewReader(blob)); err != nil || got != d {
				t.Errorf("layer %d digest %s does not hash its blob (%s, err=%v)", i, d, got, err)
			}
			size, err := l.Size()
			if err != nil {
				t.Fatal(err)
			}
			if size != int64(len(blob)) {
				t.Errorf("layer %d size = %d, blob is %d bytes", i, size, len(blob))
			}
		}
	})

	t.Run("layer specs re-stream the exact blob", func(t *testing.T) {
		// The registry stores these specs instead of the bytes, so a spec
		// regenerating anything but the measured blob would serve a digest
		// mismatch to every client.
		layers, err := img.Layers()
		if err != nil {
			t.Fatal(err)
		}
		if len(imgLayers) != len(layers) {
			t.Fatalf("assembled %d layer specs for %d layers", len(imgLayers), len(layers))
		}
		for i, il := range imgLayers {
			d, err := layers[i].Digest()
			if err != nil {
				t.Fatal(err)
			}
			if il.Digest != d {
				t.Errorf("spec %d digest %s != layer digest %s", i, il.Digest, d)
			}
			var buf bytes.Buffer
			if err := WriteLayerTar(ctx, st, &buf, il.Spec); err != nil {
				t.Fatalf("WriteLayerTar(%s): %v", il.Spec.Kind, err)
			}
			if int64(buf.Len()) != il.Size {
				t.Errorf("re-streamed %s layer is %d bytes, recorded %d", il.Spec.Kind, buf.Len(), il.Size)
			}
			got, _, err := v1.SHA256(bytes.NewReader(buf.Bytes()))
			if err != nil {
				t.Fatal(err)
			}
			if got != il.Digest {
				t.Errorf("re-streamed %s layer hashes %s, recorded %s", il.Spec.Kind, got, il.Digest)
			}
			if !bytes.Equal(buf.Bytes(), readLayerBlob(t, layers[i])) {
				t.Errorf("re-streamed %s layer differs byte-for-byte from the assembled blob", il.Spec.Kind)
			}
		}
	})

	t.Run("unknown layer kind", func(t *testing.T) {
		if err := WriteLayerTar(ctx, st, io.Discard, LayerSpec{Kind: "bogus"}); err == nil {
			t.Error("want error for an unknown layer kind")
		}
	})

	t.Run("two layers: deps then artifact", func(t *testing.T) {
		layers, err := img.Layers()
		if err != nil {
			t.Fatal(err)
		}
		if len(layers) != 2 {
			t.Fatalf("layer count = %d, want 2", len(layers))
		}
		fs := extractImageFS(t, img)
		if e, ok := fs["bin/app"]; !ok || e.content != "APP" {
			t.Errorf("artifact bin/app not at image root: %+v", e)
		}
		if _, ok := fs["jobs/store/"+depA.String()+"/lib/liba"]; !ok {
			t.Errorf("dep A not under /jobs/store")
		}
		if _, ok := fs["jobs/store/"+depB.String()+"/lib/libb"]; !ok {
			t.Errorf("dep B not under /jobs/store")
		}
		tmpEnt, ok := fs["tmp/"]
		if !ok {
			tmpEnt, ok = fs["tmp"]
		}
		if !ok || tmpEnt.hdr.Typeflag != tar.TypeDir {
			t.Errorf("writable /tmp missing: %+v", tmpEnt)
		}
		for p := range fs {
			if p == "bin/sh" || p == "jobs/shell" {
				t.Errorf("shell artifact leaked into a no-shell registry image: %s", p)
			}
		}
	})

	t.Run("config", func(t *testing.T) {
		cf, err := img.ConfigFile()
		if err != nil {
			t.Fatal(err)
		}
		if cf.OS != "linux" || cf.Architecture != "amd64" {
			t.Errorf("os/arch = %q/%q, want linux/amd64", cf.OS, cf.Architecture)
		}
		if got, want := cf.Config.WorkingDir, "/"; got != want {
			t.Errorf("WorkingDir = %q, want %q", got, want)
		}
		if got := cf.Config.Entrypoint; !eqStrings(got, []string{"/bin/app", "--addr", ":8080"}) {
			t.Errorf("Entrypoint = %v", got)
		}
		if !hasEnv(cf.Config.Env, "PATH=/bin") || !hasEnv(cf.Config.Env, "HOME=/tmp") || !hasEnv(cf.Config.Env, "LOG=info") {
			t.Errorf("Env = %v", cf.Config.Env)
		}
		if !cf.Created.Equal(epoch) {
			t.Errorf("Created = %v, want epoch", cf.Created)
		}
	})

	t.Run("deps layer is shared across images with the same closure", func(t *testing.T) {
		img2, _, err := AssembleOCIImage(ctx, st, other, []key.Key{depB, depA, depA}, key.Key{}, nil, "linux/amd64")
		if err != nil {
			t.Fatal(err)
		}
		l1, err := img.Layers()
		if err != nil {
			t.Fatal(err)
		}
		l2, err := img2.Layers()
		if err != nil {
			t.Fatal(err)
		}
		d1, err := l1[0].Digest()
		if err != nil {
			t.Fatal(err)
		}
		d2, err := l2[0].Digest()
		if err != nil {
			t.Fatal(err)
		}
		if d1 != d2 {
			t.Errorf("deps layers differ for identical (unordered, duplicated) closures: %s vs %s", d1, d2)
		}
		a1, err := l1[1].Digest()
		if err != nil {
			t.Fatal(err)
		}
		a2, err := l2[1].Digest()
		if err != nil {
			t.Fatal(err)
		}
		if a1 == a2 {
			t.Errorf("artifact layers must differ for different artifacts")
		}
	})

	t.Run("nil entrypoint", func(t *testing.T) {
		img2, _, err := AssembleOCIImage(ctx, st, self, nil, key.Key{}, nil, "linux/arm64")
		if err != nil {
			t.Fatal(err)
		}
		cf, err := img2.ConfigFile()
		if err != nil {
			t.Fatal(err)
		}
		if len(cf.Config.Entrypoint) != 0 {
			t.Errorf("Entrypoint = %v, want none", cf.Config.Entrypoint)
		}
		if !hasEnv(cf.Config.Env, "PATH=/bin") || !hasEnv(cf.Config.Env, "HOME=/tmp") {
			t.Errorf("Env = %v", cf.Config.Env)
		}
		if cf.OS != "linux" || cf.Architecture != "arm64" {
			t.Errorf("os/arch = %q/%q, want linux/arm64", cf.OS, cf.Architecture)
		}
		// Zero deps still yields a two-layer image with the store scaffolding.
		layers, err := img2.Layers()
		if err != nil {
			t.Fatal(err)
		}
		if len(layers) != 2 {
			t.Fatalf("layer count = %d, want 2", len(layers))
		}
	})

	t.Run("reproducible", func(t *testing.T) {
		again, _, err := AssembleOCIImage(ctx, st, self, []key.Key{depA, depB}, key.Key{}, &ep, "linux/amd64")
		if err != nil {
			t.Fatal(err)
		}
		da, err := img.Digest()
		if err != nil {
			t.Fatal(err)
		}
		db, err := again.Digest()
		if err != nil {
			t.Fatal(err)
		}
		if da != db {
			t.Errorf("image digests differ across identical assemblies: %s vs %s", da, db)
		}
	})

	t.Run("with shell: /bin/sh + /jobs/shell + PATH, like run and image", func(t *testing.T) {
		shell := ingestTestDir(t, ctx, st, map[string]string{"bin/sh": "SH", "bin/bash": "BASH"})
		simg, _, err := AssembleOCIImage(ctx, st, self, deps, shell, &ep, "linux/amd64")
		if err != nil {
			t.Fatalf("AssembleOCIImage with shell: %v", err)
		}
		fs := extractImageFS(t, simg)
		if _, ok := fs["jobs/store/"+shell.String()+"/bin/sh"]; !ok {
			t.Errorf("shell not under /jobs/store")
		}
		if e, ok := fs["bin/sh"]; !ok || e.hdr.Typeflag != tar.TypeSymlink || e.hdr.Linkname != "/jobs/store/"+shell.String()+"/bin/sh" {
			t.Errorf("/bin/sh symlink missing/wrong: %+v", e.hdr)
		}
		if e, ok := fs["jobs/shell"]; !ok || e.hdr.Typeflag != tar.TypeSymlink || e.hdr.Linkname != "/jobs/store/"+shell.String() {
			t.Errorf("/jobs/shell symlink missing/wrong: %+v", e.hdr)
		}
		cf, err := simg.ConfigFile()
		if err != nil {
			t.Fatal(err)
		}
		if !hasEnv(cf.Config.Env, "PATH=/bin:/jobs/store/"+shell.String()+"/bin") {
			t.Errorf("Env missing shell PATH: %v", cf.Config.Env)
		}

		// The shell lives in the deps layer: same deps+shell across different
		// artifacts share the blob; shell-less and shell-ful deps layers differ.
		simg2, _, err := AssembleOCIImage(ctx, st, other, deps, shell, nil, "linux/amd64")
		if err != nil {
			t.Fatal(err)
		}
		sl1, err := simg.Layers()
		if err != nil {
			t.Fatal(err)
		}
		sl2, err := simg2.Layers()
		if err != nil {
			t.Fatal(err)
		}
		sd1, err := sl1[0].Digest()
		if err != nil {
			t.Fatal(err)
		}
		sd2, err := sl2[0].Digest()
		if err != nil {
			t.Fatal(err)
		}
		if sd1 != sd2 {
			t.Errorf("deps layers differ for identical closure+shell: %s vs %s", sd1, sd2)
		}
		noShellLayers, err := img.Layers()
		if err != nil {
			t.Fatal(err)
		}
		nd, err := noShellLayers[0].Digest()
		if err != nil {
			t.Fatal(err)
		}
		if nd == sd1 {
			t.Errorf("shell-less and shell-ful deps layers must differ")
		}
	})

	t.Run("invalid platform", func(t *testing.T) {
		if _, _, err := AssembleOCIImage(ctx, st, self, nil, key.Key{}, nil, "weird"); err == nil {
			t.Error("want error for platform without os/arch")
		}
	})
}
