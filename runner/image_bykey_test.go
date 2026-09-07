package runner

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/amber-store/core/key"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/go-containerregistry/pkg/v1/validate"
	"github.com/jobs-build/jobs-iroh/amber"
)

// manifestRepoTags reads the RepoTags of the single image in a docker-load tar.
func manifestRepoTags(t *testing.T, tarPath string) []string {
	t.Helper()
	f, err := os.Open(tarPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	tr := tar.NewReader(f)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			t.Fatal("manifest.json not found in tar")
		}
		if err != nil {
			t.Fatal(err)
		}
		if h.Name == "manifest.json" {
			var m []struct{ RepoTags []string }
			if err := json.NewDecoder(tr).Decode(&m); err != nil {
				t.Fatal(err)
			}
			if len(m) != 1 {
				t.Fatalf("want 1 manifest entry, got %d", len(m))
			}
			return m[0].RepoTags
		}
	}
}

// putImageByKeyFixture hand-builds a build output { c/{bin/app,
// JOBS.entrypoint} } and a one-dep runtime closure under a fabricated
// definition key (resolved via the direct build-output:K fallback — no
// build-from bridge, like a local build's F).
func putImageByKeyFixture(t *testing.T, ctx context.Context, st *amber.Store) (bk, depBOK key.Key) {
	t.Helper()
	bk = testKey(t, "buildK")
	T := t.TempDir()
	cDir := filepath.Join(T, "c")
	if err := os.MkdirAll(filepath.Join(cDir, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(cDir, "bin", "app"), "#!/bin/sh\necho hi\n")
	mustWrite(t, filepath.Join(cDir, "JOBS.entrypoint"), `{"command":"bin/app","args":["x"],"env":{"LOG":"info"}}`)
	outKey, err := st.IngestDir(ctx, T)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutRef(ctx, "build-output:"+bk.String(), outKey); err != nil {
		t.Fatal(err)
	}

	// build-output-deps:K → store tree of one dep
	depBOK = ingestTestDir(t, ctx, st, map[string]string{"lib/libfoo": "FOO"})
	depsTree, err := st.BuildStoreTree(ctx, []key.Key{depBOK})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutRef(ctx, "build-output-deps:"+bk.String(), depsTree); err != nil {
		t.Fatal(err)
	}
	return bk, depBOK
}

func TestBuildImageByKey(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	platform := "linux/amd64"

	// shell:<platform>
	shellKey := ingestTestDir(t, ctx, st, map[string]string{"bin/sh": "SH", "bin/bash": "BASH"})
	if err := st.PutRef(ctx, "shell:"+platform, shellKey); err != nil {
		t.Fatal(err)
	}

	bk, depBOK := putImageByKeyFixture(t, ctx, st)

	// Build the image to a tarball.
	tarPath := filepath.Join(t.TempDir(), "img.tar")
	out, err := os.Create(tarPath)
	if err != nil {
		t.Fatal(err)
	}
	err = BuildImageByKey(ctx, st, bk, platform, "", ImageOptions{Tag: "app:test", IncludeShell: true, Output: out})
	out.Close()
	if err != nil {
		t.Fatalf("BuildImageByKey: %v", err)
	}

	// The tarball is a valid docker-load image (go-containerregistry's own
	// validator checks manifest/config/layer digest coherence — no daemon).
	img, err := tarball.ImageFromPath(tarPath, nil)
	if err != nil {
		t.Fatalf("re-open image tar: %v", err)
	}
	if err := validate.Image(img); err != nil {
		t.Fatalf("validate.Image: %v", err)
	}
	cf, err := img.ConfigFile()
	if err != nil {
		t.Fatal(err)
	}
	if got := cf.Config.Entrypoint; !eqStrings(got, []string{"/bin/app", "x"}) {
		t.Errorf("Entrypoint = %v, want [/bin/app x]", got)
	}
	if cf.Config.WorkingDir != "/" {
		t.Errorf("WorkingDir = %q, want /", cf.Config.WorkingDir)
	}
	if cf.OS != "linux" || cf.Architecture != "amd64" {
		t.Errorf("os/arch = %q/%q", cf.OS, cf.Architecture)
	}
	if !hasEnv(cf.Config.Env, "LOG=info") {
		t.Errorf("Env missing LOG=info from JOBS.entrypoint: %v", cf.Config.Env)
	}

	fs := extractImageFS(t, img)
	if e, ok := fs["bin/app"]; !ok || e.content != "#!/bin/sh\necho hi\n" {
		t.Errorf("build output c/bin/app not at image root: %+v", e)
	}
	if _, ok := fs["jobs/store/"+depBOK.String()+"/lib/libfoo"]; !ok {
		t.Errorf("runtime dep not under /jobs/store")
	}

	if tags := manifestRepoTags(t, tarPath); !eqStrings(tags, []string{"app:test"}) {
		t.Errorf("RepoTags = %v, want [app:test]", tags)
	}
}

// TestBuildImageByKeyReproducibleTar locks the strongest reproducibility
// property: the SAME store input run through BuildImageByKey twice yields a
// byte-identical docker-load tarball (epoch-0 timestamps, sorted store
// entries, deterministic layer stream).
func TestBuildImageByKeyReproducibleTar(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	platform := "linux/amd64"

	shellKey := ingestTestDir(t, ctx, st, map[string]string{"bin/sh": "SH", "bin/bash": "BASH"})
	if err := st.PutRef(ctx, "shell:"+platform, shellKey); err != nil {
		t.Fatal(err)
	}
	bk, _ := putImageByKeyFixture(t, ctx, st)

	var a, b bytes.Buffer
	if err := BuildImageByKey(ctx, st, bk, platform, "", ImageOptions{Tag: "app:test", IncludeShell: true, Output: &a}); err != nil {
		t.Fatalf("first BuildImageByKey: %v", err)
	}
	if err := BuildImageByKey(ctx, st, bk, platform, "", ImageOptions{Tag: "app:test", IncludeShell: true, Output: &b}); err != nil {
		t.Fatalf("second BuildImageByKey: %v", err)
	}
	if !bytes.Equal(a.Bytes(), b.Bytes()) {
		t.Fatalf("image tarballs differ across identical builds (%d vs %d bytes)", a.Len(), b.Len())
	}
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
}

// A --no-shell image must build from a store that has no shell:<platform> ref
// at all: the shell is only baked when IncludeShell is set, so it must not be
// resolved either — `jobs-client image --no-shell <K>` against a store
// populated only by a remote-build pull previously failed in jobs with
// "shell artifact not found".
func TestBuildImageByKeyNoShellNeedsNoShellRef(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	platform := "linux/amd64"

	// build-output:K → { c/{bin/app, JOBS.entrypoint} } — NO shell ref anywhere.
	bk := testKey(t, "buildK-noshell")
	T := t.TempDir()
	cDir := filepath.Join(T, "c")
	if err := os.MkdirAll(filepath.Join(cDir, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(cDir, "bin", "app"), "#!/bin/sh\necho hi\n")
	mustWrite(t, filepath.Join(cDir, "JOBS.entrypoint"), `{"command":"bin/app","args":[],"env":{}}`)
	outKey, err := st.IngestDir(ctx, T)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutRef(ctx, "build-output:"+bk.String(), outKey); err != nil {
		t.Fatal(err)
	}
	depsTree, err := st.BuildStoreTree(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutRef(ctx, "build-output-deps:"+bk.String(), depsTree); err != nil {
		t.Fatal(err)
	}

	tarPath := filepath.Join(t.TempDir(), "img.tar")
	out, err := os.Create(tarPath)
	if err != nil {
		t.Fatal(err)
	}
	err = BuildImageByKey(ctx, st, bk, platform, "", ImageOptions{Tag: "app:noshell", IncludeShell: false, Output: out})
	out.Close()
	if err != nil {
		t.Fatalf("BuildImageByKey without shell ref: %v", err)
	}

	img, err := tarball.ImageFromPath(tarPath, nil)
	if err != nil {
		t.Fatalf("re-open image tar: %v", err)
	}
	if err := validate.Image(img); err != nil {
		t.Fatalf("validate.Image: %v", err)
	}
	fs := extractImageFS(t, img)
	if e, ok := fs["bin/app"]; !ok || e.content != "#!/bin/sh\necho hi\n" {
		t.Errorf("build output c/bin/app not at image root: %+v", e)
	}
	for p := range fs {
		if p == "bin/sh" || p == "jobs/shell" {
			t.Errorf("shell artifact leaked into a --no-shell image: %s", p)
		}
	}
}
