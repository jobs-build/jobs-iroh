package runner

import (
	"archive/tar"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/amber-store/core/key"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/jobs-build/jobs-iroh/amber"
)

// ingestTestDir writes files (path → content) into a temp dir and ingests it
// into the store, returning the content key (BOK).
func ingestTestDir(t *testing.T, ctx context.Context, st *amber.Store, files map[string]string) key.Key {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	bok, err := st.IngestDir(ctx, dir)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	return bok
}

type fsEntry struct {
	hdr     *tar.Header
	content string
}

// extractImageFS flattens the image's layers and returns path → entry.
func extractImageFS(t *testing.T, img v1.Image) map[string]fsEntry {
	t.Helper()
	rc := mutate.Extract(img)
	defer rc.Close()
	tr := tar.NewReader(rc)
	out := map[string]fsEntry{}
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read flattened fs: %v", err)
		}
		b, _ := io.ReadAll(tr)
		// Normalise a possible leading "./" so assertions are simple.
		name := h.Name
		if len(name) > 2 && name[:2] == "./" {
			name = name[2:]
		}
		out[name] = fsEntry{hdr: h, content: string(b)}
	}
	return out
}

func TestAssembleImage(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)

	self := ingestTestDir(t, ctx, st, map[string]string{"bin/app": "APP"})
	dep := ingestTestDir(t, ctx, st, map[string]string{"lib/libfoo": "FOO"})
	shell := ingestTestDir(t, ctx, st, map[string]string{"bin/sh": "SH", "bin/bash": "BASH"})

	ep := Entrypoint{Command: "bin/app", Args: []string{"--addr", ":8080"}, Env: map[string]string{"LOG": "info"}}

	t.Run("with shell", func(t *testing.T) {
		img, err := assembleImage(ctx, st, self, []key.Key{dep}, shell, ep, "linux/amd64", true)
		if err != nil {
			t.Fatalf("assembleImage: %v", err)
		}

		// NOTE: validate.Image runs against the tarball-loaded images in
		// image_bykey_test.go / image_source_linux_test.go — the written
		// artifact. The in-memory mutate image trips validate's strict
		// nil-vs-empty Annotations comparison, a representation quirk that
		// disappears on the tarball round-trip.

		cf, err := img.ConfigFile()
		if err != nil {
			t.Fatalf("config: %v", err)
		}
		if cf.OS != "linux" || cf.Architecture != "amd64" {
			t.Errorf("os/arch = %q/%q, want linux/amd64", cf.OS, cf.Architecture)
		}
		if got, want := cf.Config.WorkingDir, "/"; got != want {
			t.Errorf("WorkingDir = %q, want %q", got, want)
		}
		wantEP := []string{"/bin/app", "--addr", ":8080"}
		if got := cf.Config.Entrypoint; !eqStrings(got, wantEP) {
			t.Errorf("Entrypoint = %v, want %v", got, wantEP)
		}
		if len(cf.Config.Cmd) != 0 {
			t.Errorf("Cmd = %v, want empty", cf.Config.Cmd)
		}
		if !hasEnv(cf.Config.Env, "PATH=/bin:/jobs/store/"+shell.String()+"/bin") {
			t.Errorf("Env missing expected PATH: %v", cf.Config.Env)
		}
		if !hasEnv(cf.Config.Env, "LOG=info") {
			t.Errorf("Env missing entrypoint var LOG: %v", cf.Config.Env)
		}

		fs := extractImageFS(t, img)
		// Build target lands at the image root.
		if e, ok := fs["bin/app"]; !ok || e.content != "APP" {
			t.Errorf("bin/app at root missing or wrong: %+v", e)
		}
		// Transitive dep is content-addressed under /jobs/store/<BOK>.
		if _, ok := fs["jobs/store/"+dep.String()+"/lib/libfoo"]; !ok {
			t.Errorf("dep not under jobs/store: keys=%v", keysOf(fs))
		}
		// Shell artifact present under /jobs/store/<shellBOK>.
		if _, ok := fs["jobs/store/"+shell.String()+"/bin/sh"]; !ok {
			t.Errorf("shell not under jobs/store")
		}
		// /bin/sh symlink → the shell's sh.
		if e, ok := fs["bin/sh"]; !ok || e.hdr.Typeflag != tar.TypeSymlink || e.hdr.Linkname != "/jobs/store/"+shell.String()+"/bin/sh" {
			t.Errorf("/bin/sh symlink missing/wrong: %+v", e.hdr)
		}
		// /jobs/shell compat symlink → the shell root.
		if e, ok := fs["jobs/shell"]; !ok || e.hdr.Typeflag != tar.TypeSymlink || e.hdr.Linkname != "/jobs/store/"+shell.String() {
			t.Errorf("/jobs/shell symlink missing/wrong: %+v", e.hdr)
		}
	})

	t.Run("reproducible — same inputs, same digest", func(t *testing.T) {
		a, err := assembleImage(ctx, st, self, []key.Key{dep}, shell, ep, "linux/amd64", true)
		if err != nil {
			t.Fatal(err)
		}
		b, err := assembleImage(ctx, st, self, []key.Key{dep}, shell, ep, "linux/amd64", true)
		if err != nil {
			t.Fatal(err)
		}
		da, err := a.Digest()
		if err != nil {
			t.Fatal(err)
		}
		db, err := b.Digest()
		if err != nil {
			t.Fatal(err)
		}
		if da != db {
			t.Errorf("image digests differ across builds: %s vs %s", da, db)
		}
	})

	t.Run("without shell", func(t *testing.T) {
		img, err := assembleImage(ctx, st, self, []key.Key{dep}, shell, ep, "linux/amd64", false)
		if err != nil {
			t.Fatalf("assembleImage: %v", err)
		}
		cf, err := img.ConfigFile()
		if err != nil {
			t.Fatalf("config: %v", err)
		}
		if !hasEnv(cf.Config.Env, "PATH=/bin") {
			t.Errorf("no-shell PATH should be /bin: %v", cf.Config.Env)
		}
		fs := extractImageFS(t, img)
		if _, ok := fs["bin/sh"]; ok {
			t.Errorf("/bin/sh symlink must be absent without shell")
		}
		if _, ok := fs["jobs/shell"]; ok {
			t.Errorf("/jobs/shell must be absent without shell")
		}
		if _, ok := fs["jobs/store/"+shell.String()+"/bin/sh"]; ok {
			t.Errorf("shell store entry must be absent without shell")
		}
		// dep still present.
		if _, ok := fs["jobs/store/"+dep.String()+"/lib/libfoo"]; !ok {
			t.Errorf("dep must still be present without shell")
		}
	})
}

func eqStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func hasEnv(env []string, want string) bool {
	for _, e := range env {
		if e == want {
			return true
		}
	}
	return false
}

func keysOf(m map[string]fsEntry) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
