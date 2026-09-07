package amber_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"testing"

	"github.com/amber-store/core/key"
	"github.com/amber-store/core/tarextract"
	"github.com/jobs-build/jobs-iroh/amber"
	"golang.org/x/sys/unix"
)

// lsNames returns the entry names of the directory at path under root.
func lsNames(t *testing.T, s *amber.Store, root key.Key, path string) []string {
	t.Helper()
	ents, err := s.Ls(t.Context(), root, path)
	if err != nil {
		t.Fatalf("Ls %s %q: %v", root, path, err)
	}
	names := make([]string, 0, len(ents))
	for _, e := range ents {
		names = append(names, e.Name)
	}
	return names
}

func TestIngestSourceDirAmberignore(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()

	src := t.TempDir()
	write(t, filepath.Join(src, ".amberignore"), []byte("*.log\n!keep.log\nbuild/\n"), 0o644)
	write(t, filepath.Join(src, "a.txt"), []byte("a"), 0o644)
	write(t, filepath.Join(src, "debug.log"), []byte("noise"), 0o644)
	write(t, filepath.Join(src, "keep.log"), []byte("kept by negation"), 0o644)
	write(t, filepath.Join(src, "build", "x.txt"), []byte("built"), 0o644)
	write(t, filepath.Join(src, "sub", ".amberignore"), []byte("secret*\n"), 0o644)
	write(t, filepath.Join(src, "sub", "secret.txt"), []byte("hidden"), 0o644)
	write(t, filepath.Join(src, "sub", "ok.txt"), []byte("visible"), 0o644)
	write(t, filepath.Join(src, "sub", "nested.log"), []byte("noise"), 0o644) // root pattern applies below

	root, err := s.IngestSourceDir(ctx, src)
	if err != nil {
		t.Fatalf("IngestSourceDir: %v", err)
	}

	if got, want := lsNames(t, s, root, ""), []string{"a.txt", "keep.log", "sub"}; !slices.Equal(got, want) {
		t.Errorf("root entries = %v, want %v", got, want)
	}
	if got, want := lsNames(t, s, root, "sub"), []string{"ok.txt"}; !slices.Equal(got, want) {
		t.Errorf("sub entries = %v, want %v", got, want)
	}
}

func TestBuildStoreTree(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()

	mkDir := func(name, content string) key.Key {
		dir := t.TempDir()
		write(t, filepath.Join(dir, name), []byte(content), 0o644)
		k, err := s.IngestDir(ctx, dir)
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	d1 := mkDir("one.txt", "one")
	d2 := mkDir("two.txt", "two")
	fk, err := s.IngestFile(ctx, []byte("file-shaped artifact"))
	if err != nil {
		t.Fatal(err)
	}

	st1, err := s.BuildStoreTree(ctx, []key.Key{d1, d2, fk})
	if err != nil {
		t.Fatalf("BuildStoreTree: %v", err)
	}
	// Any order, with repeats, gives the same key.
	st2, err := s.BuildStoreTree(ctx, []key.Key{fk, d2, d1, d1, fk})
	if err != nil {
		t.Fatal(err)
	}
	if st1 != st2 {
		t.Errorf("store tree not deterministic: %s vs %s", st1, st2)
	}

	ents, err := s.Ls(ctx, st1, "")
	if err != nil {
		t.Fatal(err)
	}
	wantNames := []string{d1.String(), d2.String(), fk.String()}
	sort.Strings(wantNames)
	var gotNames []string
	for _, e := range ents {
		gotNames = append(gotNames, e.Name)
		if e.Key.String() != e.Name {
			t.Errorf("entry %s points at %s, want key == hex name", e.Name, e.Key)
		}
		wantMode := uint64(unix.S_IFDIR | 0o555)
		if e.Key == fk {
			wantMode = unix.S_IFREG | 0o444
		}
		if e.Mode != wantMode {
			t.Errorf("entry %s mode %#o, want %#o", e.Name, e.Mode, wantMode)
		}
	}
	if !slices.Equal(gotNames, wantNames) {
		t.Errorf("store tree entries = %v, want %v", gotNames, wantNames)
	}
}

func TestBuildFromTree(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()

	envDir := t.TempDir()
	write(t, filepath.Join(envDir, "BUILD.jobs"), []byte("def build(): pass"), 0o644)
	env, err := s.IngestDir(ctx, envDir)
	if err != nil {
		t.Fatal(err)
	}
	params := []byte(`{"a":1}`)
	const platform = "linux/amd64"

	f1, err := s.BuildFromTree(ctx, env, "", params, platform, nil)
	if err != nil {
		t.Fatalf("BuildFromTree: %v", err)
	}
	if got, want := lsNames(t, s, f1, ""), []string{"env", "params", "platform"}; !slices.Equal(got, want) {
		t.Errorf("build-from entries = %v, want %v", got, want)
	}
	ents, err := s.Ls(ctx, f1, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		switch e.Name {
		case "env":
			if e.Key != env {
				t.Errorf("env entry key %s, want %s", e.Key, env)
			}
			if e.Mode != uint64(unix.S_IFDIR|0o555) {
				t.Errorf("env mode %#o", e.Mode)
			}
		case "params":
			if got := readAll(t, s, e.Key); !bytes.Equal(got, params) {
				t.Errorf("params content %q, want %q", got, params)
			}
		case "platform":
			if got := readAll(t, s, e.Key); string(got) != platform {
				t.Errorf("platform content %q, want %q", got, platform)
			}
		}
	}

	// Deterministic: same inputs, same key.
	again, err := s.BuildFromTree(ctx, env, "", params, platform, nil)
	if err != nil {
		t.Fatal(err)
	}
	if again != f1 {
		t.Errorf("BuildFromTree not deterministic: %s vs %s", again, f1)
	}

	// Override present only when non-nil — an empty override is still an
	// override file.
	fo, err := s.BuildFromTree(ctx, env, "", params, platform, []byte("override recipe"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := lsNames(t, s, fo, ""), []string{"BUILD.jobs", "env", "params", "platform"}; !slices.Equal(got, want) {
		t.Errorf("override entries = %v, want %v", got, want)
	}
	foEmpty, err := s.BuildFromTree(ctx, env, "", params, platform, []byte{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := lsNames(t, s, foEmpty, ""), []string{"BUILD.jobs", "env", "params", "platform"}; !slices.Equal(got, want) {
		t.Errorf("empty-override entries = %v, want %v", got, want)
	}

	// Every input is identity: changing any one changes F.
	env2Dir := t.TempDir()
	write(t, filepath.Join(env2Dir, "BUILD.jobs"), []byte("def build(): other"), 0o644)
	env2, err := s.IngestDir(ctx, env2Dir)
	if err != nil {
		t.Fatal(err)
	}
	variants := map[string]key.Key{}
	for name, mk := range map[string]func() (key.Key, error){
		"params":   func() (key.Key, error) { return s.BuildFromTree(ctx, env, "", []byte(`{"a":2}`), platform, nil) },
		"platform": func() (key.Key, error) { return s.BuildFromTree(ctx, env, "", params, "linux/arm64", nil) },
		"env":      func() (key.Key, error) { return s.BuildFromTree(ctx, env2, "", params, platform, nil) },
		"override": func() (key.Key, error) { return fo, nil },
	} {
		k, err := mk()
		if err != nil {
			t.Fatalf("%s variant: %v", name, err)
		}
		if k == f1 {
			t.Errorf("changing %s did not change the key", name)
		}
		variants[name] = k
	}
	_ = variants
}

func TestTreeReads(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()

	src := t.TempDir()
	const nested = "nested content"
	write(t, filepath.Join(src, "a", "b", "file.txt"), []byte(nested), 0o644)
	write(t, filepath.Join(src, "top.txt"), []byte("top content"), 0o644)
	if err := os.Symlink("top.txt", filepath.Join(src, "ln")); err != nil {
		t.Fatal(err)
	}
	root, err := s.IngestDir(ctx, src)
	if err != nil {
		t.Fatal(err)
	}

	// Ls with subpath.
	ents, err := s.Ls(ctx, root, "a/b")
	if err != nil {
		t.Fatalf("Ls a/b: %v", err)
	}
	if len(ents) != 1 || ents[0].Name != "file.txt" {
		t.Fatalf("Ls a/b = %+v, want one file.txt", ents)
	}
	fe := ents[0]
	if fe.Size != uint64(len(nested)) {
		t.Errorf("file.txt Size = %d, want %d", fe.Size, len(nested))
	}
	if fe.Mode&unix.S_IFMT != unix.S_IFREG {
		t.Errorf("file.txt mode %#o is not S_IFREG", fe.Mode)
	}

	// Symlink entry: target set, no content key.
	for _, e := range mustLs(t, s, root, "") {
		if e.Name != "ln" {
			continue
		}
		if e.LinkTarget != "top.txt" {
			t.Errorf("ln target %q", e.LinkTarget)
		}
		if e.Key != (key.Key{}) || e.Size != 0 {
			t.Errorf("ln has key %s size %d, want zero", e.Key, e.Size)
		}
	}

	// TreeSubdir found / not found; files are immediate entries too.
	ak, found, err := s.TreeSubdir(ctx, root, "a")
	if err != nil || !found {
		t.Fatalf("TreeSubdir a: found=%v err=%v", found, err)
	}
	if _, err := s.Ls(ctx, ak, ""); err != nil {
		t.Errorf("Ls of subdir key: %v", err)
	}
	if _, found, err := s.TreeSubdir(ctx, root, "nope"); err != nil || found {
		t.Errorf("TreeSubdir nope: found=%v err=%v, want false, nil", found, err)
	}

	// File / ReadFile.
	rc, err := s.File(ctx, fe.Key)
	if err != nil {
		t.Fatalf("File: %v", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read File: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Errorf("close File: %v", err)
	}
	if string(got) != nested {
		t.Errorf("File content %q, want %q", got, nested)
	}
	if got := readAll(t, s, fe.Key); string(got) != nested {
		t.Errorf("ReadFile content %q, want %q", got, nested)
	}
	if _, err := s.File(ctx, root); err == nil {
		t.Error("File with a directory key succeeded, want error")
	}

	// Tar output is extractable by tarextract and matches the source.
	tr, err := s.Tar(ctx, root, "")
	if err != nil {
		t.Fatalf("Tar: %v", err)
	}
	dest := filepath.Join(t.TempDir(), "untar")
	if err := tarextract.Extract(tr, dest); err != nil {
		t.Fatalf("extract Tar stream: %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Errorf("close Tar: %v", err)
	}
	compareTrees(t, src, dest)

	// Tar with subpath.
	tr2, err := s.Tar(ctx, root, "a")
	if err != nil {
		t.Fatalf("Tar a: %v", err)
	}
	dest2 := filepath.Join(t.TempDir(), "untar-a")
	if err := tarextract.Extract(tr2, dest2); err != nil {
		t.Fatalf("extract subpath tar: %v", err)
	}
	if err := tr2.Close(); err != nil {
		t.Errorf("close Tar a: %v", err)
	}
	compareTrees(t, filepath.Join(src, "a"), dest2)

	// Bad subpath fails.
	if _, err := s.Ls(ctx, root, "no/such/dir"); err == nil {
		t.Error("Ls of missing subpath succeeded")
	}
}

// mustLs is lsNames' entry-returning sibling.
func mustLs(t *testing.T, s *amber.Store, root key.Key, path string) []amber.Entry {
	t.Helper()
	ents, err := s.Ls(t.Context(), root, path)
	if err != nil {
		t.Fatalf("Ls %s %q: %v", root, path, err)
	}
	return ents
}

// readAll is ReadFile with test plumbing.
func readAll(t *testing.T, s *amber.Store, ck key.Key) []byte {
	t.Helper()
	b, err := s.ReadFile(t.Context(), ck)
	if err != nil {
		t.Fatalf("ReadFile %s: %v", ck, err)
	}
	return b
}
