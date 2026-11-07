package clientcli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// mktree materialises a tree under a fresh temp dir: every entry is a path
// relative to the root, directories end in "/", everything else becomes a
// file with trivial content. Returns the (symlink-resolved) root — macOS
// /var → /private/var would otherwise make every path comparison fail.
func mktree(t *testing.T, entries ...string) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		p := filepath.Join(root, filepath.FromSlash(strings.TrimSuffix(e, "/")))
		if strings.HasSuffix(e, "/") {
			if err := os.MkdirAll(p, 0o755); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("# recipe\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// --- repoRoot ---

// repoRoot must find a .git DIRECTORY (ordinary clone) and a .git FILE (git
// worktree, submodule) alike, and report "" outside any repository.
func TestRepoRoot(t *testing.T) {
	t.Run("git directory", func(t *testing.T) {
		root := mktree(t, ".git/", "a/b/c/")
		if got := repoRoot(filepath.Join(root, "a/b/c")); got != root {
			t.Errorf("repoRoot = %q, want %q", got, root)
		}
	})
	t.Run("git file (worktree)", func(t *testing.T) {
		root := mktree(t, "wt/.git", "wt/a/b/")
		want := filepath.Join(root, "wt")
		if got := repoRoot(filepath.Join(root, "wt/a/b")); got != want {
			t.Errorf("repoRoot = %q, want %q", got, want)
		}
	})
	t.Run("nearest wins (submodule)", func(t *testing.T) {
		root := mktree(t, ".git/", "sub/.git", "sub/pkg/")
		want := filepath.Join(root, "sub")
		if got := repoRoot(filepath.Join(root, "sub/pkg")); got != want {
			t.Errorf("repoRoot = %q, want %q", got, want)
		}
	})
	t.Run("no repo", func(t *testing.T) {
		// A temp dir has no .git anywhere above it inside the test sandbox;
		// guard against a stray repo above /tmp by asserting only that the
		// answer is not the tree we built.
		root := mktree(t, "a/")
		if got := repoRoot(filepath.Join(root, "a")); got == root || got == filepath.Join(root, "a") {
			t.Errorf("repoRoot = %q, want a dir outside the temp tree (or \"\")", got)
		}
	})
}

// --- defaultSource ---

// The upward search: cwd first, then ancestors, ceiling at the repo root
// (inclusive) — and never past it.
func TestDefaultSourceWalksUpToRepoRoot(t *testing.T) {
	tests := []struct {
		name    string
		entries []string
		cwd     string
		want    string // relative to the tree root
	}{
		{
			name:    "recipe in cwd",
			entries: []string{".git/", "services/api/BUILD.jobs"},
			cwd:     "services/api",
			want:    "services/api",
		},
		{
			name:    "recipe in an ancestor",
			entries: []string{".git/", "services/api/BUILD.jobs", "services/api/internal/db/"},
			cwd:     "services/api/internal/db",
			want:    "services/api",
		},
		{
			name:    "repo root holds the recipe",
			entries: []string{".git/", "BUILD.jobs", "a/b/c/"},
			cwd:     "a/b/c",
			want:    "",
		},
		{
			name:    "nearest recipe wins",
			entries: []string{".git/", "BUILD.jobs", "a/BUILD.jobs", "a/b/"},
			cwd:     "a/b",
			want:    "a",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := mktree(t, tc.entries...)
			got, err := defaultSource(filepath.Join(root, tc.cwd), "", "", "", false)
			if err != nil {
				t.Fatalf("defaultSource: %v", err)
			}
			if want := filepath.Join(root, tc.want); got != want {
				t.Errorf("defaultSource = %q, want %q", got, want)
			}
		})
	}
}

// The ceiling is hard: a recipe sitting ABOVE the repo root is invisible.
func TestDefaultSourceStopsAtRepoRoot(t *testing.T) {
	root := mktree(t, "BUILD.jobs", "repo/.git/", "repo/a/")
	_, err := defaultSource(filepath.Join(root, "repo/a"), "", "", "", false)
	if err == nil {
		t.Fatal("want error: the recipe above the repo root must not be found")
	}
	for _, want := range []string{"BUILD.jobs", filepath.Join(root, "repo/a"), filepath.Join(root, "repo"), "--source"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q must mention %q", err, want)
		}
	}
}

// Outside a repository the ceiling is the cwd itself: no walk at all.
func TestDefaultSourceOutsideRepo(t *testing.T) {
	t.Run("recipe in cwd", func(t *testing.T) {
		root := mktree(t, "a/BUILD.jobs")
		got, err := defaultSource(filepath.Join(root, "a"), "", "", "", false)
		if err != nil {
			t.Fatalf("defaultSource: %v", err)
		}
		if want := filepath.Join(root, "a"); got != want {
			t.Errorf("defaultSource = %q, want %q", got, want)
		}
	})
	t.Run("recipe only in an ancestor", func(t *testing.T) {
		root := mktree(t, "BUILD.jobs", "a/")
		_, err := defaultSource(filepath.Join(root, "a"), "", "", "", false)
		if err == nil {
			t.Fatal("want error: outside a repo the search must not leave the cwd")
		}
		if !strings.Contains(err.Error(), filepath.Join(root, "a")) {
			t.Errorf("error %q must name the cwd", err)
		}
	})
}

// An explicit --dir means the user already named the build root, so no
// search happens — even when the cwd holds no recipe at all.
func TestDefaultSourceDirSuppressesSearch(t *testing.T) {
	root := mktree(t, ".git/", "BUILD.jobs", "a/b/")
	cwd := filepath.Join(root, "a/b")
	got, err := defaultSource(cwd, "api", "", "", false)
	if err != nil {
		t.Fatalf("defaultSource: %v", err)
	}
	if got != cwd {
		t.Errorf("defaultSource = %q, want the cwd %q", got, cwd)
	}
}

// --source-root replaces the repo root as the ceiling, whether it sits above
// or below the repo root.
func TestDefaultSourceExplicitCeiling(t *testing.T) {
	t.Run("below the repo root", func(t *testing.T) {
		root := mktree(t, ".git/", "BUILD.jobs", "a/b/")
		_, err := defaultSource(filepath.Join(root, "a/b"), "", filepath.Join(root, "a"), "", false)
		if err == nil {
			t.Fatal("want error: the ceiling is a/, so the root recipe is invisible")
		}
	})
	t.Run("above the repo root", func(t *testing.T) {
		root := mktree(t, "BUILD.jobs", "repo/.git/", "repo/a/")
		got, err := defaultSource(filepath.Join(root, "repo/a"), "", root, "", false)
		if err != nil {
			t.Fatalf("defaultSource: %v", err)
		}
		if got != root {
			t.Errorf("defaultSource = %q, want %q", got, root)
		}
	})
	t.Run("cwd outside the ceiling", func(t *testing.T) {
		root := mktree(t, "x/BUILD.jobs", "y/")
		_, err := defaultSource(filepath.Join(root, "y"), "", filepath.Join(root, "x"), "", false)
		if err == nil || !strings.Contains(err.Error(), "not under the context root") {
			t.Fatalf("want a not-under-the-context-root error, got %v", err)
		}
	})
}

// --no-repo-root pins the ceiling to the cwd: the pre-arc behavior.
func TestDefaultSourceNoRepoRoot(t *testing.T) {
	root := mktree(t, ".git/", "BUILD.jobs", "a/BUILD.jobs", "a/b/")
	if _, err := defaultSource(filepath.Join(root, "a/b"), "", "", "", true); err == nil {
		t.Fatal("want error: --no-repo-root must not search a/")
	}
	got, err := defaultSource(filepath.Join(root, "a"), "", "", "", true)
	if err != nil {
		t.Fatalf("defaultSource: %v", err)
	}
	if want := filepath.Join(root, "a"); got != want {
		t.Errorf("defaultSource = %q, want %q", got, want)
	}
}

// --build-file replaces the probed name, including when it carries slashes
// (it is documented as a path relative to the build root).
func TestDefaultSourceCustomBuildFile(t *testing.T) {
	t.Run("plain name", func(t *testing.T) {
		root := mktree(t, ".git/", "a/app.jobs", "a/b/")
		got, err := defaultSource(filepath.Join(root, "a/b"), "", "", "app.jobs", false)
		if err != nil {
			t.Fatalf("defaultSource: %v", err)
		}
		if want := filepath.Join(root, "a"); got != want {
			t.Errorf("defaultSource = %q, want %q", got, want)
		}
	})
	t.Run("nested path", func(t *testing.T) {
		root := mktree(t, ".git/", "a/recipes/app.jobs", "a/b/")
		got, err := defaultSource(filepath.Join(root, "a/b"), "", "", "recipes/app.jobs", false)
		if err != nil {
			t.Fatalf("defaultSource: %v", err)
		}
		if want := filepath.Join(root, "a"); got != want {
			t.Errorf("defaultSource = %q, want %q", got, want)
		}
	})
	t.Run("BUILD.jobs must not satisfy it", func(t *testing.T) {
		root := mktree(t, ".git/", "a/BUILD.jobs", "a/b/")
		_, err := defaultSource(filepath.Join(root, "a/b"), "", "", "app.jobs", false)
		if err == nil || !strings.Contains(err.Error(), "app.jobs") {
			t.Fatalf("want an app.jobs not-found error, got %v", err)
		}
	})
}

// --- resolveContextRoot ---

// Coverage the function never had while it shelled out to git: the re-anchor
// itself, and the escape check.
func TestResolveContextRoot(t *testing.T) {
	root := mktree(t, ".git/", "services/api/", "outside/")

	t.Run("re-anchors to the repo root", func(t *testing.T) {
		gotRoot, gotDir, err := resolveContextRoot(filepath.Join(root, "services/api"), "", "", false)
		if err != nil {
			t.Fatal(err)
		}
		if gotRoot != root || gotDir != "services/api" {
			t.Errorf("got (%q, %q), want (%q, %q)", gotRoot, gotDir, root, "services/api")
		}
	})
	t.Run("composes an explicit dir", func(t *testing.T) {
		_, gotDir, err := resolveContextRoot(filepath.Join(root, "services"), "api", "", false)
		if err != nil {
			t.Fatal(err)
		}
		if gotDir != "services/api" {
			t.Errorf("dir = %q, want services/api", gotDir)
		}
	})
	t.Run("repo root itself yields an empty dir", func(t *testing.T) {
		gotRoot, gotDir, err := resolveContextRoot(root, "", "", false)
		if err != nil {
			t.Fatal(err)
		}
		if gotRoot != root || gotDir != "" {
			t.Errorf("got (%q, %q), want (%q, \"\")", gotRoot, gotDir, root)
		}
	})
	t.Run("no-repo-root ingests the source itself", func(t *testing.T) {
		src := filepath.Join(root, "services/api")
		gotRoot, gotDir, err := resolveContextRoot(src, "", "", true)
		if err != nil {
			t.Fatal(err)
		}
		if gotRoot != src || gotDir != "" {
			t.Errorf("got (%q, %q), want (%q, \"\")", gotRoot, gotDir, src)
		}
	})
	t.Run("source escaping the context root fails", func(t *testing.T) {
		_, _, err := resolveContextRoot(filepath.Join(root, "outside"), "", filepath.Join(root, "services"), false)
		if err == nil || !strings.Contains(err.Error(), "not under the context root") {
			t.Fatalf("want a not-under-the-context-root error, got %v", err)
		}
	})
}

// --- contextLine ---

func TestContextLine(t *testing.T) {
	tests := []struct {
		root, dir, buildFile string
		want                 string
	}{
		{"/home/x/repo", "services/api", "", "context: /home/x/repo  (dir services/api, recipe BUILD.jobs)"},
		{"/home/x/repo", "", "", "context: /home/x/repo  (dir ., recipe BUILD.jobs)"},
		{"/home/x/repo", "a", "app.jobs", "context: /home/x/repo  (dir a, recipe app.jobs)"},
	}
	for _, tc := range tests {
		if got := contextLine(tc.root, tc.dir, tc.buildFile); got != tc.want {
			t.Errorf("contextLine(%q,%q,%q) = %q, want %q", tc.root, tc.dir, tc.buildFile, got, tc.want)
		}
	}
}

func TestDefaultSourceTestsRecipe(t *testing.T) {
	// A test command searches for TESTS.jobs: a directory holding only a
	// BUILD.jobs is passed over for the nearest ancestor with TESTS.jobs.
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	svc := filepath.Join(root, "svc")
	if err := os.MkdirAll(filepath.Join(svc, "pkg"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{filepath.Join(root, "TESTS.jobs"), filepath.Join(svc, "BUILD.jobs")} {
		if err := os.WriteFile(p, []byte("#"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	got, err := defaultSource(filepath.Join(svc, "pkg"), "", "", "TESTS.jobs", false)
	if err != nil {
		t.Fatal(err)
	}
	if got != root {
		t.Fatalf("defaultSource(TESTS.jobs) = %s, want the repo root %s (svc has only BUILD.jobs)", got, root)
	}
	if _, err := defaultSource(filepath.Join(svc, "pkg"), "", "", "BUILD.jobs", false); err != nil {
		t.Fatalf("BUILD.jobs search: %v", err)
	}
	if got := contextLine(root, "", "TESTS.jobs"); got != "context: "+root+"  (dir ., recipe TESTS.jobs)" {
		t.Fatalf("contextLine = %q", got)
	}
}
