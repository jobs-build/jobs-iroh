//go:build linux

package runner

// Local e2e closure tests (source-closure design §11): closure= is a
// COMPLETE cover — the build dir is NOT auto-seeded, so an uncovered file
// INSIDE the build dir stays outside KP (the consumer-dir narrowing win),
// and root builds (dir == "") get covers at all. Driven through the real
// local pipeline like monorepo_linux_test.go.

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/cover"
)

const closureAPIBuildJobs = `
def build():
    return struct(
        inputs = {},
        env = {},
        script = '''
if [ -e "$SRC_ROOT/docs" ]; then echo "uncovered docs/ leaked" >&2; exit 1; fi
if [ -e "$SRC_ROOT/services/api/notes.md" ]; then echo "uncovered notes.md leaked" >&2; exit 1; fi
cat "$SRC_ROOT/lib/common/common.txt" > "$out/result"
pwd > "$out/cwd"
''',
        runtime_deps = [],
        closure = ["//lib/common", "//services/api/go.mod", "//services/api/BUILD.jobs"],
    )
`

const closureRootBuildJobs = `
def build():
    return struct(
        inputs = {},
        env = {},
        script = '''
if [ -e "$SRC/cmd/bar" ]; then echo "uncovered cmd/bar leaked" >&2; exit 1; fi
cat "$SRC/cmd/foo/foo.txt" > "$out/result"
''',
        runtime_deps = [],
        closure = ["//cmd/foo", "//go.mod", "//BUILD.jobs"],
    )
`

func writeClosureTree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for p, content := range files {
		full := filepath.Join(root, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// closureBuild runs the pipeline once for cfg, returning F, KP, transcript.
func closureBuild(t *testing.T, ctx context.Context, st *amber.Store, cfg DevelopConfig) (key.Key, key.Key, string) {
	t.Helper()
	var buf bytes.Buffer
	f, err := BuildFromSource(ctx, st, cfg, NewProgress(&buf))
	if err != nil {
		t.Fatalf("BuildFromSource: %v\nprogress:\n%s", err, buf.String())
	}
	kp, ok, err := st.GetKey(ctx, cover.PinCoverRef(f))
	if err != nil || !ok {
		t.Fatalf("pin-cover ref for %s: ok=%v err=%v", f, ok, err)
	}
	return f, kp, buf.String()
}

const closureCachedLine = "✓ build  (cached)"

func TestLocalBuild_ClosureNarrowsConsumerDir(t *testing.T) {
	ctx, st, platform, _ := devSetup(t)
	root := writeClosureTree(t, map[string]string{
		"lib/common/common.txt":   "common-v1\n",
		"services/api/BUILD.jobs": closureAPIBuildJobs,
		"services/api/go.mod":     "module example.com/api\n",
		"services/api/notes.md":   "notes-v1\n",
		"docs/x.txt":              "docs-v1\n",
	})
	cfg := DevelopConfig{SourceDir: root, Dir: "services/api", Platform: platform, CacheDir: t.TempDir()}

	readOut := func(f key.Key, name string) string {
		t.Helper()
		outKey, ok, err := st.GetKey(ctx, "build-output:"+f.String())
		if err != nil || !ok {
			t.Fatalf("build-output:%s: ok=%v err=%v", f, ok, err)
		}
		b, err := readTreeFile(ctx, st, outKey, "c/"+name)
		if err != nil {
			t.Fatalf("read c/%s: %v", name, err)
		}
		return string(b)
	}

	// (a) Builds; the sandbox contains exactly the closure (script asserts),
	// and the workdir materializes from the covered manifest+recipe files.
	f1, kp1, out1 := closureBuild(t, ctx, st, cfg)
	if strings.Contains(out1, closureCachedLine) {
		t.Fatalf("first build reported cached:\n%s", out1)
	}
	if got := readOut(f1, "result"); got != "common-v1\n" {
		t.Errorf("c/result = %q", got)
	}
	if got := strings.TrimSpace(readOut(f1, "cwd")); got != "/build/src/services/api" {
		t.Errorf("build CWD = %q, want /build/src/services/api", got)
	}

	// (b) Editing an uncovered file INSIDE the build dir re-keys F but not
	// KP: buildrun memo-hits — the consumer-dir narrowing win.
	if err := os.WriteFile(filepath.Join(root, "services", "api", "notes.md"), []byte("notes-v2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f2, kp2, out2 := closureBuild(t, ctx, st, cfg)
	if f2 == f1 {
		t.Fatal("notes.md change did not re-key F — fixture broken")
	}
	if kp2 != kp1 {
		t.Errorf("in-dir out-of-closure change moved KP: %s vs %s", kp1, kp2)
	}
	if !strings.Contains(out2, closureCachedLine) {
		t.Errorf("in-dir out-of-closure rebuild was not a cache hit:\n%s", out2)
	}

	// (c) mtime-only churn: same KP, cached (mtime immunity holds for
	// closure builds).
	monoTouchAll(t, root)
	_, kp3, out3 := closureBuild(t, ctx, st, cfg)
	if kp3 != kp1 {
		t.Errorf("mtime-only touch moved KP: %s vs %s", kp1, kp3)
	}
	if !strings.Contains(out3, closureCachedLine) {
		t.Errorf("mtime-only rebuild was not a cache hit:\n%s", out3)
	}

	// (d) Covered change rebuilds with new content.
	if err := os.WriteFile(filepath.Join(root, "lib", "common", "common.txt"), []byte("common-v2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f4, kp4, out4 := closureBuild(t, ctx, st, cfg)
	if kp4 == kp1 {
		t.Error("covered change did not re-key KP")
	}
	if strings.Contains(out4, closureCachedLine) {
		t.Errorf("covered change was served from the memo:\n%s", out4)
	}
	if got := readOut(f4, "result"); got != "common-v2\n" {
		t.Errorf("rebuilt c/result = %q", got)
	}
}

func TestLocalBuild_ClosureRootBuild(t *testing.T) {
	ctx, st, platform, _ := devSetup(t)
	root := writeClosureTree(t, map[string]string{
		"BUILD.jobs":      closureRootBuildJobs,
		"go.mod":          "module example.com/m\n",
		"cmd/foo/foo.txt": "foo-v1\n",
		"cmd/bar/bar.txt": "bar-v1\n",
	})
	cfg := DevelopConfig{SourceDir: root, Dir: "", Platform: platform, CacheDir: t.TempDir()}

	// (a) Root build with a closure works; cmd/bar is outside the sandbox.
	f1, kp1, out1 := closureBuild(t, ctx, st, cfg)
	if strings.Contains(out1, closureCachedLine) {
		t.Fatalf("first build reported cached:\n%s", out1)
	}

	// (b) Editing cmd/bar (outside the closure) memo-hits.
	if err := os.WriteFile(filepath.Join(root, "cmd", "bar", "bar.txt"), []byte("bar-v2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f2, kp2, out2 := closureBuild(t, ctx, st, cfg)
	if f2 == f1 {
		t.Fatal("cmd/bar change did not re-key F — fixture broken")
	}
	if kp2 != kp1 {
		t.Errorf("out-of-closure change moved KP: %s vs %s", kp1, kp2)
	}
	if !strings.Contains(out2, closureCachedLine) {
		t.Errorf("out-of-closure rebuild was not a cache hit:\n%s", out2)
	}

	// (c) Editing cmd/foo (covered) rebuilds.
	if err := os.WriteFile(filepath.Join(root, "cmd", "foo", "foo.txt"), []byte("foo-v2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, kp3, out3 := closureBuild(t, ctx, st, cfg)
	if kp3 == kp1 {
		t.Error("covered change did not re-key KP")
	}
	if strings.Contains(out3, closureCachedLine) {
		t.Errorf("covered change was served from the memo:\n%s", out3)
	}
}
