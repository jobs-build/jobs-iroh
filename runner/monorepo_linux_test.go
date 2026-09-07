//go:build linux

package runner

// Local e2e monorepo test (sibling-sources design §14): a git-less monorepo
// whose services/api build declares sources = ["//lib/common"] and reads the
// sibling through $SRC_ROOT, driven through the real local pipeline
// (BuildFromSource → driveFStages → namespace sandbox). Asserts the sibling
// read, the KP memo cutoff (mtime churn and out-of-cover changes are cache
// hits; covered changes rebuild), and the §9 sandbox contract (CWD == $SRC ==
// /build/src/<dir>, $SRC_ROOT == /build/src). The suite's TestMain calls
// sandbox.Init() first — the sandbox re-exec rule.

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/cover"
)

const monorepoAPIBuildJobs = `
def build():
    return struct(
        inputs = {},
        env = {},
        script = '''
if [ -e "$SRC_ROOT/docs" ]; then echo "uncovered docs/ leaked into the sandbox" >&2; exit 1; fi
cat "$SRC_ROOT/lib/common/common.txt" > "$out/result"
pwd > "$out/cwd"
printf '%s' "$SRC" > "$out/src"
printf '%s' "$SRC_ROOT" > "$out/src_root"
''',
        runtime_deps = [],
        sources = ["//lib/common"],
    )
`

// writeMonorepo lays out the fixture: the covered sibling (lib/common), the
// build dir (services/api), and an unrelated tree (docs).
func writeMonorepo(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for path, content := range map[string]string{
		"lib/common/go.mod":       "module example.com/common\n",
		"lib/common/common.txt":   "common-v1\n",
		"services/api/BUILD.jobs": monorepoAPIBuildJobs,
		"docs/x.txt":              "docs-v1\n",
	} {
		full := filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// monoTouchAll bumps every mtime under root (bytes unchanged) so the ingested
// tree — and with it F — re-keys without any content change.
func monoTouchAll(t *testing.T, root string) {
	t.Helper()
	future := time.Now().Add(2 * time.Hour)
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return err
		}
		return os.Chtimes(p, future, future)
	})
	if err != nil {
		t.Fatalf("chtimes walk: %v", err)
	}
}

func TestLocalBuild_MonorepoSiblingSources(t *testing.T) {
	ctx, st, platform, _ := devSetup(t)
	root := writeMonorepo(t)
	cfg := DevelopConfig{SourceDir: root, Dir: "services/api", Platform: platform, CacheDir: t.TempDir()}

	// build runs the pipeline once, returning F, the F→KP binding, and the
	// progress transcript (the cache-hit oracle: "✓ build  (cached)").
	build := func() (key.Key, key.Key, string) {
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
	const cachedLine = "✓ build  (cached)"

	// (a) The build succeeds and c/result carries the sibling's content.
	f1, kp1, out1 := build()
	if strings.Contains(out1, cachedLine) {
		t.Fatalf("first build reported cached:\n%s", out1)
	}
	if got := readOut(f1, "result"); got != "common-v1\n" {
		t.Errorf("c/result = %q, want the sibling file's content", got)
	}
	// Sandbox contract (§9): CWD and $SRC are the build dir, $SRC_ROOT the
	// context root.
	if got := strings.TrimSpace(readOut(f1, "cwd")); got != "/build/src/services/api" {
		t.Errorf("build CWD = %q, want /build/src/services/api", got)
	}
	if got := readOut(f1, "src"); got != "/build/src/services/api" {
		t.Errorf("$SRC = %q, want /build/src/services/api", got)
	}
	if got := readOut(f1, "src_root"); got != "/build/src" {
		t.Errorf("$SRC_ROOT = %q, want /build/src", got)
	}

	// (b) mtime-only churn: F re-keys (amber hashes mtimes) but the covered
	// tree normalizes, so the SAME KP memo-hits and buildrun is skipped.
	monoTouchAll(t, root)
	f2, kp2, out2 := build()
	if f2 == f1 {
		t.Fatal("mtime touch did not re-key F — fixture broken (nothing was re-derived)")
	}
	if kp2 != kp1 {
		t.Errorf("mtime-only touch moved KP: %s vs %s", kp1, kp2)
	}
	if !strings.Contains(out2, cachedLine) {
		t.Errorf("mtime-only rebuild was not a cache hit:\n%s", out2)
	}
	if got := readOut(f2, "result"); got != "common-v1\n" {
		t.Errorf("cached alias output = %q, want the original result", got)
	}

	// (c) An UNRELATED change (docs/) re-keys F again but stays outside the
	// covered closure: still the same KP, still cached.
	if err := os.WriteFile(filepath.Join(root, "docs", "x.txt"), []byte("docs-v2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f3, kp3, out3 := build()
	if f3 == f2 {
		t.Fatal("docs change did not re-key F — fixture broken")
	}
	if kp3 != kp1 {
		t.Errorf("out-of-cover change moved KP: %s vs %s", kp1, kp3)
	}
	if !strings.Contains(out3, cachedLine) {
		t.Errorf("out-of-cover rebuild was not a cache hit:\n%s", out3)
	}

	// (d) Changing the covered sibling re-keys KP and REBUILDS with the new
	// content.
	if err := os.WriteFile(filepath.Join(root, "lib", "common", "common.txt"), []byte("common-v2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f4, kp4, out4 := build()
	if kp4 == kp1 {
		t.Errorf("covered change did not re-key KP")
	}
	if strings.Contains(out4, cachedLine) {
		t.Errorf("covered change was served from the memo:\n%s", out4)
	}
	if got := readOut(f4, "result"); got != "common-v2\n" {
		t.Errorf("rebuilt c/result = %q, want the changed sibling content", got)
	}
}
