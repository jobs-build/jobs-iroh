//go:build linux

package runner

// Local e2e of the TEST pipeline (tests design §6, §10): a TESTS.jobs that
// mounts the directory's own build (`build`) and asserts on its artifact,
// driven through TestFromSource against the real namespace sandbox. Checks
// the memo (second run cached), the identity-by-resolved-inputs cutoff (an
// uncovered edit re-keys F and the build input's K but neither the build's
// KP nor the test's KT ⇒ both cached), covered edits re-running the test,
// and that a failing test leaves no test-passed ref.

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/cover"
)

const localTestBuildJobs = `
def build():
    return struct(
        inputs = {},
        env = {},
        script = '''
mkdir -p "$out/bin"
cat "$SRC/app/main.txt" > "$out/bin/app"
''',
        runtime_deps = [],
        closure = ["//app"],
    )
`

const localTestTestsJobs = `
def test():
    return struct(
        inputs = {"app": build},
        env = {"EXPECT": "hello"},
        script = '''
app="$(jq -r .app <<<"$JOBS_DEPS")/bin/app"
[ -f "$app" ] || { echo "artifact not mounted" >&2; exit 1; }
grep -q "$EXPECT" "$app" || { echo "artifact mismatch: $(cat "$app")" >&2; exit 1; }
grep -q ok "$SRC/tests/case.txt" || { echo "test case failed" >&2; exit 1; }
if [ -e "$SRC_ROOT/docs" ]; then echo "uncovered docs/ leaked into the sandbox" >&2; exit 1; fi
''',
        closure = ["//tests"],
    )
`

// localTestHangingTestsJobs never finishes on its own: the pinned timeout
// has to end it (tests design §6.1).
const localTestHangingTestsJobs = `
def test():
    return struct(
        inputs = {},
        env = {},
        script = '''
echo "hanging" >&2
sleep 300
''',
        timeout = "2s",
    )
`

func writeLocalTestRepo(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for path, content := range map[string]string{
		"BUILD.jobs":     localTestBuildJobs,
		"TESTS.jobs":     localTestTestsJobs,
		"app/main.txt":   "hello\n",
		"tests/case.txt": "ok\n",
		"docs/x.txt":     "docs-v1\n",
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

func TestLocalTest_BuildInputMemoAndCutoff(t *testing.T) {
	ctx, st, platform, _ := devSetup(t)
	localTestMemoAndCutoff(t, ctx, st, platform)
}

// localTestMemoAndCutoff is the body of TestLocalTest_BuildInputMemoAndCutoff
// over an already-seeded store (shell:<platform> present).
func localTestMemoAndCutoff(t *testing.T, ctx context.Context, st *amber.Store, platform string) {
	root := writeLocalTestRepo(t)
	cfg := DevelopConfig{SourceDir: root, Platform: platform, CacheDir: t.TempDir()}

	run := func() (TestOutcome, string) {
		t.Helper()
		var buf bytes.Buffer
		out, err := TestFromSource(ctx, st, cfg, NewProgress(&buf))
		if err != nil {
			t.Fatalf("TestFromSource: %v\nprogress:\n%s", err, buf.String())
		}
		return out, buf.String()
	}
	const testCached = "✓ test  (cached)"
	const buildCached = "(cached)"

	// (a) First run: the build runs, then the test runs against its artifact.
	o1, p1 := run()
	if strings.Contains(p1, testCached) {
		t.Fatalf("first run reported the test cached:\n%s", p1)
	}
	if !strings.Contains(p1, "✓ test ") {
		t.Fatalf("first run never ran the test:\n%s", p1)
	}
	for _, name := range []string{cover.TestPassedRef(o1.KT), cover.TestPassedRef(o1.F), cover.KTTreeRef(o1.KT), "build-pinned:" + o1.KT.String()} {
		if _, ok, err := st.GetKey(ctx, name); err != nil || !ok {
			t.Errorf("ref %s missing after the test passed (ok=%v err=%v)", name, ok, err)
		}
	}
	if o1.F == o1.KT {
		t.Fatal("F equals KT")
	}

	// (b) Unchanged tree: the F alias fast-paths everything.
	o2, p2 := run()
	if o2 != o1 || !strings.Contains(p2, testCached) {
		t.Fatalf("second run: outcome %+v vs %+v, progress:\n%s", o2, o1, p2)
	}

	// (c) An uncovered edit (docs/) re-keys F — and the `build` input's K —
	// but the build memo-hits on KP and the test memo-hits on KT: nothing
	// runs, and the new F gets its alias.
	if err := os.WriteFile(filepath.Join(root, "docs", "x.txt"), []byte("docs-v2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	o3, p3 := run()
	if o3.F == o1.F {
		t.Fatal("docs edit did not re-key F — fixture broken")
	}
	if o3.KT != o1.KT {
		t.Fatalf("docs edit changed KT: %s vs %s (the memo must survive out-of-cover churn)", o3.KT, o1.KT)
	}
	if !strings.Contains(p3, testCached) {
		t.Fatalf("docs edit re-ran the test:\n%s", p3)
	}
	if !strings.Contains(p3, "build") || !strings.Contains(p3, buildCached) {
		t.Fatalf("docs edit re-ran the build input:\n%s", p3)
	}
	if kt, ok, _ := st.GetKey(ctx, cover.TestPassedRef(o3.F)); !ok || kt != o1.KT {
		t.Fatalf("test-passed alias for the new F = %s (ok=%v), want %s", kt, ok, o1.KT)
	}

	// (d) A covered test edit re-runs the test (new KT) but not the build.
	if err := os.WriteFile(filepath.Join(root, "tests", "case.txt"), []byte("ok again\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	o4, p4 := run()
	if o4.KT == o1.KT {
		t.Fatal("covered test edit did not re-key KT")
	}
	if strings.Contains(p4, testCached) {
		t.Fatalf("covered test edit did not re-run the test:\n%s", p4)
	}

	// (e) A covered BUILD edit changes the artifact ⇒ the build re-runs,
	// the resolved input changes, and the test re-runs with a new KT.
	if err := os.WriteFile(filepath.Join(root, "app", "main.txt"), []byte("hello v2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	o5, p5 := run()
	if o5.KT == o4.KT {
		t.Fatal("artifact change did not re-key KT")
	}
	if strings.Contains(p5, testCached) || strings.Contains(p5, "✓ build "+buildCached) {
		t.Fatalf("artifact change did not re-run build+test:\n%s", p5)
	}
}

func TestLocalTest_FailureLeavesNoRef(t *testing.T) {
	ctx, st, platform, _ := devSetup(t)
	localTestFailureLeavesNoRef(t, ctx, st, platform)
}

// localTestFailureLeavesNoRef is the body of TestLocalTest_FailureLeavesNoRef
// over an already-seeded store.
func localTestFailureLeavesNoRef(t *testing.T, ctx context.Context, st *amber.Store, platform string) {
	root := writeLocalTestRepo(t)
	// "FAIL" — nothing that contains "ok" (the script greps for it).
	if err := os.WriteFile(filepath.Join(root, "tests", "case.txt"), []byte("FAIL\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := DevelopConfig{SourceDir: root, Platform: platform, CacheDir: t.TempDir()}
	var buf bytes.Buffer
	_, err := TestFromSource(ctx, st, cfg, NewProgress(&buf))
	if err == nil || !strings.Contains(err.Error(), "test case failed") {
		t.Fatalf("err = %v, want the failing test's stderr\nprogress:\n%s", err, buf.String())
	}
	refs, err := st.ListRefs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range refs {
		if strings.HasPrefix(r.Name, "test-passed:") {
			t.Errorf("failed test left %s", r.Name)
		}
	}
	// Fixing the case re-runs and passes.
	if err := os.WriteFile(filepath.Join(root, "tests", "case.txt"), []byte("ok\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := TestFromSource(ctx, st, cfg, NewProgress(&buf))
	if err != nil {
		t.Fatalf("TestFromSource after fix: %v\n%s", err, buf.String())
	}
	if out.KT == (key.Key{}) {
		t.Fatal("no KT after a passing run")
	}
}

func TestLocalTest_TimeoutKillsSandbox(t *testing.T) {
	ctx, st, platform, _ := devSetup(t)
	localTestTimeout(t, ctx, st, platform)
}

// localTestTimeout is the body of TestLocalTest_TimeoutKillsSandbox over an
// already-seeded store.
func localTestTimeout(t *testing.T, ctx context.Context, st *amber.Store, platform string) {
	root := writeLocalTestRepo(t)
	if err := os.WriteFile(filepath.Join(root, "TESTS.jobs"), []byte(localTestHangingTestsJobs), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := DevelopConfig{SourceDir: root, Platform: platform, CacheDir: t.TempDir()}
	var buf bytes.Buffer
	start := time.Now()
	_, err := TestFromSource(ctx, st, cfg, NewProgress(&buf))
	elapsed := time.Since(start)
	if err == nil || !strings.Contains(err.Error(), "timed out after 2s") {
		t.Fatalf("err = %v, want the timeout\nprogress:\n%s", err, buf.String())
	}
	// The sandbox child is PID 1 of its pid namespace: killing it takes the
	// sleeping script down with it, so the limit ends the run promptly
	// instead of waiting out `sleep 300`.
	if elapsed > 60*time.Second {
		t.Fatalf("timeout took %s to end the run", elapsed)
	}
	refs, err := st.ListRefs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range refs {
		if strings.HasPrefix(r.Name, "test-passed:") {
			t.Errorf("timed-out test left %s", r.Name)
		}
	}
}
