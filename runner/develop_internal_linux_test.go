//go:build linux

package runner

// Internal tests of develop_linux.go — the driver half, the devSetup harness
// the local-build tests share, and the ported PTY-shell tests (jobs'
// prepareDevelop/printScript/developRun/RunDevelop suite, signer plumbing
// dropped). The suite's TestMain (buildexec_suite_test.go) calls
// sandbox.Init() first — the sandbox re-exec rule.

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/amber-store/core/key"
	"github.com/creack/pty"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/importdef"
	"github.com/jobs-build/jobs-iroh/sandbox"
)

// TestImportFetchLabel verifies the fetch progress line shows the fetcher name +
// its arguments (params sorted by key), not the opaque content key.
func TestImportFetchLabel(t *testing.T) {
	params, err := importdef.CanonicalParams(map[string]any{"owner": "o", "repo": "r", "ref": "v1"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := importFetchLabel(importdef.Definition{Fetcher: "github", Params: params}), "fetch github owner=o ref=v1 repo=r"; got != want {
		t.Fatalf("importFetchLabel = %q, want %q", got, want)
	}
	empty, err := importdef.CanonicalParams(map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := importFetchLabel(importdef.Definition{Fetcher: "depfetch", Params: empty}), "fetch depfetch"; got != want {
		t.Fatalf("empty-params label = %q, want %q", got, want)
	}
}

const devBuildJobs = `
def plugins():
    return {"mod": imp(fetcher = "modplugin", params = {})}

def build():
    _ = plugins["mod"](manifest = source.read("greeting.txt"))
    dep = imp(fetcher = "depfetch", params = {})
    return struct(
        inputs = {"dep": dep},
        env = {},
        script = 'cat "$SRC/greeting.txt" > "$out/result"',
        runtime_deps = [],
    )
`

const devPluginFetcher = "#!/bin/sh\n" +
	"set -e\n" +
	"cat > \"$JOBS_OUTPUT_DIR/plugin\" <<'EOF'\n" +
	"#!/jobs/shell/bin/bash\n" +
	"cat > /dev/null\n" +
	"printf '\\200'\n" +
	"EOF\n" +
	"chmod +x \"$JOBS_OUTPUT_DIR/plugin\"\n"

const devDepFetcher = "#!/bin/sh\nset -e\nprintf depdata > \"$JOBS_OUTPUT_DIR/data\"\n"

// devSetup brings up a store + the shell artifact + the two fetchers, and
// returns a context, the store, the platform, and the source dir.
func devSetup(t *testing.T) (context.Context, *amber.Store, string, string) {
	t.Helper()
	if !sandbox.UserNSAvailable() {
		t.Skip("user namespaces unavailable")
	}
	ctx := context.Background()
	st := openTestStore(t)
	platform := Platform()

	// shell artifact
	shellOut := t.TempDir()
	fetch, err := filepath.Abs("../fetchers/hostshell/fetch")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(fetch)
	cmd.Env = append(os.Environ(), "JOBS_OUTPUT_DIR="+shellOut)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("hostshell fetcher: %v\n%s", err, out)
	}
	sk, err := st.IngestDir(ctx, shellOut)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutRef(ctx, "shell:"+platform, sk); err != nil {
		t.Fatal(err)
	}

	// fetchers
	devRegisterFetcher(t, ctx, st, "modplugin", platform, devPluginFetcher)
	devRegisterFetcher(t, ctx, st, "depfetch", platform, devDepFetcher)

	// source dir
	srcDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcDir, "greeting.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "BUILD.jobs"), []byte(devBuildJobs), 0o644); err != nil {
		t.Fatal(err)
	}
	return ctx, st, platform, srcDir
}

func devRegisterFetcher(t *testing.T, ctx context.Context, st *amber.Store, name, platform, script string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "fetch"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	k, err := st.IngestDir(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutRef(ctx, "fetcher:"+name+":"+platform, k); err != nil {
		t.Fatal(err)
	}
}

func TestPrepareDevelopBuildsDependencies(t *testing.T) {
	ctx, st, platform, srcDir := devSetup(t)
	params, err := importdef.CanonicalParams(map[string]any{})
	if err != nil {
		t.Fatal(err)
	}

	var progressBuf bytes.Buffer
	p := NewProgress(&progressBuf)
	spec, script, err := prepareDevelop(ctx, st, DevelopConfig{
		SourceDir: srcDir, Platform: platform, Params: params, CacheDir: t.TempDir(),
	}, p)
	if err != nil {
		t.Fatalf("prepareDevelop: %v", err)
	}
	if script == "" {
		t.Error("expected a non-empty pinned script")
	}
	if len(spec.JobsDeps) != 1 {
		t.Fatalf("want 1 resolved dep in JOBS_DEPS, got %d", len(spec.JobsDeps))
	}
	// A non-zero store key proves the dep was built and assembled into the
	// /jobs/store union; each dep path is content-addressed under /jobs/store.
	if spec.StoreKey == (key.Key{}) {
		t.Error("store tree was not assembled (zero key)")
	}
	for name, dp := range spec.JobsDeps {
		if !strings.HasPrefix(dp, "/jobs/store/") {
			t.Errorf("dep %s path %q not under /jobs/store", name, dp)
		}
	}
	if spec.SourceDir != "src" {
		t.Errorf("SourceDir should be 'src' (the KP tree's covered source — sibling-sources §9), got %q", spec.SourceDir)
	}
	// Progress output must contain the build-from and pin steps.
	progressOut := progressBuf.String()
	for _, want := range []string{"→ build-from", "✓ build-from", "→ pin", "fetch depfetch"} {
		if !strings.Contains(progressOut, want) {
			t.Errorf("progress output missing %q:\n%s", want, progressOut)
		}
	}
}

func TestPrintScript(t *testing.T) {
	var buf bytes.Buffer
	spec := BuildSpec{
		JobsDeps: map[string]string{"lib": "/jobs/store/import-abc"},
		Env:      map[string]string{"FOO": "bar"},
		Script:   "echo building",
	}
	printScript(&buf, spec, spec.Script)
	out := buf.String()
	for _, want := range []string{"echo building", "/build/src", "lib", "/jobs/store/import-abc", "FOO=bar"} {
		if !strings.Contains(out, want) {
			t.Errorf("printScript output missing %q:\n%s", want, out)
		}
	}
}

func TestDevelopRunAssemblesSandbox(t *testing.T) {
	ctx, st, platform, srcDir := devSetup(t)
	params, err := importdef.CanonicalParams(map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	spec, _, err := prepareDevelop(ctx, st, DevelopConfig{
		SourceDir: srcDir, Platform: platform, Params: params, CacheDir: t.TempDir(),
	}, NewProgress(io.Discard))
	if err != nil {
		t.Fatalf("prepareDevelop: %v", err)
	}

	var buf bytes.Buffer
	// Pick the single dep's content-addressed store path to list.
	var depPath string
	for _, p := range spec.JobsDeps {
		depPath = p
	}
	// Print $SRC/$out, the source greeting, and a listing of the dep mount. The
	// shell itself is resolved from the store via its BOK.
	cmd := []string{storeShellDir(spec.ShellBOK) + "/bin/bash", "-e", "-c",
		`printf 'SRC=%s OUT=%s\n' "$SRC" "$out"; cat "$SRC/greeting.txt"; echo; ls ` + depPath}
	code, err := developRun(ctx, st, spec, cmd, &buf, &buf, nil)
	if err != nil {
		t.Fatalf("developRun: %v\n%s", err, buf.String())
	}
	if code != 0 {
		t.Fatalf("command exit %d:\n%s", code, buf.String())
	}
	out := buf.String()
	for _, want := range []string{"SRC=/build/src", "OUT=/build/out", "hello", "data"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

// TestRunDevelopInteractivePTY drives the interactive shell over a PTY: it feeds
// a command + exit and asserts the command ran in the sandbox.
func TestRunDevelopInteractivePTY(t *testing.T) {
	ctx, st, platform, srcDir := devSetup(t)
	params, err := importdef.CanonicalParams(map[string]any{})
	if err != nil {
		t.Fatal(err)
	}

	// Redirect os.Stdin/os.Stdout to a PTY we control, so RunDevelop's raw-mode
	// + stdio forwarding has a real terminal to talk to.
	ptmx, tty, err := pty.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer ptmx.Close()
	oldIn, oldOut := os.Stdin, os.Stdout
	os.Stdin, os.Stdout = tty, tty
	t.Cleanup(func() { os.Stdin, os.Stdout = oldIn, oldOut; tty.Close() })

	got := make(chan string, 1)
	go func() {
		buf := make([]byte, 4096)
		var acc []byte
		for {
			n, err := ptmx.Read(buf)
			if n > 0 {
				acc = append(acc, buf[:n]...)
				if strings.Contains(string(acc), "READY-7531") {
					got <- string(acc)
					return
				}
			}
			if err != nil {
				got <- string(acc)
				return
			}
		}
	}()
	// Feed a command then exit.
	go func() {
		time.Sleep(2 * time.Second)
		_, _ = io.WriteString(ptmx, "echo READY-7531\n")
		time.Sleep(500 * time.Millisecond)
		_, _ = io.WriteString(ptmx, "exit\n")
	}()

	done := make(chan error, 1)
	go func() {
		done <- RunDevelop(ctx, st, DevelopConfig{
			SourceDir: srcDir, Platform: platform, Params: params, CacheDir: t.TempDir(),
		})
	}()

	// The reader goroutine sends as soon as it sees the marker (typically before
	// RunDevelop returns); block on it with its own timeout to avoid a race.
	select {
	case out := <-got:
		if !strings.Contains(out, "READY-7531") {
			t.Errorf("interactive command output not seen; got:\n%s", out)
		}
	case <-time.After(120 * time.Second):
		t.Fatal("timeout waiting for interactive command output")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunDevelop: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("RunDevelop did not exit after the shell received `exit`")
	}
}
