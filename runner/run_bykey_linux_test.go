//go:build linux

package runner_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/runner"
	"github.com/jobs-build/jobs-iroh/sandbox"
)

// TestRunByKey_ShellShebangEntrypoint executes a hand-built build output by
// key. The entrypoint is a script whose fixed #!/jobs/shell/bin/bash shebang
// only resolves through the /jobs/shell → /jobs/store/<shellBOK> compat
// symlink in the run sandbox's MATERIALIZED store — locking both invariants
// (run_linux.go: materialize-only run store, /jobs/shell symlink) plus the
// direct build-output:K fallback resolution and arg pass-through. Port of the
// load-bearing half of jobs' run-binary e2e (internal/jobscli).
func TestRunByKey_ShellShebangEntrypoint(t *testing.T) {
	if !sandbox.UserNSAvailable() {
		t.Skip("user namespaces unavailable")
	}

	ctx := context.Background()
	st := buildEvalStore(t)
	platform := runner.Platform()
	shellKey := buildShellArtifact(t, ctx, st)
	if err := st.PutRef(ctx, "shell:"+platform, shellKey); err != nil {
		t.Fatal(err)
	}

	// Hand-build a build output { c/{bin/app, JOBS.entrypoint} } and its empty
	// runtime closure under a fabricated definition key (local builds resolve
	// via the direct build-output:K fallback — no build-from bridge).
	bk, err := key.New(key.DirNode, 6, []byte("runbin"))
	if err != nil {
		t.Fatal(err)
	}
	T := t.TempDir()
	cBin := filepath.Join(T, "c", "bin")
	if err := os.MkdirAll(cBin, 0o755); err != nil {
		t.Fatal(err)
	}
	script := "#!/jobs/shell/bin/bash\nprintf 'RAN-SCRIPT:%s' \"$*\"\n"
	if err := os.WriteFile(filepath.Join(cBin, "app"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	ep := `{"command":"bin/app","args":[],"env":{}}`
	if err := os.WriteFile(filepath.Join(T, "c", "JOBS.entrypoint"), []byte(ep), 0o644); err != nil {
		t.Fatal(err)
	}
	outKey, err := st.IngestDir(ctx, T)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutRef(ctx, "build-output:"+bk.String(), outKey); err != nil {
		t.Fatal(err)
	}
	emptyClosure, err := st.BuildStoreTree(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutRef(ctx, "build-output-deps:"+bk.String(), emptyClosure); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	code, err := runner.RunByKey(ctx, st, bk, platform, "", []string{"alpha", "beta"},
		runner.RunIO{Stdout: &stdout, Stderr: &stderr})
	if err != nil {
		t.Fatalf("RunByKey: %v (stderr: %s)", err, stderr.String())
	}
	if code != 0 {
		t.Fatalf("exit code = %d, want 0 (stderr: %s)", code, stderr.String())
	}
	if got := stdout.String(); got != "RAN-SCRIPT:alpha beta" {
		t.Fatalf("stdout = %q, want %q", got, "RAN-SCRIPT:alpha beta")
	}
}
