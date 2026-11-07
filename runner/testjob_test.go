package runner_test

// RunTest with a fake executor (tests design §6): the sandbox spec comes
// from build-pinned:<KT> exactly as a build's does, a passing script records
// test-passed:<KT> → KT and nothing else, a failing one records nothing and
// reports hard, and the executor's work tree is removed either way. Runs on
// every platform — the real sandbox is exercised by the Linux e2e.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/cover"
	"github.com/jobs-build/jobs-iroh/importdef"
	"github.com/jobs-build/jobs-iroh/runner"
)

// fakeExec records the spec and returns a canned exit code with a work tree
// laid out like the real executor's (<work>/root/build/out). block makes it
// a hung script: it returns only once the context ends, the way the real
// executor does after the context kills the sandbox child (exit -1, work
// tree populated). deadline records the context deadline it was given.
type fakeExec struct {
	t        *testing.T
	exit     int
	err      error
	block    bool
	onRun    func()
	spec     runner.BuildSpec
	work     string
	deadline time.Duration
}

func (f *fakeExec) RunBuild(ctx context.Context, st *amber.Store, spec runner.BuildSpec) (runner.BuildResult, error) {
	f.spec = spec
	if d, ok := ctx.Deadline(); ok {
		f.deadline = time.Until(d)
	}
	if f.onRun != nil {
		f.onRun()
	}
	if f.err != nil {
		return runner.BuildResult{ExitCode: -1}, f.err
	}
	f.work = f.t.TempDir()
	out := filepath.Join(f.work, "root", "build", "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		f.t.Fatal(err)
	}
	if f.block {
		<-ctx.Done()
		return runner.BuildResult{ExitCode: -1, StderrTail: "still running", OutDir: out}, nil
	}
	return runner.BuildResult{ExitCode: f.exit, StderrTail: "tail", OutDir: out}, nil
}

// testFixture seeds a store with everything RunTest reads: the shell, one
// import input with its output, the pinned test, and a KT tree carrying the
// build-pinned:<KT> alias. timeoutSec is the pinned limit (0 = default).
// Returns the KT and the input's output key.
func testFixture(t *testing.T, ctx context.Context, st *amber.Store, platform string, timeoutSec int64) (kt, inputOut, shell key.Key) {
	t.Helper()
	ingestDir := func(files map[string]string) key.Key {
		dir := t.TempDir()
		for p, c := range files {
			full := filepath.Join(dir, filepath.FromSlash(p))
			if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(full, []byte(c), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		k, err := st.IngestDir(ctx, dir)
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	shell = ingestDir(map[string]string{"bin/bash": "#!"})
	p, _ := importdef.CanonicalParams(map[string]any{"url": "https://e/tc.tgz"})
	canon, err := importdef.Definition{Fetcher: "tarball+https", Params: p, Platform: platform}.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	in := builddef.Input{Kind: builddef.KindImport, Definition: canon}
	ik, _ := in.Key()
	inputOut = ingestDir(map[string]string{"go/bin/go": "toolchain"})
	if err := st.PutRef(ctx, "import-output:"+ik.String(), inputOut); err != nil {
		t.Fatal(err)
	}
	pinned, err := builddef.EncodePinned(builddef.Pinned{
		Inputs:     []builddef.PinnedInput{{Name: "tc", Kind: in.Kind, Definition: in.Definition}},
		Env:        map[string]string{"X": "1"},
		Script:     "go test ./...",
		Dir:        "svc",
		TimeoutSec: timeoutSec,
	})
	if err != nil {
		t.Fatal(err)
	}
	pinnedKey, err := st.IngestFile(ctx, pinned)
	if err != nil {
		t.Fatal(err)
	}
	kt = ingestDir(map[string]string{"job.cbor": "job", "platform": platform, "v": "2", "src/svc/x_test.go": "package svc"})
	if err := st.PutRef(ctx, "build-pinned:"+kt.String(), pinnedKey); err != nil {
		t.Fatal(err)
	}
	return kt, inputOut, shell
}

func TestRunTest_PassRecordsMemoOnly(t *testing.T) {
	ctx := context.Background()
	st, err := amber.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	const platform = "linux/amd64"
	kt, inputOut, shell := testFixture(t, ctx, st, platform, 0)
	brc := runner.BuildRunCfg{Platform: platform, ShellKey: shell, CacheDir: t.TempDir()}

	ex := &fakeExec{t: t}
	out := runner.RunTest(ctx, st, runner.NewLocalRefWriter(st), brc, ex, kt)
	if out.Failed || out.Cancelled || out.Decline || out.OutputKey != kt {
		t.Fatalf("RunTest: %+v", out)
	}
	// The spec is a build spec over the KT tree: src/ at the pinned dir,
	// the input mounted by its BOK, the pinned script and env.
	if ex.spec.SourceKey != kt || ex.spec.SourceDir != "src" || ex.spec.Workdir != "svc" {
		t.Errorf("spec source = %s/%s workdir %q", ex.spec.SourceKey, ex.spec.SourceDir, ex.spec.Workdir)
	}
	if ex.spec.Script != "go test ./..." || ex.spec.Env["X"] != "1" || ex.spec.ShellBOK != shell {
		t.Errorf("spec = %+v", ex.spec)
	}
	if got := ex.spec.JobsDeps["tc"]; got != "/jobs/store/"+inputOut.String() {
		t.Errorf("JOBS_DEPS tc = %q", got)
	}
	if !strings.HasPrefix(ex.spec.Node, "test|") {
		t.Errorf("spec node = %q", ex.spec.Node)
	}
	// Exactly the memo ref, pointing at KT; the work tree is gone.
	got, ok, err := st.GetKey(ctx, cover.TestPassedRef(kt))
	if err != nil || !ok || got != kt {
		t.Fatalf("test-passed:<KT>: %s ok=%v err=%v", got, ok, err)
	}
	refs, _ := st.ListRefs(ctx)
	for _, r := range refs {
		if strings.HasPrefix(r.Name, "build-output") {
			t.Errorf("test wrote %s", r.Name)
		}
	}
	if _, err := os.Stat(ex.work); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("work tree %s not removed (err=%v)", ex.work, err)
	}
}

func TestRunTest_FailureRecordsNothing(t *testing.T) {
	ctx := context.Background()
	st, err := amber.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	const platform = "linux/amd64"
	kt, _, shell := testFixture(t, ctx, st, platform, 0)
	brc := runner.BuildRunCfg{Platform: platform, ShellKey: shell, CacheDir: t.TempDir()}

	ex := &fakeExec{t: t, exit: 2}
	out := runner.RunTest(ctx, st, runner.NewLocalRefWriter(st), brc, ex, kt)
	if !out.Failed || out.Class != "hard" || out.ExitCode != 2 || out.Phase != "testing" || out.Stderr != "tail" {
		t.Fatalf("RunTest(exit 2) = %+v, want a hard testing failure", out)
	}
	if _, ok, _ := st.GetKey(ctx, cover.TestPassedRef(kt)); ok {
		t.Fatal("failed test recorded test-passed")
	}
	if _, err := os.Stat(ex.work); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("work tree %s not removed after failure", ex.work)
	}

	// An executor error (the sandbox could not run) is retryable, not hard.
	ex = &fakeExec{t: t, err: errors.New("no user namespaces")}
	out = runner.RunTest(ctx, st, runner.NewLocalRefWriter(st), brc, ex, kt)
	if !out.Failed || out.Class != "retryable" {
		t.Fatalf("RunTest(exec error) = %+v, want retryable", out)
	}

	// A missing pinned alias is retryable too (not pinned/derived yet).
	other, _ := amber.FileKey([]byte("no such kt"))
	out = runner.RunTest(ctx, st, runner.NewLocalRefWriter(st), brc, &fakeExec{t: t}, other)
	if !out.Failed || out.Class != "retryable" || !strings.Contains(out.Stderr, "build-pinned:") {
		t.Fatalf("RunTest(unpinned) = %+v", out)
	}
}

func TestRunTest_TimeoutIsHardFailure(t *testing.T) {
	ctx := context.Background()
	st, err := amber.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	const platform = "linux/amd64"
	kt, _, shell := testFixture(t, ctx, st, platform, 1)
	brc := runner.BuildRunCfg{Platform: platform, ShellKey: shell, CacheDir: t.TempDir()}

	// A script that never exits is killed at the pinned limit: hard, the
	// message naming the limit and the last output, no ref, work tree gone.
	ex := &fakeExec{t: t, block: true}
	start := time.Now()
	out := runner.RunTest(ctx, st, runner.NewLocalRefWriter(st), brc, ex, kt)
	if !out.Failed || out.Class != "hard" || out.Phase != "testing" ||
		!strings.Contains(out.Stderr, "timed out after 1s") || !strings.Contains(out.Stderr, "still running") {
		t.Fatalf("RunTest(hung) = %+v, want a hard timeout failure", out)
	}
	if el := time.Since(start); el > 20*time.Second {
		t.Fatalf("timeout took %s to fire", el)
	}
	if _, ok, _ := st.GetKey(ctx, cover.TestPassedRef(kt)); ok {
		t.Fatal("timed-out test recorded test-passed")
	}
	if _, err := os.Stat(ex.work); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("work tree %s not removed after timeout", ex.work)
	}

	// A cancel while the script runs is a cancel, not a timeout.
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ex = &fakeExec{t: t, block: true, onRun: cancel}
	out = runner.RunTest(cctx, st, runner.NewLocalRefWriter(st), brc, ex, kt)
	if !out.Cancelled || out.Failed {
		t.Fatalf("RunTest(cancelled) = %+v, want cancelled", out)
	}

	// A test pinned without a timeout runs under the 5 minute default.
	if runner.DefaultTestTimeout != 5*time.Minute {
		t.Fatalf("DefaultTestTimeout = %s, want 5m", runner.DefaultTestTimeout)
	}
	st2, err := amber.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	kt0, _, shell0 := testFixture(t, ctx, st2, platform, 0)
	ex = &fakeExec{t: t}
	out = runner.RunTest(ctx, st2, runner.NewLocalRefWriter(st2),
		runner.BuildRunCfg{Platform: platform, ShellKey: shell0, CacheDir: t.TempDir()}, ex, kt0)
	if out.Failed || out.Cancelled || out.OutputKey != kt0 {
		t.Fatalf("RunTest(default timeout) = %+v", out)
	}
	if ex.deadline < 4*time.Minute || ex.deadline > runner.DefaultTestTimeout {
		t.Fatalf("executor deadline %s, want ≈ the %s default", ex.deadline, runner.DefaultTestTimeout)
	}
}
