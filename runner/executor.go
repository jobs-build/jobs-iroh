// Package runner holds the JOBS stage drivers and their execution machinery:
// it resolves and runs fetchers for imports, assembles and runs the hermetic
// build sandbox, ingests outputs into amber, and publishes result refs
// through the RefWriter seam. Ported from jobs with the store seam swapped to
// jobs-iroh's *amber.Store (single local store — no remote sync, no signing)
// and FUSE dropped (materialize-only store provisioning).
package runner

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/events"
	"github.com/jobs-build/jobs-iroh/tailbuf"
)

// ExecSpec describes one fetcher invocation.
type ExecSpec struct {
	FetcherDir  string            // CWD; entrypoint ./fetch lives here
	OutputDir   string            // writable; fetcher writes results here
	Env         map[string]string // JOBS_FETCH_PARAMS / JOBS_OUTPUT_DIR / JOBS_SECRETS_FILE
	SecretsFile string            // path, or "" when no requiredTags
	// Hermetic-root inputs, used by the Linux CgroupExecutor only (Subprocess
	// runs on the host filesystem and ignores them): the store holding the
	// shell artifact (ShellKey — the embedded static userland, mounted at
	// /jobs/store/<key> and bound over /bin and /usr/bin so shebangs resolve)
	// and, for a recipe-declared fetcher, the fetcher build's runtime closure
	// (ClosureKey — a build-output-deps store tree whose entries land at the
	// same /jobs/store/<key> paths the fetcher's own build saw; zero when the
	// fetcher has no runtime deps). CacheDir holds the materialized trees,
	// reused across imports. A zero ShellKey makes the hermetic executor fail:
	// a fetcher cannot run without a userland.
	Store      *amber.Store
	ShellKey   key.Key
	ClosureKey key.Key
	CacheDir   string
	// StdoutSink/StderrSink, when non-nil, additionally receive the process's
	// full stdout/stderr (build-events output capture). Stderr keeps the 4KB
	// tail for the failure path regardless.
	StdoutSink io.Writer
	StderrSink io.Writer
	// Events (nil-safe) receives the executor's exec.heartbeat liveness/usage
	// events while ./fetch runs (Linux CgroupExecutor only).
	Events *events.Job
	// Node, when set (the scheduler path), identifies the job for
	// observability. Empty on the local path.
	Node string
}

// ExecResult is the outcome of running ./fetch.
type ExecResult struct {
	ExitCode   int
	StderrTail string
}

// Executor runs a fetcher. On Linux with user namespaces the default is
// CgroupExecutor (hermetic root + best-effort cgroup, network kept); the
// cross-platform Subprocess is the fallback and the explicit test/develop
// seam (see defaultImportExecutor).
type Executor interface {
	Run(ctx context.Context, spec ExecSpec) (ExecResult, error)
}

// execBanner is the one-line "what is actually being run" note every import
// executor writes into the job's stderr stream before exec'ing the fetcher:
// the resolved entrypoint plus the param/secret surface the process sees
// (secrets by file path only — never contents).
func execBanner(spec ExecSpec) string {
	b := "jobs: exec " + filepath.Join(spec.FetcherDir, "fetch")
	if p := spec.Env["JOBS_FETCH_PARAMS"]; p != "" {
		b += " params=" + p
	}
	if spec.SecretsFile != "" {
		b += " secrets=" + spec.SecretsFile
	}
	return b + "\n"
}

// importStderr assembles an import's stderr chain: teed to our own stderr
// stream (live local/daemon-log visibility, like the build executor), the
// 4KB failure tail, and the event sink when set.
func importStderr(spec ExecSpec, tail *tailbuf.Buffer) io.Writer {
	writers := []io.Writer{os.Stderr, tail}
	if spec.StderrSink != nil {
		writers = append(writers, spec.StderrSink)
	}
	return io.MultiWriter(writers...)
}

// Subprocess runs the fetcher's ./fetch as a plain child process — no namespace
// isolation (imports are network-capable anyway). A non-zero exit is reported in
// ExecResult, not as a Go error; a Go error means the process could not run
// (infrastructure failure, including ctx cancellation).
type Subprocess struct{}

func (Subprocess) Run(ctx context.Context, spec ExecSpec) (ExecResult, error) {
	env := os.Environ()
	for k, v := range spec.Env {
		env = append(env, k+"="+v)
	}

	tail := tailbuf.New(4 << 10)
	stderr := importStderr(spec, tail)
	io.WriteString(stderr, execBanner(spec))

	// exec can fail ETXTBSY when a concurrently forked child of this process
	// (another job slot's fork, between its clone and execve) still holds the
	// write fd of the just-extracted fetch binary (golang/go#22315). That
	// writer is always moribund — it vanishes as soon as the other child
	// execs — so retry briefly instead of failing the import. A failed Start
	// has no side effects, so re-creating the Cmd per attempt is safe.
	delay := 5 * time.Millisecond
	for attempt := 0; ; attempt++ {
		cmd := exec.CommandContext(ctx, "./fetch")
		cmd.Dir = spec.FetcherDir
		cmd.Env = env
		cmd.Stdout = spec.StdoutSink // nil = discard, as before
		cmd.Stderr = stderr

		err := cmd.Run()
		if err == nil {
			return ExecResult{ExitCode: 0, StderrTail: tail.String()}, nil
		}
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return ExecResult{ExitCode: ee.ExitCode(), StderrTail: tail.String()}, nil
		}
		if errors.Is(err, syscall.ETXTBSY) && attempt < 7 && ctx.Err() == nil {
			time.Sleep(delay)
			delay *= 2
			continue
		}
		return ExecResult{ExitCode: -1, StderrTail: tail.String()}, err
	}
}
