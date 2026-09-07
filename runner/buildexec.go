package runner

import (
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/events"
)

// ErrUnsupported is returned by a BuildExecutor on platforms without the
// rootless namespace sandbox (everything but Linux).
var ErrUnsupported = errors.New("runner: hermetic build sandbox unsupported on this platform")

// Paths inside the sandbox (post-pivot). These are fixed by the executor; the
// recipe sees them via $SRC/$out/$PATH and JOBS_DEPS. Declared here (not in
// the linux-only buildexec_linux.go) because the cross-platform store
// provisioning (storemount.go) targets sandboxStoreDir on every platform.
const (
	sandboxStoreDir = "/jobs/store" // the assembled content-addressed store (one RO bind)
	sandboxSrcDir   = "/build/src"  // writable $SRC (source subtree)
	sandboxOutDir   = "/build/out"  // writable $out (build output)
	sandboxHomeDir  = "/build"      // $HOME
	// sandboxJobsDepsFile always carries the full JOBS_DEPS JSON. The inline
	// JOBS_DEPS env var is kept only when small: a build with thousands of
	// inputs produces a map beyond the kernel's MAX_ARG_STRLEN (~128KiB per
	// env string), which would fail the child's execve with E2BIG. Recipes
	// with many inputs load it via an unexported shell assignment:
	//   [ -n "${JOBS_DEPS:-}" ] || JOBS_DEPS=$(cat "$JOBS_DEPS_FILE")
	sandboxJobsDepsFile = "/build/.jobs-deps.json"
	// sandboxScriptFile carries the build script. A generated script with
	// thousands of staging lines exceeds MAX_ARG_STRLEN as a single `-c`
	// argv element, so the script always runs from a file.
	sandboxScriptFile = "/build/.jobs-script.sh"
)

// maxInlineJobsDeps is the largest JOBS_DEPS JSON still passed inline in the
// environment (same rationale as sandbox's inline-config cap).
const maxInlineJobsDeps = 64 << 10

// storeShellDir is the in-store path of the shell artifact (bash + coreutils),
// keyed by its content (build.md §6).
func storeShellDir(shellBOK key.Key) string {
	return sandboxStoreDir + "/" + shellBOK.String()
}

// encodeJobsDeps renders the name→/jobs/store/<BOK> map as the JOBS_DEPS JSON
// object. json.Marshal sorts string keys, so the encoding is deterministic; a
// nil/empty map encodes as "{}".
func encodeJobsDeps(deps map[string]string) string {
	if deps == nil {
		deps = map[string]string{}
	}
	b, err := json.Marshal(deps)
	if err != nil {
		// map[string]string never fails to marshal.
		return "{}"
	}
	return string(b)
}

// BuildSpec fully describes one hermetic build invocation.
//
// StoreKey is the assembled content-addressed /jobs/store union tree (build.md
// §6): one read-only tree at /jobs/store holding every dependency at
// /jobs/store/<BOK> plus the shell. ShellBOK names the shell's entry within it,
// so PATH/bash resolve to /jobs/store/<ShellBOK>/bin. JobsDeps maps each input's
// recipe name to its /jobs/store/<BOK> path and is injected as the JOBS_DEPS JSON
// env var; the build script resolves dependency paths from it. SourceKey's
// subtree at SourceDir is extracted into a writable $SRC. Script runs as
// `bash -e <script file>` with $out a writable, initially-empty output dir. Env
// is merged over the sandbox-provided SRC/out/PATH/HOME. MemoryMaxBytes and
// PIDsMax are best-effort cgroup v2 caps (0 => no limit / undelegated).
type BuildSpec struct {
	StoreKey  key.Key
	ShellBOK  key.Key
	JobsDeps  map[string]string
	SourceKey key.Key
	SourceDir string
	// Workdir is the build dir within the extracted source (sibling-sources
	// design §9): when set, $SRC and the sandbox CWD point at
	// <src>/<Workdir> and $SRC_ROOT at the source root, so relative sibling
	// paths (../lib) resolve exactly as they do in the repo. "" = legacy
	// layout ($SRC = the source root, CWD = /).
	Workdir        string
	Env            map[string]string
	Script         string
	MemoryMaxBytes int64
	PIDsMax        int64
	// Caches are the seeded persistent build caches, rw-bound at their
	// declared paths (build-cache design §5). The RECEIVER of the spec owns
	// the host dirs and must removeCacheDirs them when done.
	Caches []CacheMount
	// StdoutSink/StderrSink, when non-nil, additionally receive the build
	// script's full stdout/stderr (build-events output capture). Stderr keeps
	// the 4KB tail for the failure path regardless.
	StdoutSink io.Writer
	StderrSink io.Writer
	// Node, when set (the scheduler path), identifies the job for
	// observability. Empty on the local path.
	Node string
	// Events (nil-safe) receives the executor's own phase transitions:
	// "materializing" while /jobs/store is staged into the sandbox and
	// "building" once the script is about to run — the store staging of a big
	// closure takes real time and used to be silent.
	Events *events.Job
}

// BuildResult is the outcome of a hermetic build. OutDir is the host path of the
// populated $out (the caller ingests it and does the `c/` finalization). A
// non-zero ExitCode is a build (script) failure, not a Go error; a Go error
// means the sandbox could not run.
//
// OutDir lifecycle: on a successful (err==nil) RunBuild the executor leaves the
// populated output tree on disk and the CALLER owns it — ingest OutDir, then
// remove it (and its enclosing work dir). When RunBuild returns a non-nil error,
// OutDir is empty and nothing is left to clean up.
type BuildResult struct {
	ExitCode   int
	StderrTail string
	OutDir     string
}

// BuildExecutor runs a hermetic build described by a BuildSpec. The Linux
// implementation (NamespaceBuildExecutor) stages the store read-only into the
// sandbox, gives the build a writable $SRC/$out, and runs the recipe script
// under the imported bash with net=none + cgroups.
type BuildExecutor interface {
	RunBuild(ctx context.Context, st *amber.Store, spec BuildSpec) (BuildResult, error)
}
