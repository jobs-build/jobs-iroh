//go:build linux

package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/sandbox"
	"github.com/jobs-build/jobs-iroh/tailbuf"
)

// CgroupExecutor runs a fetcher's ./fetch in a hermetic root — the same kind
// of sandbox a build gets (pivot_root, User+Mount+PID+UTS+IPC namespaces,
// best-effort cgroup) — but WITHOUT CLONE_NEWNET: the host network is kept,
// because imports are the one network-capable stage (architecture §3.2).
//
// Nothing from the host filesystem is visible inside. The root holds:
//
//	/jobs/store/<key>   read-only: the shell artifact (the embedded static
//	                    userland) and, for a recipe-declared fetcher, the
//	                    fetcher build's runtime closure — at the same paths
//	                    the fetcher's own build saw, so a fetcher that
//	                    needs a toolchain declares it as a runtime dep and
//	                    bakes the path (exactly like an image entrypoint)
//	/bin, /usr/bin      read-only binds of the shell's bin, so `#!/bin/sh`
//	                    and `#!/usr/bin/env bash` shebangs resolve
//	/jobs/fetcher       read-only: the fetcher artifact (cwd; ./fetch)
//	/jobs/out           writable: $JOBS_OUTPUT_DIR
//	/jobs/secrets.json  read-only: $JOBS_SECRETS_FILE, when the import has one
//	/tmp                writable scratch on the work filesystem (not tmpfs —
//	                    downloads are big and TMPDIR is the data volume)
//	/dev, /proc, /etc   the hermetic minimal set; /etc additionally carries
//	                    the HOST's resolver files and CA bundle, because DNS
//	                    and TLS trust are part of "network", not of the host
//
// The environment is hermetic too: PATH, HOME, TMPDIR, the JOBS_* variables
// and the proxy/SSL_CERT_* pass-through — never os.Environ().
type CgroupExecutor struct {
	MemoryMaxBytes int64
	PIDsMax        int64
}

var _ Executor = CgroupExecutor{}

// defaultImportExecutor is the production import executor: CgroupExecutor
// (hermetic root + best-effort cgroup + fetching heartbeats) with the job's
// resolved memory limit, when this host can create user namespaces. Without
// userns the sandbox re-exec cannot work at all, so degrade to the plain
// Subprocess (announced once) rather than failing every import.
func defaultImportExecutor(memMaxBytes int64) Executor {
	usernsProbe.Do(func() {
		usernsOK = usernsAvailableFn()
		if !usernsOK {
			fmt.Fprintln(os.Stderr, "jobs: user namespaces unavailable; running imports on the host filesystem without confinement")
		}
	})
	if !usernsOK {
		return Subprocess{}
	}
	return CgroupExecutor{MemoryMaxBytes: memMaxBytes}
}

var (
	usernsProbe       sync.Once
	usernsOK          bool
	usernsAvailableFn = sandbox.UserNSAvailable // test seam
)

// In-sandbox paths of the import root.
const (
	importFetcherDir  = "/jobs/fetcher"
	importOutDir      = "/jobs/out"
	importSecretsFile = "/jobs/secrets.json"
	importTmpDir      = "/tmp"
	importHomeDir     = "/tmp"
)

// importPassEnv lists the host environment variables an import inherits:
// how to reach the network (proxies) and whom to trust on it (an operator's
// SSL_CERT_* pointing at a custom bundle — bound in when it names a file).
// Everything else stays out: a fetch must not see the runner's env.
var importPassEnv = []string{
	"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "ALL_PROXY",
	"http_proxy", "https_proxy", "no_proxy", "all_proxy",
	"SSL_CERT_FILE", "SSL_CERT_DIR",
}

// hostCABundles are the usual locations of the system CA bundle, tried in
// order when SSL_CERT_FILE does not name one (Debian/Ubuntu/distroless,
// Fedora/RHEL, Alpine/macOS-style, NixOS).
var hostCABundles = []string{
	"/etc/ssl/certs/ca-certificates.crt",
	"/etc/pki/tls/certs/ca-bundle.crt",
	"/etc/ssl/cert.pem",
	"/etc/ssl/certs/ca-bundle.crt",
}

func (e CgroupExecutor) Run(ctx context.Context, spec ExecSpec) (ExecResult, error) {
	if spec.Store == nil || spec.ShellKey == (key.Key{}) {
		return ExecResult{}, errors.New("hermetic import: no shell artifact (ShellKey) to build the sandbox root from")
	}

	// Materialized store trees, cached by content key under CacheDir and
	// shared read-only by every import on this runner.
	shellDir, err := stagedTree(ctx, spec.Store, spec.CacheDir, spec.ShellKey)
	if err != nil {
		return ExecResult{}, fmt.Errorf("stage shell artifact: %w", err)
	}
	binDir, err := stagedBinDir(spec.CacheDir, spec.ShellKey, shellDir)
	if err != nil {
		return ExecResult{}, fmt.Errorf("stage /bin for shell artifact: %w", err)
	}
	storeDir := ""
	if spec.ClosureKey != (key.Key{}) {
		storeDir, err = stagedTree(ctx, spec.Store, spec.CacheDir, spec.ClosureKey)
		if err != nil {
			return ExecResult{}, fmt.Errorf("stage fetcher runtime closure: %w", err)
		}
	}

	work, err := os.MkdirTemp("", "jobs-import-root-")
	if err != nil {
		return ExecResult{}, fmt.Errorf("create import root: %w", err)
	}
	defer os.RemoveAll(work)
	newRoot := filepath.Join(work, "root")
	scratch := filepath.Join(work, "tmp")
	for _, d := range []string{newRoot, scratch} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return ExecResult{}, fmt.Errorf("mkdir %s: %w", d, err)
		}
	}

	mounts, env, err := importMountPlan(newRoot, shellDir, binDir, storeDir, scratch, spec)
	if err != nil {
		return ExecResult{}, err
	}

	// Best-effort cgroup; nil means no delegation — the process runs without caps.
	cgName := fmt.Sprintf("jobs-import-%d-%d", os.Getpid(), time.Now().UnixNano())
	cg, err := sandbox.CreateCgroup(cgName, sandbox.CgroupLimits{
		MemoryMaxBytes: e.MemoryMaxBytes,
		PIDsMax:        e.PIDsMax,
	})
	if err != nil {
		return ExecResult{}, fmt.Errorf("create import cgroup: %w", err)
	}
	defer func() {
		if cg != nil {
			_ = cg.Close()
		}
	}()

	tail := tailbuf.New(4 << 10)
	stderr := importStderr(spec, tail)
	io.WriteString(stderr, execBanner(spec))

	cfg := sandbox.Config{
		Command: []string{importFetcherDir + "/fetch"},
		Dir:     importFetcherDir,
		Env:     env,
		NewRoot: newRoot,
		Mounts:  mounts,
		// Net:false → NO CLONE_NEWNET → the host network is kept (imports need
		// network). Everything else is isolated exactly like a build.
		Namespaces: sandbox.Namespaces{
			User: true, Mount: true, PID: true, Net: false, UTS: true, IPC: true,
		},
		Cgroup: cg,
		Stdout: spec.StdoutSink, // nil = discard, as before
		Stderr: stderr,
	}

	// Liveness + cgroup usage while ./fetch runs (a large download is
	// otherwise silent); stop before the deferred cg.Close.
	stopHB := startExecHeartbeat(spec.Events, "fetching", execHeartbeatInterval, cg.Stats)
	code, runErr := sandbox.Run(ctx, cfg)
	stopHB()
	if runErr != nil {
		// Infrastructure failure: the process did not run to completion.
		return ExecResult{}, fmt.Errorf("run import sandbox: %w", runErr)
	}
	// sandbox.Run returns (exitCode, nil) for any command exit (zero or non-zero).
	return ExecResult{ExitCode: code, StderrTail: tail.String()}, nil
}

// importMountPlan lays out the import root under newRoot: it writes the
// hermetic /etc and /dev, and returns the sandbox mounts plus the hermetic
// environment. shellDir is the materialized shell artifact (its root holds
// bin/), binDir the generated /bin farm for it (stagedBinDir), storeDir the
// materialized runtime-closure tree ("" when none), scratch the host dir
// bound at /tmp. The store binds go first so the shell bind over
// /jobs/store/<shell> lands inside them; the per-path mountpoints are
// created on the host side here because the child cannot mkdir inside a
// read-only bind.
func importMountPlan(newRoot, shellDir, binDir, storeDir, scratch string, spec ExecSpec) ([]sandbox.Mount, []string, error) {
	shellStore := storeShellDir(spec.ShellKey) // /jobs/store/<shell key>

	// /jobs/store: the closure tree (entries named by key), with the shell
	// bound on top at its own key. The shell's mountpoint must exist inside
	// the closure dir before that dir is bound read-only — create it on the
	// host side (an empty dir in a cached tree is harmless and idempotent).
	var mounts []sandbox.Mount
	if storeDir != "" {
		if err := os.MkdirAll(filepath.Join(storeDir, spec.ShellKey.String()), 0o755); err != nil {
			return nil, nil, fmt.Errorf("mkdir shell mountpoint in closure: %w", err)
		}
		mounts = append(mounts, sandbox.Mount{Source: storeDir, Target: sandboxStoreDir, ReadOnly: true})
	}
	mounts = append(mounts,
		sandbox.Mount{Source: shellDir, Target: shellStore, ReadOnly: true},
		// /bin and /usr/bin: the shell's bash/jq plus EVERY busybox applet
		// (mktemp, sleep, tr, find, sort, wget, sha256sum, ...), so shebangs
		// resolve and a fetch script has the whole static userland, not just
		// the handful of applets the shell artifact symlinks for builds.
		sandbox.Mount{Source: binDir, Target: "/bin", ReadOnly: true},
		sandbox.Mount{Source: binDir, Target: "/usr/bin", ReadOnly: true},
		sandbox.Mount{Source: spec.FetcherDir, Target: importFetcherDir, ReadOnly: true},
		sandbox.Mount{Source: spec.OutputDir, Target: importOutDir},
		sandbox.Mount{Source: scratch, Target: importTmpDir},
	)
	if spec.SecretsFile != "" {
		mounts = append(mounts, sandbox.Mount{Source: spec.SecretsFile, Target: importSecretsFile, ReadOnly: true})
	}

	devMounts, err := hermeticDevMounts(newRoot)
	if err != nil {
		return nil, nil, err
	}
	mounts = append(mounts, devMounts...)

	caFile, err := writeImportEtc(newRoot)
	if err != nil {
		return nil, nil, err
	}
	if caFile != "" {
		mounts = append(mounts, sandbox.Mount{Source: caFile, Target: caFile, ReadOnly: true})
	}

	env := importEnv(spec, shellStore, caFile)
	return mounts, env, nil
}

// importEnv is the hermetic fetcher environment: structural variables, the
// spec's JOBS_* (with the output/secrets paths rewritten to their in-sandbox
// locations), then the network pass-through from the host. SSL_CERT_FILE is
// pinned to the bundle bound into the root so Go, curl, OpenSSL-based tools
// and the Go toolchain inside a fetch all find the same trust store,
// whatever the host distribution calls it.
func importEnv(spec ExecSpec, shellStore, caFile string) []string {
	merged := map[string]string{
		"PATH":   shellStore + "/bin:/usr/bin:/bin",
		"HOME":   importHomeDir,
		"TMPDIR": importTmpDir,
	}
	for _, name := range importPassEnv {
		if v, ok := os.LookupEnv(name); ok && v != "" {
			merged[name] = v
		}
	}
	for k, v := range spec.Env {
		merged[k] = v
	}
	if _, ok := spec.Env["JOBS_OUTPUT_DIR"]; ok {
		merged["JOBS_OUTPUT_DIR"] = importOutDir
	}
	if spec.SecretsFile != "" {
		merged["JOBS_SECRETS_FILE"] = importSecretsFile
	}
	if caFile != "" {
		merged["SSL_CERT_FILE"] = caFile
		delete(merged, "SSL_CERT_DIR")
	}
	env := make([]string, 0, len(merged))
	for k, v := range merged {
		env = append(env, k+"="+v)
	}
	// Deterministic order keeps the child's payload stable (and tests simple).
	sort.Strings(env)
	return env
}

// writeImportEtc stages the import root's /etc: the hermetic baseline
// (hermeticetc.go) and then, because this sandbox HAS network, the host's
// resolver configuration (resolv.conf, hosts, nsswitch.conf — copied, so a
// NixOS symlink farm is fine) and a minimal passwd/group so tools that look
// up the current user (cargo, go's telemetry, bash's ~) get an answer. It
// returns the host CA bundle path to bind in (SSL_CERT_FILE if it names a
// file, else the first conventional bundle present), or "" when the host
// has none we recognize — TLS then fails inside the fetch, which is the
// honest outcome.
func writeImportEtc(newRoot string) (string, error) {
	if err := writeHermeticEtc(newRoot); err != nil {
		return "", err
	}
	etc := filepath.Join(newRoot, "etc")
	for _, name := range []string{"resolv.conf", "hosts", "nsswitch.conf"} {
		b, err := os.ReadFile("/etc/" + name)
		if err != nil {
			continue // keep the hermetic baseline for that file
		}
		if name == "hosts" && !strings.Contains(string(b), "localhost") {
			b = append([]byte("127.0.0.1 localhost\n::1 localhost\n"), b...)
		}
		if err := os.WriteFile(filepath.Join(etc, name), b, 0o644); err != nil {
			return "", fmt.Errorf("write sandbox /etc/%s: %w", name, err)
		}
	}
	passwd := "root:x:0:0:root:" + importHomeDir + ":/bin/sh\nnobody:x:65534:65534:nobody:/:/bin/sh\n"
	if err := os.WriteFile(filepath.Join(etc, "passwd"), []byte(passwd), 0o644); err != nil {
		return "", fmt.Errorf("write sandbox /etc/passwd: %w", err)
	}
	if err := os.WriteFile(filepath.Join(etc, "group"), []byte("root:x:0:\nnobody:x:65534:\n"), 0o644); err != nil {
		return "", fmt.Errorf("write sandbox /etc/group: %w", err)
	}
	return hostCABundle(), nil
}

// hostCABundle picks the CA bundle file to bind into the import root.
func hostCABundle() string {
	if f := os.Getenv("SSL_CERT_FILE"); f != "" {
		if st, err := os.Stat(f); err == nil && st.Mode().IsRegular() {
			return f
		}
	}
	for _, f := range hostCABundles {
		if st, err := os.Stat(f); err == nil && st.Mode().IsRegular() {
			return f
		}
	}
	return ""
}

// stagedTree returns a host directory holding the materialized store tree k,
// creating it under <cacheDir>/trees/<k> on first use. Materialization
// happens into a sibling temp dir that is renamed into place: a directory
// rename is atomic and fails on a non-empty target, so the final path exists
// only once it is complete, and the loser of a concurrent race discards its
// copy and uses the winner's. Trees are never evicted: they are the shell
// and the fetchers' toolchain closures, shared by every import that names
// them, and the cache lives on the data volume.
func stagedTree(ctx context.Context, st *amber.Store, cacheDir string, k key.Key) (string, error) {
	if cacheDir == "" {
		cacheDir = os.TempDir()
	}
	trees := filepath.Join(cacheDir, "trees")
	if err := os.MkdirAll(trees, 0o755); err != nil {
		return "", err
	}
	dst := filepath.Join(trees, k.String())
	if _, err := os.Stat(dst); err == nil {
		return dst, nil
	}
	tmp, err := os.MkdirTemp(trees, "staging-")
	if err != nil {
		return "", err
	}
	if err := materializeStore(ctx, st, k, tmp); err != nil {
		os.RemoveAll(tmp)
		return "", err
	}
	if err := os.Rename(tmp, dst); err != nil {
		os.RemoveAll(tmp)
		if _, serr := os.Stat(dst); serr != nil {
			return "", fmt.Errorf("stage tree %s: %w", k, err)
		}
	}
	return dst, nil
}

// stagedBinDir builds (once per shell key, under <cacheDir>/trees/<key>.bin)
// the directory bound at /bin and /usr/bin inside the import root: a symlink
// per entry of the shell artifact's bin/ plus one per busybox applet, all
// pointing into /jobs/store/<shell>/bin — in-sandbox paths, so the farm is
// valid only where the shell is mounted. The applet list comes from the
// artifact's own busybox (`busybox --list`); a shell without busybox just
// gets its bin/ mirrored. Same atomic-rename publication as stagedTree.
func stagedBinDir(cacheDir string, shellKey key.Key, shellDir string) (string, error) {
	if cacheDir == "" {
		cacheDir = os.TempDir()
	}
	trees := filepath.Join(cacheDir, "trees")
	if err := os.MkdirAll(trees, 0o755); err != nil {
		return "", err
	}
	dst := filepath.Join(trees, shellKey.String()+".bin")
	if _, err := os.Stat(dst); err == nil {
		return dst, nil
	}
	tmp, err := os.MkdirTemp(trees, "bin-staging-")
	if err != nil {
		return "", err
	}
	inSandboxBin := storeShellDir(shellKey) + "/bin"
	names := map[string]bool{}
	entries, err := os.ReadDir(filepath.Join(shellDir, "bin"))
	if err != nil {
		os.RemoveAll(tmp)
		return "", fmt.Errorf("read shell bin: %w", err)
	}
	for _, e := range entries {
		names[e.Name()] = true
	}
	for _, a := range busyboxApplets(filepath.Join(shellDir, "bin", "busybox")) {
		if !names[a] {
			names[a] = true
			if err := os.Symlink("busybox", filepath.Join(tmp, a)); err != nil {
				os.RemoveAll(tmp)
				return "", err
			}
		}
	}
	for _, e := range entries {
		if err := os.Symlink(inSandboxBin+"/"+e.Name(), filepath.Join(tmp, e.Name())); err != nil {
			os.RemoveAll(tmp)
			return "", err
		}
	}
	if err := os.Rename(tmp, dst); err != nil {
		os.RemoveAll(tmp)
		if _, serr := os.Stat(dst); serr != nil {
			return "", fmt.Errorf("stage bin dir: %w", err)
		}
	}
	return dst, nil
}

// busyboxApplets returns the applet names the static busybox at path
// provides, or nil when there is no usable busybox (no applets to add).
func busyboxApplets(path string) []string {
	if _, err := os.Stat(path); err != nil {
		return nil
	}
	out, err := exec.Command(path, "--list").Output()
	if err != nil {
		return nil
	}
	var names []string
	for _, line := range strings.Split(string(out), "\n") {
		if a := strings.TrimSpace(line); a != "" {
			names = append(names, a)
		}
	}
	return names
}
