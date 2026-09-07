//go:build linux

package runner_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/bootstrap"
	"github.com/jobs-build/jobs-iroh/runner"
	"github.com/jobs-build/jobs-iroh/sandbox"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// The hermetic import executor: a fetcher runs in a pivot_root'ed sandbox
// built from the embedded shell (plus the fetcher's runtime closure), with
// the host network kept. These specs need user namespaces and the embedded
// shell seed for this platform.
var _ = Describe("CgroupExecutor (hermetic import root)", func() {
	var (
		ctx        context.Context
		st         *amber.Store
		shellKey   key.Key
		cacheDir   string
		fetcherDir string
		outputDir  string
	)

	BeforeEach(func() {
		if !sandbox.UserNSAvailable() {
			Skip("user namespaces unavailable")
		}
		ctx = context.Background()
		var err error
		st, err = amber.Open(GinkgoTB().TempDir())
		Expect(err).NotTo(HaveOccurred())
		shellKey, err = bootstrap.SeedShellAs(ctx, st, runner.Platform(), "shell:test")
		if err != nil {
			Skip("embedded shell seed unavailable for " + runner.Platform() + ": " + err.Error())
		}
		cacheDir = GinkgoTB().TempDir()
		fetcherDir = GinkgoTB().TempDir()
		outputDir = GinkgoTB().TempDir()
	})

	AfterEach(func() {
		if st != nil {
			_ = st.Close()
		}
	})

	// writeFetch writes ./fetch (0755) in fetcherDir with the given shebang
	// and body. Inside the sandbox only the shell artifact exists, so the
	// shebang must resolve against it (/bin/sh, /usr/bin/env bash).
	writeFetch := func(shebang, script string) {
		GinkgoHelper()
		p := filepath.Join(fetcherDir, "fetch")
		Expect(os.WriteFile(p, []byte(shebang+"\n"+script+"\n"), 0o755)).To(Succeed())
	}

	spec := func() runner.ExecSpec {
		return runner.ExecSpec{
			FetcherDir: fetcherDir,
			OutputDir:  outputDir,
			Env:        map[string]string{"JOBS_OUTPUT_DIR": outputDir, "JOBS_FETCH_PARAMS": `{"a":1}`},
			Store:      st,
			ShellKey:   shellKey,
			CacheDir:   cacheDir,
		}
	}

	runExec := func(sp runner.ExecSpec) runner.ExecResult {
		GinkgoHelper()
		bctx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		res, err := runner.CgroupExecutor{}.Run(bctx, sp)
		Expect(err).NotTo(HaveOccurred(), "CgroupExecutor.Run must not return an infra error")
		return res
	}

	It("runs the fetcher under the embedded shell and hands its output back to the host", func() {
		writeFetch("#!/usr/bin/env bash", `set -eu
echo "$JOBS_OUTPUT_DIR" > "$JOBS_OUTPUT_DIR/outdir"
echo "$JOBS_FETCH_PARAMS" | jq -r .a > "$JOBS_OUTPUT_DIR/param"
pwd > "$JOBS_OUTPUT_DIR/cwd"
command -v bash > "$JOBS_OUTPUT_DIR/bash"`)
		res := runExec(spec())
		Expect(res.ExitCode).To(Equal(0), "fetch should exit 0; stderr: "+res.StderrTail)

		read := func(name string) string {
			b, err := os.ReadFile(filepath.Join(outputDir, name))
			Expect(err).NotTo(HaveOccurred(), name+" must be visible on the host (bound at /jobs/out)")
			return strings.TrimSpace(string(b))
		}
		Expect(read("outdir")).To(Equal("/jobs/out"), "JOBS_OUTPUT_DIR is the in-sandbox path")
		Expect(read("param")).To(Equal("1"), "jq from the shell artifact works")
		Expect(read("cwd")).To(Equal("/jobs/fetcher"), "cwd is the fetcher artifact")
		Expect(read("bash")).To(HavePrefix("/jobs/store/"+shellKey.String()+"/bin/"), "PATH leads with the shell's bin")
	})

	It("resolves /bin/sh shebangs and hides the host filesystem", func() {
		// A host-only path that must NOT be visible inside: this test's temp
		// dir itself (it sits on the host under TMPDIR).
		marker := filepath.Join(GinkgoTB().TempDir(), "host-only")
		Expect(os.WriteFile(marker, []byte("x"), 0o644)).To(Succeed())
		writeFetch("#!/bin/sh", `set -eu
[ ! -e "`+marker+`" ] || { echo "host path leaked" >&2; exit 3; }
[ ! -e /proc/self/exe ] && { echo "no /proc" >&2; exit 4; }
[ -e /usr/bin/env ] || { echo "no /usr/bin/env" >&2; exit 5; }
[ -w /tmp ] || { echo "/tmp not writable" >&2; exit 6; }
[ -e /etc/hosts ] || { echo "no /etc/hosts" >&2; exit 7; }
[ -n "${HOME:-}" ] && [ "$HOME" = /tmp ] || { echo "HOME=$HOME" >&2; exit 8; }
# the whole busybox applet set is on PATH, not just the shell's few symlinks
t="$(mktemp -d)" && [ -d "$t" ] || { echo "no mktemp" >&2; exit 9; }
sleep 0 && echo abc | tr a-c A-C | grep -q ABC || { echo "no sleep/tr" >&2; exit 10; }
echo ok > "$JOBS_OUTPUT_DIR/ok"`)
		res := runExec(spec())
		Expect(res.ExitCode).To(Equal(0), "stderr: "+res.StderrTail)
		_, err := os.Stat(filepath.Join(outputDir, "ok"))
		Expect(err).NotTo(HaveOccurred())
	})

	It("does not leak the runner's environment, only the network pass-through", func() {
		GinkgoTB().Setenv("JOBS_TEST_SECRET_ENV", "leak")
		GinkgoTB().Setenv("HTTPS_PROXY", "http://proxy.test:3128")
		writeFetch("#!/bin/sh", `set -eu
[ -z "${JOBS_TEST_SECRET_ENV:-}" ] || { echo "env leaked" >&2; exit 3; }
[ "${HTTPS_PROXY:-}" = "http://proxy.test:3128" ] || { echo "proxy not passed" >&2; exit 4; }
echo ok > "$JOBS_OUTPUT_DIR/ok"`)
		res := runExec(spec())
		Expect(res.ExitCode).To(Equal(0), "stderr: "+res.StderrTail)
	})

	It("keeps the host network: TCP dial to 8.8.8.8:53 succeeds", func() {
		conn, err := net.DialTimeout("tcp", "8.8.8.8:53", 3*time.Second)
		if err != nil {
			Skip("host has no outbound network: " + err.Error())
		}
		conn.Close()
		writeFetch("#!/usr/bin/env bash", `exec 3<>/dev/tcp/8.8.8.8/53 && exit 0 || exit 1`)
		res := runExec(spec())
		Expect(res.ExitCode).To(Equal(0),
			"network should be reachable inside the import sandbox (no CLONE_NEWNET); stderr: "+res.StderrTail)
	})

	It("mounts the fetcher's runtime closure at /jobs/store/<key>", func() {
		// A fake runtime dep: a tree with bin/hello, wrapped in a store tree
		// (the build-output-deps shape: entries named by key).
		depDir := GinkgoTB().TempDir()
		Expect(os.MkdirAll(filepath.Join(depDir, "bin"), 0o755)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(depDir, "bin", "hello"), []byte("#!/bin/sh\necho hello-from-closure\n"), 0o755)).To(Succeed())
		depKey, err := st.IngestSourceDir(ctx, depDir)
		Expect(err).NotTo(HaveOccurred())
		closure, err := st.BuildStoreTree(ctx, []key.Key{depKey})
		Expect(err).NotTo(HaveOccurred())

		writeFetch("#!/bin/sh", `set -eu
/jobs/store/`+depKey.String()+`/bin/hello > "$JOBS_OUTPUT_DIR/hello"
# the shell sits next to it under its own key
[ -x /jobs/store/`+shellKey.String()+`/bin/bash ] || { echo "no shell in store" >&2; exit 3; }`)
		sp := spec()
		sp.ClosureKey = closure
		res := runExec(sp)
		Expect(res.ExitCode).To(Equal(0), "stderr: "+res.StderrTail)
		b, err := os.ReadFile(filepath.Join(outputDir, "hello"))
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(string(b))).To(Equal("hello-from-closure"))

		// The materialized trees are cached under CacheDir and reused.
		for _, k := range []key.Key{shellKey, closure} {
			_, err := os.Stat(filepath.Join(cacheDir, "trees", k.String()))
			Expect(err).NotTo(HaveOccurred(), "tree "+k.String()+" cached")
		}
	})

	It("binds the secrets file read-only at its in-sandbox path", func() {
		secrets := filepath.Join(GinkgoTB().TempDir(), "secrets.json")
		Expect(os.WriteFile(secrets, []byte(`{"t":{"scope":"s","secret":"v"}}`), 0o600)).To(Succeed())
		writeFetch("#!/usr/bin/env bash", `set -eu
[ "$JOBS_SECRETS_FILE" = /jobs/secrets.json ] || { echo "JOBS_SECRETS_FILE=$JOBS_SECRETS_FILE" >&2; exit 3; }
jq -r .t.secret < "$JOBS_SECRETS_FILE" > "$JOBS_OUTPUT_DIR/secret"
! ( echo x >> "$JOBS_SECRETS_FILE" ) 2>/dev/null || { echo "secrets writable" >&2; exit 4; }`)
		sp := spec()
		sp.SecretsFile = secrets
		sp.Env["JOBS_SECRETS_FILE"] = secrets
		res := runExec(sp)
		Expect(res.ExitCode).To(Equal(0), "stderr: "+res.StderrTail)
		b, err := os.ReadFile(filepath.Join(outputDir, "secret"))
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(string(b))).To(Equal("v"))
	})

	It("reports a non-zero fetch exit as the result, not an error", func() {
		writeFetch("#!/bin/sh", `echo boom >&2; exit 75`)
		res := runExec(spec())
		Expect(res.ExitCode).To(Equal(75))
		Expect(res.StderrTail).To(ContainSubstring("boom"))
	})

	It("fails without a shell artifact to build the root from", func() {
		writeFetch("#!/bin/sh", `exit 0`)
		sp := spec()
		sp.ShellKey = key.Key{}
		_, err := runner.CgroupExecutor{}.Run(ctx, sp)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("shell"))
	})
})
