//go:build linux

package runner

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/bootstrap"
	"github.com/jobs-build/jobs-iroh/events"
	"github.com/jobs-build/jobs-iroh/sandbox"
)

// TestBuildExecHeartbeatWiring: the namespace build executor emits
// exec.heartbeat(phase=building) while the script runs.
func TestBuildExecHeartbeatWiring(t *testing.T) {
	if !sandbox.UserNSAvailable() {
		t.Skip("user namespaces unavailable")
	}
	ctx := context.Background()
	st := openTestStore(t)

	// Shell artifact (static bash+busybox) straight from the hostshell fetcher.
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
	shellKey, err := st.IngestDir(ctx, shellOut)
	if err != nil {
		t.Fatal(err)
	}
	storeKey, err := st.BuildStoreTree(ctx, []key.Key{shellKey})
	if err != nil {
		t.Fatal(err)
	}
	srcDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcDir, "f.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	srcKey, err := st.IngestDir(ctx, srcDir)
	if err != nil {
		t.Fatal(err)
	}

	sink := &capSink{}
	shrinkHeartbeatInterval(t, 50*time.Millisecond)

	spec := BuildSpec{
		StoreKey:  storeKey,
		ShellBOK:  shellKey,
		SourceKey: srcKey,
		Script:    "busybox sleep 1", // sleep isn't a linked applet in the vendored shell
		Events:    events.NewJob(sink, "build|F", "r-test", nil),
	}
	res, err := NamespaceBuildExecutor{}.RunBuild(ctx, st, spec)
	if err != nil {
		t.Fatalf("RunBuild: %v", err)
	}
	if res.ExitCode != 0 {
		t.Fatalf("exit %d, stderr: %s", res.ExitCode, res.StderrTail)
	}
	// Success leaves the work tree for the caller (finalizeOutput's job).
	defer os.RemoveAll(filepath.Dir(filepath.Dir(filepath.Dir(res.OutDir))))

	hbs := heartbeatEvents(t, sink)
	if len(hbs) == 0 {
		t.Fatal("want exec.heartbeat events during a 1s build at 50ms interval")
	}
	// The "materializing" phase heartbeat may tick during assembly; the ones
	// after the script starts must carry phase=building.
	sawBuilding := false
	for _, hb := range hbs {
		switch hb.Data["phase"] {
		case "building":
			sawBuilding = true
		case "materializing":
		default:
			t.Fatalf("unexpected heartbeat phase %v", hb.Data["phase"])
		}
	}
	if !sawBuilding {
		t.Fatal("want at least one building-phase heartbeat")
	}
	// Whenever the host delegates cgroup v2 (a cgroup existed), cpu.stat is
	// readable regardless of enabled controllers — the usage must be carried
	// on the settled (last) building heartbeat.
	if _, _, delegated := sandbox.DetectCgroupDelegation(); delegated {
		last := hbs[len(hbs)-1]
		if _, ok := last.Data["cpu_ms"]; !ok {
			t.Fatalf("delegated host: want cpu_ms on heartbeats, got %+v", last.Data)
		}
	}
}

// TestImportExecHeartbeatWiring: the import cgroup executor emits
// exec.heartbeat(phase=fetching) while the fetcher runs.
func TestImportExecHeartbeatWiring(t *testing.T) {
	if !sandbox.UserNSAvailable() {
		t.Skip("user namespaces unavailable")
	}
	ctx := context.Background()
	fdir := t.TempDir()
	if err := os.WriteFile(filepath.Join(fdir, "fetch"), []byte("#!/bin/sh\nsleep 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	// The hermetic import root needs the shell artifact in a store.
	st, err := amber.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	shellKey, err := bootstrap.SeedShellAs(ctx, st, Platform(), "shell:test")
	if err != nil {
		t.Skip("embedded shell seed unavailable: " + err.Error())
	}

	sink := &capSink{}
	shrinkHeartbeatInterval(t, 50*time.Millisecond)

	spec := ExecSpec{
		FetcherDir: fdir,
		OutputDir:  t.TempDir(),
		Events:     events.NewJob(sink, "import|K", "r-test", nil),
		Store:      st,
		ShellKey:   shellKey,
		CacheDir:   t.TempDir(),
	}
	res, err := CgroupExecutor{}.Run(ctx, spec)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.ExitCode != 0 {
		t.Fatalf("exit %d, stderr: %s", res.ExitCode, res.StderrTail)
	}

	hbs := heartbeatEvents(t, sink)
	if len(hbs) == 0 {
		t.Fatal("want exec.heartbeat events during a 1s fetch at 50ms interval")
	}
	for _, hb := range hbs {
		if hb.Data["phase"] != "fetching" {
			t.Fatalf("phase = %v", hb.Data["phase"])
		}
	}
}
