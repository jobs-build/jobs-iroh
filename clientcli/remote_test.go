package clientcli

import (
	"bytes"
	"context"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/api"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/cover"
	"github.com/jobs-build/jobs-iroh/serve"
	"github.com/jobs-build/jobs-iroh/wire"
)

func TestTreeDefinitionCanonicalIdentity(t *testing.T) {
	st, err := amber.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	sourceKey, err := st.IngestFile(context.Background(), []byte("source stand-in"))
	if err != nil {
		t.Fatal(err)
	}
	params, err := parseParams([]string{"a=1"})
	if err != nil {
		t.Fatal(err)
	}

	canon, k, err := treeDefinition(sourceKey, "svc", "BUILD.alt", "linux/amd64", params)
	if err != nil {
		t.Fatal(err)
	}

	// The bytes are canonical: decode → re-canonicalize is the identity.
	def, err := builddef.DecodeDefinition(canon)
	if err != nil {
		t.Fatal(err)
	}
	again, err := def.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(canon, again) {
		t.Fatal("treeDefinition bytes are not canonical")
	}

	// K is the def bytes' file key — the same identity the server derives.
	want, err := (builddef.Input{Kind: builddef.KindBuild, Definition: canon}).Key()
	if err != nil {
		t.Fatal(err)
	}
	if k != want {
		t.Fatalf("definition key %s, want %s", k, want)
	}

	// The tree source round-trips to the ingested root.
	tk, err := builddef.DecodeTreeKey(def.Source.Definition)
	if err != nil {
		t.Fatal(err)
	}
	if tk != sourceKey || def.Source.Kind != builddef.KindTree {
		t.Fatalf("source input: kind=%q key=%s, want tree %s", def.Source.Kind, tk, sourceKey)
	}
	if def.Dir != "svc" || def.BuildFile != "BUILD.alt" || def.Platform != "linux/amd64" {
		t.Fatalf("definition fields: %+v", def)
	}
}

// TestSnapshotChangeLineDedupes: the non-TTY watch path prints one line per
// snapshot DELTA — an unchanged snapshot maps to the same line, which the
// stream loop dedupes.
func TestSnapshotChangeLineDedupes(t *testing.T) {
	snap := api.Snapshot{
		Phase:  "running",
		Counts: wire.Counts{Total: 3, Done: 1, Running: 1, Waiting: 1},
		Nodes: []api.NodeSnap{
			{Node: "buildfrom_ab", Phase: wire.PhaseRunning},
			{Node: "buildvalue_ab", Phase: wire.PhaseWaiting},
		},
	}
	a := snapshotChangeLine(snap)
	if a != snapshotChangeLine(snap) {
		t.Fatal("identical snapshots must map to identical lines")
	}
	if !strings.Contains(a, "1/3 done") || !strings.Contains(a, "buildfrom_ab") {
		t.Fatalf("line content: %q", a)
	}
	if strings.Contains(a, "buildvalue_ab") {
		t.Fatalf("waiting node must not be listed as in flight: %q", a)
	}
	if strings.Contains(a, "failed") {
		t.Fatalf("no-failure snapshot must not mention failures: %q", a)
	}
	snap.Counts.Done = 2
	if snapshotChangeLine(snap) == a {
		t.Fatal("changed counts must change the line")
	}
	snap.Counts.Failed = 1
	if !strings.Contains(snapshotChangeLine(snap), "1 failed") {
		t.Fatalf("failed count missing: %q", snapshotChangeLine(snap))
	}
}

// TestRemoteBuildEndToEnd drives the real remote-build command against a
// real jobs-server over loopback iroh. There is no runner: the server store
// is pre-seeded with the build's output refs, so the submit's doneness
// fast-path completes the request instantly and the command runs its full
// path — ingest, push, submit, watch to terminal, pull home.
func TestRemoteBuildEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	ready := make(chan *serve.Server, 1)
	done := make(chan error, 1)
	srvCtx, stopSrv := context.WithCancel(ctx)
	t.Cleanup(func() {
		stopSrv()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("server run: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("server did not shut down")
		}
	})
	go func() {
		done <- serve.Run(srvCtx, serve.Options{
			DataDir:  t.TempDir(),
			BindAddr: netip.AddrPortFrom(netip.IPv6Loopback(), 0),
			Ready:    func(s *serve.Server) { ready <- s },
		})
	}()
	var srv *serve.Server
	select {
	case srv = <-ready:
	case <-time.After(30 * time.Second):
		t.Fatal("server not ready")
	}
	serverID := srv.Endpoint.ID().String()
	serverAddr := srv.Endpoint.LocalAddr().String()

	// The build source the client will ingest and push.
	srcDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcDir, "BUILD.jobs"), []byte("# recipe\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Pre-seed doneness server-side: ingest the same source (IngestSourceDir
	// is deterministic, so the client computes the same tree key and K) and
	// write the refs a finished build would have left. F's true value is
	// irrelevant to the fast-path — any complete tree works; the tree key
	// itself keeps the test honest about ref plumbing.
	treeKey, err := srv.Store.IngestSourceDir(ctx, srcDir)
	if err != nil {
		t.Fatal(err)
	}
	params, err := parseParams(nil)
	if err != nil {
		t.Fatal(err)
	}
	_, k, err := treeDefinition(treeKey, "", "", "linux/amd64", params)
	if err != nil {
		t.Fatal(err)
	}
	outDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(outDir, "artifact"), []byte("built!"), 0o644); err != nil {
		t.Fatal(err)
	}
	outKey, err := srv.Store.IngestDir(ctx, outDir)
	if err != nil {
		t.Fatal(err)
	}
	f := treeKey // stand-in F
	if err := srv.Store.PutRef(ctx, "build-from:"+k.String(), f); err != nil {
		t.Fatal(err)
	}
	if err := srv.Store.PutRef(ctx, "build-output:"+f.String(), outKey); err != nil {
		t.Fatal(err)
	}
	if err := srv.Store.PutRef(ctx, "build-output-deps:"+f.String(), outKey); err != nil {
		t.Fatal(err)
	}

	dataDir := t.TempDir()
	app := App()
	var out, errOut bytes.Buffer
	app.Writer = &out
	app.ErrWriter = &errOut
	err = app.RunContext(ctx, []string{"jobs-client", "remote-build",
		"--server", serverID,
		"--addr", serverAddr,
		"--data-dir", dataDir,
		"--source", srcDir,
		"--platform", "linux/amd64",
	})
	if err != nil {
		t.Fatalf("remote-build: %v\nstderr:\n%s", err, errOut.String())
	}
	if !strings.Contains(out.String(), "output: "+outKey.String()) {
		t.Fatalf("stdout missing output key %s:\n%s\nstderr:\n%s", outKey, out.String(), errOut.String())
	}
	if !strings.Contains(errOut.String(), "submitted request ") {
		t.Fatalf("stderr missing submit line:\n%s", errOut.String())
	}
	// Non-TTY watch: the terminal snapshot collapses to the plain verdict
	// line (no ANSI on a buffer-backed ErrWriter).
	if !strings.Contains(errOut.String(), "build: DONE · ") {
		t.Fatalf("stderr missing watch verdict line:\n%s", errOut.String())
	}
	if strings.Contains(errOut.String(), "\x1b[") {
		t.Fatalf("non-TTY stderr must carry no ANSI:\n%q", errOut.String())
	}

	// The outputs were pulled home into the client store.
	cst, err := amber.Open(filepath.Join(dataDir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer cst.Close()
	got, ok, err := cst.GetKey(ctx, "build-output:"+f.String())
	if err != nil || !ok || got != outKey {
		t.Fatalf("local build-output ref: key=%s ok=%v err=%v (want %s)", got, ok, err, outKey)
	}

	// One-shot status against the same server: the finished request shows in
	// the requests table; the fleet table renders (empty — no runner here).
	stApp := App()
	var stOut, stErr bytes.Buffer
	stApp.Writer = &stOut
	stApp.ErrWriter = &stErr
	err = stApp.RunContext(ctx, []string{"jobs-client", "status",
		"--server", serverID, "--addr", serverAddr})
	if err != nil {
		t.Fatalf("status: %v\nstderr:\n%s", err, stErr.String())
	}
	for _, want := range []string{"REQUEST", "RUNNER", "done"} {
		if !strings.Contains(stOut.String(), want) {
			t.Fatalf("status output missing %q:\n%s", want, stOut.String())
		}
	}
	if strings.Contains(stOut.String(), "\x1b[") {
		t.Fatalf("status output must be plain text:\n%q", stOut.String())
	}

	// One-shot logs against the same server: no runner ever produced output
	// here, so a valid node yields the empty-view note (the full logs frame
	// path — request, stored view, terminal close — still runs).
	lgApp := App()
	var lgOut, lgErr bytes.Buffer
	lgApp.Writer = &lgOut
	lgApp.ErrWriter = &lgErr
	err = lgApp.RunContext(ctx, []string{"jobs-client", "logs",
		"--server", serverID, "--addr", serverAddr,
		"--node", nodeName("buildrun", 1)})
	if err != nil {
		t.Fatalf("logs: %v\nstderr:\n%s", err, lgErr.String())
	}
	if lgOut.Len() != 0 {
		t.Fatalf("logs stdout must be empty for a node without output:\n%q", lgOut.String())
	}
	if !strings.Contains(lgErr.String(), "(no captured output)") {
		t.Fatalf("logs stderr missing empty-view note:\n%s", lgErr.String())
	}
}

// TestRemoteTestEndToEnd drives remote-test against a real server with a
// pre-seeded passed test (no runner): the test definition's K is what the
// server fast-paths on, the verdict names the request a test, and the
// memo (test-passed:F → KT, plus test-passed:<KT>) is pulled home.
func TestRemoteTestEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	ready := make(chan *serve.Server, 1)
	done := make(chan error, 1)
	srvCtx, stopSrv := context.WithCancel(ctx)
	t.Cleanup(func() {
		stopSrv()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("server run: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("server did not shut down")
		}
	})
	go func() {
		done <- serve.Run(srvCtx, serve.Options{
			DataDir:  t.TempDir(),
			BindAddr: netip.AddrPortFrom(netip.IPv6Loopback(), 0),
			Ready:    func(s *serve.Server) { ready <- s },
		})
	}()
	var srv *serve.Server
	select {
	case srv = <-ready:
	case <-time.After(30 * time.Second):
		t.Fatal("server not ready")
	}
	serverID := srv.Endpoint.ID().String()
	serverAddr := srv.Endpoint.LocalAddr().String()

	srcDir := t.TempDir()
	for name, content := range map[string]string{"BUILD.jobs": "# build\n", "TESTS.jobs": "# tests\n"} {
		if err := os.WriteFile(filepath.Join(srcDir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	treeKey, err := srv.Store.IngestSourceDir(ctx, srcDir)
	if err != nil {
		t.Fatal(err)
	}
	params, err := parseParams(nil)
	if err != nil {
		t.Fatal(err)
	}
	_, k, err := definitionFor(treeKey, "", "", "linux/amd64", params, true)
	if err != nil {
		t.Fatal(err)
	}
	_, bk, err := treeDefinition(treeKey, "", "", "linux/amd64", params)
	if err != nil {
		t.Fatal(err)
	}
	if k == bk {
		t.Fatal("test definition has the build's K")
	}
	// Pre-seed a passed test: build-from:K → F, test-passed:F → KT (any
	// complete tree stands in for the KT tree).
	ktDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(ktDir, "job.cbor"), []byte("kt"), 0o644); err != nil {
		t.Fatal(err)
	}
	kt, err := srv.Store.IngestDir(ctx, ktDir)
	if err != nil {
		t.Fatal(err)
	}
	f := treeKey // stand-in F
	if err := srv.Store.PutRef(ctx, "build-from:"+k.String(), f); err != nil {
		t.Fatal(err)
	}
	if err := srv.Store.PutRef(ctx, cover.TestPassedRef(f), kt); err != nil {
		t.Fatal(err)
	}

	dataDir := t.TempDir()
	app := App()
	var out, errOut bytes.Buffer
	app.Writer = &out
	app.ErrWriter = &errOut
	err = app.RunContext(ctx, []string{"jobs-client", "remote-test",
		"--server", serverID,
		"--addr", serverAddr,
		"--data-dir", dataDir,
		"--source", srcDir,
		"--platform", "linux/amd64",
	})
	if err != nil {
		t.Fatalf("remote-test: %v\nstderr:\n%s", err, errOut.String())
	}
	if !strings.Contains(out.String(), "test:   "+f.String()) || !strings.Contains(out.String(), "passed: "+kt.String()) {
		t.Fatalf("stdout missing test/passed keys:\n%s\nstderr:\n%s", out.String(), errOut.String())
	}
	if !strings.Contains(errOut.String(), "(test "+k.String()+")") {
		t.Fatalf("stderr missing the test submit line:\n%s", errOut.String())
	}
	if !strings.Contains(errOut.String(), "test: DONE · ") {
		t.Fatalf("stderr missing the test verdict line:\n%s", errOut.String())
	}

	// The memo was pulled home: the F alias and the KT-level marker.
	cst, err := amber.Open(filepath.Join(dataDir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer cst.Close()
	for _, name := range []string{cover.TestPassedRef(f), cover.TestPassedRef(kt)} {
		got, ok, err := cst.GetKey(ctx, name)
		if err != nil || !ok || got != kt {
			t.Fatalf("local %s: key=%s ok=%v err=%v (want %s)", name, got, ok, err, kt)
		}
	}

	// status marks the request as a test.
	stApp := App()
	var stOut, stErr bytes.Buffer
	stApp.Writer = &stOut
	stApp.ErrWriter = &stErr
	if err := stApp.RunContext(ctx, []string{"jobs-client", "status", "--server", serverID, "--addr", serverAddr}); err != nil {
		t.Fatalf("status: %v\nstderr:\n%s", err, stErr.String())
	}
	if !strings.Contains(stOut.String(), "KIND") || !strings.Contains(stOut.String(), "  test  ") {
		t.Fatalf("status output lacks the test kind:\n%s", stOut.String())
	}
}
