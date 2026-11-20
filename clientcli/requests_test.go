package clientcli

import (
	"bytes"
	"context"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/jobs-build/jobs-iroh/api"
	"github.com/jobs-build/jobs-iroh/serve"
)

// TestDeleteAndRetryCommands drives `jobs-client delete` and `retry` (issue
// #22: a failed request could only be cleared from the TUI) against a real
// server holding one request.
func TestDeleteAndRetryCommands(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	ready := make(chan *serve.Server, 1)
	done := make(chan error, 1)
	srvCtx, stopSrv := context.WithCancel(ctx)
	t.Cleanup(func() {
		stopSrv()
		select {
		case <-done:
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

	// A request whose closure is already done (doneness fast path), so the
	// submit needs no runner.
	srcDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcDir, "BUILD.jobs"), []byte("# recipe\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	treeKey, err := srv.Store.IngestSourceDir(ctx, srcDir)
	if err != nil {
		t.Fatal(err)
	}
	params, err := parseParams(nil)
	if err != nil {
		t.Fatal(err)
	}
	canon, k, err := treeDefinition(treeKey, "", "", "linux/amd64", params)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"build-from:" + k.String(), "build-output:" + treeKey.String(), "build-output-deps:" + treeKey.String()} {
		if err := srv.Store.PutRef(ctx, name, treeKey); err != nil {
			t.Fatal(err)
		}
	}
	sub, err := srv.Sched.Submit(ctx, api.SubmitRequest{Def: canon})
	if err != nil {
		t.Fatal(err)
	}

	run := func(args ...string) (string, string, error) {
		app := App()
		var out, errOut bytes.Buffer
		app.Writer, app.ErrWriter = &out, &errOut
		app.ExitErrHandler = func(*cli.Context, error) {} // keep os.Exit out of tests
		full := append([]string{"jobs-client", args[0], "--server", srv.Endpoint.ID().String(),
			"--addr", srv.Endpoint.LocalAddr().String()}, args[1:]...)
		err := app.RunContext(ctx, full)
		return out.String(), errOut.String(), err
	}
	exitCode := func(err error) int {
		var ec cli.ExitCoder
		if errors.As(err, &ec) {
			return ec.ExitCode()
		}
		return -1
	}

	if _, _, err := run("delete"); exitCode(err) != 2 {
		t.Fatalf("delete without a selection: %v, want exit 2", err)
	}
	if out, _, err := run("retry", "--failed"); err != nil || !strings.Contains(out, "no failed requests") {
		t.Fatalf("retry --failed with none failed: %q %v", out, err)
	}
	if _, stderr, err := run("retry", "--request-id", sub.RequestID); exitCode(err) != 1 || !strings.Contains(stderr, "not failed") {
		t.Fatalf("retry of a done request: %v / %q, want exit 1 'not failed'", err, stderr)
	}
	if out, stderr, err := run("delete", "--request-id", sub.RequestID); err != nil || !strings.Contains(out, "deleted "+sub.RequestID) {
		t.Fatalf("delete: %q %q %v", out, stderr, err)
	}
	if reqs := srv.Sched.Requests(); len(reqs) != 0 {
		t.Fatalf("requests after delete: %+v", reqs)
	}
	if _, stderr, err := run("delete", "--request-id", sub.RequestID); exitCode(err) != 1 || !strings.Contains(stderr, "not found") {
		t.Fatalf("delete of a gone request: %v / %q", err, stderr)
	}
}
