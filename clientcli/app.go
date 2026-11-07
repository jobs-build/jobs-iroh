// Package clientcli wires the jobs-client command surface: hermetic LOCAL
// builds (`build`), build-then-execute (`run`), the interactive build-sandbox
// shell (`develop`, flock held for the whole session), and OCI image export
// (`image`, by --source or by build key) over the embedded amber-store-core
// store under --data-dir, plus the remote surface against a jobs-server —
// `remote-build` (push source, submit, watch, pull outputs home), `watch`,
// and the thin `admin` frame calls (stats/fleet/requests/refs). It is the
// jobs-iroh port of jobs' internal/jobscli with the store seam swapped
// (methods on *amber.Store) and the signing/grant plumbing deleted — refs are
// plain unsigned refstore records; transport identity is the server's iroh
// endpoint key.
package clientcli

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/urfave/cli/v2"

	"github.com/jobs-build/jobs-iroh/version"
)

// signalCtx derives the command context cancelled on Ctrl-C / SIGTERM so an
// in-flight build tears down its sandbox and the action's defers (store
// close, flock release) still run.
func signalCtx(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
}

// App builds the jobs-client CLI. A bare invocation prints help.
func App() *cli.App {
	return &cli.App{
		Name:    "jobs-client",
		Version: version.Version,
		Usage:   "jobs-iroh end-user CLI: hermetic local builds and remote builds against a jobs-server",
		Commands: []*cli.Command{
			buildCmd(),
			runCmd(),
			developCmd(),
			testCmd(),
			imageCmd(),
			remoteBuildCmd(),
			remoteTestCmd(),
			watchCmd(),
			logsCmd(),
			diagnoseCmd(),
			statusCmd(),
			adminCmd(),
			gcCmd(),
			tuiCmd(),
		},
	}
}
