package clientcli

import (
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/jobs-build/jobs-iroh/api"
	"github.com/jobs-build/jobs-iroh/wire"
)

// statusCmd is the one-shot server overview: the live requests table and the
// runner fleet table, as plain aligned text (tabwriter, no ANSI, no TUI) —
// greppable and script-friendly, unlike the interactive `tui`.
func statusCmd() *cli.Command {
	return &cli.Command{
		Name:  "status",
		Usage: "one-shot server status: build requests and the runner fleet, as plain tables",
		Flags: serverFlags(),
		Action: func(c *cli.Context) error {
			ctx, stop := signalCtx(c.Context)
			defer stop()
			ac, err := dialAPI(ctx, c.String("server"), c.StringSlice("addr"), alpnAdmin)
			if err != nil {
				return err
			}
			defer ac.Close()
			var rr api.RequestsReply
			if err := ac.call(ctx, api.TRequests, nil, api.TRequestsReply, &rr); err != nil {
				return err
			}
			var fl api.FleetReply
			if err := ac.call(ctx, api.TFleet, nil, api.TFleetReply, &fl); err != nil {
				return err
			}
			writeStatus(c.App.Writer, rr.Requests, fl.Runners, time.Now())
			return nil
		},
	}
}

// writeStatus renders the two tables. now is a parameter so tests get stable
// ages.
func writeStatus(w io.Writer, reqs []api.RequestInfo, runners []wire.RunnerInfo, now time.Time) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "REQUEST\tKIND\tK\tPHASE\tDONE\tRUN\tFAIL\tWAIT\tAGE")
	if len(reqs) == 0 {
		fmt.Fprintln(tw, "(none)")
	}
	for _, r := range reqs {
		cnt := r.Counts
		kind := "build"
		if r.Test {
			kind = "test"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d/%d\t%d\t%d\t%d\t%s\n",
			r.RequestID, kind, shortHex(r.K), r.Phase,
			cnt.Done, cnt.Total, cnt.Running, cnt.Failed, cnt.Waiting,
			formatDur(now.Sub(time.Unix(0, r.CreatedNs))))
	}
	tw.Flush()

	fmt.Fprintln(w)
	tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "RUNNER\tPLATFORM\tSIZE\tINFLIGHT\tFREE CPU\tFREE MEM\tSEEN")
	if len(runners) == 0 {
		fmt.Fprintln(tw, "(none)")
	}
	for _, r := range runners {
		name := r.Name
		if name == "" {
			name = r.ID
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%dm\t%s\t%s ago\n",
			name, r.Platform, r.Size, r.InFlight,
			r.FreeCPU, formatBytes(r.FreeMem),
			formatDur(now.Sub(time.Unix(0, r.SeenNs))))
	}
	tw.Flush()
}
