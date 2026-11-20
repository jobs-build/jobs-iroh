package clientcli

import (
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/jobs-build/jobs-iroh/api"
	"github.com/jobs-build/jobs-iroh/tui"
)

// serverFlags are the connection flags shared by every remote observation
// command.
func serverFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{Name: "server", Required: true, EnvVars: []string{"JOBS_SERVER"}, Usage: "endpoint ID of the jobs-server"},
		&cli.StringSliceFlag{Name: "addr", EnvVars: []string{"JOBS_SERVER_ADDR"}, Usage: "direct server address host:port (repeatable; skips discovery and relays)"},
	}
}

// --- watch ---

func watchCmd() *cli.Command {
	return &cli.Command{
		Name:  "watch",
		Usage: "stream one build request's progress until it finishes",
		Flags: append(serverFlags(),
			&cli.StringFlag{Name: "request-id", Required: true, Usage: "request ID printed at submit time"},
			&cli.BoolFlag{Name: "no-logs", Usage: "do not stream the output of running build steps (classic view only)"},
			&cli.BoolFlag{Name: "no-tui", Usage: "disable the full-screen build view; use the classic progress block"},
		),
		Action: func(c *cli.Context) error {
			ctx, stop := signalCtx(c.Context)
			defer stop()
			bc, err := dialAPI(ctx, c.String("server"), c.StringSlice("addr"), alpnBuild)
			if err != nil {
				return err
			}
			defer bc.Close()
			lv := cliLiveView(c) // one shared view: tracker Printlns and the
			// watch block must go through the same cursor arithmetic

			// Full-screen build view on an interactive terminal; classic
			// fallback for old servers and already-terminal requests.
			if useBuildTUI(c) {
				reqID := c.String("request-id")
				handled, out, terr := runBuildTUI(ctx, c, bc, reqID, "", lv)
				if handled {
					if terr != nil {
						return terr
					}
					return finishWatchTUI(c, out, reqID)
				}
			}
			var tracker *logTracker
			if !c.Bool("no-logs") {
				tracker = newLogTracker(ctx, bc, lv)
				defer tracker.close()
			}
			final, err := streamWatch(ctx, bc, c.String("request-id"), lv, tracker)
			if err != nil {
				return err
			}
			if final.Phase == "failed" {
				// Same failure UX as remote-build: the failing nodes'
				// stored log tails, then the one-line summary.
				printFailureLogs(ctx, bc, final, errWriter(c), tracker.streamedNodes())
				fmt.Fprintf(errWriter(c), "full failure report (all attempts, durable): jobs-client diagnose --server %s --request %s\n",
					c.String("server"), c.String("request-id"))
				if s := failureSummary(final); s != "" {
					return cli.Exit("request failed: "+s, 1)
				}
			}
			if final.Phase != "done" {
				return cli.Exit("request "+final.Phase, 1)
			}
			return nil
		},
	}
}

// finishWatchTUI maps a build-view outcome onto watch's exit contract
// (like remote-build's finishTUI, minus the pull-home).
func finishWatchTUI(c *cli.Context, out tui.BuildOutcome, requestID string) error {
	ew := errWriter(c)
	switch {
	case out.Detached:
		fmt.Fprintf(ew, "detached — re-attach: jobs-client watch --server %s --request-id %s\n", c.String("server"), requestID)
		return nil
	case out.HaveFinal && out.Final.Phase == "done":
		return nil
	case out.HaveFinal && out.Final.Phase == "failed":
		fmt.Fprintf(ew, "full failure report (all attempts, durable): jobs-client diagnose --server %s --request %s\n", c.String("server"), requestID)
		if s := failureSummary(out.Final); s != "" {
			return cli.Exit("request failed: "+s, 1)
		}
		return cli.Exit("request failed", 1)
	default:
		return cli.Exit("request cancelled", 1)
	}
}

// --- admin ---

// adminCmd is the thin frame-call admin surface: it doubles as the admin
// tooling until the TUI milestone.
func adminCmd() *cli.Command {
	return &cli.Command{
		Name:  "admin",
		Usage: "observe a jobs-server: stats, runner fleet, live requests, refs",
		Subcommands: []*cli.Command{
			adminStatsCmd(),
			adminFleetCmd(),
			adminRequestsCmd(),
			adminRefsCmd(),
			adminGCCmd(),
			adminPinCmd(true),
			adminPinCmd(false),
		},
	}
}

// adminCall dials the admin ALPN and performs one request/reply exchange.
func adminCall(c *cli.Context, t string, body any, wantType string, reply any) error {
	ctx, stop := signalCtx(c.Context)
	defer stop()
	ac, err := dialAPI(ctx, c.String("server"), c.StringSlice("addr"), alpnAdmin)
	if err != nil {
		return err
	}
	defer ac.Close()
	return ac.call(ctx, t, body, wantType, reply)
}

func adminStatsCmd() *cli.Command {
	return &cli.Command{
		Name:  "stats",
		Usage: "server store/scheduler statistics",
		Flags: serverFlags(),
		Action: func(c *cli.Context) error {
			var st api.StatsReply
			if err := adminCall(c, api.TStats, nil, api.TStatsReply, &st); err != nil {
				return err
			}
			w := c.App.Writer
			fmt.Fprintf(w, "store bytes:   %d\n", st.StoreBytes)
			fmt.Fprintf(w, "refs:          %d\n", st.RefCount)
			fmt.Fprintf(w, "requests:      %d\n", st.Requests)
			fmt.Fprintf(w, "nodes tracked: %d\n", st.NodesTracked)
			fmt.Fprintf(w, "uptime:        %s\n", time.Duration(st.UptimeNs).Round(time.Second))
			if st.GC != nil {
				g := st.GC
				fmt.Fprintf(w, "gc retention:  %s\n", time.Duration(g.RetentionNs))
				if g.LastSweepNs == 0 {
					fmt.Fprintf(w, "gc sweep:      never\n")
				} else {
					pct := 0.0
					if tot := g.LiveBytes + g.GarbageBytes; tot > 0 {
						pct = 100 * float64(g.GarbageBytes) / float64(tot)
					}
					fmt.Fprintf(w, "gc sweep:      %s ago (expired %d, total %d)\n",
						time.Since(time.Unix(0, g.LastSweepNs)).Round(time.Second), g.ExpiredLast, g.ExpiredTotal)
					fmt.Fprintf(w, "gc store:      live %d, garbage %d (%.1f%%), pinned %d\n",
						g.LiveBytes, g.GarbageBytes, pct, g.Pinned)
				}
				if g.LastCycleNs != 0 {
					fmt.Fprintf(w, "gc last cycle: %s ago, reaped %d packs, freed %d bytes in %s\n",
						time.Since(time.Unix(0, g.LastCycleNs)).Round(time.Second),
						g.LastCycleReaped, g.LastCycleFreed, time.Duration(g.LastCycleWallNs).Round(time.Millisecond))
				}
				if g.LastError != "" {
					fmt.Fprintf(w, "gc last error: %s\n", g.LastError)
				}
				if g.Halted != "" {
					fmt.Fprintf(w, "gc HALTED:     %s\n", g.Halted)
				}
			}
			for _, warn := range st.Warnings {
				fmt.Fprintf(w, "WARNING:       %s\n", warn)
			}
			return nil
		},
	}
}

func adminFleetCmd() *cli.Command {
	return &cli.Command{
		Name:  "fleet",
		Usage: "connected runner fleet",
		Flags: serverFlags(),
		Action: func(c *cli.Context) error {
			var fl api.FleetReply
			if err := adminCall(c, api.TFleet, nil, api.TFleetReply, &fl); err != nil {
				return err
			}
			w := c.App.Writer
			if len(fl.Runners) == 0 {
				fmt.Fprintln(w, "no runners")
				return nil
			}
			for _, r := range fl.Runners {
				fmt.Fprintf(w, "%s  %-16s %-12s %-8s inflight=%d freeCPU=%dm freeMem=%d seen=%s ago\n",
					r.ID, r.Name, r.Platform, r.Size, r.InFlight, r.FreeCPU, r.FreeMem,
					time.Since(time.Unix(0, r.SeenNs)).Round(time.Second))
			}
			return nil
		},
	}
}

func adminRequestsCmd() *cli.Command {
	return &cli.Command{
		Name:  "requests",
		Usage: "live build requests, newest first",
		Flags: serverFlags(),
		Action: func(c *cli.Context) error {
			var rr api.RequestsReply
			if err := adminCall(c, api.TRequests, nil, api.TRequestsReply, &rr); err != nil {
				return err
			}
			w := c.App.Writer
			if len(rr.Requests) == 0 {
				fmt.Fprintln(w, "no requests")
				return nil
			}
			for _, r := range rr.Requests {
				fmt.Fprintf(w, "%s  %-9s %-12s done %d/%d failed %d  k=%s  age=%s\n",
					r.RequestID, r.Phase, r.Platform,
					r.Counts.Done, r.Counts.Total, r.Counts.Failed,
					shortHex(r.K),
					time.Since(time.Unix(0, r.CreatedNs)).Round(time.Second))
			}
			return nil
		},
	}
}

func adminRefsCmd() *cli.Command {
	return &cli.Command{
		Name:  "refs",
		Usage: "browse server refs by prefix",
		Flags: append(serverFlags(),
			&cli.StringFlag{Name: "prefix", Usage: "only refs whose name starts with this prefix"},
			&cli.IntFlag{Name: "limit", Usage: "cap the listing (0 = all)"},
		),
		Action: func(c *cli.Context) error {
			// The server answers one page per call (api.MaxRefsPage at most):
			// walk the pages with the After cursor until the listing or the
			// --limit runs out, printing as we go — a store of any size
			// lists without one giant frame (issue #22).
			ctx, stop := signalCtx(c.Context)
			defer stop()
			ac, err := dialAPI(ctx, c.String("server"), c.StringSlice("addr"), alpnAdmin)
			if err != nil {
				return err
			}
			defer ac.Close()
			w := c.App.Writer
			limit := c.Int("limit")
			printed := 0
			after := ""
			for {
				req := api.RefsRequest{Prefix: c.String("prefix"), After: after}
				if limit > 0 {
					req.Limit = min(limit-printed, api.MaxRefsPage)
				}
				var reply api.RefsReply
				if err := ac.call(ctx, api.TRefs, req, api.TRefsReply, &reply); err != nil {
					return err
				}
				for _, r := range reply.Refs {
					if after != "" && r.Name <= after {
						continue // an old server ignores After: drop what we printed
					}
					printRefRow(w, r)
					printed++
					if limit > 0 && printed == limit {
						return nil
					}
				}
				if !reply.More || len(reply.Refs) == 0 {
					break
				}
				after = reply.Refs[len(reply.Refs)-1].Name
			}
			if printed == 0 {
				fmt.Fprintln(w, "no refs")
			}
			return nil
		},
	}
}

// printRefRow prints one `admin refs` line: name, key, last access, pin.
func printRefRow(w io.Writer, r api.RefInfo) {
	access, pin := "-", ""
	if r.LastAccessNs > 0 {
		access = time.Since(time.Unix(0, r.LastAccessNs)).Round(time.Second).String() + " ago"
	}
	if r.Pinned {
		pin = "pin"
	}
	fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", r.Name, hex.EncodeToString(r.Key), access, pin)
}

// shortHex renders a key's first 8 hex chars for table output.
func shortHex(b []byte) string {
	s := hex.EncodeToString(b)
	if len(s) > 8 {
		return s[:8]
	}
	return s
}

func adminGCCmd() *cli.Command {
	return &cli.Command{
		Name:  "gc",
		Usage: "run one GC sweep+cycle now and print the report",
		Flags: append(serverFlags(),
			&cli.Float64Flag{Name: "garbage", Value: -1,
				Usage: "force the pack selection line 0..1 (default: server policy)"},
		),
		Action: func(c *cli.Context) error {
			req := api.GCRequest{}
			if g := c.Float64("garbage"); g >= 0 {
				req.Garbage = &g
			}
			var st api.GCStats
			if err := adminCall(c, api.TGC, req, api.TGCReply, &st); err != nil {
				return err
			}
			w := c.App.Writer
			pct := 0.0
			if tot := st.LiveBytes + st.GarbageBytes; tot > 0 {
				pct = 100 * float64(st.GarbageBytes) / float64(tot)
			}
			fmt.Fprintf(w, "disk:      %d bytes\n", st.DiskBytes)
			fmt.Fprintf(w, "refs:      %d (%d pinned, %d expired this sweep)\n", st.RefCount, st.Pinned, st.ExpiredLast)
			fmt.Fprintf(w, "store:     live %d, garbage %d (%.1f%%)\n", st.LiveBytes, st.GarbageBytes, pct)
			if st.LastCycleNs != 0 {
				fmt.Fprintf(w, "cycle:     reaped %d packs, freed %d bytes in %s\n",
					st.LastCycleReaped, st.LastCycleFreed, time.Duration(st.LastCycleWallNs).Round(time.Millisecond))
			}
			if st.LastError != "" {
				return cli.Exit("gc cycle error: "+st.LastError, 1)
			}
			return nil
		},
	}
}

func adminPinCmd(pin bool) *cli.Command {
	name, usage, frame := "pin", "keep a ref forever (exempt from GC expiry)", api.TPin
	if !pin {
		name, usage, frame = "unpin", "clear a ref's pin; it then lives by its access clock", api.TUnpin
	}
	return &cli.Command{
		Name:      name,
		Usage:     usage,
		ArgsUsage: "<ref-name>",
		Flags:     serverFlags(),
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return cli.Exit("exactly one ref name required", 2)
			}
			var row api.RefInfo
			if err := adminCall(c, frame, api.PinRequest{Name: c.Args().First()}, api.TPinReply, &row); err != nil {
				return err
			}
			state := "unpinned"
			if row.Pinned {
				state = "pinned"
			}
			fmt.Fprintf(c.App.Writer, "%s\t%s\n", row.Name, state)
			return nil
		},
	}
}
