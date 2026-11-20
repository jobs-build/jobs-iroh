package clientcli

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/jobs-build/jobs-iroh/api"
)

// deleteCmd and retryCmd are the request-lifecycle commands a failed build
// needs from the CLI (issue #22: only the TUI could delete a request, and a
// failed node stays failed while any request holds it). Both take explicit
// --request-id values (repeatable) or --failed for every failed request.

func requestSelectFlags() []cli.Flag {
	return append(serverFlags(),
		&cli.StringSliceFlag{Name: "request-id", Usage: "request ID to act on (repeatable)"},
		&cli.BoolFlag{Name: "failed", Usage: "act on every failed request on the server"},
	)
}

func deleteCmd() *cli.Command {
	return &cli.Command{
		Name:  "delete",
		Usage: "cancel and forget build/test requests, releasing what they held (a failed node is held failed while any request wants it)",
		Flags: requestSelectFlags(),
		Action: func(c *cli.Context) error {
			return forRequests(c, "deleted", func(ctx context.Context, ac *apiConn, id string) (string, error) {
				return "", ac.call(ctx, api.TDelete, api.CancelRequest{RequestID: id}, api.TOK, nil)
			})
		},
	}
}

func retryCmd() *cli.Command {
	return &cli.Command{
		Name:  "retry",
		Usage: "re-run failed build/test requests in place: their failed nodes run again with a fresh retry budget (watch re-attaches by the same ID)",
		Flags: requestSelectFlags(),
		Action: func(c *cli.Context) error {
			return forRequests(c, "retried", func(ctx context.Context, ac *apiConn, id string) (string, error) {
				var r api.RetryReply
				if err := ac.call(ctx, api.TRetry, api.CancelRequest{RequestID: id}, api.TRetryReply, &r); err != nil {
					return "", err
				}
				return fmt.Sprintf(" (%d failed nodes re-activated)", r.Reactivated), nil
			})
		},
	}
}

// forRequests resolves the selected request IDs (--request-id values, plus
// every failed request with --failed) and applies op to each over one admin
// connection, reporting per request; any failure makes the exit status 1
// after the rest were tried.
func forRequests(c *cli.Context, verb string, op func(context.Context, *apiConn, string) (string, error)) error {
	ids := c.StringSlice("request-id")
	if len(ids) == 0 && !c.Bool("failed") {
		return cli.Exit("name the requests with --request-id (repeatable) or pass --failed", 2)
	}
	ctx, stop := signalCtx(c.Context)
	defer stop()
	ac, err := dialAPI(ctx, c.String("server"), c.StringSlice("addr"), alpnAdmin)
	if err != nil {
		return err
	}
	defer ac.Close()
	if c.Bool("failed") {
		var rr api.RequestsReply
		if err := ac.call(ctx, api.TRequests, nil, api.TRequestsReply, &rr); err != nil {
			return err
		}
		for _, r := range rr.Requests {
			if r.Phase == "failed" {
				ids = append(ids, r.RequestID)
			}
		}
	}
	w := c.App.Writer
	if len(ids) == 0 {
		fmt.Fprintln(w, "no failed requests")
		return nil
	}
	failed := 0
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		detail, err := op(ctx, ac, id)
		if err != nil {
			failed++
			fmt.Fprintf(c.App.ErrWriter, "%s: %v\n", id, err)
			continue
		}
		fmt.Fprintf(w, "%s %s%s\n", verb, id, detail)
	}
	if failed > 0 {
		return cli.Exit(fmt.Sprintf("%d of %d requests not %s", failed, len(seen), verb), 1)
	}
	return nil
}
