package clientcli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/amber-store/core/key"
	"github.com/urfave/cli/v2"

	"github.com/jobs-build/jobs-iroh/amberclient"
	"github.com/jobs-build/jobs-iroh/api"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/runner"
	"github.com/jobs-build/jobs-iroh/tui"
	"github.com/jobs-build/jobs-iroh/wire"
)

// treeDefinition constructs the canonical build Definition for a pushed
// source tree: a tree-source Input over the ingested source root, with the
// same dir/params/platform/build-file knobs the local path feeds
// localBuildFrom. IngestSourceDir + these fields are the whole identity —
// the server's buildfrom stage resolves the same env subtree, the same
// recipe override and the same params file, so the remote F equals the F a
// local build of the same source computes (the cache-join invariant).
func treeDefinition(sourceKey key.Key, dir, buildFile, platform string, params []byte) (canon []byte, k key.Key, err error) {
	in, err := builddef.TreeInput(sourceKey)
	if err != nil {
		return nil, key.Key{}, err
	}
	// Every subdir build carries widened-context semantics, marked
	// structurally (sibling-sources design §3.2: always-on; old servers
	// reject the unknown field at the submit canonicality check).
	ctxv := 0
	if dir != "" {
		ctxv = builddef.CtxWidened
	}
	def := builddef.Definition{
		Source:    in,
		Dir:       dir,
		Platform:  platform,
		Params:    params,
		BuildFile: buildFile,
		Ctx:       ctxv,
	}
	canon, err = def.Canonical()
	if err != nil {
		return nil, key.Key{}, fmt.Errorf("canonicalize definition: %w", err)
	}
	k, err = def.Key()
	if err != nil {
		return nil, key.Key{}, fmt.Errorf("definition key: %w", err)
	}
	return canon, k, nil
}

// remoteConfig carries the remote-build flags: where the server is, what to
// build, and the optional resource raise for the target build.
type remoteConfig struct {
	server     string
	dataDir    string
	sourceRoot string
	noRepoRoot bool
	source     string
	dir        string
	buildFile  string
	platform   string
	cpu        string
	memory     string
	conns      int
}

// targetLabel is the display name sent with the submit: the resolved
// context dir when the target is a subdir build, else the source root's
// base name. Display-only — identity is unaffected.
func (cfg *remoteConfig) targetLabel() string {
	if cfg.dir != "" {
		return cfg.dir
	}
	return filepath.Base(cfg.source)
}

func remoteBuildCmd() *cli.Command {
	cfg := &remoteConfig{}
	return &cli.Command{
		Name:  "remote-build",
		Usage: "push a source tree to a jobs-server, build it there, pull the output home",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "server", Required: true, EnvVars: []string{"JOBS_SERVER"}, Usage: "endpoint ID of the jobs-server", Destination: &cfg.server},
			&cli.StringSliceFlag{Name: "addr", EnvVars: []string{"JOBS_SERVER_ADDR"}, Usage: "direct server address host:port (repeatable; skips discovery and relays)"},
			&cli.StringFlag{Name: "data-dir", EnvVars: []string{"JOBS_DATA_DIR"}, Value: defaultDataDir(), Usage: "client data directory (embedded store + cache)", Destination: &cfg.dataDir},
			&cli.StringFlag{Name: "source", Usage: "source directory to ingest and push as the build source (default: the nearest ancestor of the current directory holding BUILD.jobs, searched up to the repo root)", Destination: &cfg.source},
			&cli.StringFlag{Name: "dir", Usage: "build root within the source (where BUILD.jobs lives)", Destination: &cfg.dir},
			&cli.StringFlag{Name: "source-root", Usage: "explicit context root (--source must live under it); default: the git repo root above --source", Destination: &cfg.sourceRoot},
			&cli.BoolFlag{Name: "no-repo-root", Usage: "disable the git-root context default (ingest --source itself)", Destination: &cfg.noRepoRoot},
			&cli.StringFlag{Name: "build-file", Usage: "recipe path relative to dir (default BUILD.jobs)", Destination: &cfg.buildFile},
			&cli.StringFlag{Name: "platform", EnvVars: []string{"JOBS_PLATFORM"}, Value: runner.Platform(), Usage: "target platform, e.g. linux/amd64", Destination: &cfg.platform},
			&cli.StringSliceFlag{Name: "param", Usage: "key=value build param (repeatable)"},
			&cli.StringFlag{Name: "cpu", Usage: "raise the target build's CPU requirement (e.g. 2000m)", Destination: &cfg.cpu},
			&cli.StringFlag{Name: "memory", Usage: "raise the target build's memory requirement (e.g. 4Gi)", Destination: &cfg.memory},
			&cli.BoolFlag{Name: "no-logs", Usage: "do not stream the output of running build steps (classic view only)"},
			&cli.BoolFlag{Name: "no-tui", Usage: "disable the full-screen build view; use the classic progress block"},
			&cli.IntFlag{Name: "conns", Value: 4, Usage: "parallel connections for the push/pull transfers (1-16; 1 disables sharding)", Destination: &cfg.conns},
		},
		Action: cfg.run,
	}
}

// run drives the four remote-build phases (jobs remote-build's shape): ingest
// + push the source tree, submit the canonical definition, watch snapshots to
// terminal (SIGINT sends a cancel frame first), then pull the output home.
// Progress renders through the liveView: an in-place block on a TTY stderr,
// plain change-lines otherwise.
func (cfg *remoteConfig) run(c *cli.Context) error {
	ctx, cancel := context.WithCancel(c.Context)
	defer cancel()
	ew := errWriter(c)
	lv := cliLiveView(c)
	addrs := c.StringSlice("addr")

	params, err := parseParams(c.StringSlice("param"))
	if err != nil {
		return err
	}

	// SHARED lock: push/pull only ever add objects and write fresh ref
	// names — no local build mutates shared state under us. MaybeGC
	// self-disables under this shared lock for exactly that reason (its
	// sweep does mutate shared state).
	cs, err := openClientStore(cfg.dataDir, lockShared)
	if err != nil {
		return err
	}
	defer cs.Close()

	// Same two calls in the same order as the local path (localConfig
	// .resolveContext) — divergence would silently kill the local↔remote F
	// join.
	if root, rdir, rerr := resolveSource(cfg.source, cfg.dir, cfg.sourceRoot, cfg.buildFile, cfg.noRepoRoot); rerr != nil {
		return rerr
	} else {
		cfg.source, cfg.dir = root, rdir
	}
	lv.Println(contextLine(cfg.source, cfg.dir, cfg.buildFile))
	sourceKey, err := cs.Store.IngestSourceDir(ctx, cfg.source)
	if err != nil {
		return fmt.Errorf("ingest source %s: %w", cfg.source, err)
	}
	canon, k, err := treeDefinition(sourceKey, cfg.dir, cfg.buildFile, cfg.platform, params)
	if err != nil {
		return err
	}

	// max(1, …): an explicit --conns 0 means "off", never the library's
	// 0-means-default (which would silently shard 4-way).
	ac, err := amberclient.Dial(ctx, amberclient.Options{
		EndpointID: cfg.server, Addrs: addrs, ALPN: alpnAmberAdmin, Conns: max(1, cfg.conns),
	})
	if err != nil {
		return err
	}
	defer ac.Close()

	scratch, err := clientPushRef()
	if err != nil {
		return err
	}
	lv.Println(fmt.Sprintf("pushing source tree %s", sourceKey))
	push := newXferProgress(lv, "push")
	pushStats, err := ac.PushWithProgress(ctx, cs.Store, scratch, sourceKey, push.cb)
	push.setBytes(pushStats.Bytes)
	push.finish(err == nil)
	if err != nil {
		return err
	}

	bc, err := dialAPI(ctx, cfg.server, addrs, alpnBuild)
	if err != nil {
		return err
	}
	defer bc.Close()

	var res *api.ResourceSpec
	if cfg.cpu != "" || cfg.memory != "" {
		res = &api.ResourceSpec{CPU: cfg.cpu, Memory: cfg.memory}
	}
	var sub api.Submitted
	err = bc.call(ctx, api.TSubmit, api.SubmitRequest{
		Def:        canon,
		Resources:  res,
		ScratchRef: scratch,
		Label:      cfg.targetLabel(),
	}, api.TSubmitted, &sub)
	if err != nil {
		return err
	}
	lv.Println(fmt.Sprintf("submitted request %s (build %s)", sub.RequestID, k))

	// Full-screen build view on an interactive terminal (the view owns
	// ctrl-c: confirm-cancel; q detaches). Falls through to the classic
	// block path against an old server or an already-terminal build.
	if useBuildTUI(c) {
		handled, out, terr := runBuildTUI(ctx, c, bc, sub.RequestID, cfg.targetLabel(), lv)
		if handled {
			if terr != nil {
				return terr
			}
			return cfg.finishTUI(ctx, c, out, ac, cs, k, sub.RequestID, lv)
		}
	}

	// SIGINT/SIGTERM: send the cancel frame on its own stream, then let the
	// watch loop unwind via ctx so every deferred teardown still runs. A
	// second signal falls back to the default handler (kill).
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sig)
	go func() {
		if _, ok := <-sig; !ok {
			return
		}
		signal.Stop(sig)
		lv.Println("interrupt: cancelling build")
		cctx, cdone := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cdone()
		if err := bc.call(cctx, api.TCancel, api.CancelRequest{RequestID: sub.RequestID}, api.TOK, nil); err != nil {
			lv.Println(fmt.Sprintf("cancel failed: %v", err))
		}
		cancel()
	}()

	var tracker *logTracker
	if !c.Bool("no-logs") {
		tracker = newLogTracker(ctx, bc, lv)
		defer tracker.close()
	}
	final, err := streamWatch(ctx, bc, sub.RequestID, lv, tracker)
	if err != nil {
		if ctx.Err() != nil {
			return cli.Exit("build cancelled", 130)
		}
		return err
	}

	switch final.Phase {
	case "done":
		return cfg.pullHome(ctx, c, ac, cs, k, lv)
	case "failed":
		printFailureLogs(ctx, bc, final, ew, tracker.streamedNodes())
		fmt.Fprintf(ew, "re-attach: jobs-client watch --server %s --request-id %s\n", cfg.server, sub.RequestID)
		fmt.Fprintf(ew, "full failure report (all attempts, durable): jobs-client diagnose --server %s --request %s\n", cfg.server, sub.RequestID)
		if s := failureSummary(final); s != "" {
			return cli.Exit("build FAILED: "+s, 1)
		}
		return cli.Exit(fmt.Sprintf("build FAILED (request %s)", sub.RequestID), 1)
	default: // cancelled
		return cli.Exit("build cancelled", 130)
	}
}

// finishTUI maps a build-view outcome onto remote-build's exit contract:
// done → pull home (stdout keys as always); failed → hints + exit 1 (the
// output was inspectable in the view, so no log recap); detach → re-attach
// hint, exit 0; cancelled → exit 130.
func (cfg *remoteConfig) finishTUI(ctx context.Context, c *cli.Context, out tui.BuildOutcome, ac *amberclient.Client, cs *clientStore, k key.Key, requestID string, lv *liveView) error {
	ew := errWriter(c)
	switch {
	case out.Detached:
		fmt.Fprintf(ew, "detached — the build continues; re-attach: jobs-client watch --server %s --request-id %s\n", cfg.server, requestID)
		return nil
	case out.HaveFinal && out.Final.Phase == "done":
		return cfg.pullHome(ctx, c, ac, cs, k, lv)
	case out.HaveFinal && out.Final.Phase == "failed":
		fmt.Fprintf(ew, "re-attach: jobs-client watch --server %s --request-id %s\n", cfg.server, requestID)
		fmt.Fprintf(ew, "full failure report (all attempts, durable): jobs-client diagnose --server %s --request %s\n", cfg.server, requestID)
		if s := failureSummary(out.Final); s != "" {
			return cli.Exit("build FAILED: "+s, 1)
		}
		return cli.Exit(fmt.Sprintf("build FAILED (request %s)", requestID), 1)
	default: // cancelled (own confirm or external)
		return cli.Exit("build cancelled", 130)
	}
}

// pullHome fetches the finished build's refs into the local store —
// build-from:K first (K→F), then build-output:F and build-output-deps:F —
// under one cumulative "[pull]" counter, and prints the identity and output
// key like a local build.
func (cfg *remoteConfig) pullHome(ctx context.Context, c *cli.Context, ac *amberclient.Client, cs *clientStore, k key.Key, lv *liveView) error {
	pull := newXferProgress(lv, "pull")
	var mu sync.Mutex
	var baseDone, baseTotal int // finished pulls' objects, folded into the display
	var bytes int64
	cb := func(done, total int) {
		mu.Lock()
		d, t := baseDone+done, baseTotal+total
		mu.Unlock()
		pull.cb(d, t)
	}
	step := func(name string) (key.Key, error) {
		root, stats, err := ac.PullWithProgress(ctx, cs.Store, name, cb)
		mu.Lock()
		baseDone += stats.Objects
		baseTotal += stats.Objects
		bytes += stats.Bytes
		mu.Unlock()
		return root, err
	}

	f, err := step("build-from:" + k.String())
	if err != nil {
		pull.finish(false)
		return fmt.Errorf("pull build-from: %w", err)
	}
	outKey, err := step("build-output:" + f.String())
	if err != nil {
		pull.finish(false)
		return fmt.Errorf("pull build-output: %w", err)
	}
	if _, err := step("build-output-deps:" + f.String()); err != nil &&
		!errors.Is(err, amberclient.ErrRefNotFound) {
		pull.finish(false)
		return fmt.Errorf("pull build-output-deps: %w", err)
	}
	pull.setBytes(bytes)
	pull.finish(true)
	fmt.Fprintf(c.App.Writer, "build:  %s\n", f.String())
	fmt.Fprintf(c.App.Writer, "output: %s\n", outKey.String())
	// pullHome is the shared tail of both the classic and TUI remote-build
	// paths (run's "done" case and finishTUI's), so sweeping here after it
	// completes covers both without duplicating the call at each caller.
	cs.MaybeGC(ctx)
	return nil
}

// streamWatch opens a watch stream and renders coalesced snapshots through
// the live view until the terminal snapshot, which it returns after
// collapsing the block to a one-line verdict. A TTY gets the in-place block
// (counts header, running rows with server-computed elapsed, failure rows);
// a non-TTY gets one change-line per snapshot delta. A non-nil tracker
// follows each snapshot's running nodes' output alongside.
func streamWatch(ctx context.Context, bc *apiConn, requestID string, lv *liveView, tracker *logTracker) (api.Snapshot, error) {
	stream, stop, err := bc.openRequest(ctx, api.TWatch, api.WatchRequest{RequestID: requestID})
	if err != nil {
		return api.Snapshot{}, err
	}
	defer stop()
	defer amberclient.CloseStream(stream) // full termination — see apiConn.call
	defer lv.Collapse("")                 // erase any leftover block on every exit path

	start := time.Now()

	// Once-a-second block redraw between snapshots (liveProgress's refresh
	// pattern): the server pushes snapshots only on state changes, so this is
	// what keeps elapsed ticking through a long quiet compile and restores
	// the block after log Printlns scroll it away. blockSnap guards the
	// redraws: false until the first snapshot and from the terminal one on,
	// so the refresher can never resurrect a collapsed block.
	var mu sync.Mutex
	var lastSnap api.Snapshot
	var blockSnap bool
	defer func() { mu.Lock(); blockSnap = false; mu.Unlock() }() // before the Collapse defers (LIFO)
	if lv.IsTTY() {
		done := make(chan struct{})
		defer close(done)
		go func() {
			t := time.NewTicker(time.Second)
			defer t.Stop()
			for {
				select {
				case <-done:
					return
				case <-t.C:
					mu.Lock()
					if blockSnap {
						lv.Update(snapshotBlock(lastSnap, time.Since(start)))
					}
					mu.Unlock()
				}
			}
		}()
	}

	var last string
	for {
		rt, body, err := api.ReadFrame(stream)
		if err != nil {
			if ctx.Err() != nil {
				return api.Snapshot{}, ctx.Err()
			}
			return api.Snapshot{}, fmt.Errorf("watch stream: %w", err)
		}
		var snap api.Snapshot
		if err := decodeReply(rt, body, api.TSnapshot, &snap); err != nil {
			return api.Snapshot{}, err
		}
		tracker.sync(snap)
		if lv.IsTTY() {
			mu.Lock()
			lastSnap, blockSnap = snap, !snap.Terminal
			lv.Update(snapshotBlock(snap, time.Since(start)))
			mu.Unlock()
		} else if line := snapshotChangeLine(snap); line != last {
			lv.Println(line)
			last = line
		}
		if snap.Terminal {
			tracker.close() // drain trailing output above the verdict line
			lv.Collapse(watchSummary(lv, snap.Phase, time.Since(start)))
			return snap, nil
		}
	}
}

// labelNode renders a node with its display label when one is known —
// "apk_acl_libs (buildrun:22413ab6)" — and falls back to the bare
// kind:keyprefix form otherwise.
func labelNode(name, label string) string {
	if label == "" {
		return shortNode(name)
	}
	return label + " (" + shortNode(name) + ")"
}

// shortNode renders a node name as kind:keyprefix for one-line status output.
func shortNode(name string) string {
	kind, k, err := wire.ParseNodeName(name)
	if err != nil {
		return name
	}
	return kind + ":" + k.String()[:8]
}

// logFetcher fetches one node's stored log view; *apiConn implements it,
// tests fake it.
type logFetcher interface {
	fetchLogs(ctx context.Context, node string) (api.LogView, error)
}

// fetchLogs performs one logs frame exchange (stored view, no follow).
func (a *apiConn) fetchLogs(ctx context.Context, node string) (api.LogView, error) {
	var view api.LogView
	err := a.call(ctx, api.TLogs, api.LogsRequest{Node: node}, api.TLogView, &view)
	return view, err
}

// printFailureLogs fetches and prints the captured output of the terminal
// snapshot's failing nodes (the hard failures, not the derived
// failed-upstream ones) — the stored head/gap/tail view, i.e. the log tail
// of the newest attempt. Nodes in streamed had their output followed live
// (--logs), so their recap points at the scroll instead of repeating it.
// Best-effort: log fetch problems are reported and swallowed — the build
// error is the story.
func printFailureLogs(ctx context.Context, lf logFetcher, snap api.Snapshot, w io.Writer, streamed map[string]bool) {
	for _, n := range snap.Nodes {
		if n.Phase != wire.PhaseFailed {
			continue
		}
		fmt.Fprintf(w, "\n--- %s failed (gen %d): %s\n", n.Node, n.Gen, n.ErrSummary)
		if streamed[n.Node] {
			fmt.Fprintln(w, "(output streamed above)")
			continue
		}
		view, err := lf.fetchLogs(ctx, n.Node)
		if err != nil {
			fmt.Fprintln(w, "(logs unavailable:", err, ")")
			continue
		}
		if len(view.Head) == 0 && len(view.Tail) == 0 {
			fmt.Fprintln(w, "(no captured output)")
			continue
		}
		w.Write(view.Head)
		if view.GapSize > 0 {
			fmt.Fprintf(w, "\n... [%d bytes omitted] ...\n", view.GapSize)
		}
		w.Write(view.Tail)
		fmt.Fprintln(w)
	}
}

// clientPushRef mints the scratch ref name for one source push; the server
// deletes it after the submit commits.
func clientPushRef() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "client-push/" + hex.EncodeToString(b[:]), nil
}

// errWriter is the CLI's stderr (App.ErrWriter when set, so tests can
// capture progress output).
func errWriter(c *cli.Context) io.Writer {
	if c.App.ErrWriter != nil {
		return c.App.ErrWriter
	}
	return os.Stderr
}
