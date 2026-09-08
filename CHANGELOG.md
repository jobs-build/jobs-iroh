# Changelog

## v0.33.0 — 2026-09-08

- **go-iroh v0.2.0, transport-iroh v0.3.0.** A behaviour release of the
  transport: net_report now runs by default whenever relays are
  configured (the explicit `iroh.WithNetReport` calls in `serve` and
  `amberclient` are now the default and stay), path MTU discovery is on
  (the don't-fragment bit is set, so a datagram too large for the path
  fails instead of fragmenting), mDNS answers SRV and TXT questions and
  discovers over IPv6 as well as IPv4, and `relay.Map` is safe for
  concurrent use but must not be copied. No exported go-iroh signature
  changed; no wire, API or ALPN change here, and the test suite passes
  unchanged. transport-iroh v0.3.0 carries the same bump and a corrected
  throughput note: one endpoint receives ~350 MB/s on loopback and a
  1 Gbit WAN's line rate, so sharded transfers only matter above that.

## v0.32.0 — 2026-09-07

- **`amberiroh/` de-vendored.** The store-sync transport is imported from
  `github.com/amber-store/transport-iroh/amberiroh` (v0.2.0) again instead
  of the copy this repo carried since 2026-07-27. Everything that copy grew
  — `DataEndpoints` advertising, server-side stream retirement,
  throughput-weighted want dealing, TPin and the GC hooks — was ported
  upstream first, so behavior, wire format and ALPNs are unchanged. Future
  protocol or server changes land upstream and arrive here as a version
  bump.

## v0.31.1 — 2026-09-07

- **amber-store-core moved to `github.com/amber-store/core`.** The
  repository was transferred from `jobs-build/amber-store-core` to
  `amber-store/core` and its module renamed; v0.0.4 is the first version
  served under the new path. jobs-iroh's requirement (v0.0.2 → v0.0.4) and
  every import were switched, with gofmt re-sorting the import blocks the
  new path now leads. No functional change; no wire, API, or ALPN changes.

## v0.31.0 — 2026-08-25

- **Runner and client local-store GC** (docs/design/2026-08-25-runner-client-gc.md),
  built on the same engine as v0.30.0's server-side GC: the tracker +
  mark-sweep collector + sweep pipeline were extracted verbatim from
  `serve/gc.go` into a new host-agnostic `gcsweep/` package (serve now
  wraps it in a thin adapter to `api.GCStats`; wire and behavior
  unchanged). The extraction also grew a trees sweep: `<cache>/trees/<k>`
  materializations (and their `<k>.bin` companion farms) are removed once
  the store no longer holds `k`, `staging-`/`bin-staging-` temp dirs are
  exempt while young and collected past 24h, and abandoned `fetcher-*`
  work dirs are collected past 24h.
  `jobs-runner` gains four flags — `--gc-retention` (default 720h, 0
  disables), `--gc-interval` (default 1h), `--gc-rate`, `--gc-min-free`
  (`JOBS_GC_*` env equivalents) — that start a `gcsweep` loop over its own
  private cache after the boot self-test passes; everything there is
  re-pullable, so a wrong expiry only costs one re-pull.
  `jobs-client` opens a sweeper on every store open and, after
  build/run/develop/image/remote-build, opportunistically sweeps at most
  once per 24h (`<data-dir>/gc.stamp`), printing one stderr line only when
  something was reclaimed; retention defaults to 720h via
  `JOBS_GC_RETENTION` (0 disables). New `jobs-client gc [--garbage]
  [--retention]` command forces an immediate local sweep and prints
  disk/refs/store/trees/cycle stats — distinct from the existing `admin
  gc`, which sweeps the server. Client outputs are locally authoritative
  ("wasteful never wrong"), so an expired ref just costs a rebuild. No
  wire or API changes.

## v0.30.0 — 2026-08-25

- **GC and auto-cleanup** (docs/design/2026-08-25-gc-auto-cleanup.md). The
  server now tracks when every ref was last used — its own doneness/cache
  reads, remote pulls and pushes, and (new) the runner reporting every ref
  it read while driving a job (`wire.Result.ReadRefs`, additive CBOR) — and
  an hourly sweep (`--gc-interval`, default 1h) deletes refs unread for
  `--gc-retention` (default 720h/30d; 0 disables expiry and cycles),
  output before deps, then drives amber-store-core's mark-and-sweep
  collector (`--gc-rate`, `--gc-min-free`) when expiry ran, garbage passed
  50%, or free space is tight, logging one "gc sweep" line with disk
  usage, ref/pinned counts, expired count, and live/garbage bytes. Every
  ref PUT (`amber.PutRef`, `amberiroh.handlePush`) now goes through the
  collector's `PrepareRef` write barrier so a concurrent sweep can never
  reap a just-written ref. Bootstrap seeds (`shell:`/`fetcher:`/
  `seed-src:`) and pinned refs never expire; `build-output`/
  `build-output-deps` share one access clock and one pin state. New
  `reftrack/` package owns the tracker and its `<data-dir>/refaccess.cbor`
  snapshot. `jobs-registry` asserts keep-forever pins (new `TPin` amber
  frame) for every image it serves, coalesced to roughly hourly, so a
  published image never ages out even if nothing else touches its refs;
  it self-disables against an older server that doesn't understand `TPin`
  and drains in-flight asserts at shutdown. New admin surface: `jobs-client
  admin gc [--garbage 0.4]` forces an immediate sweep, `admin pin <ref>` /
  `admin unpin <ref>` set/clear the never-expire flag, `admin stats` grows
  a GC block (retention, last sweep, expired/pinned counts, live/garbage
  bytes and percentage, last cycle), and `admin refs` (CLI and TUI) grows
  last-access age and pin columns. Expiring a ref only un-memoizes a build
  — "running twice is wasteful but never wrong" — so a rebuild after a
  wrong or premature expiry is the worst case, never corruption. Bumps
  amber-store-core to v0.0.2 (adds the `gc` mark-sweep package;
  `fstree.CheckComplete` now also returns the tree's `[]key.Key`). No ALPN
  bump: old runners simply don't report `ReadRefs` (their warm-cache reads
  stay invisible to the tracker, worst case an avoidable rebuild), and an
  old registry never pins (status quo).
- go-iroh pinned at its first tagged release and then upgraded to v0.1.0
  (was an Aug-15 pseudo-version; full suite green on the upgrade).

## v0.29.0 — 2026-08-19

- **`tui` exports the build-graph folding API**: `FoldSnapshot`,
  `FlattenTree`, `BuildGraph`, `BuildRow`, `TreeRow` (rename-only —
  behavior and wire identical to v0.28.0). External jobs-iroh frontends
  (jobs-build/assimilate's two-level deploy TUI) fold snapshots with the
  same rules instead of duplicating them.
- **Server and runner images are part of the release.**
  `scripts/release-images.sh` builds `dmilhdef/jobs-iroh-server`,
  `dmilhdef/jobs-iroh-runner` and `dmilhdef/jobs-registry` (linux/amd64 +
  arm64, `v<version>` and `latest`) from a clean tag checkout; Dockerfiles
  for the server and runner join the registry's under `deploy/`. Until now
  only the registry image was published, and every Kubernetes rollout of a
  new tag needed the other two built by hand.

## v0.28.0 — 2026-08-19

- **Full-screen build TUI for `remote-build` and `watch`**
  (docs/design/2026-08-19-remote-build-tui.md). On an interactive
  terminal (stdin + stderr TTYs) both commands open a bubbletea view:
  the build graph as a navigable tree — one logical row per
  build/import, the buildvalue stage chain (eval → resolve → pin →
  build) folded into the row's state with per-stage elapsed, done
  durations and `(cached)` markers — plus an output pane following the
  selected row's captured output live (stored head/gap/tail first,
  then chunks; scroll pauses follow, `G` resumes). Shared dep subtrees
  repeat under every parent (per-path expand/collapse; cached-done
  subtrees start collapsed). `q` detaches (the build keeps running;
  re-attach hint printed, exit 0), `ctrl-c`/`c` confirm-cancels (exit
  130); a finished build auto-exits — success pulls the output home
  exactly as before, failure keeps the view open for inspection and
  exits 1 after `q`. `--no-tui` (or a non-TTY, or an old server
  without graph snapshots, or an already-terminal request) falls back
  to the classic block view unchanged. New wire: additive
  `NodeSnap.Deps`/`.Cached` (+ the shared `api.ServerError` type);
  no ALPN bump — old/new peers interoperate, old servers just get the
  classic view.

## v0.27.0 — 2026-08-19

- **Builds are rejected when no runner serves their arch; a runnerless
  build fails after 5 minutes** (#8). The scheduler's fleet fold
  (`runners.hello` + 5 s heartbeats) doubles as a scheduling gate: a
  runner is live while its last hello/heartbeat is within 15 s (three
  missed beats). A submit whose closure still needs runner work on a
  platform with no live runner returns `unavailable` instead of
  stalling forever — checked after the graph join, so a fully cached
  closure still completes without any fleet. An accepted request whose
  platform then loses its last live runner for a continuous 5 minutes
  is failed by a watchdog: interest drops exactly like cancel (queued
  messages purged, late results dropped), the request stays
  inspectable until delete, and the reason rides the new additive
  request-level `Snapshot.Error` / `RequestStatus.Error` field, which
  `remote-build`/`watch` print when no node carries the failure. A
  runner reappearing before the deadline resets the clock; runnerd
  re-hellos on every NATS (re)connect, so a server restart re-learns
  platforms from the fleet itself. No `jobs-runner-nats` ALPN bump —
  old runners already hello with their platform and keep producing
  correct results.
- **Synced fetcher/plugin copies removed** (#7). `fetchers/` keeps only
  what this repo is authoritative for: the embedded-seed sources
  (`github`, `hostmusl`, `hostshell`, `tarballhttps`) and the shared
  `tarextract` library. Everything else (alpineapk, bundler, cargo,
  gomod, goplugin, node, npm, pybackend, pypi, rubygems, tarballxz, uv
  and `plugins/goplugin`) lives in its own `jobs-build/fetcher-*` /
  `plugin-*` repo, is pinned by recipes at its point of use, and the
  in-repo copies could only drift silently.
- **go-iroh bumped to latest master** (`8aca5f0`, #10).

## v0.26.0 — 2026-08-19

- **`BUILD.jobs`: jobs-iroh builds itself.** A root recipe for the JOBS
  self-build: plugin-go (pinned at `1c640d5`, the first rev with
  `go_closure`) turns `go.sum` into one `gomod` import per module, the
  go1.26.6 toolchain arrives via `tarball+https`, and a sandboxed
  offline `go build` (`GOPROXY=off`, `CGO_ENABLED=0`) produces all four
  binaries in `$out/bin` with a `bin/jobs-server` entrypoint. The
  `go_closure` complete cover keys identity to the Go sources actually
  reached from the four mains (plus `go.mod`/`go.sum` and the embed
  targets `bootstrap/seed` and `fetchers.toml`), so edits to `docs/`,
  `deploy/` or nix files join the KP-keyed memo instead of rebuilding.
  Declares c2-m4 resources and a per-toolchain persistent Go build
  cache.
- **Local pipeline runs imports in the hermetic root.** The v0.25.0
  hermetic import root covered only the runner daemon: `jobs-client`'s
  local pipeline still ran `./fetch` through the plain Subprocess
  executor, so a fetcher's baked `/jobs/store` paths dangled and the
  host userland leaked in — fetcher-gomod v0.1.0 (toolchain from
  `env.sh`, not the host) could not run a single local module fetch.
  The local driver now uses the same hermetic executor as the runner
  (with the announced Subprocess fallback where user namespaces are
  unavailable), and `RunImport`'s runtime-closure resolve gains the
  direct `build-output-deps:K` fallback for local builds without the
  `build-from:K` bridge — the same fallback `ResolveBuildArtifact`
  already applied to `build-output`.

## v0.25.1 — 2026-08-19

- **Runner capacity honours the pod's cgroup limit; `--cpu` / `--memory`
  caps.** Auto-detection read `memory.max`/`cpu.max` at the cgroup mount
  root only, which is the container in a private cgroup namespace but the
  MACHINE in the host namespace (a privileged Kubernetes pod): a runner
  with a 32 GiB pod limit advertised the node's 121 GiB and would have
  admitted far more than the pod could hold. Detection now walks from the
  process's own cgroup (`/proc/self/cgroup`) up to the root and takes the
  tightest `memory.max` and `cpu.max` on the chain — which also sees
  through the leaf-holder child the runner parks itself in. New
  `--cpu` / `--memory` flags (`JOBS_RUNNER_CPU` / `JOBS_RUNNER_MEM`, the
  override the code comments always promised) cap either dimension
  verbatim; `--size` still wins when set.

## v0.25.0 — 2026-08-19

- **Imports run in a hermetic root — with network.** The import executor
  used to run a fetcher's `./fetch` with user/mount/pid namespaces but NO
  pivot_root: the fetcher saw the runner host's filesystem, so
  `#!/usr/bin/env bash`, `jq`, and the host `go`/`cargo`/`node` were
  silently load-bearing. A runner built from a minimal image could not
  run a single import, and a fetch's result depended on whatever the
  host had installed. Imports now get the same kind of sandbox a build
  gets — pivot_root into an assembled root, User+Mount+PID+UTS+IPC
  namespaces, best-effort cgroup — minus `CLONE_NEWNET`: the host
  network is kept, because imports are the one network-capable stage.
  Inside: the embedded **shell artifact** at `/jobs/store/<key>` with
  its bash/jq *and every busybox applet* bound at `/bin` and `/usr/bin`
  (shebangs resolve; `mktemp`, `sleep`, `tr`, `find`, `wget`, … exist),
  the fetcher artifact read-only at `/jobs/fetcher` (cwd), the output
  writable at `/jobs/out` (`JOBS_OUTPUT_DIR`), the secrets file at
  `/jobs/secrets.json`, a writable `/tmp` on the work filesystem,
  hermetic `/dev` and `/proc`, and an `/etc` carrying the hermetic
  baseline plus the HOST's `resolv.conf`/`hosts`/`nsswitch.conf` and CA
  bundle (`SSL_CERT_FILE` is pinned to it) — DNS and TLS trust are part
  of "network", not of the host. The environment is hermetic too: PATH,
  HOME=/tmp, TMPDIR, the `JOBS_*` variables and only the proxy /
  `SSL_CERT_*` pass-through — never `os.Environ()`.
  **Fetcher contract change:** a fetcher can rely on the static shell
  userland and nothing else from the host. A fetcher that needs a
  toolchain (the `gomod` fetcher's `go`, cargo, node, uv, …) declares it
  as `runtime_deps` in its own `BUILD.jobs`: a recipe-declared fetcher's
  **runtime closure** (`build-output-deps:F`) is mounted into the import
  root at the same `/jobs/store/<key>` paths the fetcher's build saw, so
  the build can bake the path (an `env.sh` the fetch script sources —
  exactly how an image entrypoint finds its closure). The scheduler's
  PullRefs for import nodes gained `shell:<platform>` and the fetcher's
  `build-output-deps:F`. Materialized trees (shell, closures, the /bin
  farm) are cached per key under `<data-dir>/cache/trees` and shared by
  every import on the runner. Hosts without user namespaces keep the
  plain host subprocess fallback, as does the local `develop` path.

## v0.24.2 — 2026-08-10

- **Back on upstream go-iroh.** Upstream merged everything the
  `draganm/go-iroh` fork existed for: the QAD public-address-discovery
  and relay-to-direct hole-punching fixes (tmc/go-iroh#4) and the
  dual-close fix on the `net.Conn` stream wrappers (tmc/go-iroh#3, the
  exactly-100 stream starvation). `go.mod` drops the fork replace and
  pins upstream HEAD (`c57a8ef`), which also brings upstream's
  punch-ready-connection follow-up. No behavior change intended; the
  full suite and the darwin/arm64 cross-vet pass against the upstream
  module. Older tags (v0.17.0–v0.24.1) keep resolving their fork
  pseudo-versions through the Go module proxy cache.

- **Build work trees live under `<data-dir>/work`, swept at boot.**
  Stage-driver scratch (sandbox work trees, source extracts, output
  staging) previously landed in the OS temp dir — on NixOS a RAM-backed
  tmpfs capped at 50% of RAM. One GitLab-sized attempt materializes
  ~8.4 GB of store plus a ~19 GB build tree there, and attempts killed
  mid-run leak their trees (the sandbox child dies before cleanup runs),
  so a 63 GB `/tmp` filled to 96% and builds died with ENOSPC while the
  disk had room. The daemon now points `TMPDIR` at `<data-dir>/work`
  right after the store open (whose lock guarantees one daemon per data
  dir) and sweeps the directory on every boot, reclaiming leaked trees
  and keeping scratch on the filesystem that holds the store. Size your
  runner disks for peak concurrent scratch, or bound it with `--slots`.

## v0.24.0 — 2026-07-30

- **Runners use their full machine capacity.** Auto-detected runners
  previously floored their admission capacity to the largest size-class
  ladder rung — capped at `c4-m16`, so a 32-core/125 GiB box scheduled as
  4 cores/16 GiB and ran exactly one `m16`-class job at a time. The
  reserve-adjusted detected capacity (cgroup-aware, minus 10%) now feeds
  the admission ledger and lane selection directly: the ladder classifies
  jobs, not runners. An explicit `--size` still caps capacity to exactly
  that rung; the new `--slots` flag (`JOBS_RUNNER_SLOTS`) optionally caps
  concurrent jobs on top of the resource accounting. The hello/heartbeat
  advertises the real numbers, with the size label now meaning "the
  biggest job class this runner accepts". Measured on the test fleet: a
  24-leaf build ran all 24 jobs simultaneously (12 per 32-core box,
  ledger arithmetic exact) and finished in one wave.
- **A runner restart no longer hard-fails in-flight builds.** A terminal
  `^C` reaches the sandboxed build children (same process group) before
  the daemon's drain path can label the attempt cancelled, so the stage
  driver saw its child die and reported class `hard` — failing the whole
  request on a mere redeploy. A `Failed`/`Decline` outcome under a
  cancelled job context is no longer trusted as a verdict: it maps to
  `cancelled`, naks, and redelivers elsewhere.

## v0.23.0 — 2026-07-30

- **Small-job transfer throughput: relayed shards are parked, not used or
  evicted.** Field evidence (runner at Hetzner, server behind residential
  NAT) showed shard punches landing in minutes while the 30s relay-grace
  eviction killed each punch mid-ramp — every small pull/push paid a
  redial and dealt its want rounds across relay channels. The pool now
  parks relayed connections (held for the punch, invisible to transfers,
  replaced in the background only after ~5 min), and transfers are
  **reserve-first**: once the server's data endpoints are known, DataConns
  promises exactly the direct-path shards that were reserved up front, so
  the server never waits out its 10s gather window for shards that cannot
  come. All shards parked ⇒ single-channel on the direct control stream —
  strictly faster for small closures — with sharding resuming the moment a
  punch lands. Parked shards never demote the connection.
- **Pool growth dials moved off the transfer critical path.** A transfer
  that can be served from live pooled connections returns immediately;
  growth toward the concurrency target lands in the background for the
  next transfer. Only a cold pool waits, bounded by the attach budget.
- Measured (evw-prod3 → NAT'd server, 1 MiB closure, one long-lived
  client): sequential pulls p50 350ms → 200ms (2.8 → 4.6 pulls/s per
  lane); 6-way concurrent bursts p50 450ms → 310ms with the
  round-to-round drift gone. 512 MiB cold pulls unchanged (same
  optimistic first-transfer path, verified 3 shards attached both ways).

## v0.22.1 — 2026-07-29

- **The recipe's `name =` now overrides the default labels.** A build
  declaring `name = "shiny demo build"` shows that name on its buildrun
  (and pipeline) in every labeled surface, beating the dir/dep-name
  defaults. The name rides the `build-pinned` ref proposal's new optional
  `Label` (it was already kept out of the Pinned bytes, so KP identity is
  untouched); the server applies it when the pin commits. Old runners
  simply don't send it; old servers ignore it.
- The per-transfer `shards attached` and per-pin label log lines drop to
  Debug — steady-state client output stays clean.

## v0.22.0 — 2026-07-29

- **Build progress shows names, not CAS keys.** Every surface that printed
  `buildrun_2241…` now prints the thing's name first: recipe dep names
  (`apk_acl_libs`), build dirs (`apps/web`), fetcher specs
  (`fetch alpine …`), and the submitted target (the resolved context dir),
  as `name (kind:key8)` with the key kept for grepping. Applies to
  `remote-build`/`watch` live progress and change lines, the `--logs` line
  prefixes, `diagnose` reports (labels are stored in failure records, so
  they survive retries and restarts), and the local `build`/`develop`
  step lines.
- Labels are display-only: nothing enters canonical definitions or any
  identity, all wire additions are optional CBOR fields, old/new peers
  interoperate rendering today's keys. Sources: `PinnedInput.Name`,
  plugin/resolution-dep map keys, `Definition.Dir`, fetcher names, and a
  new optional `SubmitRequest.Label` from the client.

## v0.21.0 — 2026-07-29

- **Want rounds are dealt by observed per-channel throughput.** Round-robin
  dealing let one slow channel (say, a shard stuck on the relay while its
  siblings punched) pace every round, since a round barriers on its slowest
  shard. `Receive` now samples each channel's wire bytes per round and
  deals the next round proportionally (keys weighed by their embedded
  lengths); a starved channel keeps a probe-sized share so it can earn its
  weight back after upgrading.
- **go-iroh fork: `net.Conn` stream wrappers now close both directions.**
  The root of v0.20.3's stream starvation lived in the wrapper contract:
  `Close()` only FIN-closed the send side, so whenever the peer's FIN
  arrived after this side's last read — the common case over real latency —
  the stream never completed and its `MAX_STREAMS` credit never returned.
  `streamConn.Close` now also `CancelRead`s (fork commit `783e412`, with
  130-stream regression tests on direct and relay-won-punched
  connections). jobs-iroh's own belt-and-braces from v0.20.3 (server-side
  `CancelRead` on every close path, 90-use pool rotation) stay in place.

## v0.20.3 — 2026-07-29

- **Pooled shard connections no longer starve after exactly 100 transfers.**
  Live-debugged on a Hetzner runner ↔ NAT'd server pair: transfers ran in
  waves and stalled because every pooled shard connection hit quic-go's
  initial 100-stream budget — attach/op streams never fully retired
  server-side on relay-won-then-hole-punched connections, so `MAX_STREAMS`
  credit never flowed back and the 101st `OpenStreamSync` hung for the
  whole attach budget. Two-sided fix: the server now fully terminates
  every stream it closes (`CancelRead`/STOP_SENDING completes the
  client→server half without reading to EOF), and the client pool rotates
  entries after 90 uses — so even against older servers the cost is a ~1s
  proactive redial per 90 transfers instead of a starved 8s attach.
  (Why the transport auto-retirement differs between loopback and migrated
  paths remains a go-iroh fork investigation; the reproducer and the
  exactly-100 signature are documented in the commit history.)
- A zero-attach on *reused* pool entries no longer latches the sticky
  demote: the stale entries are discarded for redial and the demote stays
  reserved for fresh dials failing (the true topology verdict).
- Diagnostics that made this findable are now permanent: per-shard attach
  results (`shards attached`, attach WARNs distinguishing stream-open from
  attach-write), pooled-connection path logging, and server-side
  connect/disconnect logs on the data-endpoint routers.

## v0.20.2 — 2026-07-29

- **A transfer that starts right after boot no longer gets pinned to the
  relay.** Field logs showed a job dispatched 0.5s after runner start:
  the control path was still relayed (punches land ~5s in), so the shard
  gate skipped extras and the whole multi-minute pull ran single-channel —
  starting on the relay, and unable to widen mid-flight since a transfer's
  channel set is fixed at start. On a connection younger than 30s the gate
  now waits up to 10s for the punch before deciding; a settled relay-bound
  path still skips immediately, so relay-only topologies see no new
  stalls.
- Diagnosis confirmed server-side health along the way: all data
  endpoints QAD-learn their public candidate ~3s after server boot (the
  new `data endpoint advertised` lines).

## v0.20.1 — 2026-07-29

- **Relay-stuck shard connections are no longer locked in by the pool.**
  Field capture showed a v0.20.0 runner with a direct control path whose
  bulk transfer bytes still crossed the iroh relay: its shard connections
  had attached via the relay, never hole-punched to direct, and the pool —
  which reuses any live connection — preserved them indefinitely (v0.19.0
  accidentally avoided this by redialing per transfer). The pool now
  tracks each entry's transport path; an idle entry still relayed ~30s
  after its dial (punching normally lands in ~5s) is evicted and redialed
  at the next acquire, getting a fresh punch attempt instead of reusing
  the relay forever.
- Shard connections' paths are now visible: the runner logs every pooled
  connection's path at dial and on every change (`shard connection path`,
  like the control links), and the eviction logs a WARN naming the stuck
  endpoint. The server logs each data endpoint's advertised address set as
  it evolves (`data endpoint advertised`) — whether those records ever
  contain a public QAD candidate is the next diagnostic for punch
  failures.
- Client-only + server logging; no wire changes.

## v0.20.0 — 2026-07-28

- **Shard connections are now pooled and reused across transfers.** Since
  v0.19.0 every sharded transfer re-paid the punch ramp per shard — relay
  connect, `TAttach`, ~5s riding the relay until the direct path lands —
  and threw the warmed NAT mappings away at transfer end. Connections now
  live in a per-client pool: each transfer attaches a fresh stream on
  pooled connections (the attach protocol binds streams, not connections,
  to a transfer), so the ramp is paid once per connection. The pool keeps
  `Conns` total connections at rest (default 4, control included), grows
  under concurrent transfers toward `PoolMax` (new option, default 12),
  and shrinks back after ~90s idle. Dead or identity-rotated connections
  (server restarts) are evicted and redialed transparently.
- Runnerd, the registry and jobs-client all inherit the pool through
  `amberclient` — no flag changes; within one `remote-build` the source
  push and output pull now share connections too.
- Client-only change: no wire or server changes, works against any server
  version.

## v0.19.0 — 2026-07-28

- **Sharded store transfers now hole-punch through NAT.** A NAT'd server's
  dedicated data endpoints were unreachable from outside — their
  kernel-assigned UDP ports have no NAT mapping, and the control
  connection's punched pinhole is four-tuple-scoped — so every shard attach
  timed out, the client demoted itself, and whole builds' inputs crossed on
  a single QUIC connection (~1.5 MB/s observed Hetzner ↔ NAT'd server).
  Data endpoints now carry their own iroh identities with a relay home
  connection and QUIC address discovery, advertised in-band on
  `TAccept`/`TRef`; shard dials race the dedicated port with the advertised
  candidates and, on discovery-dialed clients, bind the relay/net-report
  stack — a relay-won shard punches to direct mid-transfer exactly like the
  control connection does. The record's presence also signals the widened
  10s attach gather window (early-exit unchanged).
- Extras are now skipped — without the sticky demote — while the control
  path itself is relayed: extra relay connections move no additional bytes,
  and sharding resumes on the next transfer once hole punching lands.
- Compatibility: old runners/clients ignore the new field; their
  dedicated-port dial fails the new endpoints' identity check and falls
  back to the control candidates, so they still shard onto the main socket
  (correct, just without the socket spread). Old servers omit the field and
  new clients run the previous path unchanged. No ALPN bump.

## v0.18.0 — 2026-07-28

- **Cancelling a build now removes its queued jobs from the work queue.**
  Cancellation used to be pure bookkeeping: job messages already published
  to the JOBS stream stayed there, so runners picked up and fully executed
  work nobody wanted (results dropped on arrival). The scheduler now
  remembers each queued job's stream sequence and best-effort deletes the
  message when a node loses its last interested request — cancel, delete
  and watcher-loss eviction alike. Work no runner has picked up never runs.
- Scope: an attempt already delivered to a runner still completes and its
  result is dropped, exactly as before — "wasteful but never wrong" is
  unchanged, there is just less waste. Deleting the message of a running
  attempt also stops the work queue redelivering it to another runner if
  the first dies mid-run. Job messages orphaned by a server restart still
  drain through runners as before.

## v0.17.1 — 2026-07-28

- **A relayed connection now goes direct in seconds, not never.** v0.17.0
  made the server publish a punchable public address, but nothing punched at
  the right moment: go-iroh only attempted a hole punch when the dial itself
  carried IP addresses — exactly what a relay-won dial lacks — leaving the
  first attempt to a 60-second upgrade tick. Measured on a relay-forced
  connection: direct path selected after 65s before, after 5s now. The
  `draganm/go-iroh` fork gains `NATTraversalRemoteAddrsReady` (closed when
  the first remote traversal candidate arrives — a peer `ADD_ADDRESS` frame
  or a seeded address) and punches on it immediately.
- The 60s upgrade tick becomes a pure fallback and stops doing harm: on an
  already-direct connection it no longer spends a QNT round; on a relayed
  one it no longer runs `ValidateDirectPath` (which opens a path over the
  *current* four-tuple — the relay itself — so it could never help and only
  stalled the path actor for its full 5s validation timeout, once per tick
  per remote, while also leaking a phantom path each time).
- New go-iroh regression test pins the bound: a relay-only dial must reach a
  selected direct path within seconds of the handshake.
- Expected runner behavior on upgrade: `server connection goes through a
  relay` at dial, then `server connection is direct` a few seconds later
  once hole punching lands.

## v0.17.0 — 2026-07-28

- **A NAT'd server now discovers and publishes its real public address.**
  Seconds after startup the published discovery record gains
  `ip:<public-ip>:<endpoint-port>` — a candidate remote peers can actually
  dial and hole-punch toward — alongside the LAN addresses. This closes the
  loop v0.16.0 opened: runners across the internet can now reach a server
  behind a home NAT directly instead of riding the relay forever.
- The fix lives in go-iroh; `go.mod` now replaces `github.com/tmc/go-iroh`
  with the `draganm/go-iroh` fork (branch `fix-qad-observed-addr-race`),
  which carries:
  - `relay`: URL-built relay maps (including the built-in default map)
    enable QUIC address discovery. **This was the true root cause** — with
    the QUIC config nil in every default relay entry, no QAD probe ever ran,
    so no endpoint could ever learn its public mapping. (v0.16.0's notes
    blamed a read race in the QAD client; that race exists and is also
    fixed, but it was secondary — the probe never ran at all.)
  - `iroh`/`netreport`: QAD probes ride the endpoint's own transport and
    UDP socket, so the discovered mapping is the endpoint's real public
    `ip:port`. Previously each probe opened a throwaway socket, learning a
    mapping that died with the socket and whose port nothing listened on —
    and, since every probe rode a different mapping, relays disagreed and
    NAT-type detection misreported symmetric NAT.
  - `iroh`: external candidates split into pinned (`AddExternalAddr`) and
    discovered (net report); a landing report no longer silently discards
    the interface addresses the server and client pin.
  - `netreport`: the observed-address read waits for the relay's report
    instead of losing the post-handshake race.
- Verified end to end on a NAT'd host: second `server advertised` record
  carries the public mapping on the endpoint's own port, LAN addresses
  intact, ~3s after startup.

## v0.16.0 — 2026-07-28

- **A runner on a public IP can now reach a NAT'd server directly.** Nothing
  in the tree ever published an address the other side could dial, so every
  such runner sat on a relay permanently, funnelling all CAS traffic through
  a third party. Three independent causes, all fixed.
- **`iroh.WithNetReport()` was never passed anywhere.** It is opt-in, and
  without it the QAD probe that discovers a host's public mapping never runs
  at all: the endpoint's external candidate set stays empty forever, so
  `Endpoint.Addr` never carries a public address and no NAT traversal
  candidate is ever advertised. Now enabled for `jobs-server` and for
  `amberclient`'s discovery dial.
- **The server published its discovery record once and never updated it.**
  `announce` snapshotted the endpoint's addresses before seeding them and
  handed the frozen result to the pkarr publisher, whose background loop
  re-published those same bytes every five minutes — so anything the
  endpoint learned afterwards, which is the entire point of a net report,
  never reached the record. It now follows `Endpoint.WatchAddr()` and
  re-publishes on change.
- Every published record re-merges the pinned direct addresses, because a
  landing net report *replaces* the endpoint's external candidate set rather
  than extending it. Without the merge the record would trade its LAN
  addresses for the public one instead of carrying both.
- **`amberclient` seeded no candidates at all.** A client's bound address is
  the wildcard, which go-iroh rejects as a candidate, so a peer had nothing
  to reach it at but the relay — and a relayed connection had nothing to
  upgrade toward. It now seeds the machine's interface addresses. This is
  what fixes the common asymmetric case: when the runner is on a public IP
  its interface address *is* its reachable address, so a NAT'd server only
  has to send to it — the NAT mapping opens outbound and there is no hole to
  punch.
- New package `hostaddr`: which of the machine's own addresses are worth
  offering a peer, moved out of `serve` now that both ends need the same
  answer. Loopback, link-local, down interfaces and container bridges stay
  excluded.

Known upstream limitation: go-iroh's QAD probe does not currently return an
observed address — `internal/netreport` dials the relay and reads
`conn.ObservedAddr()` immediately, while the relay sends `OBSERVED_ADDRESS`
after the handshake, so the probe always loses that race and reports latency
only. Verified against two independent relay deployments with UDP 7842
confirmed reachable to both. A server behind NAT therefore still cannot
discover its own public mapping; give it `--bind <fixed port>` plus
`--advertise-addr <public ip>:<port>` and a port forward if peers must dial
*it* directly. The client-side seeding above is what makes the direct path
work today, and enabling net reports is what makes the rest land as soon as
the upstream race is fixed.

## v0.15.0 — 2026-07-28

- **A runner now says how it reaches the server.** Both connections report
  their transport path — `server connection is direct` at INFO, or `server
  connection goes through a relay` at WARN — with the address and RTT, tagged
  `link=scheduling` (the NATS tunnel) or `link=store` (amber sync). A relayed
  path funnels every CAS byte through a third party: it is the single biggest
  predictor of store throughput and the usual reason a connection demotes
  itself out of sharded transfers, and until now nothing said so.
- Path changes are reported too, not just the dial: a connection that starts
  out relayed and upgrades to direct once hole punching lands (typically a
  moment after the dial) logs the upgrade, as does a later fall back to the
  relay.
- The store connection is now dialed at startup rather than lazily at the
  first transfer, so the link that matters most reports itself at boot. The
  dial is best-effort — failure logs a warning and the existing
  lazy-dial-and-retry path is unchanged.
- New in `amberclient`: `Path`, `ConnPath`, `Client.Path` and `WatchPath`.
  The path in use comes from iroh's `Conn.Paths()`, honouring the transport's
  own `Selected` flag and falling back to validated → direct-over-relayed →
  lowest RTT.

## v0.14.1 — 2026-07-27

- **macOS builds again.** `runner/plugincaller_other.go` (the `!linux`
  fallback) never received the `Dir` field its Linux twin gained with
  sibling sources in v0.11.0, so every non-Linux build failed with
  `unknown field Dir in struct literal of type SandboxedPluginCaller`.
  Broken in v0.11.0 through v0.14.0; Linux was never affected.
- **A widened build now fails loudly on the non-Linux fallback** instead of
  resolving plugins against the wrong directory. The `SubprocessPlugin`
  bridge's request carries only `{call, source}` — there is no field to
  announce the consumer dir in — so a `dir != ""` build would have handed
  the plugin the context root and got back a wrong import set, i.e. a wrong
  cover and a wrong KP. It refuses, matching how that fallback already
  refuses resolution deps. Root builds (`dir == ""`) are unaffected.
- The macOS cross-compile check is documented in `README.md` and
  `CLAUDE.md`: `CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go vet ./...`
  (`vet`, so `_test.go` files are type-checked too).

## v0.14.0 — 2026-07-27

- **`jobs-client` builds where you stand.** `--source` is now optional on
  `build`, `run`, `develop`, `remote-build` and `image`. Omitted, it resolves
  from the current directory: the nearest ancestor of the cwd holding the
  recipe, searched no higher than the repository root. `cd services/api &&
  jobs-client build` does what you mean — the whole repo becomes the ingest
  context (so `../lib` and `//lib/common` still resolve) and `services/api`
  becomes the build root. Every command now prints what it resolved:
  `context: /home/you/repo  (dir services/api, recipe BUILD.jobs)`.
  `--source-root` moves the search ceiling, `--no-repo-root` pins it to the
  cwd, and an explicit `--dir` suppresses the search entirely.
  - **Identity is unchanged.** The same `(root, dir)` pair still produces the
    same `F`; this only decides which pair the CLI picks when you name none.
    No `KPVersion` bump, no ALPN bump — every cached pin, KP tree and build
    output stays valid, and the local↔remote cache join is intact.
  - **`image` picks its mode from the positional build key**, not from
    `--source`: `image -o x.tar <K>` images an existing output, `image -o
    x.tar` builds the cwd target. Passing both is now an error instead of
    silently ignoring the key.
  - **The `git` binary is no longer required.** Repo-root detection is a pure
    `.git` walk (directory *or* file, so worktrees and submodules work),
    replacing the `git rev-parse --show-toplevel` subprocess. `GIT_DIR` and
    `GIT_CEILING_DIRECTORIES` stop influencing which tree gets ingested.

## v0.13.0 — 2026-07-27

- **The module moved to `github.com/jobs-build/jobs-iroh`.** The repo now
  lives in the `jobs-build` org and the module path changed with it —
  update imports and `GOPRIVATE=github.com/jobs-build/*`. Tags `v0.1.0`–
  `v0.12.0` are **not** resolvable under the new path (those commits still
  declare the old module path), so v0.13.0 is the first release consumable
  as `jobs-build/jobs-iroh`. GitHub redirects keep plain `git` clones of
  the old URL working. `amber-store-core` moved too and is consumed by
  pseudo-version as before.
- **amber-store-iroh is now in-tree as `amberiroh/`.** jobs-iroh was its
  only external consumer, and two of the five server ALPNs —
  `jobs-runner-amber/1.0` and `jobs-amber-admin/1.0` — are served directly
  by its `Server`, so the split bought no reuse while costing a cross-repo
  pseudo-version round trip on every protocol change. Its four packages
  (`protocol`, `wantsync`, `server`, `relaymode`) are merged into one and
  the dependency is gone; `amber-store-core` remains external. All 50
  upstream tests came along with their assertions unchanged.
- **Nothing on the wire changed.** The ALPN and every frame constant moved
  verbatim, so a 0.13 server and a 0.12 runner still interoperate on the
  amber ALPNs. No `jobs-runner-nats` fence bump and no `KPVersion` bump —
  every cached pin, KP tree and build output stays valid.
- **Dead code removed:** the standalone accept loop (`Server.Serve`/
  `serveConn`) that only upstream's `amber-serve` CLI used. jobs-iroh
  mounts the handler through its own iroh Router, so the path was
  unreachable by construction.

Design: `docs/superpowers/specs/2026-07-27-amberiroh-integration-design.md`

## v0.12.0 — 2026-07-26

- **`closure=` on the `build()` return — a COMPLETE cover of the source
  context.** `closure = [...]` lists files and directories (dirs cover
  their whole subtree) that form the build's KP identity and sandbox
  `$SRC`. The build dir is **not** auto-seeded, so an edit outside the
  closure — a README next to your code, an unrelated `cmd/` — no longer
  rebuilds. Mutually exclusive with `sources=`.
- **Root builds included.** `dir == ""` builds may now declare a closure:
  the classic single-module repo finally gets precise rebuild cutoff.
- **goplugin `go_closure=["."]`** — a gosha-style transitive import walk,
  pure Go, hermetic (no toolchain, no network). It parses every `.go` file
  (build-tag-ignored and `_test.go` included), resolves imports across
  `replace`/`use` siblings, and returns the `//`-rooted cover: reached
  package dirs (recursive, so `//go:embed`, cgo `.c`/`.h`, `.s`/`.syso`
  and `testdata/` ride along), module manifests, `go.work(.sum)`.
  Module-root packages enumerate files + embed globs + `testdata/`.
- **Pin-time guardrails.** The closure must cover the build dir (no
  runtime `cd` surprises); missing declared paths, empty entry lists and
  non-matching embed patterns fail loudly at pin/eval.
- **Compatibility:** the runner ALPN fence is bumped to
  `jobs-runner-nats/3.0` — 0.12 runners require a 0.12 server and vice
  versa (old runners would silently mis-pin closure recipes). No
  `KPVersion` bump: every cached pin, KP tree and build output stays valid.

Design: `docs/design/2026-07-27-source-closure.md`

## v0.11.0 — 2026-07-26

**Sibling sources: monorepo-aware builds.** A build in a monorepo
subdirectory can now reference sibling paths — Go `replace ../lib`
directives, cargo path-deps and workspace manifests, Maven parent poms,
shared proto dirs, symlinks into siblings — and rebuilds only when the
parts of the repo it actually covers change.

- **Whole-repo context, covered-closure rebuilds.** `jobs-client
  build/run/develop/remote-build/image --source <dir>` anchors the context
  at the enclosing git repository root (`--source-root` overrides,
  `--no-repo-root` disables). The pin stage computes the covered closure —
  declared `sources = ["//lib/common", ...]` on the `build()` return,
  plugin-discovered paths, and chased symlinks — and the expensive build
  stage is keyed by **KP** = hash{pinned job, platform, covered subset}:
  edits outside the closure never rebuild, `git checkout` mtime churn
  never rebuilds (covered trees normalize to the ZIP epoch), and identical
  covered content joins across arbitrary monorepo changes, in flight
  included.
- **Sibling builds.** `subbuild("//lib/x")` builds a sibling directory as
  its own cached build over the same context; consumers share one build
  per commit and the sibling rebuilds only when its own cover changes.
  Cycles are detected at scheduling time and fail with the full chain.
- **Generated sources.** `build()` may return `generated = {"//Cargo.lock":
  bytes}` — pin-synthesized files overlaid into the covered tree (≤1 MiB
  total). This is what makes cargo workspaces practical: plugin-cargo
  reduces the workspace manifest to the member closure and prunes
  `Cargo.lock` to the reachable subgraph, so unrelated members' dep churn
  stops rebuilding your crate.
- **Plugins discover the closure.** plugin-go (and the in-tree goplugin):
  pass `go_mod = source.read("go.mod")` (and/or `go_work`) and the
  response becomes `{modules, sources}` — replace/use directives enumerate
  the local sibling closure (Go honors replaces only in the main module,
  so the consumer manifest is complete). plugin-cargo gains workspace
  mode. Plugin sandboxes now mount the whole context read-only at
  `/jobs/source` with the consumer dir in the request.
- **Sandbox contract.** Widened builds run with CWD at the BUILD.jobs dir,
  `$SRC` pointing there and `$SRC_ROOT` at the repo root — `../sibling`
  paths resolve exactly as in the repo. Legacy root builds are unchanged.
- **New ref namespaces:** `pin-cover/<v>:F → KP`, `kp-tree/<KP>`,
  `build-pinned:<KP>`, `build-output:<KP>`, `build-output-deps:<KP>` (the
  KP pair is written deps-first; the F-named outputs are server aliases).
  Derived refs re-derive on demand after crashes; the latent `f-tree/<F>`
  crash window is fixed the same way.

Breaking changes:

- **Identity re-key.** Every `dir != ""` build definition now carries
  `ctx: 2` and hashes differently (new K); its F covers the whole context.
  Root builds (`dir == ""`) keep their exact identities. Existing caches
  for subdir builds rebuild once.
- **Runner fence.** The runner↔server NATS tunnel ALPN is now
  `jobs-runner-nats/2.0`: old runners fail loudly at dial time and must be
  upgraded together with the server (an old runner would otherwise build
  widened definitions with wrong narrow semantics).
- **Old servers reject new clients' subdir builds** with "definition is
  not canonical CBOR" — that is the (admittedly terse) unknown-field
  rejection; upgrade the server.
- **`.git` is never ingested** (any nesting level). Sources that
  previously ingested `.git` re-key.
- `--source` inside a git repo now ingests from the repo root by default —
  identity changes for such invocations; `--no-repo-root` restores the old
  anchoring.

Ecosystem: jobs-build/plugin-go gains monorepo mode (replace/go.work
discovery); jobs-build/plugin-cargo gains workspace mode (member closure,
reduced manifest + resolution-aware pruned `Cargo.lock` as generated
sources); new examples jobs-build/example-go-monorepo and
jobs-build/example-cargo-workspace.

Design: `docs/design/2026-07-26-sibling-sources.md`

## v0.10.0 — 2026-07-23

- **jobs-registry serves uncompressed layers.** Image layers are now
  `application/vnd.oci.image.layer.v1.tar` — the blob *is* the tar, so a
  layer's digest is also its diffID. Gzip bought nothing on a LAN pulling
  from a store that already deduplicates, and cost a whole extra pass over
  the content at assembly plus a decompression on every client.
- **Layers are never written to disk.** A layer tar is a pure function of
  the content-addressed store plus a layer spec (a dep set + shell, or one
  artifact key), so the registry records the spec — a few hundred bytes —
  instead of the blob: assembly streams each layer once to measure it, and
  a blob GET regenerates the tar straight into the response. `HEAD`,
  `Range` resumes and `Content-Length` still work over a blob that exists
  nowhere. Only the manifest and config are cached, and the per-image
  records carry the layer specs, so a restarted registry with no
  jobs-server in sight still serves every layer it ever assembled.
- **Measured on a 256MiB artifact** (half incompressible): assembly — the
  wait before a first pull can start — drops from 1.95s to 0.79s, the blob
  cache from 137MB to 1073B, and a cold `docker pull` from 2.83s to 1.86s.
  Serving is now tar generation at ~1.3GB/s rather than `sendfile`, so a
  warm pull went 0.82s → 1.00s on loopback and a pull costs CPU per client
  where it used to cost none. The sample k8s CPU request moves `100m` → `1`.
- **`--cache-ttl` is no longer the disk knob.** It expires a few KB per
  image now; what grows is the amber store and the per-image records,
  neither of which is reclaimed. Size registry volumes for every build
  ever served.
- **Upgrade note: layer and manifest digests change.** Every image
  reassembles on its first pull after upgrading, and manifests clients
  already hold name digests that no longer exist — `docker pull
  <host>/jobs:<K>` by tag works throughout and yields the same image
  content. Records written by earlier versions read as incomplete and
  rewrite themselves; their gzip blobs age out on the ordinary sweep.
- **Multi-range blob requests are answered whole.** `http.ServeContent`
  serves multiple ranges from a goroutine it never joins — fatal for a
  generator-backed stream the handler is about to close — and re-generates
  the layer per range. RFC 9110 permits ignoring `Range`, and no registry
  client asks for more than one.

## v0.9.0 — 2026-07-23

- **Store transfers now run over parallel QUIC connections.** Every CAS
  sync path — runner input pulls and output pushes, registry image sync,
  `remote-build`'s source push and output pull — shards its transfer: the
  client attaches up to 3 extra connections (4 total by default) to the
  server's transfer token, each on its own endpoint and UDP socket since
  one socket's loop caps well below a fast link, and the want loop deals
  every round across all channels. Tune or disable with `--sync-conns`
  (jobs-runner, jobs-registry) and `--conns` (remote-build); `1` — or an
  explicit `0` — turns it off.
- **The server binds dedicated data sockets.** `jobs-server
  --data-endpoints N` (default 3) adds extra UDP endpoints under the same
  identity key for shards to land on, spreading receive load across
  sockets; their ports are offered in-band to sharding clients only.
- **Degradation is defended in depth.** The server serves whatever shards
  attach within its 5s gather window, and a later attach would be a
  silently dead channel — so each shard's attach runs under a 3s budget, a
  connection that attaches zero shards demotes itself to single-channel for
  its lifetime (e.g. relay-only paths), a failed sharded transfer retries
  once unsharded before erroring (push is force-mode, pull resumable —
  running twice is never wrong), push verdicts ride the control channel
  alone, and pull shard reads carry a 30s watchdog. Unreachable data ports,
  failed attaches, or a non-sharding server all degrade to the single
  control stream.
- **Attach streams survive their handoff.** The server's stream handler no
  longer cancels a `TAttach` stream it handed to an in-progress transfer —
  previously that severed live data channels, which is also why **servers
  should be upgraded before clients**: a v0.8.x server accepts shards then
  breaks them, and an upgraded client heals by falling back to
  single-channel with an Info log.

## v0.8.2 — 2026-07-23

- **Every image request is logged with its outcome and timing.** A
  manifest-by-tag request logs `image requested`, then `image served`
  (with a cache-hit flag) or `image request failed`, each with elapsed
  time. Severity is classified: an unknown build key is an ordinary 404
  (Info), upstream or assembly faults Warn, and a client that hung up or
  a daemon shutting down logs Debug — nothing failed there.
- **Slow pulls attribute at a glance.** Assemblies and every per-ref
  amber pull log their duration plus objects/bytes moved, so time spent
  splits visibly between server sync, image assembly, and blob serving.
  Pull stats accumulate across a redial retry (the resumable want loop
  picks up where the dropped connection stopped, so the logged transfer
  matches the logged elapsed), and the previously silent
  drop-and-redial now logs.
- **Blob serves report what actually happened.** `blob served` carries
  the real HTTP status and bytes actually sent — a 416, a Range slice,
  or a client dropping mid-layer no longer masquerades as a full serve —
  while keeping the kernel sendfile path for multi-GB layers.
- **Licensed AGPL-3.0.**
- **Deploy docs.** `deploy/jobs-registry/` gains `docker run` and k8s
  walkthroughs (including a kubelet `localhost:5000` proxy manifest) and
  a copy-paste docker quickstart; the multi-arch image is published as
  `dmilhdef/jobs-registry` on Docker Hub.

## v0.8.1 — 2026-07-23

- **Registry images bake the shell — script entrypoints run under docker.**
  v0.8.0 registry images shipped without `/bin/sh` or `/jobs/shell`, so any
  build whose entrypoint is a script (every Python-style launcher) ran fine
  under `jobs-client run` but died under `docker run` with
  `exec …: no such file or directory` (the shebang's interpreter was
  missing). jobs-registry now pulls `shell:<platform>` from the server and
  bakes it into the deps layer with the `/bin/sh` + `/jobs/shell` symlinks
  and shell-aware `PATH`, exactly matching `jobs-client run` and the
  single-layer `image` default. `--no-shell` opts out; a platform the server
  has no shell for degrades to a shell-less image instead of failing.
  Already-cached images reassemble with new digests on their next pull.

## v0.8.0 — 2026-07-23

- **New binary: `jobs-registry` — docker pull your builds.** A read-only
  OCI Distribution registry that serves build outputs as pullable images:
  `docker pull <registry>:5000/jobs:<build-K>` (one `jobs` repository whose
  tags are build keys). Images have two layers —
  the runtime closure under `/jobs/store/<key>` and the artifact at the
  root — assembled on the fly (`runner.AssembleOCIImage`, sharing the
  deterministic tar machinery with `jobs-client image`, so blob digests are
  reproducible). The registry owns a private amber store synced on demand
  from the jobs-server over `jobs-amber-admin/1.0` (K→F resolved from the
  ref listing, so the source closure never syncs), caches blobs on disk
  with last-read-based expiry (`--cache-ttl`, default 24h), and reassembles
  expired images from its local store without the server. Pull-only, plain
  HTTP (TLS belongs to the ingress); sample k8s manifests in
  `deploy/jobs-registry/`.

## v0.7.0 — 2026-07-23

- **Build output streams by default.** `jobs-client remote-build` and
  `watch` no longer need `--logs`: the output of the steps a watched
  request is running streams live alongside the progress display out of
  the box. Running a build and not seeing its output was the surprise, so
  visibility is now the default — `--no-logs` opts out (the `--logs` flag
  is gone). Note the runner must be ≥ v0.6.0 to emit the exec banner and
  `bash -x` trace; against older runners only the script's own output
  streams.

## v0.6.0 — 2026-07-23

- **Executors announce what they actually run.** Every import executor
  writes a one-line banner into the job's output stream before exec'ing
  the fetcher — the resolved `fetch` entrypoint, the `JOBS_FETCH_PARAMS`
  JSON, and the secrets file path (never contents) — and fetcher stderr
  now tees to the runner's own stderr, so it shows live in local builds
  and the daemon log, like build stdout always has.
- **Build scripts run in shell debug mode.** The recipe script executes
  under `bash -ex` instead of `-e`: the captured output opens with a
  banner naming the true command and sandbox layout (`jobs: exec
  …/bin/bash -ex /build/.jobs-script.sh ($SRC=/build/src,
  $out=/build/out, net=none)`) followed by a `+ …` trace of every command
  the build runs. Build stderr tees to the runner's stderr stream too, so
  the trace is visible in local `jobs-client build` runs and daemon logs,
  streams live over `remote-build --logs`, and lands in failure tails and
  `diagnose` reports. Note: chatty scripts get much louder under `-x` —
  the log pipeline's bounds (lossy fan-out, ring-buffered storage) absorb
  it, but heavy loops now trace every iteration.

## v0.5.0 — 2026-07-23

- **Remote builds now stream their output live.** `jobs-client remote-build
  --logs` (and `watch --logs`) follows the output of the steps a watched
  request is running: each active node gets its own log-follow stream (up
  to 8, running nodes claiming slots first), and its stdout/stderr prints
  as `kind:key │ line` scroll above the progress block. Followers attach
  while a node is still queued — snapshots are coalesced, so a fast job
  may never be observed running — and linger half a second past completion
  so trailing chunks drain. When a build fails, the recap prints "(output
  streamed above)" instead of repeating logs that were already watched.
  The TTY progress block also redraws once a second now, so elapsed ticks
  between snapshots and log scroll can no longer push the block off screen
  for the length of a quiet compile.
- **`jobs-client logs --server <id> --node <name> [--follow]`** prints one
  node's captured output: the stored head/gap/tail as raw bytes on stdout
  (pipeable), and with `--follow` a live stream of new chunks until
  interrupted, `tail -f`-style (a retried attempt is marked on stderr).
  The server has spoken `logs{node, follow}` on `jobs-build/1.0` since
  v0.1.0 for the admin TUI; this is the first plain-CLI surface for it.
  Full node names come from `jobs-client diagnose`.

## v0.4.0 — 2026-07-23

- **Build failures are now durably diagnosable.** Every failed (or
  budget-burning retried) attempt folds a record into a new `FAILURES`
  JetStream stream at the moment the scheduler decides its fate: origin
  (runner result / commit / gate / server), disposition (retry + backoff,
  or terminal), the runner's verbatim result (class, exit, runner ID,
  rusage, and the proposed ref batch on gate/commit rejections), retry
  counters, request tags, timing, and a trimmed snapshot of the attempt's
  captured output (256KiB head + 512KiB tail). Previously the log ring was
  reset by the very retry that made a failure terminal, leaving a 4KiB
  stderr tail as the only trace. Records survive retries and server
  restarts (7 days, last 8 attempts per node) and are diagnostics only —
  never load-bearing.
- **`jobs-client diagnose --server <id> (--request <id> | --node <name>)`**
  renders the trail as a self-contained failure report — per attempt:
  verdict, runner, error, timing, rusage, captured output between explicit
  markers — designed to be pasted into an issue or an LLM conversation
  as-is. `--json` emits the machine shape (logs as readable strings,
  RFC3339 timestamps); `--logs-dir` dumps the output bytes verbatim.
  Served by a new `diagnose` frame on `jobs-admin/1.0`; by-request
  resolution works after a server restart by scanning the records' request
  tags. `remote-build` and `watch` now print the exact diagnose command
  when a build fails.

## v0.3.0 — 2026-07-23

- **Sandbox remounts no longer fail on hardened mounts.** The read-only (and
  strictatime) `MS_REMOUNT|MS_BIND` pass now repeats the flags the bind
  inherited from its source — nosuid/nodev/noexec/ro plus the exact atime
  mode, read back via statfs. In a user namespace every inherited flag is
  locked, and a remount that omitted them was refused: `sandbox child setup:
  remount-ro …: operation not permitted` on hosts whose TMPDIR sits on a
  nosuid/nodev/noatime mount (nix-shell or hardened /tmp). Repeating the
  flags changes nothing — the bind already carried them.
- **jobs-runner now runs a boot self-test build** before dialing the server:
  it seeds the embedded shell under a self-test-only ref and drives a
  one-script build through the real local pipeline (namespace sandbox
  included). A host whose sandbox is broken — mount restrictions, missing
  user namespaces, exotic TMPDIR mounts — previously advertised capacity and
  hard-failed every job it was handed (e.g. `sandbox child setup: remount-ro
  …: operation not permitted`); it now refuses to start with the real error.
  `--skip-self-test` (`JOBS_RUNNER_SKIP_SELF_TEST`) is the escape hatch.

## v0.2.0 — 2026-07-23

- **jobs-server now announces itself for discovery** (ported from
  amber-store-iroh's amber-serve): direct addresses on every interface
  (auto-detected with container-bridge filtering, or `--advertise-addr`
  verbatim), re-announced over mDNS on the local link every second, and
  published via pkarr over the internet together with the nearest built-in
  relay (`--relay` to pin one; relay connect is best-effort and bounded, so
  an offline host still starts; `--no-announce` skips the export entirely).
  Clients resolving by bare endpoint ID — which previously failed with "no
  address found" — now find the server via mDNS on the LAN and pkarr/DNS
  elsewhere, racing every candidate.
- Client dials (amberclient, hence runner, CLI and TUI) bind with relays
  enabled on the discovery path, so the server's published relay actually
  works as a fallback when no direct candidate is reachable.

## v0.1.0 — 2026-07-23

First release. jobs-iroh is a way simpler, **non-distributed** reimplementation
of [jobs](https://github.com/draganm/jobs): one server, N runners, a client —
connected only by [iroh](https://iroh.computer) QUIC. No k8s, no HTTP, no
WebSockets, no CRDTs, no gossip, no signing keys. The hermetic build model
(Starlark recipes, canonical-CBOR identity, namespace sandbox, content-
addressed store, self-bootstrapping fetchers/shell) is ported from jobs
intact; every example in
[jobs-build/examples](https://github.com/jobs-build/examples) builds and runs,
locally and remotely.

### The three binaries

- **jobs-server** — one iroh endpoint, five ALPNs (`jobs-build/1.0`,
  `jobs-runner-nats/1.0`, `jobs-runner-amber/1.0`, `jobs-admin/1.0`,
  `jobs-amber-admin/1.0`); embedded NATS/JetStream (`DontListen`, tunneled
  over iroh streams) and embedded
  [amber-store-core](https://github.com/amber-store/core);
  in-memory scheduler (join = get-or-create by content key, doneness = ref
  existence, ported fail-closed ref gate); work queue
  `jobs.<platform>.<class>`, RESULTS stream with publish-before-ack +
  `Nats-Msg-Id` dedup, status KV, live logs on core NATS held in server
  memory only.
- **jobs-runner** — dials the server twice (NATS tunnel + CAS sync); serves
  every size class ≤ its `--size` on the ladder `c0.2-m1 … c4-m16`
  (auto-detected by default) with admission accounting; pulls input closures
  by ref, runs the stage drivers in the rootless namespace sandbox, pushes
  outputs, reports results. A killed runner's work redelivers — running twice
  is wasteful but never wrong.
- **jobs-client** — local hermetic builds (`build`, `run`, `develop` with a
  PTY shell in the exact build sandbox, reproducible OCI `image` export) over
  an embedded store, plus the remote surface (`remote-build` with live watch
  and pull-home, `watch`, `status`, `admin`, and a bubbletea `tui` for
  builds/logs/fleet/stats/refs). Local and remote submits of the same source
  produce the same K, so caches join across paths.

### Compatibility

All 10 current examples pass, local and remote, build **and** run-verified:
subbuild, develop/myapp, go-build (+ image + develop), python-build,
python-sdist-build, python-rust-build, python-rust-sdist-build, rust-build,
rails-build, phoenix-build.

### Deviations from jobs (v1 stances)

Runner adoption dropped (queue redelivery covers it); single-connection CAS
sync; access is open to whoever knows the endpoint ID (amber-store-iroh's
trust model); FUSE store mounts deleted (materialize-only); build events ride
NATS instead of a collector service; no secrets/runner tags; no store GC.
Known follow-ups are listed in `docs/design/2026-07-22-architecture.md`.
