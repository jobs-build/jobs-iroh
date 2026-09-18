# CLAUDE.md

## What this is

jobs-iroh is a way simpler, **non-distributed** port of jobs: one server, N
runners, a client, connected only by iroh QUIC — no k8s, HTTP, WebSockets,
CRDTs, gossip, or signing keys. It embeds NATS (JetStream, `DontListen`) for
scheduling and amber-store-core as the content-addressed store. Three core
binaries (`jobs-server`, `jobs-runner`, `jobs-client`) and five ALPNs on one
server endpoint: `jobs-build/1.0`, `jobs-runner-nats/3.0`,
`jobs-runner-amber/1.0`, `jobs-admin/1.0`, `jobs-amber-admin/1.0`. The build
model (Starlark recipes, canonical-CBOR identity, hermetic sandbox,
self-bootstrapping fetchers/shell) is ported from jobs intact. A fourth,
optional binary — `jobs-registry` — is a read-only OCI registry serving build
outputs as pullable images (HTTP is its outward face only; it talks to the
server exclusively over iroh).

## Docs

- `docs/architecture/architecture.md` — **design source of truth**, written
  ground-up for this system. Keep code consistent with it; flag
  disagreements early.
- `docs/design/*.md` — dated design/implementation specs (historical
  record).
- `docs/research/*.md` — subsystem maps of the SOURCE systems the port draws
  from (jobs, amber-store-core, amber-store-iroh, nats-iroh). File:line
  citations point into those upstream trees, not this repo.

## Build & test

Go toolchain comes from the Nix devShell:

```sh
direnv allow                       # or:
nix develop -c go test ./...
nix develop -c go build ./...
```

`GOPRIVATE=github.com/jobs-build/*` is required for module fetches
(set in `.envrc`). `nix develop -c gofmt -l .` must print nothing —
treat any output as a failure.

**Cross-compile for macOS after touching any `_linux.go`/`_other.go` pair.**
Build tags mean `go build` on Linux never type-checks the `!linux` twin, and
there is no CI — a field added to one side and not the other compiles here
and breaks every macOS developer (this is exactly how `SandboxedPluginCaller
.Dir` shipped broken from v0.11.0 through v0.14.0):

```sh
nix develop -c env CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go vet ./...
```

`vet` rather than `build` — it type-checks `_test.go` files too. Windows is
not a target (amber-store-core is unix-only).

The repo-root `BUILD.jobs` is the JOBS self-build (all four binaries,
offline `go build` via plugin-go + gomod imports). Verify it end-to-end with
`nix develop -c go run ./cmd/jobs-client build`; a rerun must be `(cached)`.
Its pins (plugin-go rev+sha256, Go toolchain version+sha256 — must satisfy
go.mod's `go` directive, `GOTOOLCHAIN=local` never auto-downloads) are
documented in the recipe header and bumped together.

## Release process

Every release is: bump `version/version.go` (the ONLY change in the release
commit), tag, push, GitHub release. **Pushing the tag starts the
[Release images](.github/workflows/release-images.yml) workflow**, which
publishes the three images to GHCR (`ghcr.io/jobs-build/jobs-iroh-server`,
`ghcr.io/jobs-build/jobs-iroh-runner`, `ghcr.io/jobs-build/jobs-registry`).
The images are part of the release, not an optional extra — a tag without
them leaves the `:latest` tags pointing at older code than the tag suggests,
and a Kubernetes rollout of the tag has nothing to pull — so watch the run to
the end. Patch releases included.

```sh
V=0.14.1                                    # version/version.go already bumped
git add version/version.go CHANGELOG.md
git commit -m "Release v$V: <headline>" && git tag "v$V"
git push origin main && git push origin "v$V"
nix develop -c gh release create "v$V" --verify-tag --repo jobs-build/jobs-iroh \
  --title "v$V — <headline>" --notes "…"

# The tag push started the image workflow; wait for it.
gh run list --repo jobs-build/jobs-iroh --workflow release-images.yml --limit 1
gh run watch --repo jobs-build/jobs-iroh --exit-status <run id>
```

The workflow runs one job per image. From a checkout of the tag it refuses a
tag that doesn't match `version/version.go`, cross-compiles the binary for
linux/amd64 + linux/arm64 (CGO off, `-trimpath`) into `deploy/<binary>/` and
fails unless it is stamped `vcs.modified=false`, builds the COPY-only
Dockerfile with `docker buildx` for both platforms (no QEMU, no
provenance/SBOM attestations, so no `unknown/unknown` rows), pushes `v$V` —
and `latest` only when the tag is the highest `v*` tag — and checks the pushed
index holds exactly linux/amd64 and linux/arm64. To publish or republish an
existing tag, run it by hand: `gh workflow run release-images.yml --repo
jobs-build/jobs-iroh -f tag=v$V`. Pull requests touching the workflow or a
Dockerfile build the images without pushing. Server and registry run as
distroless `nonroot` (65532; give them their `/data`), the runner as root
(its sandboxes need it). Releases up to v0.35.0 went to Docker Hub as
`dmilhdef/jobs-iroh-server`, `dmilhdef/jobs-iroh-runner` and
`dmilhdef/jobs-registry`; those tags stay, nothing new is pushed there. Keep
`CHANGELOG.md` in step: it is the in-tree record, the GitHub release notes are
the outward one.

## Package map

| Package | What it is |
|---|---|
| `amber/` | Store seam over amber-store-core. Pinned chunk params (ByteOpts 32Ki/128Ki/256Ki, ItemBits 7) are **identity-critical** — never change them. Sibling-sources ops: PruneTree/NormalizeTree (covered trees, uid/gid=0 + ZIP-epoch mtimes — KP is mtime-immune), OverlayTree (generated sources), BuildKPTree ({job.cbor, platform, v, src/}). Source ingest excludes `.git` at every level. SetObserver/SetRefGuard GC seams (nil-safe: unset = current behavior). |
| `cover/` | **Identity-critical shared** closure walker + KP derivation (sibling-sources design): pin expands declared+discovered paths (component-wise in-store symlink chase, ELOOP budget 40, dangling warn-keep, escaping fail + `sources_allow_escaping`) into `Pinned.Sources`; `WalkClosure` expands complete covers (no dir seed, workdir validation) into `Pinned.Closure`; `cover.Derive` (used identically by pin runner, server pin-commit, local pipeline) = prune + generated overlay + KP tree. Bump `amber.KPVersion` on ANY semantic change here or in PruneTree. |
| `natsiroh/` | NATS-over-iroh tunnel (dialer + stream proxy). The dialer writes a `0x00` stream preamble because the NATS server speaks first. |
| `wire/` | Frozen scheduler wire contracts: node names, phases, Job/Result CBOR, size-class ladder, NATS subject/stream layout. |
| `api/` | Frozen client API frames (4-byte BE length + CBOR `{t,b}` envelope) for the build/admin ALPNs. |
| `events/` | Build-event schema + OutputWriter (32KiB chunks, 64KiB/100ms flush) — events ride core NATS via the Sink seam. |
| `sched/` | Server scheduler: in-memory node graph (join = get-or-create, doneness = ref existence), unfold, ref gate, JOBS/RESULTS/status-KV folds, retry classes, per-kind PullRefs, log fold rings, durable FAILURES records + Diagnose. Nodes carry display-only labels (recipe dep names/dirs/fetchers, root from SubmitRequest.Label) — never identity. Forwards `wire.Result.ReadRefs` to the tracker before commit logic. |
| `reftrack/` | Server-side ref access tracker: touch/pin/expire + CBOR snapshot (`<data-dir>/refaccess.cbor`); protected classes shell:/fetcher:/seed-src:; build-output(-deps) family shares one clock. |
| `gcsweep/` | Host-agnostic GC engine (extracted from serve): reftrack + collector + sweep pipeline + trees/fetcher-dir sweep; embedded by server, runner (hourly loop) and client (stamp-gated per-command sweep + `gc` command). |
| `serve/` | jobs-server composition: iroh Router × 5 ALPNs, embedded NATS + embedded store, build/admin API handlers, bootstrap seeding, GC runner (thin `gcsweep` adapter to `api.GCStats`). |
| `runnerd/` | jobs-runner daemon: boot self-test build gate, lane consumers per fitting size class, admission accounting, pull-inputs → drive stage → push-outputs → result-before-ack (MsgId dedup). |
| `amberiroh` (external) | Store sync over iroh QUIC, imported from **`github.com/amber-store/transport-iroh/amberiroh`** — the facade over transport-iroh's `protocol`, `wantsync`, `server` and `relaymode`. This repo carried a vendored copy from 2026-07-27 to 2026-09-07; it is gone and must not come back: protocol or server changes land upstream first, then the pin here is bumped. Wire protocol (length-prefixed CBOR frames, amberpack payloads chunked into `TData`), the have/want transfer loop, and the `Server` that `serve/` mounts on `jobs-runner-amber/1.0` + `jobs-amber-admin/1.0` through `HandleStream` — the router owns dispatch. `ALPN` (`amber-store-iroh/1`) is a **wire constant**: renaming it breaks every peer. TAccept/TRef advertise per-endpoint `DataEndpoints` records (identity + candidates); their presence signals the 10s attach gather window. TPin pin-asserts + OnAccess/OnPin/RefGuard hooks for the GC tracker/collector. |
| `amberclient/` | Importable amber sync client over `amberiroh`: dial by endpoint ID, Push/Pull (+WithProgress), refs list. Transfers are sharded (`Conns`, default 4): extra QUIC connections attach to the server's transfer token, want rounds deal across all channels; degrades to the single control stream. Shard dials authenticate the advertised data-endpoint identity and, on discovery dials, bind the relay/net-report stack to hole-punch; extras are skipped (not demoted) while the control path is relayed. Shard conns are pooled (punch once, reuse; grow to PoolMax=12 under concurrency — growth dials in the background, never on a transfer's critical path; shrink after idle). Transfers are reserve-first once the server's data endpoints are cached: DataConns promises only reserved **direct-path** entries; relayed entries are parked (held for the punch, never dealt to, abandoned after ~5 min), so small transfers ride the direct control stream instead of relay shards. |
| `runner/` | Ported stage drivers + sandbox executors; local build/run pipeline (`driveFStages`), develop PTY shell, OCI image export (single-layer docker-load tar + two-layer `AssembleOCIImage` for the registry). |
| `registryd/` | jobs-registry daemon: read-only OCI Distribution API (images named `jobs:<K>` — one repo, tags are build keys), on-demand K→F resolve + amberclient sync into a private store, two-layer image assembly (shell baked by default like `run`/`image`), uncompressed layers streamed from the CAS per request (never cached; the record's layer recipes are the index), manifest/config blob cache with last-read TTL sweep, offline reassembly from records. Pins served images on the server (TPin, hourly re-assert). |
| `clientcli/` | jobs-client command surface: local + remote commands, store flock, liveView TTY progress (NO_COLOR-aware). `contextroot.go` owns source resolution — `repoRoot` (pure `.git` walk, the ONLY repo detection; no `git` subprocess), `defaultSource` (cwd walk-up for an omitted `--source`), `resolveContextRoot` (re-anchor to the context root). Local and remote MUST both go through `resolveSource` in that order or the local↔remote F join breaks. |
| `tui/` | bubbletea admin TUI over `jobs-admin/1.0` (builds watch/logs/cancel/delete, fleet, stats, refs) + the standalone build view (`RunBuildWatch` over the `BuildStreams` seam; `buildtree.go` folds `NodeSnap.Deps` into logical rows) that remote-build/watch run on a TTY. Never block in Update — network I/O only inside tea.Cmd goroutines. |
| `builddef/`, `recipe/` | Build definition identity (canonical CBOR) + Starlark recipe evaluation — ports, seam-swapped. |
| `bootstrap/` | Embedded seed artifacts (shell + fetchers per platform), idempotent seeding. |
| `fetchers/` | ONLY the embedded-seed sources: `github`, `hostmusl`, `hostshell`, `tarballhttps` (+ shared `tarextract`). Every other fetcher/plugin lives in its own `jobs-build/fetcher-*`/`plugin-*` repo, pinned by recipes — the in-repo copies were removed (issue #7); goplugin (incl. `go_closure`, source-closure design §8) is authoritative in `plugin-go`. |
| `sandbox/`, `tailbuf/`, `resources/`, `importdef/` | Verbatim ports from jobs — keep drift-free against upstream. |
| `cmd/jobs-server`, `cmd/jobs-runner`, `cmd/jobs-client`, `cmd/jobs-registry` | The mains (each calls `sandbox.Init()` first). |

## Binaries & commands

- `jobs-server --data-dir <dir> [--bind host:port] [--relay url]
  [--advertise-addr ip[:port]]… [--no-announce] [--data-endpoints N]
  [--gc-retention 720h] [--gc-interval 1h] [--gc-rate N] [--gc-min-free N]
  [--log-level …]` — one iroh endpoint, five ALPNs, embedded NATS + amber
  store; `--data-endpoints` (default 3) binds extra UDP sockets with their own
  punchable identities for sharded store transfers. Prints its endpoint ID on
  startup and announces it for discovery: direct interface addresses
  (auto-detected unless --advertise-addr) over mDNS on the LAN and via pkarr
  over the internet, nearest relay as fallback (relay connect is best-effort —
  an offline host still starts). GC runs hourly: refs unread for the
  retention are deleted and the store mark-sweeps; registry-served images
  are pinned forever; 0 disables.
- `jobs-runner --server <endpoint-id> [--addr host:port]… [--size c1-m2]
  [--cpu N] [--memory NGi] [--slots N] [--name …] [--data-dir …]
  [--skip-self-test] [--sync-conns N]
  [--gc-retention 720h] [--gc-interval 1h] [--gc-rate N] [--gc-min-free N]`
  — runs a boot self-test build
  (embedded shell, real sandbox) and refuses to start if it fails, then dials
  the server twice (NATS tunnel + amber sync), pulls work-queue jobs for
  every fitting class. Admission capacity is the full detected machine
  (cgroup-aware: tightest limit from the process's own cgroup up to the
  root, minus 10% reserve) — the ladder classifies jobs, not runners;
  `--cpu`/`--memory` cap a dimension verbatim, `--size` caps capacity to a
  rung, `--slots` caps concurrent jobs.
  Build work trees live under `<data-dir>/work` (TMPDIR is pointed there;
  swept every boot) — never the OS temp dir, which is a RAM-backed tmpfs
  on NixOS and fills at 50% of RAM. The four `--gc-*`/`JOBS_GC_*` flags
  (same knobs as `jobs-server`) start a `gcsweep` loop over the runner's own
  private cache after the boot self-test passes, on by default
  (`--gc-retention 0` disables); everything in that cache is re-pullable,
  so a wrong expiry only costs one re-pull.
- `jobs-registry --server <endpoint-id> [--addr host:port]… [--listen :5000]
  [--data-dir …] [--cache-ttl 24h] [--default-platform os/arch]
  [--no-shell] [--sync-conns N]` — read-only OCI registry: `docker pull
  <host>:5000/jobs:<build-K>` serves a build output as a two-layer image
  (runtime closure + platform shell, artifact), synced on demand
  from the server into a private store. Layers are **uncompressed**
  (`…layer.v1.tar`) and streamed straight from the store on every request —
  never materialised on disk — so only the manifest and config are cached
  blobs (`--cache-ttl` sweeps those); images reassemble from the local store
  without the server.
- `jobs-client` — every source-building command resolves `--source` from the
  **current directory** when it is omitted: the nearest ancestor of the cwd
  holding the recipe, searched no higher than the repo root (`--source-root`
  overrides the ceiling, `--no-repo-root` pins it to the cwd, an explicit
  `--dir` suppresses the search). The resolved `context: <root> (dir …, recipe
  …)` is always printed to stderr. Identity is unaffected — the same
  `(root, dir)` pair still yields the same F. Every source-building command
  and `remote-build` opportunistically sweeps the local store afterward
  (`clientStore.MaybeGC`), stamp-gated to at most once per 24h
  (`<data-dir>/gc.stamp`) so it stays silent on all but the rare triggering
  run; retention defaults to 720h via `JOBS_GC_RETENTION` (0 disables).
  Client outputs are locally authoritative, so an expired ref just costs a
  rebuild.
  - `build|run|develop [--source <dir>] [--dir …] [--build-file …] [--platform …]
    [--shell-ref …] [--param k=v]…` — local hermetic build / build-then-exec
    entrypoint / interactive PTY shell in the build sandbox (flock held for the
    whole session).
  - `image -o <tar> [--tag …] [--no-shell] [--source <dir>] [<build-K>]` —
    docker-loadable OCI image from a build output. The **positional key** picks
    the mode: given one, image it as-is; given none, build `--source` (the two
    are mutually exclusive).
  - `remote-build --server <id> [--source <dir>] [--cpu …] [--memory …]
    [--no-logs] [--no-tui] [--conns N]` — push source, submit, watch, pull
    output home. On an interactive terminal (stdin+stderr TTYs) the watch
    is a full-screen TUI (`tui.RunBuildWatch`): navigable build-graph tree
    (logical rows fold the buildvalue stage chain; states, durations,
    `(cached)`) + an output pane following the selected node; `q` detaches
    (exit 0), `ctrl-c` confirm-cancels (130), success auto-exits into the
    pull, failure stays for inspection (then exit 1). `--no-tui`, non-TTY,
    old servers (no `NodeSnap.Deps`) and already-terminal requests fall
    back to the classic block view, where `--no-logs` still applies.
  - `watch --server <id> --request-id <id> [--no-logs] [--no-tui]` —
    re-attach to a build; same TUI/fallback matrix minus the pull.
  - `logs --server <id> --node <name> [--follow]` — one node's captured
    output (stored head/gap/tail, raw bytes on stdout); `--follow` keeps
    streaming live chunks until interrupted.
  - `diagnose --server <id> (--request <id> | --node <name>) [--attempts N]
    [--json] [--logs-dir <dir>]` — durable failure report (all failed
    attempts with origin/class/exit, runner, timing, rusage, captured
    output); survives retries and server restarts. `--json` is the
    machine/LLM-friendly shape.
  - `status --server <id>` — one-shot plain-text requests + fleet tables.
  - `admin stats|fleet|requests|refs|gc|pin|unpin --server <id>` — thin
    frame calls; `gc [--garbage 0.4]` forces an immediate sweep tick,
    `pin <ref>`/`unpin <ref>` set/clear the never-expire flag.
  - `gc [--data-dir …] [--garbage 0.4] [--retention …]` — forces an
    immediate sweep of the **local** store (distinct from `admin gc`,
    which sweeps the server); prints disk/refs/store/trees/cycle stats.
    `--retention` overrides `JOBS_GC_RETENTION` for this run only.
  - `tui --server <id>` — interactive admin TUI.

## Sandbox re-exec rule

Every `main()` and every sandbox-driving `TestMain` must call
`sandbox.Init()` first — the sandbox works by re-exec'ing the binary.

## Invariants

- **Identity** = canonical CBOR (fxamacker `CanonicalEncOptions` for defs,
  `CoreDetEncOptions`+`NilContainerAsEmpty` for fstree; no-params = CBOR null
  `0xf6`, never empty map) + the pinned chunker params above.
- **Doneness = ref existence.** Checked at node creation; also crash
  recovery. "Running twice is wasteful but never wrong."
- **Objects before ref**: verify object completeness (`fstree.CheckComplete`)
  before writing any ref.
- Refs are **UNSIGNED** `reference.Reference` records — no sshsign/grants;
  transport identity is the iroh endpoint key.
- **Sibling sources** (docs/design/2026-07-26-sibling-sources.md — read it
  before touching identity, sched, pin, or the sandbox): every `dir != ""`
  def carries `ctx: 2` (widened context; `Definition.Canonical()` MUST copy
  every field or the submit canonicality check strips + rejects it). Buildrun
  is **KP-keyed** (`build-output:<KP>` is doneness AND the cross-context
  memo); `build-output(-deps):F` are server-written aliases — deps STRICTLY
  before output, aliases before the buildvalue goes done. Derived refs
  (`pin-cover/<v>:F`, `kp-tree/<KP>`, `build-pinned:<KP>`, `f-tree/<F>`)
  re-derive on demand — absence after done is a crash window, never a
  failure. Old runners are fenced by the `jobs-runner-nats/3.0` ALPN — bump
  it again whenever an old runner would produce wrong results rather than
  clean errors.
- **Source closure** (docs/design/2026-07-27-source-closure.md): `closure=`
  on the `build()` return is a **COMPLETE cover** — the build dir is NOT
  auto-seeded, mutually exclusive with `sources=`, allowed for root builds
  (`dir == ""`), carried in `Pinned.Closure`, and validated at pin time to
  cover the build dir (the sandbox workdir must exist). goplugin's
  `go_closure` kwarg computes it (pure-Go transitive import walk;
  module-root packages enumerate files + embed globs). No `KPVersion` bump
  (existing derivations are byte-identical); the `/3.0` ALPN is the fence.
- **GC expiry is rebuild-safe**: deleting a cold output ref un-memoizes,
  never corrupts (doneness = ref existence); bootstrap seeds and pinned
  refs never expire; every ref PUT goes through the collector's PrepareRef
  guard (`amber.PutRef` + transport-iroh's `server.handlePush` — a new PUT path MUST take
  the guard too). GC gotchas: `GetKey`/`GetRef` fire the access observer (a
  read IS a touch) — tests asserting expiry must check presence via
  `ListRefs`, which doesn't touch. Refs seed at first sight (safe-upgrade),
  so nothing can expire on a store's first sweep — GC tests need a seeding
  sweep before aging. ONE collector per store: `gc.Open` wipes
  `<store>/closures` and installs the guard — close a Sweeper before
  constructing another (`clientcli/gc.go`'s --retention path shows the
  pattern). `gc.Status` scores only SEALED packs (segments seal at
  256 MiB), so small/young stores report live=0/garbage=0 and pack-reaping
  is unobservable at test scale. `cache/trees/` holds in-flight
  `staging-`/`bin-staging-` temp dirs — any tooling touching trees/ must
  exempt them (the sweep collects them only past 24h).
