# jobs-iroh architecture

**Status:** describes the implemented system as of v0.7.0 (2026-07-23). This
document is the design source of truth; dated implementation specs live in
[`../design/`](../design/).

jobs-iroh is a small, self-contained build system: **one server, N runners,
one client**, connected by exactly one transport — iroh QUIC. There is no
other infrastructure to operate: no HTTP services, no external database, no
orchestration layer. The server is a single binary that embeds everything it
needs (a NATS/JetStream instance for scheduling, a content-addressed object
store for artifacts); runners and clients each own a private store of the
same format and synchronize with the server over the wire.

Four pillars carry the whole design:

- **Content-addressed identity.** Every job is identified by the hash of its
  canonically encoded definition; every artifact by the hash of its content
  tree. Identical inputs are the same job everywhere — cache joins are a hash
  lookup, not a protocol.
- **Doneness = ref existence.** A named reference in the store pointing at a
  complete output *is* the completion record. There is no build database to
  keep consistent with reality; crash recovery is a re-scan.
- **Hermetic execution.** Build scripts run in a rootless Linux namespace
  sandbox with no network, a read-only dependency store, and a fully static
  embedded userland. Only import (fetch) steps get the network.
- **At-least-once scheduling.** Because jobs are idempotent by identity,
  running one twice is wasteful but never wrong — so delivery only has to be
  at-least-once, which an embedded JetStream work queue provides for free.

## 1. Topology

```
        jobs-client                          jobs-runner (×N)
   private store (flock)                private store + sandbox
        │ jobs-build/1.0                      │ jobs-runner-nats/4.0
        │ jobs-admin/1.0                      │ jobs-runner-amber/1.0
        │ jobs-amber-admin/1.0                │
        └───────────────┐        ┌────────────┘
                 iroh QUIC — one server endpoint, five ALPNs
                          jobs-server
        scheduler · embedded NATS (JetStream) · embedded amber store
```

Three binaries:

- **`jobs-server`** — the coordination point. One iroh endpoint (one identity
  key), five ALPNs dispatched by an iroh router. Embeds a NATS server with
  JetStream (`DontListen`: no TCP listener — the only ways in are in-process
  connections and the iroh tunnel), the amber store, the scheduler, and the
  bootstrap seed artifacts.
- **`jobs-runner`** — execution. Dials the server twice (NATS tunnel + store
  sync), owns a private local store as a materialization cache, and runs
  build stages inside the sandbox.
- **`jobs-client`** — a fully local build tool (no server needed) *and* the
  remote client: submit, watch, logs, diagnose, admin, TUI.

A fourth, optional binary — **`jobs-registry`** (§10.1) — is a read-only OCI
registry that serves build outputs as pullable container images. Like the
runner it is purely a dial-side peer: a private store synced on demand over
`jobs-amber-admin/1.0`, plus an HTTP face speaking the Distribution API.

| ALPN | Peer | Purpose |
|---|---|---|
| `jobs-build/1.0` | client | submit a build or test, watch progress, fetch logs, cancel |
| `jobs-runner-nats/4.0` | runner | NATS tunnel — each stream proxies to the embedded NATS server |
| `jobs-runner-amber/1.0` | runner | store sync (objects + refs) |
| `jobs-admin/1.0` | client / TUI | requests, fleet, stats, refs browse, cancel/delete, diagnose |
| `jobs-amber-admin/1.0` | client / registry | store sync — push source trees up, pull outputs (and images-to-be) home |

Server data dir (`--data-dir`, default `~/.local/share/jobs-iroh/server`):
`server.key` (hex-encoded iroh secret key, 0600 — deleting it changes the
endpoint identity), `store/` (object packstore + Pebble refstore), `nats/`
(JetStream storage). On boot the server seeds the bootstrap artifacts (§7)
and logs its endpoint ID — the value every `--server` flag consumes.

**Discovery.** Unless `--no-announce`, the server announces itself three
ways: direct interface addresses (auto-detected and filtered — loopback,
link-local, and container-bridge interfaces are dropped — or `--advertise-addr`
verbatim) over **mDNS** on the LAN (re-announced every second) and **pkarr**
over the internet (re-published ~5 min), with the nearest **relay** as a
fallback path (the relay probe is best-effort and bounded — an offline host
still starts). Clients and runners resolve a bare endpoint ID through the
same stack (mDNS + pkarr + DNS) and race all candidate paths; an explicit
`--addr` skips discovery entirely.

Sandboxed execution requires Linux; seed artifacts ship for `linux-amd64`
and `linux-arm64`. The server and the client's observation commands are
OS-independent; local builds and runners are Linux-only.

## 2. Identity

Two hash families name everything. Both are amber *file keys*: the input
bytes are chunked with pinned parameters — content-defined byte chunking
(min 32 KiB / normal 128 KiB / max 256 KiB) and item-chunker bit width 7 —
and the root hash of the resulting tree is the key. These parameters are
**identity-critical**: changing them changes every key and breaks
deduplication between stores. Never change them.

Definitions are hashed as canonical CBOR:

- Definition types encode with fxamacker/cbor `CanonicalEncOptions` (sorted
  keys, shortest forms). Content trees (fstree) encode with
  `CoreDetEncOptions` + `NilContainerAsEmpty`.
- A definition with no params encodes params as CBOR null (`0xf6`), **never**
  as an empty map — the two hash differently.
- Inputs are sorted and deduplicated before every encode.

**K — job identity.** `FileKey(canonical CBOR of the definition)`.

- An *import* definition is `{fetcher, params, requiredTags, platform,
  fetcherDef?}` — a fetch job, pinned to a platform. `fetcherDef` (optional)
  is the canonical definition of the build that produces the fetcher itself
  (§7); absent means a named seed fetcher.
- A *build* definition is `{source input, dir, platform, params, buildJobs?,
  buildFile?}` where `source input` is a wrapper `{kind ∈ import|build|tree,
  definition}`. Because a build definition embeds its source input's complete
  inner definition, K content-addresses the entire transitive definition
  graph.

**F — build-content identity.** The file key of the *build-from tree*: a
store tree `{env/ (the source subtree), params, platform, [BUILD.jobs
override]}` assembled from actual content. The `buildfrom` stage computes
K → F and records it as the ref `build-from:K`. Everything downstream —
plugin resolution, pinning, execution — is keyed by F, so two different
definitions that produce byte-identical source content share all of the
actual build work.

Scheduler node names use the grammar `<kind>_<hexkey>` (subject-safe,
parsed fail-closed).

## 3. The build model

### 3.1 Recipes

A build is described by a Starlark file, `BUILD.jobs` by default, at the
definition's `dir` (an inline override or alternate filename can ride in the
definition). Evaluation is hermetic: no `load()`, `print` is captured. The
user defines up to two functions:

- **`plugins()`** (optional) → `{name: input}` of plugin builds, or
  `struct(plugins = {…}, deps = {…})` where `deps` are resolution-time
  dependencies readable during `build()`.
- **`build()`** (required) → `struct(inputs = {name: input}, env = {str:
  str}, script = str, runtime_deps = [input], caches = {path: id},
  resources = struct(cpu=, memory=), name = str)` — `caches`, `resources`,
  and `name` optional; `name` is display-only and never part of identity;
  `runtime_deps ⊆ inputs` is enforced.

Predeclared symbols:

| Symbol | Meaning |
|---|---|
| `platform`, `params` | the build's platform string and decoded params |
| `source.read(path)` / `source.exists(path)` | read the source tree at evaluation time |
| `imp(fetcher, params, requiredTags=[], platform?)` | declare an import (fetch) input; `fetcher` is a seed name or a `fetcher(...)` value |
| `bld(source, dir, platform, params, …)` | declare a build input (build-of-a-build) |
| `subbuild(dir, …)` | a build input over a strict subdirectory of the same source tree |
| `fetcher(name, url=, sha256=, build=)` | declare a fetcher that the system builds itself (§7) |
| `deps[name].read/exists/path()` | resolution deps declared by `plugins()` |
| `plugins[name](**kwargs)` | call a plugin (available in `build()` only) |

A minimal recipe:

```python
def build():
    toolchain = imp(fetcher = "tarball+https",
                    params = {"url": "https://go.dev/dl/go1.24.linux-amd64.tar.gz"})
    return struct(
        inputs       = {"toolchain": toolchain},
        env          = {"GOFLAGS": "-mod=mod"},
        script       = "go build -o $out/app .",
        runtime_deps = [],
    )
```

Plugins are sandboxed helper binaries (`./plugin`, CBOR over stdio, exit 75
= retryable) that expand ecosystem lockfiles into import inputs — e.g. the
Go plugin turns a `go.sum` into one module-proxy import per dependency, so
every module is its own cached, content-addressed fetch.

**Tests** ([tests design](../design/2026-11-07-tests.md)) are a second
recipe, `TESTS.jobs`, next to `BUILD.jobs`. It defines `test()` — the same
struct as `build()` minus `runtime_deps` (an error if present) — and may
define `plugins()` exactly like a build. One extra predeclared symbol,
`build`, is an Input naming **this directory's own build** (same context,
dir, platform and params, default `BUILD.jobs`) — the very definition
`jobs-client build`/`remote-build` of that directory constructs, so a test
that lists it (`inputs = {"app": build}`) joins the user's build node and
finds the artifact at `/jobs/store/<BOK>` through `JOBS_DEPS`. A test
produces no output: `$out` exists and is discarded; exit 0 is a pass,
anything else a hard failure. Passes are recorded so an unchanged test never
runs twice (§3.2, §3.3).

### 3.2 The stage pipeline

Every build value is produced by a fixed pipeline of stages. Five stage
kinds are *placeable* (schedulable onto runners); a sixth, `buildvalue` (K),
is a server-internal orchestrator that walks its build's stages sequentially
— consumers depend on `buildvalue`, never on individual stages.

| Stage | Key | Does | Publishes |
|---|---|---|---|
| `import` | K | runs the fetcher in a **hermetic, network-enabled** sandbox (the shell userland + the fetcher's runtime closure at `/jobs/store/<key>`, `JOBS_FETCH_PARAMS`, `JOBS_OUTPUT_DIR`), ingests the result | `import-output:K` |
| `buildfrom` | K | resolves the source input's content tree, splices `dir` as `env/`, computes F; pure store computation, no sandbox | `build-from:K` + `build-from-tree:F` (one batch) |
| `pluginresolve` | F | evaluates `plugins()` (no plugins callable there), ingests plugin/dep definitions | `build-plugin-resolved:F` |
| `pin` | F | materializes resolution deps, evaluates `build()` with **sandboxed** plugin callers, validates | `build-pinned:F` |
| `buildrun` | F | resolves every input's output + runtime closure, assembles the read-only store union, runs the script hermetically | `[build-cache:…]*, build-output-deps:F, build-output:F` (ordered) |
| `testrun` | KT | the buildrun assembly and sandbox over `build-pinned:<KT>`, under the pinned `timeout` (default 5m); keeps nothing — exit 0 records the pass, a timeout is a hard failure | `[build-cache:…]*, test-passed:<KT>` |

The *pinned* job (`build-pinned:F`) is the fully resolved description —
concrete inputs, env, script, runtime-dep keys, caches, resources, a test's
timeout — the thing `buildrun` actually executes.

A **test** shares `buildfrom`, `pluginresolve` and `pin` with builds (the
test F-tree carries its recipe as a top-level `TESTS.jobs` entry — always
spliced, since the entry is the mode marker the eval stages branch on — so a
test's F never collides with a build's) and ends in `testrun`, driven by a
`testvalue` orchestrator. Its run identity is **not** KP: a pinned job names
inputs by *definition*, and the `build` input's definition embeds the whole
source tree, so every commit would re-key the test even when the build
output is a memo hit. Instead the testvalue requires every pinned input
itself, waits for them, and derives **KT** = key of `{job.cbor, platform,
v, src/}` where `src/` is the KP tree's covered source and `job.cbor` is a
`TestJob` — script, env, dir, cache ids, and the inputs **resolved to
content keys** (the `c/` artifact + runtime-closure tree of a build, the
output tree of an import). Resources and the timeout are left out on
purpose. A test re-runs
iff its script, environment, covered sources, platform or any input's
*bytes* changed; a docs-only commit re-keys K and F, re-runs the cheap eval
stages, memo-hits the build on KP and the test on KT. `cover.DeriveTest` is
the single, identity-critical derivation the server and the local pipeline
share, like `cover.Derive` for KP.

### 3.3 Ref namespaces

The store's named refs are the entire system state:

| Ref | Points at |
|---|---|
| `import:K`, `build:K` | definition-present bookkeeping (the def object itself) |
| `import-output:K` | an import's fetched tree |
| `build-from:K` | F (the K→F bridge) |
| `build-from-tree:<hexF>` | the F-tree itself (F-keyed so F-only stages resolve it by name) |
| `build-plugin-resolved:F` | plugin-resolution result |
| `build-pinned:F` | the pinned job |
| `build-output:F` | the structured output tree (`c/…`) — **the doneness marker** |
| `build-output-deps:F` | flat store tree of the runtime closure |
| `build-cache:<id>:<platform>` | mutable persistent cache state (e.g. a module cache) |
| `fetcher:<name>:<platform>`, `shell:<platform>` | bootstrap seed artifacts |
| `seed-src:<ref>:<hash8>` | per-blob seeding idempotence marker |
| `test:K` | a submitted test definition (the `build:K` analogue; the registry's tag listing skips these Ks) |
| `build-pinned:<KT>`, `kt-tree/<KT>` | the pinned test under its run key + the KT tree's pull carrier (server-derived, re-derived on demand) |
| `test-passed:<KT>` | KT — **the test doneness marker and memo**; absent for a failed test, so it re-runs on the next submit |
| `test-passed:<F>` | KT — the F-level alias the testvalue fast-path and `remote-test`'s pull-home read |

### 3.4 Correctness invariants

- **Doneness = ref existence.** Checked when a scheduler node is created
  (fast-path done) — which is also the crash-recovery story: on restart the
  server re-drives live requests and finished subtrees short-circuit.
- **Objects before ref.** Before any ref is written, the referenced tree is
  verified complete (`fstree.CheckComplete`) — runner-side after every pull,
  server-side before every commit. A ref never points at a partial object
  graph.
- **The ref gate.** The server commits runner-proposed refs through an exact
  per-stage allow-table: each stage kind may publish only its own names
  (§3.2); `build-from:K` and `build-from-tree:F` must appear in the same
  batch and cross-check; `build-cache:<id>` is accepted only for cache ids
  declared in the pinned job (buildrun and testrun alike); batches commit in
  payload order (caches → `build-output-deps:F` → `build-output:F` last, so
  the doneness marker is written last; a testrun's `test-passed:<KT>` last
  likewise); anything else fails closed.
- **Refs are unsigned** (`{name, key, createdAt}`, last-write-wins). There is
  no signing or grant layer; transport identity is the iroh endpoint key
  (§10).

## 4. The store

Every process embeds the same store, built on the `amber-store-core`
library: an object **packstore** (chunked, deduplicated, content-addressed)
plus a Pebble-backed **refstore** (name → key), guarded by a flock — one
process per store; the client additionally serializes its own invocations
with a store-level flock (exclusive for local builds, shared for
remote-build).

The store seam (`amber/`) provides: ingest (`IngestFile`, `IngestDir`,
`IngestSourceDir` — the latter honors `.amberignore` and drops control
files), pure hashing (`FileKey` computes a key without storing — the same
code path as ingest, so hash-then-store can never disagree), tree assembly
(`BuildFromTree`, `BuildStoreTree`, `TreeSubdir`), resolution
(`ResolveBuildOutput`/`ResolveBuildArtifact` follow `build-from:K` → F →
output), reads (`Ls`, `ReadFile`, `Tar`), and **materialization** — restoring
a tree to disk via in-process tar export/extract. There is no FUSE layer;
inputs are always materialized to real files, then bind-mounted read-only
into sandboxes.

**Sync.** Object/ref transfer between stores uses the amber sync protocol
(wire format from the `amber-store-iroh` dependency), mounted on the two
store ALPNs. The importable client (`amberclient/`) keeps one control QUIC
connection with one stream per operation — `Push`/`Pull` (`…WithProgress`),
`Refs` — and **shards transfers**: with `Conns` > 1 (default 4, max 16) a
push or pull requests `DataConns` extra channels, the server answers with a
transfer token, and the client attaches one stream per extra QUIC
connection — each on its own endpoint and UDP socket, since one socket's
loop caps throughput — so the want loop deals every round across all
channels. The server can bind dedicated data endpoints
(`--data-endpoints`, default 3), each with its **own punchable identity**:
when announcing, every data endpoint holds a relay home connection and
QAD-learns its public mapping, and `TAccept`/`TRef` advertise per-endpoint
`DataEndpoints` records — identity plus live dial candidates — alongside
the `DataPorts` LAN fast path. A sharding client races the dedicated port
with the advertised candidates under the record's identity; on
discovery-dialed clients the shard endpoints bind the control dial's
relay/net-report stack, so a relay-won shard hole-punches to direct
mid-transfer exactly like the control connection does. The record's
presence signals the 10s attach gather window (up from 5s; gather still
early-exits when every promised shard lands), and each shard's attach runs
under a budget safely below that window (a late attach would be a dead
channel). Clients skip extras entirely — without demoting — while the
control path itself is relayed; on a young connection (< ~30s) the gate
first waits briefly for the punch to land, so a transfer that starts
moments after boot is not pinned single-channel to the relay for its whole
run. Unreachable candidates, failed attaches, or
a server without sharding degrade the transfer toward the single control
stream: a connection that attaches zero shards demotes itself to
single-channel for its lifetime, and a failed sharded transfer is retried
once unsharded (push is force-mode, pull resumable — running twice is never
wrong) before the error surfaces. Old clients ignore the records: their
dedicated-port dial fails the identity check and falls back to the control
candidates, sharding onto the main socket. Shard connections are pooled per
client: acquired per transfer (a fresh stream + `TAttach` each), released
open, grown under concurrent transfers (`Conns`→`PoolMax` totals, default
4→12) and shrunk back after ~90s idle — so the punch ramp is paid once per
connection, not per transfer. Growth dials run in the **background**: a
transfer that can be served from live entries never waits on a dial (only
a cold pool does, bounded by the attach budget). Once a server's
data-endpoint advertisement is cached (its first sharded reply), transfers
are **reserve-first**: the shard entries are picked before the request goes
out and `DataConns` promises exactly the shards that will attach, so the
server never sits out its gather window for shards that cannot come. Only
**direct-path** entries are reserved — an entry whose connection still
rides a relay is *parked*: held in the pool for its punch to land (punches
through hostile NATs take minutes; evicting them, as pre-v0.23 relayGrace
did, kills every punch mid-ramp and locks shard traffic onto the relay),
never handed to a transfer (sharding a want round across relay channels
just gates the round on the relay's RTT), and abandoned for a background
replacement only after ~5 min. With every shard parked the transfer runs
single-channel on the (direct) control stream and sharding resumes when a
punch lands. Nodes carry display-only labels — recipe dep
names, dirs, fetcher names, the client-sent target label — assigned at
unfold from `PinnedInput.Name`/plugin-dep map keys and surfaced through
`NodeSnap.Label`, failure records, and the local driver's progress steps;
labels never enter identity. Every pooled connection's path is logged
(dial and changes). Push is
force-mode (last-write-wins); Pull verifies every object against its key
(the peer is untrusted) and writes the local ref only after the full
closure is present — objects-before-ref holds across the wire too.

## 5. Transport

**Client API framing** (`jobs-build/1.0`, `jobs-admin/1.0`): each frame is a
4-byte big-endian length + a CBOR envelope `{t: string, b: bytes}`; max
frame 16 MiB; one request per stream, but watch/log streams stay open for
server-pushed frames. Build-ALPN requests: `submit`, `watch`, `logs`,
`cancel`. Admin adds: `requests`, `fleet`, `stats`, `refs`, `delete`,
`diagnose`. Replies: `submitted`, `snapshot`, `logview`/`logchunk`, `ok`,
`error` (+ the admin `*-reply` frames). While serving a request the server
watches the stream; anything unexpected from the client (including close)
cancels the work.

**The NATS tunnel** (`jobs-runner-nats/3.0`, `natsiroh/`): the runner's
stock NATS client is given a custom dialer that opens an iroh stream and
returns it as a `net.Conn`; the server side proxies each accepted stream to
an in-process connection of the embedded NATS server. One wrinkle: a QUIC
stream does not exist for the peer until the opener sends a byte, but in
NATS the *server* speaks first — so the dialer writes a single `0x00`
preamble byte (consumed by the proxy) to force stream creation. The NATS
client reconnects forever; the connection source re-dials iroh when the
underlying connection dies.

## 6. The scheduler

The scheduler lives in server memory; JetStream carries the queues and the
durable trail.

**Node graph.** An in-memory table keyed `(kind, key)`; get-or-create *is*
the join — two requests needing the same subtree converge on the same node
with zero coordination. States: `waiting → ready → queued → running →
publishing → done | failed`. Unfolding is per kind: imports depend on their
fetcher's build; buildfrom on its source value; pin reads
`build-plugin-resolved:F`; buildrun and testrun read `build-pinned:<key>`.
A `testvalue` (the test orchestrator, §3.2) walks buildfrom → pluginresolve
→ pin like a buildvalue, then requires every pinned input *under itself*,
derives KT once they are all done, requires `testrun(KT)` — whose doneness
fast-path on `test-passed:<KT>` is the memo — and writes the
`test-passed:F` alias before going done. Definitions travel in-band inside
job messages, so a runner never misses a def read.
Requests hold *interest* in nodes; cancellation drops interest, and a node
with no interest is dropped from memory, its queued job message best-effort
deleted from the work queue. An attempt already in a runner completes and
its result is dropped on arrival. Shared subtrees survive as long as
anyone needs them.

**Placement.** A placeable node's requirement = max(kind default, pinned
`resources`, request override), rounded **up** to a fixed size-class ladder:
`c0.2-m1, c1-m1, c1-m2, c1-m4, c2-m4, c2-m8, c2-m16, c4-m8, c4-m16`
(`cN` = N cores as millicpu, `mN` = GiB). The job is published to the
subject for its `(platform, class)`; any runner big enough for that class
may take it.

**NATS layout** (one embedded server; JetStream streams + KV):

| Piece | Config | Purpose |
|---|---|---|
| stream `JOBS` | WorkQueuePolicy, subjects `jobs.<platform>.<class>`, duplicates 10 m | one message per node attempt; payload = `Job{node, kind, key, gen, def, platform, class, pullRefs, cpuMilli, memBytes}`; msg-id `job-<node>-<gen>` |
| durable consumers `wq-<platform>-<class>` | AckExplicit, AckWait 90 s, MaxDeliver 25, MaxAckPending 4096 | one per (platform, class), **shared** by every runner that fits the class |
| stream `RESULTS` | LimitsPolicy, `results.<node>`, MaxAge 7 d, duplicates 15 m | terminal attempt reports `Result{class, exit, errSummary, refs[], rusage, scratchRef}`; msg-id `result-<node>-<gen>` dedups redelivery |
| stream `FAILURES` | LimitsPolicy, `failures.<node>`, MaxAge 7 d, 8 msgs/subject, 1 GiB cap | durable per-attempt failure records (§9) |
| KV `status` | node keys + `req.<id>` | current phase per node / per request; KV revision is the watch cursor |
| core `logs.<node>` | no stream | live output chunks, fire-and-forget |
| core `runners.hello`, `runners.<id>.hb` | no stream | fleet presence + capacity for the admin view |

**Result commit.** The server consumes `RESULTS`: gate-check the proposed
ref names → `CheckComplete` every key against the server store → write refs
in payload order → delete the runner's scratch ref → advance the graph →
update `status`. A gate rejection or completeness failure converts an "ok"
result into a failure with origin `gate`/`commit`.

**Failure handling.** Result classes: `ok`, `hard`, `retryable`, `control`,
`cancelled`. One hard failure fails the node. Retryable failures (exit 75 —
the conventional "try again" exit for fetchers and plugins) get a budget of
3 with 1 s → 30 s capped backoff; control-class results (infrastructure
problems, e.g. a pull failure) re-enqueue without burning budget;
MaxDeliver 25 is the poison-message backstop. `failed-upstream` is derived
per request from its snapshot. FAILED is memory-only — retry is resubmit
(doneness-by-ref makes that cheap). Node generations seed from the wall
clock so JetStream dedup windows survive server restarts. There is no
runner-reconnect adoption: an orphaned in-flight job simply redelivers after
its ack deadline lapses — wasteful but never wrong.

## 7. Bootstrap and self-hosting fetchers

A fresh system needs a floor to stand on. Four seed artifacts per platform
are embedded in the binaries as zstd tarballs and published into the store
on server boot: `shell:<platform>` (a fully static userland — busybox-style
tools + bash, no host paths), and the seed fetchers
`fetcher:tarball+https:<platform>`, `fetcher:hostmusl:<platform>`,
`fetcher:github:<platform>`. Seeding is idempotent per blob: a
`seed-src:<ref>:<hash>` marker records exactly which blob was published, so
restarts skip unchanged seeds and a changed seed re-rolls.

Everything else bootstraps from there. A recipe can declare `fetcher(name,
url=…, sha256=…)` — sugar for a `tarball+https` import that *builds the
fetcher itself*; its canonical definition rides inside the import definition
(`fetcherDef`), making the fetcher an ordinary content-addressed dependency.
Language plugins bundle companion fetcher pins (a `fetchers.toml` inside the
plugin artifact), so e.g. Go module fetches use a fetcher the system built.
In-tree fetchers cover tarballs (https/xz/extract), GitHub sources, host
toolchain imports (`hostshell`, `hostmusl`), and package ecosystems (Go
modules, npm, PyPI, crates, RubyGems, Alpine APK) plus the per-language
plugin companions.

## 8. Execution

### 8.1 Sandbox

Rootless, Linux-only, no daemon. `sandbox.Run` re-execs the current binary
(`/proc/self/exe`) with a sentinel argv[0] and the sandbox config in the
environment; **every `main()` (and every sandbox-driving `TestMain`) must
call `sandbox.Init()` first** — it detects the re-exec'd child, performs
setup inside the new namespaces, and execs the target command. Namespaces:
user + mount + PID + net + UTS + IPC — the net namespace means **no network**
(imports get the same sandbox without the net namespace, see below). The child makes the mount tree
private, applies the mount plan (read-only binds need a remount pass),
mounts a fresh `/proc`, `pivot_root`s, and execs. Cgroups v2 limits
(`memory.max`, `pids.max`) apply best-effort via `clone3` with a cgroup fd.

Inside a build: the dependency union is materialized and bind-mounted
read-only at `/jobs/store/<key>`; the source subtree is writable at
`$SRC=/build/src`; outputs go to `$out=/build/out`; declared caches are
bound writable at their paths; `/tmp` is a tmpfs; `/dev` is a hermetic
minimal set; the input name → path map is carried at
`/build/.jobs-deps.json` and the script at `/build/.jobs-script.sh`. The
shell is the embedded static userland — nothing from the host leaks in.

Inside an import: the same pivot_root'ed root, but the host **network is
kept** (no net namespace). `/jobs/store/<key>` holds the shell artifact and,
for a recipe-declared fetcher, the fetcher build's runtime closure
(`build-output-deps:F`) at the paths its build saw — a fetcher that needs a
toolchain declares it as `runtime_deps` and bakes the path, like an image
entrypoint; `/bin` and `/usr/bin` are the shell's bash/jq plus every busybox
applet (shebangs resolve, `mktemp`/`sleep`/`wget`/… exist); the fetcher
artifact is read-only at `/jobs/fetcher` (cwd, `./fetch`), the output tree
writable at `/jobs/out` (`JOBS_OUTPUT_DIR`), the secrets file at
`/jobs/secrets.json`, `/tmp` is writable scratch on the work filesystem;
`/etc` carries the hermetic baseline plus the host's resolver files and CA
bundle (`SSL_CERT_FILE` pinned to it). The environment is PATH, HOME, TMPDIR,
the `JOBS_*` variables and the proxy/`SSL_CERT_*` pass-through — never the
runner's `os.Environ()`. Nothing else from the host is visible: a fetcher
may rely on the static shell userland and its declared runtime deps, and on
nothing else. Hosts without user namespaces (and the local `develop`
path) run fetchers as a plain host subprocess instead.

### 8.2 The runner daemon

- **Boot self-test.** Before serving, the runner builds a tiny real build
  (embedded shell seeded under a self-test-only ref, per-boot nonce params
  so the result is never cached) through the full pin → run pipeline in the
  real sandbox, then deletes the refs. If the sandbox doesn't actually work
  here, the runner refuses to start (`--skip-self-test` overrides).
- **Lanes.** A runner's capacity is its full detected machine capacity
  (cgroup-aware — the tightest `memory.max`/`cpu.max` from its own cgroup
  up to the root, so a Kubernetes pod limit applies whatever the cgroup
  namespace mode — minus a 10% reserve; `--cpu`/`--memory` cap either
  dimension verbatim, `--size` caps it to a ladder rung, `--slots` caps
  concurrent jobs) — the ladder classifies jobs, not runners. It consumes every ladder class that fits within that capacity,
  binding the shared durable consumer per class. A single sweep loop
  walks classes largest-first; a job is fetched only after its class's
  rung is reserved in an admission ledger (Σ held cpu/mem ≤ capacity),
  so the runner never over-commits.
- **Per-job flow:** fetch → pull every ref closure in the job's `pullRefs`
  (skip if present-and-complete; one re-pull on incompleteness, then hard
  fail) → ingest the in-band definition (verifying its hash equals the
  node's key) → run the stage driver → push all output roots as one tree
  under a scratch ref `runner-push/<runner>/<node>-<gen>` → **publish the
  result, then ack** the job message. Result-before-ack plus JetStream
  msg-id dedup makes the crash window safe: a redelivered job either finds
  the result already published (deduped) or re-runs idempotently.
- While executing, the runner heartbeats the message (`InProgress` every
  5 s against the 90 s ack window); a dead runner just stops heartbeating
  and the job redelivers elsewhere. Presence heartbeats
  (`runners.<id>.hb`, every 5 s, capacity + in-flight) feed the fleet view;
  `runners.hello` announces on every (re)connect.
- **No-runners policy.** The fleet fold doubles as a scheduling gate: a
  runner is *live* while its last hello/heartbeat is within 15 s (three
  missed beats). A submit whose closure still needs runner work on a
  platform with no live runner is rejected outright (`unavailable`) — a
  fully cached closure completes without any fleet. An accepted request
  whose platform then loses its last live runner for a continuous 5 minutes
  is failed by a watchdog (checked every 10 s): interest drops exactly like
  cancel, and the reason rides the snapshot's request-level `error` field.
  A runner reappearing before the deadline resets the clock; hello-on-
  reconnect means a server restart re-learns platforms from the fleet
  itself.

## 9. Observability

**Live progress.** Watch streams deliver coalesced snapshots (per-node
phase, generation, server-computed elapsed, error summaries, counts).
Runner-side events (exec start/phase/output/heartbeat…) ride core NATS
through a sink seam; build output is chunked by an output writer (32 KiB
chunks, flushed at 64 KiB or 100 ms, 16 MiB per stream then truncate-and-
mark) and published fire-and-forget to `logs.<node>`.

**Log store.** The server folds `logs.>` into per-(node, generation) ring
buffers — 1 MiB head + 3 MiB tail, newest generation wins. Log fetches
return head + gap marker + tail, then follow live. Logs are memory-only by
design — with one exception:

**Durable failure records.** At the moment the scheduler decides an
attempt's fate (retry or fail), it folds a `FailureRecord` into the
`FAILURES` stream: origin (`result` — runner-reported; `gate`/`commit` —
an ok result the server refused; `server` — no runner attempt), disposition,
the runner's verbatim result (class, exit, rusage, proposed refs), retry
counters, interested request ids, timing, and a trimmed snapshot of the log
ring (256 KiB head + 512 KiB tail, sized so one record fits one NATS
message). Bounded (7 d, 8 attempts/node, 1 GiB) and strictly best-effort —
losing a record costs observability, never correctness. The admin `diagnose`
frame (and `jobs-client diagnose`, with `--json` as the machine-friendly
shape) reads the trail back — per failing node, every recorded attempt with
its captured output — surviving retries and server restarts.

**Admin surface.** `status` (one-shot tables), `admin
stats|fleet|requests|refs`, and a bubbletea TUI (builds with watch/logs/
cancel/delete, fleet, stats, refs browse). TUI rule: never block in
`Update` — network I/O only inside command goroutines.

## 10. The client

**Local builds** (`build`, `run`, `develop`, `image`) need no server: the
client drives the stage pipeline in-process against its private store,
skipping any stage whose output ref already exists. Identity is what makes
local and remote interchangeable — the same source yields the same K/F
everywhere, so caches join across machines by construction.

- `run` executes the built artifact's entrypoint (`JOBS.entrypoint` in the
  output tree) in a run sandbox, passing the exit code through.
- `develop` opens an interactive bash PTY inside the *exact* build sandbox
  (script printed, not run) — the store flock is held for the whole session.
- `image` assembles a reproducible single-layer OCI tarball (epoch-zero
  timestamps) from the artifact + runtime closure, loadable with
  `docker load`.
- `test` runs the directory's `TESTS.jobs` through the same local pipeline
  (the context search looks for `TESTS.jobs`; `--test-file` overrides),
  building every input the recipe names — the directory's own build when
  listed — and either memo-hits `test-passed:<KT>` or runs the test and
  records it. Prints the test identity F and the run identity KT.

**Remote builds** (`remote-build`): ingest the source tree locally → push it
under a client scratch ref over `jobs-amber-admin/1.0` → submit the
canonical definition over `jobs-build/1.0` (remote F equals local F by
construction) → watch to terminal, rendering an in-place TTY progress block
(`NO_COLOR`-aware; falls back to append-only lines without a TTY) with the
running steps' build output streaming alongside by default (`--no-logs` opts
out) → on success, pull the output and its runtime closure home. SIGINT
sends a cancel frame and exits 130; a second signal kills. `watch`
re-attaches to a running request; `logs --follow` streams one node's output.
`remote-test` is the same flow for a test definition: on `done` it pulls
`build-from:K` (→ F) and `test-passed:F` (→ KT) home and writes
`test-passed:<KT>` locally, so a later local `test` of the same tree is a
memo hit at both levels. The watch views name the request by kind (a
snapshot holding a `testvalue` is a test), and `status` has a KIND column.

### 10.1 The registry (`jobs-registry`)

`jobs-registry` serves build outputs on the protocol container platforms
already speak: a **read-only** OCI Distribution registry (pull only —
GET/HEAD; push endpoints answer 405) with the jobs-server endpoint ID as its
one load-bearing configuration (`--server` / `JOBS_SERVER`). Images are
named `jobs:<K>`: one repository, whose tags are build keys **K** (64
lowercase hex) — a K is already an immutable content address, so
`/v2/jobs/tags/list` is the catalog of submitted builds (a bare F from a
directly pushed `build-output:F` also pulls, but is not listed).
`docker pull <registry>/jobs:<K>`.

Images have **two layers**: the runtime closure (each dep at
`/jobs/store/<BOK>`, exactly the layout the artifact was linked against,
plus the platform shell with its `/bin/sh` and `/jobs/shell` symlinks —
script entrypoints carry fixed shebangs, and `run`/`image` provide the shell
too; `--no-shell` opts out, and an unseeded platform degrades to shell-less)
and the artifact (the output `c/` tree at the image root, plus a writable
`/tmp`). The deps layer is a pure function of the sorted dep set + shell, so
builds sharing a closure share the blob and clients re-pull only the
artifact layer. Assembly shares the deterministic tar normalisation with
`jobs-client image` (epoch mtimes, sorted entries), so blob digests are
reproducible; a missing `JOBS.entrypoint` is tolerated (the image just has
no entrypoint). The config's os/arch comes from the build's stored
definition (`build:K`), falling back to `--default-platform`.

Layers are served **uncompressed** —
`application/vnd.oci.image.layer.v1.tar`, so a layer's blob digest is also
its diffID. Gzip bought nothing here and cost twice: a whole extra pass over
the content at assembly, and a decompression on every client pulling over a
LAN from a store that already deduplicates. Uncompressed layers also make
the bytes **derivable**: the tar is a pure function of a `runner.LayerSpec`
(a dep set + shell, or an artifact key) and the content-addressed store, so
the registry records the *spec* — a few hundred bytes — instead of the blob,
and regenerates the tar straight into the response on every request.
**Layers are never materialised on disk**: assembly streams each layer once
to measure it (digest + size) and throws the bytes away, and a blob GET is a
tar generated from the CAS as the client reads it. `http.ServeContent` over
a seekable view of that generator (arithmetic seeks, restart-and-discard for
ranges) keeps HEAD, `Range` resumes and `Content-Length` correct.

On a first pull the registry resolves K→F from the server's **ref listing**
(the `build-from:K` ref's *value* is F — pulling that ref would drag the
whole source env in), syncs `build-output:F` / `build-output-deps:F` into
its own flocked store via `amberclient.Pull` (verified, objects-before-ref),
assembles the image, and caches the only two blobs it keeps — the manifest
and the config, both small JSON — in `<data-dir>/blobs`. A blob file's
**mtime is its last-read time**; a periodic sweep deletes blobs unread for
`--cache-ttl` (default 24h). Per-image records under `<data-dir>/repos`
persist and now carry each layer's descriptor plus its spec; they are what
makes a blob answerable, rebuilt into an in-memory digest→spec index at
startup. So an expired image reassembles from the local store without the
server, and a request for a swept manifest recovers by reassembling the
image that referenced it. Concurrent first pulls of one K singleflight into
a single assembly — but concurrent *layer* pulls do not: each streams its
own tar, so serving costs CPU proportional to clients where it used to cost
one `sendfile`. Multi-range blob requests are answered whole, which bounds
a request to one pass over the content (and keeps the generator off
`ServeContent`'s unjoined goroutine).

The disk story is now lopsided: `--cache-ttl` expires only manifests and
configs, a few KB per image, while the store and the per-image records —
neither of which is ever reclaimed — hold everything that matters. The
store GC follow-up (§11) is what would fix that; until then the registry's
volume must be sized for every build it has ever served, and the only
reclamation is deleting the data dir.

## 11. Trust model

Whoever knows the server's endpoint ID may connect to any ALPN — the
deployment model is a LAN, tailnet, or otherwise access-controlled network,
with the iroh endpoint key as transport identity and QUIC providing
encryption and authentication of *that key*. The ref gate protects against
accidents, not malice: a runner could bypass it by writing refs directly
through the open store ALPN. Accepted for v1; the known follow-ups are
per-ALPN endpoint allowlists, a name-filter hook on the runner store ALPN,
secrets and runner tags for placement, and store GC/retention (refs and
NATS streams age out; the object store currently only grows).

The registry adds a boundary of its own: its HTTP face. Object *content* is
verified against keys on every sync (a MITM cannot forge objects), but
name→key bindings are only as trustworthy as the server whose endpoint ID
the registry pins, and the registry's HTTP side authenticates and encrypts
nothing itself — TLS and access control belong to the ingress in front of
it, the same access-controlled-network assumption the ALPNs make. OCI
digests give pull clients end-to-end integrity below the manifest.

## 12. Repository layout

| Package | What it is |
|---|---|
| `amber/` | the store seam (§4) — pinned chunk params live here |
| `amberclient/` | importable store-sync client (§4) |
| `natsiroh/` | the NATS-over-iroh tunnel (§5) |
| `wire/` | frozen scheduler wire contracts: node grammar, phases, Job/Result/FailureRecord CBOR, class ladder, NATS names |
| `api/` | frozen client API frames for the build/admin ALPNs (§5) |
| `events/` | build-event schema + output writer + sink seam (§9) |
| `sched/` | the scheduler (§6): node graph, unfold, ref gate, folds, retries, failure records |
| `serve/` | jobs-server composition (§1): router × 5 ALPNs, embedded NATS + store, API handlers, seeding |
| `runnerd/` | the runner daemon (§8.2) |
| `runner/` | stage drivers, sandbox executors, local pipeline, develop/run/image |
| `registryd/` | the OCI registry daemon (§10.1): Distribution API, on-demand sync, layers streamed from the CAS, manifest/config blob cache + sweep |
| `clientcli/` | jobs-client commands (§10), store flock, TTY progress |
| `tui/` | the admin TUI (§9) |
| `builddef/`, `importdef/` | definition types + canonical identity (§2) |
| `recipe/` | Starlark recipe evaluation (§3.1) |
| `bootstrap/` | embedded seeds + idempotent seeding (§7) |
| `fetchers/`, `plugins/` | fetcher implementations + the Go plugin (§7, §3.1) |
| `sandbox/` | the namespace sandbox (§8.1) |
| `tailbuf/`, `resources/` | small support packages (bounded output tails, resource math) |
| `cmd/jobs-server`, `cmd/jobs-runner`, `cmd/jobs-client`, `cmd/jobs-registry` | the mains — each calls `sandbox.Init()` first |
