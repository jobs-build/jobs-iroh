# JOBS Scheduler Semantics — Implementation Spec for jobs-iroh

**Extracted:** 2026-07-22, from `~/fables-for-robots/jobs` (branch state after the
2026-07-17 actor-scheduler cutover). This is a *semantics* spec: what a single-process
goroutine+NATS reimplementation must preserve, extracted from the grain scheduler (`sched/`),
its design spec (`docs/superpowers/specs/2026-07-17-actor-model-orchestration-design.md`), and
the surviving architecture docs (`architecture/{tldr,server,build,import}.md`). All file:line
references are into the jobs repo.

The one law everything else derives from (tldr.md:11-34, actor spec §3, lines 35-48):

> **Doneness is derived, never stored** — a node is DONE iff its output ref exists in the CAS.
> **Running the same job twice is wasteful but never wrong.** All scheduler state is a cache
> over the CAS and may be discarded at any time.

This licenses at-least-once messaging, idempotent handlers, crash-restart-as-re-derivation, and
duplicate-run convergence via last-write-wins on identical ref names.

---

## 1. Node kinds and the stage graph

Six node kinds (`sched/ids/ids.go:17-35`), five placeable + one orchestrator:

| Kind | Keyed by | Placeable | Needs def bytes | Output ref (doneness) |
|---|---|---|---|---|
| `import` | K (import def key) | yes | yes | `import-output:K` (ids.go:88-89) |
| `buildvalue` | K (build def key) | **no** (orchestrator) | yes | none — two-hop `build-from:K → F → build-output:F` (ids.go:81-84, amber/remotesync.go:378-386) |
| `buildfrom` | K | yes | yes | `build-from:K` (ids.go:90-91) |
| `pluginresolve` | F | yes | no | `build-plugin-resolved:F` (ids.go:92-93) |
| `pin` | F | yes | no | `build-pinned:F` (ids.go:94-95) |
| `buildrun` | F | yes | no | `build-output:F` (ids.go:96-97) |

- **K** = amber key of the canonical-CBOR job definition (`builddef/definition.go:50-64`,
  `amber.FileKey`, amber/build.go:63). Import defs: `{fetcher, params, requiredTags?, platform,
  FetcherDef?}` (import.md:77-105). Build defs: `{source Input, dir, platform, params,
  buildJobs?, buildFile?}` (builddef/definition.go:57-64). Identity is canonical CBOR
  (`fxamacker/cbor/v2` CanonicalEncOptions); input/dep slices must be stable-ordered + deduped
  before encoding (CLAUDE.md "Identity must match across layers").
- **F** = the content key of the build-from tree: the `buildfrom` stage maps K → F
  (`runner/buildfrom.go:54-56`). Equivalent builds (same source bytes + params + platform,
  regardless of how the definition was constructed) share F and therefore share the
  pluginresolve/pin/buildrun stages and the cached output — **K vs F keying is the join
  mechanism** (build.md:1-9).
- **Node/grain identity string:** `<kind>_<lowercase-hex-key>` (ids.go:46-51). Parsing fails
  closed on unknown kinds and non-canonical hex (ids.go:58-79). In the actor scheduler this
  string doubles as the actor name: **activation is put-if-absent cluster-wide, so the join IS
  the addressing scheme** (ids.go:1-4). A jobs-iroh reimplementation needs the same property:
  one authoritative in-memory table keyed by `(kind, key)` with get-or-create semantics.

### The buildvalue pipeline (K → F → output)

`buildvalue/K` is the build's *value marker* — the node consumers depend on. It is never
placed on a runner; it walks the four stages sequentially by registering interest in exactly
one stage node at a time (skeleton.go:865-983):

```
buildvalue/K:
  activation → doneness = ResolveBuildOutput(K)  (two-hop; join/crash fast-path)
  stage 1: register interest in buildfrom/K   (carrying the build def)
  buildfrom DONE → resolve F from build-from:K ref  (skeleton.go:938-958)
  stage 2: register interest in pluginresolve/F
  stage 3: register interest in pin/F
  stage 4: register interest in buildrun/F
  buildrun DONE → buildvalue DONE → notify watchers, revoke stage interest
```

Each stage grain independently fast-paths to DONE at activation when its own ref already
exists (skeleton.go:368-395) — **the join short-circuit is the fast-path chain itself**
(grains/buildvalue.go:13-18). The buildvalue decodes its own def to learn the platform, which
it then stamps on every F-stage registration (skeleton.go:425-431, 884-895; F-stages cannot
derive platform from a def they don't have — msg.DepDecl.Platform, msg/msg.go:61-65).

### Per-kind static deps (the unfold table)

Derived by `KindLogic.Deps` at unfold time (amber I/O allowed, run off the mailbox):

| Kind | Deps derived from | Deps |
|---|---|---|
| `import` | its def | none, **unless** def carries `FetcherDef` → one dep: `buildvalue/K_f` where `K_f = Key(FetcherDef)` (grains/import.go:26-43). N imports pinning the same fetcher share one K_f — the fetcher build runs once. |
| `buildfrom` | its def | the build's `source` Input → `import/K_src` or `buildvalue/K_src`; tree source → no deps (grains/stages.go:73-86) |
| `pluginresolve` | nothing | none — the F tree is self-contained (stages.go:106-108) |
| `pin` | `build-plugin-resolved:F` blob | every resolved plugin Input + every resolution-dep Input → value nodes (stages.go:130-158). The blob provably exists: pin's interest is only registered after pluginresolve reported DONE. |
| `buildrun` | `build-pinned:F` blob | every pinned input's value node; also returns `Pinned.Resources` (stages.go:179-203) |
| `buildvalue` | — | pipeline-driven, `Deps` is unreachable (grains/buildvalue.go:26-30) |

Input → dep-node mapping (`inputDecl`, stages.go:20-38): `kind=import` → `import/K_in`,
`kind=build` → `buildvalue/K_in` (the *value* node, so the consumer transparently gets the
whole K→F pipeline and its join), `kind=tree` → **no node** (already-present content, resolved
by the build-from stage by key).

This is the old engine's `registerInputIO` semantics, relocated: an Input carries its full
definition (build.md:241-286), so meeting one is sufficient to register/schedule it — the def
rides the `RegisterInterest`/`EdgeReport`/`StartAttempt` messages (actor spec §6, lines
126-134); unfold-discovered defs are durable inside their parents' pinned/plugin-resolved
trees, submitted roots additionally under `import:K`/`build:K` bookkeeping refs.

---

## 2. The unfold algorithm (node lifecycle skeleton)

One FSM shared by all kinds (`sched/grains/skeleton.go`); phases (skeleton.go:22-31):
`init → needdef? → waiting → placing → running → publishing → done | failed`.

```
on first message (activation):
  doneness check (own output ref; buildvalue: two-hop), pull-on-miss from central
    exists → DONE: notify watchers, passivate            (skeleton.go:397-420)
    else if kind needs def and none supplied → needdef: send NeedDef to all watchers
    else → unfold: KindLogic.Deps(def, platform) → dep decls (+pinned resources)
  unfold result (skeleton.go:456-482):
    register own interest in each dep (carrying that dep's def + platform)
    EdgeReport{node, deps} → every build-actor watcher    (graph discovery fan-out)
    phase = waiting; zero deps → straight to placing
  dep transitions (skeleton.go:517-564):
    a dep reporting phase=done marks it; when ALL deps done → placing
  placing: RequestSlot(broker/<platform>) — re-driven on every watcher renewal
    (the broker may have lost its queue; renewals are the retry engine, skeleton.go:299-301)
  GrantSlot → gen++ → Ask session StartAttempt (skeleton.go:632-674)
    declined/unreachable → release lease, backoff 1s, re-place (skeleton.go:676-698)
  AttemptDone{success} → publishing: gate → sign → publish refs (skeleton.go:760-811)
    publish OK → DONE → notify → revoke dep interest
    gate rejection → HARD FAIL (broken/hostile runner)
    publish failure → reset() → re-derive from scratch (ref may or may not exist)
  AttemptDone{failure} → see §5
any state: zero watchers (post-grace) → abort attempt, release slot,
  revoke dep interest, reset to init                      (skeleton.go:328-364)
```

Key semantics to preserve:

- **Interest is leased.** Watchers renew every `RenewEvery` (60s); a grain expires a watcher
  after `ExpireAfter` (180s = 3 missed renewals); a freshly-(re)activated node treats zero
  watchers as cancellation only after `ActivationGrace` (180s) (system/config.go:8-63,
  skeleton.go:315-343). Cancellation and watcher-crash cleanup are the *same* path: zero
  interest → abort → reset. In a single process this can become refcounts + context
  cancellation, but keep the *lease* shape if request state can be lost independently of node
  state (e.g. NATS KV watcher restart).
- **All cross-node sends are async, unordered, at-least-once** (skeleton.go:1029-1047).
  Convergence comes from renewals re-driving state, never from any single delivery. Every
  receiver must tolerate reordering: the build actor guards with (phase-rank, attempt-gen)
  monotonicity (buildactor.go:29-32, 380-411); output buffers dedup on Seq (output.go:26-31);
  the broker's RequestSlot is idempotent per node (broker.go:153-177).
- **Two watcher classes.** Dependent grains watch their deps (get transitions only). Build
  actors watch with `IsBuild=true` (get EdgeReport + transitions + AttemptProgress +
  AttemptOutput relays; skeleton.go:505-513, 1020-1027). A late-joining build watcher is
  replayed the already-discovered deps (skeleton.go:307-310).
- **Def supply protocol:** a node reactivated without its def answers registrations with
  `InterestAck{NeedDef:true}` and broadcasts `NeedDef`; watchers re-register carrying the def
  (skeleton.go:410-417, 492-503). Defs travel in-band; there is no def-lookup service.
- **Epoch guard:** every async task result carries the epoch at spawn; `reset()` bumps the
  epoch so stale results are dropped (skeleton.go:112, 347-364). Any goroutine
  reimplementation needs the same stale-result fencing.

**Join semantics recap:** many parents needing one K/F converge on the same node entry
(put-if-absent by name); each just adds a watcher. First watcher activates it; doneness
fast-path makes an already-built node free; concurrent duplicate activation (impossible in a
single process) would converge via identical ref names, last-write-wins.

---

## 3. DONENESS: the complete ref-name catalog

Exact formats (keys render as 64-char lowercase hex; `K`/`F`/`T` below are such keys):

### Result refs — runner-produced, server-gated+signed+published

Published by the node's publish step (gate.Allowed → Publisher.SignAndPublish,
skeleton.go:760-789; gate/gate.go, gate/publish.go). The allow-table (gate/gate.go:26-42) is
the security core — a runner can push objects, but can never make the server sign a name
outside its own assigned node:

| Ref | Points to | Producing stage | Gate rule |
|---|---|---|---|
| `import-output:K` | raw fetched content tree | import (runner/importjob.go) | exactly this one name (gate.go:144-145) |
| `build-from:K` | F (the build-from tree key) | buildfrom (runner/buildfrom.go:54-56) | exact name (gate.go:147) |
| `build-from-tree:F` | F (self-referential, name==value) | buildfrom, same batch | only for the exact F reported as `build-from:K`'s value **in the same batch** (gate.go:47-60,133-135) |
| `build-from-tree:<t>` | t (name==value) | pluginresolve/pin (tree-source inputs inside emitted defs, runner/buildeval.go:225-244) | any canonical-lowercase-hex key whose entry value equals the name's key (gate.go:115-139) |
| `build-plugin-resolved:F` | CBOR `{definition, plugins{name→Input}, deps{name→Input}}` | pluginresolve (runner/buildeval.go:91) | exact name (gate.go:149) |
| `build-pinned:F` | CBOR Pinned `{inputs[{name,kind,definition}], env, script, runtimeDeps, resources?, caches?}` | pin (runner/buildeval.go:214-219) | exact name (gate.go:151) |
| `build-output:F` | `{c/, meta}` tree | buildrun (runner/buildrun.go:153-156) | exact name (gate.go:153) |
| `build-output-deps:F` | materialized transitive runtime-closure store tree | buildrun, **ordered before** build-output:F in the batch (buildrun.go:146-156) | exact name (gate.go:153) |
| `build-cache:<id>:<platform>` | pruned cache-state tree; **the one mutable ref** (last-writer-wins) | buildrun, first in batch | buildrun only; `<id>` must be declared in `build-pinned:F`'s caches, `<platform>` must equal the placement platform (gate.go:99-113, DeclaredCaches publish.go:117-141) |

Batch ordering invariant (buildrun.go:146-156): entries publish sequentially, stop on first
error → `build-output:F` lands last, so a crash never leaves a done-looking build with a
missing runtime closure. **Doneness is derived from `build-output:F` alone.**

Every name additionally passes `reference.ValidateName`; `import:`, `build:`, `target:`,
`fetcher:`, `shell:` and anything else **fail closed** (gate.go:84-92).

Publish policy (gate/publish.go:34-112): sign all entries first (any failure aborts the whole
batch), then publish each to central with capped-exponential retry bounded by ¾ of
`PublishDeadline` (`JOBS_SYNC_DEADLINE`, default 120s — publish.go:145-152). **No durable
republish queue**: a node retries while alive; a crash means the node re-runs (wasteful, never
wrong — publish.go:16-20). Publish failure → node `reset()` → next renewal re-checks doneness
(skeleton.go:796-806).

### Bookkeeping refs — server-written

| Ref | Points to | Writer | Notes |
|---|---|---|---|
| `import:K` / `build:K` | the def blob (key = K) | submit handler, **submitted roots only** (httpapi.go:139-149,166,262) | def durability + by-key tooling; unfold-discovered defs are NOT registered — they live inside parent pinned trees (actor spec §9, lines 208-211). The runner edge also writes them *locally* as plumbing so stage drivers resolve defs without a remote trip (session/execcore.go:125-139). |
| `request:<id>` | CBOR `RequestDep{RequestID, Targets[{node,def,resources}], Deleted}` | server, at submit (buildactor.go:87-121) | the request's **activation source only** — never scanned, never re-driven from. Delete overwrites it with `{Deleted:true}` (tombstone; buildactor.go:330-345). |
| `build-from-tree:T` | T | server, at tree-source submit (`jobs remote-build`) — deterministic, **no retry**: a completeness rejection = client upload incomplete → 422 (httpapi.go:196-220, publish.go:63-70) | |
| `target:K` | — | **dropped** in the actor scheduler (actor spec §9, lines 209-211); request lifecycle is actor/record-side now. The old engine's cancel tombstones are gone too. |

### Outside the scheduler's view

`fetcher:<name>:<platform>` and `shell:<platform>` — bootstrap-seed refs, self-seeded at
startup, read by runners only (import.md:114-141, CLAUDE.md bootstrap section). Valid for seed
leaves only ({tarball+https, hostmusl, github} + shell); a missing named fetcher is a hard
failure. `seed-src:` markers manage seed refresh.

### Two-hop output resolution

`ResolveBuildOutput(K)`: `build-from:K → F → build-output:F`, pull-on-miss at each hop
(amber/remotesync.go:378-386); `ResolveBuildOutputDeps(K)` likewise to `build-output-deps:F`
(remotesync.go:388-395). Fetcher artifacts resolve by content the same way:
`build-from:K_f → F → build-output:F → c/` (`amber.ResolveBuildArtifact`, import.md:1-11).
Consumers must never key on `build-output:K` — output refs are F-keyed since build-from
content addressing (build.md:1-9).

---

## 4. Placement & resources

**Requirement** = `max(per-kind default, Pinned.Resources, live watcher-carried API requests)`
(skeleton.go:577-602). Defaults (`resources/resources.go:39-46`): import + light stages
(buildfrom/pluginresolve/pin) 200m CPU / 512Mi; buildrun 1000m / 1Gi. Kind → default mapping
via `KindLogic.ResourceKind()` (stages.go:89,111,162,206; import.go:46). API-requested
resources ride interest registrations and **expire with the requester's lease**
(msg/msg.go:13-21, skeleton.go:273-277) — this replaced the old engine's accumulating map.
**Resources are scheduling metadata, never identity** — never in K/F (actor spec §3, line 44).

**Broker** (one per platform, `broker_<platform>` with `/`→`-`; ids.go:104-106;
sched/broker/broker.go): all-soft state —

- sessions: `{platform, capacity, controlCap, committed{node→SlotCommit}, lastAnnounce}`;
  swept stale after 3×HeartbeatEvery (=15s) (broker.go:113-126).
- two FIFO queues: default + control; control matched first (broker.go:240-243).
- greedy first-fit: for each pending, first session where
  `used + req ≤ capacity` (CPU and mem independently); `used` = confirmed commits +
  unconfirmed provisional leases (broker.go:187-236).
- **control class** (`state.ClassControl` = "control"): a control-plane verdict (e.g.
  WriteRefs ack timeout on a congested server — runner/importjob.go:57-62,
  state/model.go:46-52). Control re-runs **bypass resource packing** (the fleet already had
  capacity for them) but are capped at `controlCap` (default 20) concurrent per session
  (broker.go:216-235). A node whose last failure was control re-requests with `Control:true`
  (skeleton.go:616, 648).
- grant = provisional lease `{id, node, session, res, expires = now + 2×PlaceTimeout}`
  (broker.go:253-262); re-granted verbatim on duplicate request; superseded by the session's
  confirmed commit announce; expires silently if never redeemed — **no offer-rollback
  machinery exists because nothing needs rolling back** (broker.go:1-8).
- the **session (runner) is the authoritative capacity check** and may decline a stale grant;
  the grain then backs off 1s and re-places (session.go:116-134, execcore.go:63-89,
  skeleton.go:676-698). Admission also rejects duplicates by node name (execcore.go:72-74).

**Platform matching:** a node's platform comes from its def (buildvalue decodes it,
skeleton.go:425-431) or from the watcher's registration (DepDecl.Platform for F-stages and
discovered inputs). Legacy empty-platform imports broadcast RequestSlot to every
`KnownPlatforms` broker; first grant wins, losers are handed back via the unneeded-grant path
(skeleton.go:604-630, 632-637; config.go:62). **Gotcha:** a *directly submitted* import
(`POST /submit`) currently registers with no platform (httpapi.go:172, buildactor.go:548-555)
and therefore takes the broadcast path even though its def is platform-pinned — jobs-iroh
should pass the def's platform through instead.

**jobs-iroh mapping:** runner sizes c0.2-m1..c4-m16 map directly onto
`ResourceReq{CPUMilli, MemBytes}` (c1-m2 → 1000/2Gi). Keep the greedy-packing + authoritative
runner admission split; the broker becomes a per-platform goroutine fed by a NATS WQ stream —
but note the WQ delivery is only the *transport*; the idempotent per-node request/re-request
loop (renewal-driven) is what makes lost grants harmless.

---

## 5. Retry & failure semantics

Failure classes originate runner-side (`runner.Outcome.Class`, runner/importjob.go:30-66):

| Situation | Class |
|---|---|
| fetcher/plugin/script exits non-zero (≠75); recipe raises/wrong shape; missing BUILD.jobs; `runtime_deps ⊄ inputs`; gate rejection | **hard** (importjob.go:211-213, build.md §11 table lines 554-573) |
| exit **75** (`EX_TEMPFAIL`) | **retryable** — code-requested retry without burning budget (importjob.go:210-211, import.md:156) |
| cannot pull def/source/inputs; push failure; sandbox setup failure; cancelled-by-context; runner-side decline | **retryable** (importjob.go:42-44, execcore.go:166-171) |
| WriteRefs/publish ack timeout (control-plane congestion) | **control** (importjob.go:57-62) |
| losing a duplicate-execution race ("not assigned") | silence — treated as cancel, no outcome (importjob.go:46-56) |

Grain-side handling (skeleton.go:715-758):

- **success** → publishing (gate/sign/publish) → DONE.
- **retryable/control** → `consecRetryable++`; if `> maxConsecutiveRetryable` (**3**,
  skeleton.go:33-35) → escalate to **hard** ("retry budget exhausted"); else re-place with
  exponential backoff 1s·2^(n−1) capped at 30s (skeleton.go:37-41, 744-752).
- **hard** (and any unknown class — fails closed) → **FAILED immediately**.

**This is a deliberate simplification vs. the old engine** (server.md:197-227: 3 hard attempts
→ FAILED, 5 consecutive retryables → INFRA_FAILED, generation-bump manual retry): the grain
scheduler has **no hard-attempt budget (one hard failure fails the node), no INFRA_FAILED
status, and no manual-retry generation register**. FAILED is:

- **memory-only and sticky while watched** (skeleton.go:832-838): watchers are notified
  (`NodeTransition{failed, class, summary}`); new registrants get the failed transition
  immediately (skeleton.go:303-305).
- **cleared by silence**: zero watchers → reset; passivation (idle TTL 5min) or cancel+delete
  clears it; a later fresh activation re-derives from the CAS and simply tries again —
  **retry-by-resubmit** (actor spec §7, lines 152-154). Nothing durable ever records failure;
  durable truth is only ever success (the output ref).
- The bounded stderr tail rides the transition (`failSummary`; small `tailbuf` in grains,
  full bounded output in build actors).

**Upstream-failed is a per-request derivation, not a node state**: each build actor computes,
over *its own* edge set, which nodes sit above a hard-failed node
(`failedUpstream`, buildactor.go:445-481) and reports them as phase `failed-upstream` in
snapshots only (buildactor.go:499-503). The request phase derives as: all targets done → done;
any target failed or failed-upstream → failed; else running (buildactor.go:413-443).
**Note:** the old engine's `upstream-failed` auto-cancel sweep is gone — a failed request keeps
renewing interest in its whole closure until cancel/delete/TTL (Tick → renewAll,
buildactor.go:236-252), so sibling subtrees keep building (and caching) after a failure.
jobs-iroh may choose to restore the waste-saver (revoke interest in exclusive subtrees on
request failure) — it is an optimization, never a correctness matter.

**Attempt generations** (`gen`, skeleton.go:125,639) are per-node monotonic counters bumped on
each StartAttempt and used for: stale-result fencing, output-buffer keying (newest gen wins,
buildactor.go:302-312, 525-541), and adoption (an announced attempt with higher gen adopts,
skeleton.go:842-863). They are not the old CRDT retry-generation.

**NATS translation notes:** "MaxDeliver-ish" limits map to `maxConsecutiveRetryable=3` +
backoff — but do **not** let JetStream redelivery be the retry mechanism for job execution;
the node FSM owns retries (a WQ redelivery of a job message must be idempotent against the
node table: if the node is running/done, ack and drop). Exit-75 and class mapping stay
runner-side exactly as `execcore.Execute` does (execcore.go:113-179).

---

## 6. Request lifecycle

**Submit** (httpapi.go:151-295):

1. Canonicalize the def (import: `{fetcher,params,tags,platform}`; build: full Definition;
   both via canonical CBOR). Malformed → 400.
2. Ingest def bytes locally, **push objects + sign bookkeeping ref (`import:K`/`build:K`) to
   central before acking** — submit requires central up (httpapi.go:136-149; actor spec §4,
   lines 66-68). Central down → 502.
3. Tree source (`remote-build`): verify + publish `build-from-tree:T` engine-signed, no retry;
   incomplete upload → 422 (httpapi.go:196-220).
4. Mint `request_id` (`r`+8 random hex bytes, httpapi.go:99-103); write the `request:<id>`
   record (activation source); spawn the request actor; reply **202**
   `{k, request_id, status_url:/builds/<id>, events_url:/builds/<id>/events}`.
5. Target node: import submit → `import/K`; build submit → `buildvalue/K` (+ optional
   ResourceReq parsed from `resources{cpu,memory}`) (httpapi.go:172, 286).

**Request actor (BuildActor)** (`sched/buildactor/buildactor.go`): per-request watcher owning
targets, discovered edges, per-node phase timeline, and bounded captured output
(4MiB/attempt = 1MiB head + 3MiB ring tail, gap counted; config.go:57-58, output.go). Drives
everything from a self-rearming tick every RenewEvery: re-register interest in every known
node, renew the directory entry, check TTL (buildactor.go:236-252, 354-365). Graph discovery:
EdgeReport → add edge, register direct interest in newly-seen nodes (buildactor.go:254-272).
Completion: request is done when every target reports done — for a build target that is the
buildvalue grain's two-hop doneness, mirroring `ResolveBuildOutput(K)` (actor spec §9, lines
210-214).

**Cancellation:** `POST /requests/{id}/cancel` → phase `cancelled`, `revokeAll` interest,
actor survives for inspection (buildactor.go:321-328). Revocation (or just silence) drives
every exclusively-held node to zero interest → abort attempt → release slot → reset →
passivate (skeleton.go:328-343). Shared nodes keep running for their other watchers —
reference-counting-by-interest replaces the old reachability GC (server.md §8).

**Delete:** `DELETE /builds/{id}` → tombstone the `request:<id>` record (`Deleted:true` —
any later activation sees deleted), revoke, drop directory entry; logs and history vanish
(buildactor.go:330-345, requestdep.go:17-24). **TTL:** terminal-state actors self-retire after
`BuildTTL` (default 7d, 0 = explicit-delete only; config.go:60, buildactor.go:582-592).

**Observation surface** (httpapi.go:80-97): `GET /builds` (directory grain — leased entries,
3×RenewEvery expiry, directory.go:56-90), `GET /builds/{id}` (snapshot), `GET
/builds/{id}/events` (SSE of cursor-gated snapshots, 250ms poll, terminal-phase close,
httpapi.go:333-372), `GET /builds/{id}/logs/{node}?stream=` (head + "N bytes omitted" + tail),
`GET /api/runners` (broker stats per KnownPlatform). Snapshot cursor bumps on every mutation
(buildactor.go:543-544) — a natural fit for a NATS KV bucket keyed by request id with revision
as cursor.

---

## 7. Runner edge (what the scheduler assumes of runners)

- **Handshake** (gateway/gateway.go:115-174): `{name, bearer, platform, capacity, controlCap,
  wipeEpoch, transport pubkey, live attempts, proto}` → verify bearer (per-runner token
  registry OR shared-secrets file re-read per handshake, constant-time; gateway/auth.go:39-99)
  → mint short-lived read+push-objects grant bound to the runner's transport key (TTL 15min,
  auth.go:101-139) → `Welcome{session, centralURL/key, grant, wipeEpoch, heartbeatMillis,
  proto}`. Proto major mismatch = clean refusal. Reconnect replaces the prior session.
- **Heartbeats** every 5s (`HeartbeatEvery`); session dead after 15s (`SessionDeadAfter`)
  (config.go:55-56, session.go:75-100). Every announce carries the runner's authoritative
  committed-slots list; the session re-announces to the broker every tick and re-announces
  attempts to owning grains (adoption; session.go:107-114, 217-230).
- **Adoption:** a node receiving `AnnounceAttempts` for itself while not
  done/publishing/failed adopts the live attempt (gen max-merge) instead of re-running
  (skeleton.go:842-863). Runners keep executing through server outages and re-attach on
  reconnect. In jobs-iroh (single server, runners over iroh/NATS) this is still needed for
  *server restart*: on reconnect a runner must announce in-flight jobs so the fresh scheduler
  adopts rather than double-runs.
- **Execution:** `StartAttempt{node, gen, leaseID, def?, platform, resources, control}` →
  admission check → run the real stage driver (`RunImport`/`RunBuildFrom`/`RunPluginResolve`/
  `RunPin`/`RunBuild`) with a `captureRefWriter` that **pushes each ref's object tree to
  central, then collects name+key** — the runner never signs (execcore.go:113-179, 207-252).
  `MemBytes` enforced as sandbox `memory.max`; CPU reservation-only (execcore.go:196).
- **Output/progress:** `AttemptOutput{node, gen, stream, chunk, seq}` and `AttemptProgress`
  relay runner → session → node → build watchers (one hop each; skeleton.go:229-233). In
  jobs-iroh these are the core-NATS in-memory subjects.
- **Wipe epoch** rides handshake/Welcome; runners reset store+cache and persist the epoch.

---

## 8. What collapses in a single-server jobs-iroh — and what must remain

**Collapses (delete these concepts):**

- goakt cluster/discovery/relocation, `RequestDep` as a relocation payload, the
  bind-where-you-run Deps registry (system/deps.go), the gateway kill+respawn ensure-loop
  (gateway.go:37-93), split-activation convergence, the cross-node CBOR envelope codec
  (msg/codec.go), remote actor lookup (session.go:166-187).
- CRDT/gossip/anti-entropy/tombstones (server.md §3.3, §8) — already deleted in jobs itself.
- Grants and per-ref signing *may* collapse: with iroh ALPNs (`jobs-runner-amber/1.0`) the
  connection itself is the authenticated channel to the CAS. But **keep the gate allow-table
  as the authorization core** — "which node kind may cause which ref name to exist" is
  scheduler semantics, not transport (gate/gate.go:26-42). Whether refs are cryptographically
  signed or just server-written into amber-store-core is a storage-layer decision.
- `target:K` refs, cancel tombstones, durable republish queues, the event
  collector/buildview pipeline (all already gone).

**Must remain (the invariants):**

1. Doneness = output-ref existence; all scheduler state re-derivable; status computed never
   stored.
2. Objects-before-ref, with `build-output-deps:F` (and cache refs) ordered before
   `build-output:F` in one batch.
3. The full gate allow-table incl. the same-batch build-from-tree binding and the
   declared-cache + placement-platform check.
4. K vs F keying + the buildvalue two-hop; join by put-if-absent on `(kind, key)`.
5. Unfold-by-need with defs traveling in-band; the per-kind dep table of §1.
6. Idempotent, unordered, at-least-once internal messaging discipline — NATS gives
   at-least-once anyway; every handler must already tolerate replays and reordering
   (phase-rank guards, seq-deduped output, idempotent slot requests).
7. Requirement = max(default, Pinned.Resources, live requests); greedy packing; runner-side
   authoritative admission; control-class bypass.
8. Retryable-vs-hard classification incl. exit 75; bounded consecutive-retryable budget (3)
   with capped backoff; FAILED as memory-only, retry-by-resubmit.
9. Interest/refcount-driven cancellation: zero watchers → abort+reset; request cancel =
   revoke-all; shared subtrees survive while any request needs them.
10. Adoption of announced in-flight attempts across server restart/reconnect.
11. Submit pushes def objects + bookkeeping refs to the CAS before acking (202 + request id).

**NATS shape suggestions** (mapping, not mandate): node FSMs = goroutines (or a keyed
state-table actor loop) with per-node inboxes; jobs WQ stream = broker queues (per platform,
plus control class); RESULTS stream (LimitsPolicy) keyed by job id = AttemptDone journal for
crash re-derivation *hints* only (never truth — truth is refs); KV bucket = request snapshots
(revision = cursor); core-NATS subjects = live output relays (in-memory only, exactly like
today's bounded buffers). Renewal ticks remain the universal retry engine.

---

## 9. draganm/amber-store leakage — the port cut-points

Everything below must be re-based onto `github.com/amber-store/core`:

| Leak | Where | What jobs-iroh needs |
|---|---|---|
| `key.Key` (32-byte content key), `key.Parse`, canonical lowercase `key.String()` | ids (ids.go:11,58-79), grains, httpapi (httpapi.go:17,450-456), session, builddef (definition.go:11), gate | an equivalent key type; the canonical-hex round-trip check is load-bearing in ParseGrainName and the gate |
| **K derivation** = `amber.FileKey(canonicalCBOR)` → amber-store chunking/keying (amber/build.go:63) | builddef.Input.Key (definition.go:50-52), importdef | amber-store-core's key function. Changing the algorithm changes every K/F — acceptable for a fresh system, but recipes/plugins/`jobs-build/examples` must produce identities consistently across client/server/runner |
| `reference.ValidateName`, `reference.Reference`/`Decode`, `Store.PutRef` | gate (gate.go:16,85), publish (publish.go:104-108) | ref-name validation + a ref record type |
| `sshsign` via `amber.Signer.SignRecord/SignAndPut` (namespace "amber-store-ref") | publish.go:45, httpapi.go:145, buildactor.go:117 | whatever ref-write authority amber-store-core has; may degrade to plain authenticated writes over `jobs-amber-admin/1.0` |
| `grant.Sign` + `allowlist.CapRead/CapPushObjects` | gateway/auth.go:15-16,129-134 | replaceable by iroh connection-level auth per ALPN |
| `remotesync` (`PushTree`, `PublishRef`, `remotesync.Opts`, pull-on-miss `GetKey`, stall watchdogs) via `amber.Embedded.Underlying()` | execcore.go:230-241, publish.go:93-103, amber/remotesync.go | amber-store-core sync primitives; keep the ¾-deadline server-side / full-deadline runner-side timer relationship (publish.go:79-88 — the runner's own equal timer must win, issue #153) |
| `amber.Store` seam + `amber.Embedded` | system/deps.go:21-31, everywhere | the embedded-store interface of amber-store-core |
| type-asserts `p.Store.(*amber.Embedded)` for central push/publish | publish.go:98, execcore.go:230,262 | make push/publish first-class on the store seam instead of a backend assert |

The `runner/` stage drivers, `builddef`/`importdef`/`recipe`, `sandbox`, `fetchers/*`,
`bootstrap` seed are ported wholesale per the plan; their amber usage funnels through the
`amber` package seam (CLAUDE.md package map) — re-basing that one package carries most of the
port.

---

## 10. Tunables (production defaults, system/config.go:49-64)

| Parameter | Default | Meaning |
|---|---|---|
| RenewEvery | 60s | watcher interest renewal + build-actor tick |
| ExpireAfter | 180s | watcher lease expiry (3 missed renewals) |
| ActivationGrace | 180s | zero-interest ≠ cancel until this after (re)activation |
| GrainIdleTTL | 5min | node passivation (clears FAILED, frees memory) |
| HeartbeatEvery / SessionDeadAfter | 5s / 15s | runner liveness |
| PlaceTimeout | 10s | broker/session ask bound; broker lease TTL = 2× |
| maxConsecutiveRetryable | 3 | retryable budget before hard-fail (skeleton.go:35) |
| retry backoff | 1s..30s exp | re-place backoff (skeleton.go:38-41) |
| PublishDeadline | 120s (`JOBS_SYNC_DEADLINE`) | ref publish window; server retries ≤ ¾ |
| OutputHeadCap / OutputTailCap | 1MiB / 3MiB | per-attempt captured output |
| BuildTTL | 7d (0 = explicit delete) | terminal request retention |
| broker controlCap | 20 | concurrent control-class runs per session |
| grant TTL | 15min, refreshed at TTL/3 | runner CAS credential |
| resource defaults | import/light 200m/512Mi; build 1000m/1Gi | resources/resources.go:39-41 |
