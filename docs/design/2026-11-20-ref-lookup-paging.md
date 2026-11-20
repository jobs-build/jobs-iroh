# Ref lookup instead of the full ref list (issue #22)

Status: implemented in jobs-iroh alone (transport-iroh unchanged at v0.4.1). Supersedes the
"a corrupt snapshot starts empty" rule of
`2026-08-25-gc-auto-cleanup.md` §3.

## What broke

A server on v0.36.0 reached 132,875 refs after 12 days with the default
30-day retention. Three limits sat near 131,072 refs:

1. transport-iroh's frame decoder used fxamacker's default of 131,072
   array elements, and `TRefList` answered every ref in one frame — so the
   registry, which listed every ref to resolve one K→F, answered every new
   image pull with 503.
2. `MaxFrame` was 16 MiB in both the amber protocol and `api/`; at ~130
   bytes a ref, the list passes it at about the same count (`jobs-client
   admin refs` without `--limit` already failed with `read reply: EOF`).
3. `reftrack.Load` decoded the access snapshot with the default 131,072
   map pairs. `Flush` wrote it, the next start could not read it, and the
   GC started empty: every clock re-seeded and every pin dropped — so the
   shorter retention the operator restarted for expired nothing, and one
   retention later everything unread in between would have gone, images
   in use included.

Recovering (wiping the store) exposed two more problems: concurrent first
builds failed an import with "fetcher build … output not resolvable yet"
on every retry, and the failed node then stayed failed for every resubmit
because the failed requests still held it — with no `jobs-client`
command to delete them.

## Who lists refs, and what they need

| Caller | Needs | Now |
|---|---|---|
| registry `resolveF` (every first pull) | `build-from:K`'s value, or whether `build-output:K` exists | two point lookups (a pull that wants nothing) |
| registry `serveTags` | `build-from:*` minus `test:*` | admin `refs` pages by prefix |
| admin `refs`, TUI | display | one clamped page per call, `after` cursor |
| `sched.Stats` | a count | name-only count, cached 10 s |
| `gcsweep.Sweep` | every name (reconcile) | `RefNames`, in process, no record decode |

## The amber protocol: unchanged

The ticket first planned a `TRefGet` op and a paged `TRefList` upstream
(transport-iroh v0.5.0, with listing frames up to 128 MiB). It turned out
not to be needed, so transport-iroh stays at v0.4.1:

- **A point lookup is a pull that wants nothing.** `amberclient.GetRef`
  sends `TPull{Name}` with no data connections, reads the server's `TRef`
  (the raw record — K→F is its key), and answers with an empty `TWants`.
  The server's send loop ends cleanly on an empty want list before any
  object moves: no transfer, no shard token, no gather wait, no error log
  (an absent name is `unknown-ref`, logged by v0.4.1 like any pull of a
  missing ref). The server counts it as a read of the ref (OnAccess), which
  is right for the registry: the binding it serves stays warm. Every
  transport-iroh server speaks it, so there is no version coupling.
- **Browsing uses jobs-iroh's own paged admin `refs` API**, not
  `TRefList`: the registry dials `jobs-admin/1.0` for its tag list (two
  prefix walks, `test:` and `build-from:`), one connection per listing.
- The amber frame limits stay at 16 MiB and 131,072 elements. Nothing in
  jobs-iroh sends `TRefList` any more except `amberclient.Refs` (kept for
  small stores and tests); peers v0.36.0 and older still do, hence the
  warning below.

An upstream `TRefGet` / paged `TRefList` remains a possible clean-up (one
round trip instead of two, a listing without the admin ALPN); a patch for
it was prepared and not needed.

jobs-iroh's admin pages scan every name server-side (amber-store-core's
refstore has no ranged iterator), decoding only the page. That is a CPU
cost, not a frame or memory-per-reply one; a `refstore` range scan is the
follow-up that makes pages O(page).

## jobs-iroh

- `api/` limits are raised only where a client reads a reply
  (`ReadFrame`/`DecodeBody`: `MaxFrame` 128 MiB, element limits tied to
  it). Requests — what `serve` reads from any peer — keep 16 MiB and the
  library's 131,072 elements (`ReadRequest`/`DecodeRequest`): a 1-byte CBOR
  element can decode into a 64-byte struct, and a first draft that raised
  the limits on both sides let a 20 MB frame decode into 1.28 GiB.
  Readers grow the raw buffer as bytes arrive.
  `RefsRequest.After` + `RefsReply.More/Matched`; the server clamps every
  page to `api.MaxRefsPage` (5,000) — `Limit 0`, which meant "all",
  included: an old client now gets the first page instead of `EOF`.
  `admin refs` and the TUI (`n`/`p`) walk pages.
- `reftrack.MaxEntries` = the library's maximum map size; `Flush` refuses
  past it so a written snapshot always loads.
- **An unreadable snapshot halts the GC.** A *missing* `refaccess.cbor` is
  a first start; a *present* one that will not read or decode makes
  `gcsweep.New` log an error and halt: `Sweep` and `Pin` return
  `ErrHalted`, no flush ever rewrites the file (it is the only copy of the
  real clocks and pins), `GCStats.Halted` and `StatsReply.Warnings` say
  why. Recovery is an operator decision: restore a readable copy, or move
  the file aside (fresh clocks, no pins). Each sweep of a halted sweeper
  re-reads the file and resumes once it loads or is gone — no restart,
  and a transient read error heals itself. Snapshot writes fsync the file
  and its directory, so a crash cannot leave a torn snapshot that would
  halt the GC. This applies to runners and clients too — their caches are
  re-pullable, but a silently re-seeded GC is the failure this design
  removes.
- `StatsReply.Warnings` also warns from 90% of ~129,000 refs on: peers
  from v0.36.0 and older still list everything in one frame, which fills
  16 MiB at about that count (before their 131,072-element limit).

## The first-build import failure

The scheduler hands a runner ref *names* (`PullRefs`); the runner skipped
the pull when it already had a complete local ref of that name. Names like
`build-from:K` and `build-output:F` are mutable bindings, and a runner's
store holds its own: every stage driver writes its results locally
(`collectRefWriter`) before proposing them, including attempts the server
then discards, and a runner cache outlives a server wipe. An import's
fetcher resolves `build-from:K_f → F → build-output:F` locally; a stale
`build-from:K_f` sends it to an F whose output this runner never pulled —
"not resolvable yet", deterministically, on every retry the 1+2+4 s budget
allows.

`wire.Job.PullKeys` carries the server's key for each `PullRefs` entry at
enqueue (additive; old runners ignore it, old servers omit it). The runner
trusts a local copy only when it names that key, and re-pulls otherwise.
No ALPN bump: an old runner behaves exactly as before.

## Sticky failed nodes

`failNodeLocked` always said "retry is by resubmit", but a resubmit joined
the failed node and failed at once while any earlier request still held
it. Now a submit re-activates the failed nodes of the closure it joined
(post-order: deps first; doneness fast-path, else re-unfold and re-queue
with a fresh budget; orchestrators re-advance) when the platform has a
live runner. `retry` (build ALPN, `jobs-client retry --request-id … |
--failed`) does the same for an existing request in place; a request the
no-runners watchdog failed re-joins its target, and a request already
running again — a sibling's retry re-activated their shared failed node —
is answered with 0 re-activated, not an error, so `--failed` works for
issue #22's several requests failing on one import. A submit re-activates
only failures from before its own join: a failure its join produced
synchronously (a sub-build cycle) would only fail again. `jobs-client delete
--request-id … | --failed` removes requests (admin `delete`). FAILURES
records are untouched, so `diagnose` keeps the history across retries.
