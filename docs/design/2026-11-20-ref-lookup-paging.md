# Ref lookup instead of the full ref list (issue #22)

Status: implemented (jobs-iroh, transport-iroh v0.5.0). Supersedes the
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
| registry `resolveF` (every first pull) | `build-from:K`'s value, or whether `build-output:K` exists | two `TRefGet` point lookups |
| registry `serveTags` | `build-from:*` minus `test:*` | `TRefList` pages by prefix |
| admin `refs`, TUI | display | one clamped page per call, `after` cursor |
| `sched.Stats` | a count | name-only count, cached 10 s |
| `gcsweep.Sweep` | every name (reconcile) | `RefNames`, in process, no record decode |

## transport-iroh v0.5.0

- `TRefGet{Name}` (14) → `TRef{Record}` or `unknown-ref`; no transfer. It
  fires the access hook, like a pull: a lookup is a read. Old servers
  answer `bad-request`; `amberclient.GetRef` maps that to `ErrUnsupported`
  and the registry falls back to a listing.
- `TRefList{Prefix, After, Limit}` (fields 18–20): `Limit > 0` returns at
  most `MaxRefPage` (10,000) refs sorting after `After`, `More` (21) set
  when matches remain, a page ended early before half a frame. `Limit 0`
  is the old unpaged listing, refused with `bad-request` instead of sent
  unreadable when it passes `MaxFrame`. Old servers ignore the fields:
  clients filter the reply and stop on a missing `More`.
- `MaxFrame` 128 MiB; decoder array/map limits equal to it; `ReadMsg`
  grows its buffer as bytes arrive.

amber-store-core's refstore has no ranged iterator, so a page still scans
every name server-side (decoding only the page). That is a CPU cost, not a
frame or memory-per-reply one; a `refstore` range scan is the follow-up
that makes pages O(page).

## jobs-iroh

- `api.MaxFrame` 128 MiB, same decoder limits, incremental read.
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
  the file aside (fresh clocks, no pins) and restart. This applies to
  runners and clients too — their caches are re-pullable, but a silently
  re-seeded GC is the failure this design removes.
- `StatsReply.Warnings` also warns from 90% of 131,072 refs on: peers from
  v0.36.0 and older still list everything in one frame.

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
no-runners watchdog failed re-joins its target. `jobs-client delete
--request-id … | --failed` removes requests (admin `delete`). FAILURES
records are untouched, so `diagnose` keeps the history across retries.
