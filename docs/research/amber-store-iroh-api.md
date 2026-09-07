# amber-store-iroh — subsystem map for the jobs-iroh port

Source: `~/fables-for-robots/amber-store-iroh` (module `github.com/jobs-build/amber-store-iroh`, Go 1.26.5, read-only).
All `file:line` cites are into that repo unless prefixed. go-iroh cites are into the module cache copy of
`github.com/tmc/go-iroh@v0.0.0-20260714221401-b17af420bb03`.

**Headline for jobs-iroh:** this repo is ALREADY built on `github.com/amber-store/core`
(go.mod requires `amber-store-core v0.0.0-20260720222444-a37d35fa4ecf`); there is **zero** `draganm/amber-store`
anywhere in it (verified by grep). The draganm→core port cut-points live in the old `jobs` repo, not here.
`protocol/`, `wantsync/`, `server/`, `relaymode/` are importable library packages and reusable as-is;
the entire client-side push/pull orchestration is `package main` under `cmd/amber` and must be forked or
upstreamed into a library package.

---

## 1. Repo layout

| Path | Role |
|---|---|
| `protocol/protocol.go` | frame codec: ALPN const, frame types, `Msg`, `WriteMsg`/`ReadMsg`, `RemoteError` |
| `protocol/pack.go` | amberpack payload chunking over TData/TDataEnd frames |
| `wantsync/wantsync.go` | both halves of the have/want loop (`Receive`, `Send`, `Wants`, `Progress`, `Stats`) |
| `server/server.go` | `Server`: per-stream dispatch, CAS, sharded-transfer registry, accept loops |
| `relaymode/relaymode.go` | `--relay` flag → go-iroh `relay.Mode` |
| `cmd/amber-serve/` | server binary: store open, key file, endpoint bind, advertising, data endpoints |
| `cmd/amber/` | client binary: local store commands + push/pull/refs, dialing, sharding, progress |
| `docs/superpowers/specs/2026-07-21-amber-store-iroh-design.md` | approved wire-protocol/design spec |
| `docs/superpowers/plans/2026-07-21-amber-store-iroh.md` | implementation plan (per-task APIs) |

amber-store-core packages consumed: `packstore`, `refstore`, `reference`, `fstree`, `amberpack`, `key`,
`ingest`, `chunkers`, `tarexport`, `tarextract` (server.go:16-20, wantsync.go:12-15, import.go:12-13,
restore.go:7-8, chunk.go:7-8).

---

## 2. How the iroh server hosts a store

### 2.1 `server.New` and the Server type

```go
func New(log *slog.Logger, objects *packstore.Store, refs *refstore.Store) *Server   // server/server.go:137
func (s *Server) SetDataPorts(ports []uint16)                                       // server/server.go:143 — call before Serve
func (s *Server) HandleStream(remote string, rw io.ReadWriteCloser)                 // server/server.go:164
func (s *Server) Serve(ctx context.Context, ep *iroh.Endpoint, grace time.Duration) error // server/server.go:429
```

- `New` takes **open** stores; the caller keeps ownership and closes them after the server stops
  (server.go:135-139). Defaults: `attachWait` 5s, empty per-name ref-lock map.
- Store types in the signature are amber-store-core (`packstore.Store`, `refstore.Store`) — exactly what
  jobs-iroh embeds, so `server.New` can be called directly from jobs-server with the embedded store.
- Open pattern used by both binaries (cmd/amber-serve/main.go:119-124, cmd/amber/store.go:17-31):
  `packstore.Open(filepath.Join(dir,"packstore"), packstore.WithSync(true))` and
  `refstore.Open(filepath.Join(dir,"refs"), true)`. **Stores are single-owner** — never open one directory
  from two live processes (store.go:19, design spec :50-51). jobs-server embedding the store means runners
  and clients must always go through the wire protocol, never share the directory.

### 2.2 ALPN: constant, but only enforced at Bind/Connect — not by Server

- `protocol.ALPN = "amber-store-iroh/1"` (protocol/protocol.go:18). It is a **const**; nothing in
  `server/` reads it. The plan pins the string "everywhere" via this const (plan doc, Global Constraints).
- Server side, the ALPN is applied at endpoint bind: `iroh.Bind(ctx, iroh.WithSecretKey(sk),
  iroh.WithALPNs(protocol.ALPN), iroh.WithRelayMode(...))` (cmd/amber-serve/main.go:143-148).
  `Server.Serve`/`HandleStream` never inspect the negotiated ALPN.
- **Consequence for jobs-iroh:** mounting the amber protocol under `jobs-runner-amber/1.0` and
  `jobs-amber-admin/1.0` requires no change to `server/` at all — bind the shared endpoint with the
  jobs ALPN set and route by `conn.ALPN()` (go-iroh conn.go:347).
- Client side, the ALPN **is hardcoded**: `ep.Connect(actx, netaddr.NewEndpointAddr(id, ta), protocol.ALPN)`
  (cmd/amber/dial.go:323 inside `raceConnect`). Any jobs-iroh client dialing under a jobs ALPN must fork or
  parameterize the dial path (it is `package main` anyway; see §5).

### 2.3 Accept loop: reusable per-connection/per-stream; Serve does NOT own the endpoint

Layering, outermost first:

1. `Serve(ctx, ep, grace)` (server.go:429-457) — accept loop over `ep.Accept(ctx)`; one goroutine per
   connection; 100ms backoff on persistent accept failure (server.go:437-441); after ctx cancel waits up
   to `grace` for in-flight handlers (server.go:449-456). It does **not** bind, shut down, or configure
   the endpoint — the caller does (main.go:143-152 binds, `defer ep.Shutdown`). But `Serve` does consume
   the endpoint's whole `Accept` stream, so it cannot share an endpoint that also serves other ALPNs
   unless it is the only accept-loop.
2. `serveConn(ctx, conn)` (server.go:463-482, **unexported**) — per-connection stream-accept loop:
   `conn.AcceptStreamConn(ctx)` → goroutine per stream → `HandleStream(conn.RemoteID().String(), stream)`.
   Closes the conn only when the peer goes away or via `context.AfterFunc(ctx, conn.Close)`
   (server.go:465) — closing a QUIC conn discards undelivered stream data (server.go:460-462). ~20 lines,
   trivial to re-implement in jobs-iroh.
3. `HandleStream(remote, rw io.ReadWriteCloser)` (server.go:164-197, **exported**) — the real seam.
   One operation per stream: reads the first frame, dispatches `TRefList`/`TPush`/`TPull`, FIN-closes after
   the final response; `TAttach` streams change ownership to the in-progress transfer and are NOT closed
   here (server.go:174-182). It is transport-agnostic: server_test.go drives it over in-memory pipes
   (server/server_test.go:78, :405, :423).

**jobs-iroh mounting recipe:** run your own accept loop (or go-iroh's `Router` — `NewRouter(ep,
map[string]ProtocolHandler, cfg)`, go-iroh router.go:93, handler iface router.go:22-33, per-conn
`Accept(ctx, conn)`), switch on `conn.ALPN()`, and for amber connections replicate `serveConn`'s
stream loop feeding `srv.HandleStream`. Both jobs ALPNs (`jobs-runner-amber/1.0`,
`jobs-amber-admin/1.0`) can feed the same `*server.Server` — the protocol is identical; if admin-vs-runner
policy differences are ever needed they must be layered outside `HandleStream` (it has no auth hook,
only the `remote` string used for logging, server.go:162-163). go-iroh also supports live ALPN updates via
`Endpoint.SetALPNs` (go-iroh endpoint.go:575).

Note `Serve` may run concurrently on several endpoints against one `Server` — amber-serve runs it on the
main endpoint plus every data endpoint (main.go:236-239, :246), all sharing the transfer-token registry.

### 2.4 Server behavior details

- **Per-ref-name mutex** for CAS commits; entries never removed, bounded by distinct names pushed
  (server.go:146-158).
- **Push** (`handlePush`, server.go:232-277): validate name (`reference.ValidateName`) and root
  (`key.Parse`); early CAS check (server.go:243-247), sharded-channel setup (server.go:249-252),
  `wantsync.Receive(channels, s.objects, root, s.jobs, nil)` (server.go:253), then re-check CAS under the
  per-name lock and commit `reference.Reference{Name, Key: root[:], CreatedAt}.Encode()` → `refs.Put`
  (server.go:257-271), reply `TOK{Key: root}` (server.go:272).
- **Push accounting log**: ref, client, offered (whole-tree object count via `fstree.ReachableKeys`),
  transferred, bytes, duration, MB/s (server.go:283-303).
- **CAS semantics** (`checkCAS`, server.go:343-368): `ExpectedOld` must equal current key;
  `nil` = "ref must not exist" (`refstore.ErrNotFound` → current nil, server.go:353-354). Mismatch sends
  `TErr{Code:"cas-mismatch", Current: currentKey}` and aborts. `--force` omits the precondition entirely
  (`CAS:false`, push.go:123).
- **Pull** (`handlePull`, server.go:370-424): `refs.Get(name)` → `TErr unknown-ref` on miss
  (server.go:372-374); reply `TRef{Record: raw}` — raw ref record **verbatim**, opaque signature fields
  preserved (server.go:378); with `DataConns>0` also `Token`+`DataPorts` inside the TRef frame
  (server.go:380-387); then one `wantsync.Send` goroutine per channel (server.go:410-419).
- **Error frames**: every local failure is reported to the peer as `TErr` except errors that *came from*
  the peer (`*protocol.RemoteError` must not be echoed back — `failLocal`, server.go:205-214).
- **Shutdown**: SIGINT/SIGTERM ctx; stop accepting, close conns, bounded 10s grace
  (`shutdownGrace`, main.go:34; design spec :75-77). Interrupted transfers are resumable by design.

---

## 3. Wire protocol

Spec: docs/superpowers/specs/2026-07-21-amber-store-iroh-design.md :113-195. One bidirectional QUIC
stream per operation; **initiator writes first** (a QUIC stream is invisible to the acceptor until data
flows — push.go:121-122, spec :119-121); responder FIN-closes after the exchange.

### 3.1 Framing (protocol/protocol.go)

- Frame = 4-byte big-endian payload length + canonical-CBOR `Msg` (`WriteMsg` :101-118,
  `ReadMsg` :120-143). Encoding uses `cbor.CanonicalEncOptions()` (:89-99) — same fxamacker/cbor v2.9.2
  family as jobs' canonical CBOR identity; determinism is stylistic, not load-bearing (spec :116-118).
- `MaxFrame = 16<<20` (16 MiB) bounds one frame (:22); `ChunkSize = 1<<20` (1 MiB) per TData chunk (:25).
- Clean EOF before a header → `io.EOF`; mid-frame cut → `io.ErrUnexpectedEOF` (:120-129).

### 3.2 Frame types (protocol.go:29-42) and Msg fields (:65-82)

| Type | Dir | Meaning / fields |
|---|---|---|
| `TPush=1` | c→s | push request: `Name, Root, CAS, ExpectedOld, DataConns` |
| `TPull=2` | c→s | pull request: `Name, DataConns` |
| `TRefList=3` | c→s | list all refs |
| `TRef=4` | s→c | raw reference record: `Record` (+ `Token`, `DataPorts` on sharded pull) |
| `TRefs=5` | s→c | listing: `Refs []RefInfo{Name,Key,CreatedAt(ns),User}` (:57-62) |
| `TWants=6` | recv→send | `Keys [][]byte`; **empty = done** |
| `TData=7` | send→recv | amberpack chunk: `Data` |
| `TDataEnd=8` | send→recv | end of one pack payload |
| `TOK=9` | s→c | push committed: `Key` |
| `TErr=10` | either | terminal: `Code, Text, Current` |
| `TAttach=11` | c→s | attach this stream to transfer `Token` |
| `TAccept=12` | s→c | sharded push accepted: `Token, DataPorts` |

Error codes: `cas-mismatch`, `unknown-ref`, `bad-request`, `internal` (:45-50).
`RemoteError{Code,Text,Current}` surfaces TErr as a Go error (`RemoteFromMsg`, :145-159).

### 3.3 Pack streaming (protocol/pack.go)

- `SendPack(w, iter.Seq2[fstree.Object, error])` (:16-31) and the zero-copy variant
  `SendPackRecords(w, iter.Seq2[[]byte, error])` (:76-91): amberpack writer over a `chunkWriter` that
  emits 1 MiB `TData` frames and a `TDataEnd` terminator (:33-68). An iterator error aborts the pack
  **without** the terminator; the sender then best-effort writes a `TErr` (wantsync.go:413-416).
- Zero-copy path: stored packstore records are wire-format identical (header + still-zstd-compressed
  payload); `st.GetRecord(k)` → `amberpack.ParseRecord` (framing/CRC validation, yields uncompressed
  length `hdr.Ulen` for progress) → `pw.AddRecord(rec)` — no decompress/re-encode (wantsync.go:390-412,
  pack.go:71-75, README:45-47).
- `NewPackReader(r)` (:93-141) reads TData frames as one `io.Reader` through `TDataEnd`; a `TErr` inside
  a pack surfaces as `*RemoteError` (:130-131). **Gotcha:** amberpack's decoder stops at its own end
  marker without consuming `TDataEnd`; consumers must drain (`io.Copy(io.Discard, pr)`) before reading
  further frames (:96-100, wantsync.go:263-268).

### 3.4 Push/pull exchanges

**Push** (spec :130-155, client push.go:121-144, server server.go:232-277):
1. Client: `TPush{Name, Root, CAS:!force, ExpectedOld, DataConns: conns-1}` (push.go:123).
2. Server: optional immediate `TErr cas-mismatch`; else (sharded) `TAccept{Token, DataPorts}`.
3. **Server-driven want loop**, frontier starts `{root}`: rounds of `TWants{Keys}` ← client answers each
   with a pack of exactly those objects. Empty `TWants` on every channel ends the loop.
4. Server re-checks CAS under lock, commits ref, replies `TOK{Key}`.
5. Client updates tracking ref `remotes/<serverID>/<name>` (push.go:146-154).

**Pull** (spec :157-165, client pull.go:76-137, server server.go:370-424): `TPull{Name, DataConns}` →
`TRef{Record[,Token,DataPorts]}` → same want loop, roles swapped (client = receiver/driver) → client
`refs.Put(name, m.Record)` verbatim + tracking ref (pull.go:128-137). Local ref update unconditional —
client store is single-owner (spec :164-166).

Round trips O(tree depth); only missing objects cross the wire; interrupted transfers rerun safely
(spec :151-155).

### 3.5 Sharded transfers (spec :167-188)

- Request: `DataConns = conns-1` in TPush/TPull. Server caps at `maxDataConns = 16` (server.go:50,
  :313-315, :396); client clamps `--conns` to 1..16 (`connsFlag`, dial.go:291-300); default `--conns 4`
  (push.go:46, pull.go:39).
- Server offers `Token` (16 random bytes) + `DataPorts` (TAccept for push server.go:320, TRef for pull
  server.go:380-386), then `gather`s attached streams for at most `attachWait` (5s) and proceeds
  **leniently** with whatever attached (server.go:94-113, :324).
- Client opens each extra connection **on its own fresh endpoint** (`iroh.Bind` per conn — one endpoint's
  UDP socket loop is the bottleneck; dial.go:100-127), targets `DataPorts[i%len]` at the IP the control
  connection actually reached, falls back to the control candidates (dial.go:105-122), opens a stream and
  sends `TAttach{Token}` (attachExtras, dial.go:263-289). Attach failures reduce parallelism, never fail
  the transfer.
- Server routes `TAttach` streams by token to the in-progress transfer; unknown token → `TErr bad-request`
  (server.go:174-182); undelivered attaches closed on `drop` (server.go:117-133).
- Wants are dealt **round-robin** across channels (`shardWants`, wantsync.go:328-334); a channel with an
  empty shard gets **no frame** that round (an empty TWants would terminate its sender); the final empty
  TWants goes to **every** channel (wantsync.go:199-207, :217-224).
- Data endpoints on the server are bound with the **same secret key** → same endpoint ID, distinct UDP
  ports; direct-path only, not published, learned in-band (main.go:219-243; `--data-endpoints` default 3,
  clamp 0..15, main.go:109-112, :220-225). Each data endpoint runs a full `srv.Serve` loop.
- **Backward compat by omission** (spec :183-188): old server ignores `DataConns` and opens with TWants —
  client replays the consumed frame via `io.MultiReader` and stays single-channel
  (`runSenders`, push.go:174-214, fallback :186-194); old client never sets `DataConns`.

---

## 4. wantsync package — purpose & API

Both halves of the have/want loop, over plain `io.ReadWriter` channels (fully transport-agnostic; loop
tests run on in-memory pipes).

```go
func Wants(st *packstore.Store, frontier []key.Key, jobs int) ([]key.Key, error)      // wantsync.go:40
func Receive(channels []io.ReadWriter, st *packstore.Store, root key.Key,
             jobs int, prog Progress) (Stats, error)                                  // wantsync.go:174
func Send(rw io.ReadWriter, st *packstore.Store, prog Progress) error                 // wantsync.go:364

type Progress interface {           // wantsync.go:107-113; nil legal; must be concurrency-safe
    Requested(objects int, bytes int64)   // bytes = keys' embedded logical lengths (upper bound)
    Transferred(objects int, bytes int64) // exact payload bytes that moved
    Wire(bytes int64)                     // bytes crossing the stream (compressed + framing)
}
type Stats struct { Rounds, Requested, Received int; Bytes int64 }  // wantsync.go:126-132
```

Semantics that matter:

- **Prune rule** (wantsync.go:34-66): a frontier key is pruned only if present **and**
  `fstree.CheckComplete(k, st.Get, st.Has, jobs)` confirms the whole subtree — presence alone is not
  enough because interrupted transfers store parents before children. This is what makes resume correct.
  Missing = `*fstree.MissingObjectError` or `packstore.ErrNotFound` (:68-74).
- **Round cap**: `maxWantsPerRound = 32<<10` keys; remainder carried to the next round so one frame never
  exceeds `MaxFrame` (:19-31, carry rejoin :319-322).
- **Receive** (:174-324): per-round, deals wants across channels, decodes all active channels' packs
  concurrently into one merged stream, and stores via
  `st.WriteParallel(seq, packstore.WriteOpts{Verify: true})` — every object verified against its key,
  **the peer is untrusted** (:291); children of received tree objects (`fstree.ChildKeys`) become the next
  frontier (:252-258). Durability from `packstore.WithSync(true)` at open.
- **Delivery check** (`checkDelivered`, :336-359): fails if the sender omitted any requested key — judged
  by pack arrival, not store presence (a present-but-incomplete key already satisfies `Has`, so presence
  would let a sender skip exactly the resume rounds).
- **Send** (:361-419): answers each TWants with a pack of exactly the requested records; dedupes repeats
  (:421-434); `st.SortByLocation(keys)` for read locality (:388); read failure → best-effort `TErr` then
  return (:413-416).
- **Error-type recovery**: amberpack's reader wraps read errors with `%v`, erasing `*RemoteError`;
  `errTrackingReader` preserves the original so CAS/remote errors survive the pack path (:436-452,
  :299-310).

---

## 5. Client-side programmatic API (push/pull) — the fork point

**Everything below is `package main` in `cmd/amber` — not importable.** The reusable, importable
machinery is `protocol` + `wantsync` (+ `relaymode`); the orchestration jobs-client must re-implement
(or upstream into a `client` package) is roughly:

- `dialServer(ctx, serverID, directAddrs, relayURL) (*serverConn, error)` — dial.go:161-257 (§6).
- `serverConn{conn, ep, id, cands}` with `Extra(ctx, i, ports)` (new endpoint per extra conn,
  dial.go:100-127), `remoteIP()` (dial.go:131-141), `Close()` (dial.go:144-154).
- Push flow `runPush` (push.go:65-165): resolve local ref → read tracking ref for `ExpectedOld`
  (push.go:84-93) → open stream, write TPush → `runSenders` (single/sharded/old-server fallback,
  push.go:174-214) → read TOK/TErr → write tracking ref → `pushError` rewrites cas-mismatch into
  "pull first, or push --force" (push.go:217-231).
- Pull flow `runPull` (pull.go:53-148): TPull → TRef → decode record, parse root →
  `wantsync.Receive(channels, objects, root, 0, xfer)` (+ optional `attachExtras` when the TRef carried a
  token, pull.go:114-119) → `refs.Put` name + tracking ref verbatim (pull.go:128-137).
- Refs listing `runRefs` (refs.go:32-73): TRefList → TRefs; needs no local store.
- Progress: `XferProgress` implements `wantsync.Progress` (xfer.go:18-67), bounded by the **root key's
  logical length** (`root.Length()` — the tree's whole footprint, known before round 1; dedup makes it an
  upper bound, so `Finish()` clamps the bar; push.go:107-119, pull.go:99-110, xfer.go:10-17). Renders
  content rate and wire rate separately.
- Tracking refs: `trackingPrefix = "remotes/"` (ref.go:16), `trackingRef(serverID, name)` =
  `remotes/<endpoint-id>/<name>` (dial.go:47-49). Reserved namespace: hidden from `ref list`
  (ref.go:64-66), refused by `ref set`/`import --ref`/push/pull name checks (ref.go:108-110,
  import.go:70-72, push.go:66-68, pull.go:57-59). This maps 1:1 onto jobs-iroh's
  `jobs-amber-admin/1.0` client ref-sync needs.
- Local commands (import/ls/export/restore/ref) are thin wrappers over amber-store-core `ingest.Dir`,
  `fstree.ListEntries/LookupEntry`, `tarexport`, `tarextract` (import.go:105-135, ls.go, export.go,
  restore.go) with spec parsing `KEY[/PATH] | ref:NAME[@PATH]` (spec.go:15-95) and chunker flags
  (chunk.go:20-33). jobs-client's "local builds use the same code paths" can reuse these patterns.

---

## 6. Endpoint dialing, discovery, advertising

### 6.1 Client dialing (dial.go:161-257)

- Server named by **endpoint ID** (`irohkey.ParseEndpointID`); jobs-iroh runners/clients dialing a known
  endpoint ID is exactly this path.
- **Direct mode** (`--addr host:port`, repeatable, hostnames resolve to all their IPs —
  `parseDirectAddrs`, dial.go:54-78): bind a plain ephemeral endpoint, wrap addrs as
  `netaddr.IPAddr{Addr}` candidates, `raceConnect` — **no discovery, no relays** (dial.go:167-186).
  This is how the offline e2e tests connect (e2e_test.go:33-80) and how jobs-iroh LAN runners should.
- **Discovery mode**: ephemeral secret key (open access → no stable client key, dial.go:188-191);
  resolvers **unioned**, in order: passive mDNS with 1s lookup timeout (its `Start` blocks — own
  goroutine, dial.go:202-206), `iroh.N0PkarrResolver`, `iroh.N0DNSAddressLookup` (dial.go:193-208);
  `iroh.Bind(WithSecretKey, WithAddressLookup, WithRelayMode)` (dial.go:214-219). go-iroh's `Connect`
  does **no discovery itself** — it only dials addresses already in the `EndpointAddr`, so resolve first
  and union every resolver's candidates so the relay remains fallback (dial.go:226-250).
- **`raceConnect`** (dial.go:305-352): dials every candidate concurrently, first winner taken, losers
  canceled/closed. Exists because go-iroh's own multi-candidate connect walks a *sorted* list with a
  per-candidate budget, and unreachable container-bridge 10.x/172.x addresses sort before LAN 192.168.x,
  exhausting the handshake window (dial.go:305-309). Hardcodes `protocol.ALPN` at :323 — **fork point**
  for jobs ALPNs.
- Relay pinning: `relaymode.FromFlag(url)` → `relay.ModeDefault()` or
  `relay.ModeCustom(relay.MapFromURLs(u))` (relaymode/relaymode.go:14-23).

### 6.2 Server binding & advertising (cmd/amber-serve/main.go)

- Key file: `loadOrCreateSecretKey(path)` — hex-encoded seed, `0600`, generated on first run; deleting it
  changes the endpoint ID (main.go:57-81, README:19). Default `--key server.key` (gitignored).
- Bind: `iroh.Bind(ctx, WithSecretKey(sk), WithALPNs(protocol.ALPN), WithRelayMode(mode))`
  (main.go:143-148); `ep.Online(ctx)` waits for relay connectivity (main.go:154-156).
- Relay selection: `--relay URL` wins; else `relay.DefaultMap().PreferNearest(3s probe,
  HTTPConnectProber)` — bounded, best-effort, falls back to default map (main.go:40-55).
- Direct-address advertising: the wildcard bind address is not dialable and gets stripped from published
  records, which would leave clients relay-only (main.go:158-163). So: `--advertise-addr ip[:port]`
  verbatim (parseAdvertiseAddrs, addrs.go:93-107), else machine unicast interface addresses on the bound
  port, excluding down interfaces, container bridges (`docker, br-, cni, flannel, veth, virbr, lxc` —
  addrs.go:25), loopback, link-local, unspecified (advertisedAddrPorts, addrs.go:44-69). Each address:
  `ep.AddExternalAddr(ap)` + folded into the advertised `EndpointAddr` (main.go:177-183).
- mDNS publish for same-LAN resolution even with stale pkarr (main.go:187-196; publisher active, client
  side passive).
- pkarr publish to n0's relay with custom `AddrFilter: publishableAddrs` — the library default
  (RelayOnlyFilter) would strip direct addresses; unfiltered would leak the wildcard bind addr
  (main.go:201-208, addrs.go:109-123); republished every 5 min in the background.
- Test-only nuance: bind to a concrete loopback (`WithBindAddr(::1)`) so `ep.Addr()` is dialable
  (e2e_test.go:46-52) — jobs-iroh in-process tests should copy this.

**jobs-iroh delta:** jobs-server owns one endpoint with five ALPNs, so amber-serve's `main()` is a
recipe, not reusable code — the key-file loader, relay probe, advertising filter set (addrs.go — pure,
easily lifted), mDNS/pkarr publication, and data-endpoint pattern all transfer directly.

---

## 7. Identity / auth model

- **Open by design**: anyone who knows the endpoint ID can push and pull; no authn/authz, ref signature
  fields carried opaquely and never checked (README:9-11, server.go:26-27, spec :19-21, non-goals
  :247-253). `HandleStream` has no auth hook; `remote` is used for logging only (server.go:162-163).
- Server: one stable secret key file (§6.2). Clients: ephemeral keys per invocation (dial.go:188-191).
- **jobs-iroh implication:** if jobs' CAS should not be world-writable to anyone holding the endpoint ID,
  authorization must be added *around* HandleStream (e.g. an allowlist of runner endpoint IDs checked at
  conn accept, per ALPN) — nothing in this repo provides it.

---

## 8. Reusable vs fork/wrap for jobs-iroh

**Import and use as-is** (all already on amber-store-core; no draganm anywhere):
- `protocol` — frame codec, pack streaming, `RemoteError`. The ALPN const is unused by the code paths
  jobs-iroh needs server-side.
- `wantsync` — both loop halves over `io.ReadWriter`; `Progress` hook; verified writes.
- `server` — `New`, `HandleStream`, `SetDataPorts`, `Serve` (per-endpoint). Mount under jobs ALPNs by
  accepting connections yourself and calling `HandleStream` per stream (§2.3).
- `relaymode` — trivial flag mapper.
- `cmd/amber-serve/addrs.go` helpers — pure functions, copy or upstream.

**Fork / re-implement** (package main, or hardcoded):
- All of `cmd/amber`'s network orchestration: `dialServer`/`raceConnect`/`serverConn.Extra`/
  `attachExtras` (ALPN hardcoded at dial.go:323), `runPush`/`runSenders`, `runPull`, `runRefs`,
  `XferProgress`, tracking-ref bookkeeping. ~600 lines total; alternatively upstream a `client` package
  into amber-store-iroh and depend on it.
- `cmd/amber-serve/main.go` — recipe only (single-ALPN, owns the process).

**Behavioral contracts to preserve when forking the client:**
initiator-writes-first; drain pack readers through TDataEnd; empty-TWants-terminates-sender (never send
an empty TWants to a channel mid-round); final empty TWants to every channel; replay-frame fallback for
non-sharding peers; tracking-ref CAS discipline.

---

## 9. Perf constraints already documented

- go-iroh transport tops out **~16 MB/s per UDP socket** (loopback measurement); parallel streams on one
  connection do **not** help — hence sharding across connections on separate sockets/endpoints
  (README:52-56, dial.go:94-96, server.go:39-43, main.go:110-112).
- Defaults: client `--conns 4` (1..16), server `--data-endpoints 3` (0..15), server-side per-transfer cap
  `maxDataConns=16` (push.go:46, main.go:111, server.go:50).
- Linux kernel UDP buffers must be raised or quic-go warns and throughput suffers:
  `sysctl -w net.core.rmem_max=8388608 net.core.wmem_max=8388608` (README:48-51).
- Zero-copy record path: disk-to-wire verbatim, no decompress/re-encode (README:45-47, wantsync.go:390-393).
- Every unreachable advertised address costs connecting peers handshake budget — hence the bridge/down
  interface filter and `raceConnect` (README:36-40, addrs.go:21-25, dial.go:305-309).
- Want rounds ≈ O(tree depth); 32Ki-key round cap; `MaxFrame` 16 MiB; `ChunkSize` 1 MiB.
- Completeness-walk parallelism: `Server.jobs` (0 = GOMAXPROCS, server.go:32) — note there is currently
  **no setter**; amber-serve leaves it 0.

## 10. Misc facts a fresh engineer will need

- Two `key` packages collide: amber-store-core `key.Key` (32-byte content keys, embedded logical length
  via `k.Length()`) vs go-iroh `key.EndpointID`/`key.SecretKey`; convention is `irohkey` alias
  (plan doc Global Constraints; dial.go:18).
- `key.Key.Length()` = logical (uncompressed, subtree-aggregate) size — used for progress upper bounds
  (wantsync.go:116-124, push.go:116).
- Received packs are deliberately NOT staged through core's `inbox`; direct
  `WriteParallel(Verify:true)` instead (plan doc, Spec deviation note).
- Store layout convention: `<dir>/packstore` + `<dir>/refs` (store.go:17-19, main.go:92).
- Tests: protocol/wantsync unit tests over pipes; server tests drive `HandleStream` directly
  (server_test.go); offline e2e binds real endpoints on loopback and connects by `--addr`
  (e2e_test.go:30-80) — no public infrastructure touched. Same pattern recommended for jobs-iroh e2e.
