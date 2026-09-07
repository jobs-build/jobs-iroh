# amber-store-core API map — embedding as the jobs-iroh CAS

Source examined (read-only module cache):
`$GOMODCACHE/github.com/amber-store/core@v0.0.0-20260720222444-a37d35fa4ecf`
(all `file:line` cites below are relative to that root unless prefixed `jobs/`, which means
`~/fables-for-robots/jobs/`).

Module: `github.com/amber-store/core`, Go 1.26.3 (go.mod:1-3).
Notable deps: pebble/v2 (refstore), fxamacker/cbor/v2, klauspost/compress (zstd),
zeebo/blake3, PlakarKorp/go-cdc-chunkers (ultracdc), FastFilter/xorfilter, go-git (gitignore
matcher only), urfave/cli/v2 (CLI only) (go.mod:5-16).

**Relationship to draganm/amber-store**: amber-store-core is the extracted *local core* of
draganm/amber-store. Verified by diff: `key/key.go`, `fstree/encode.go`, `amberpack/record.go`,
`chunkers/byte.go` are byte-identical modulo import path; core's `packstore` only adds `Wipe()`
(packstore/packstore.go:640). **All on-disk and on-wire formats and all computed keys are
identical.** Existing amber trees, roots, and jobs' content-derived identities carry over
unchanged if the same chunking parameters are used (see "Chunking determinism" below).
What core deliberately does NOT have: `client`, `daemon`, `server`, `embedded`, `remoteclient`,
`remotes`, `remotesync`, `sshsign`, `allowlist`, `grant` — the entire networked/auth layer.
Those are exactly the packages jobs imports today and are the port cut-points (§ Cut-points).

---

## 1. Opening / embedding a store

There is **no unified Store type**. Consumers wire two independent pieces and own the
directory layout (README.md:63-66):

```go
objects, err := packstore.Open(filepath.Join(dir, "packstore"),
    packstore.WithSync(true))                      // packstore/packstore.go:92
refs, err := refstore.Open(filepath.Join(dir, "refs"), true)  // refstore/refstore.go:39
```

Conventional layout `<dir>/packstore` + `<dir>/refs` is what the CLI uses
(cmd/amber-store/store.go:17-32).

### Concurrency / locking model

- **packstore**: `Open` takes an exclusive non-blocking `flock` on the directory fd
  (packstore/packstore.go:104-107) — **one process per store dir, hard-enforced**; second
  `Open` fails with "already open". Within the process the Store is fully concurrent-safe:
  `appendMu` serializes the write path, `mu` (RWMutex) guards segment lists for readers;
  lock order appendMu→mu (packstore/packstore.go:73-86). Reads (`Get/Has/GetRecord`) are
  RLock-only and go: active-segment in-RAM index → sealed segments newest-first, mmap'd
  (packstore/packstore.go:472-499).
- **refstore**: Pebble DB; reads lock-free, writes serialized by `writeMu`
  (refstore/refstore.go:18-25). Pebble also takes its own dir lock — again single-process.
- **Consequence for jobs-iroh**: exactly one process owns a store directory. Server, runner,
  and client-local-build each own their *own* store dir; all cross-process access must go
  over the iroh protocols. This matches the plan (client embeds store+runner for local
  builds; runner pulls/pushes over jobs-runner-amber).
- Durability: `WithSync(true)` (default) fsyncs; a *failed fsync poisons the write path*
  permanently (sticky `failed` error, packstore/packstore.go:349-355) — reads continue,
  writes stop until reopen. Options: `WithSegmentSize` (default 256 MiB rotation,
  packstore/packstore.go:29), `WithSync` (packstore/packstore.go:56).
- Crash recovery is automatic in `Open`: sealed segments validated, the single active
  segment tail-scanned and truncated to its last valid record; a crashed seal-rename is
  completed (packstore/packstore.go:175-231).

### packstore.Store API surface (all on `*Store`)

| Method | Signature / semantics | Cite |
|---|---|---|
| `Put` | `(k key.Key, data []byte) error` — dedup by `Has`, encode record, append+fsync | packstore.go:449 |
| `Get` | `(k) ([]byte, error)` — caller-owned bytes; `ErrNotFound` sentinel | packstore.go:472, :22 |
| `Has` | `(k) (bool, error)` | packstore.go:615 |
| `GetRecord` | full 46-byte-header+still-compressed record, wire-identical — **zero-copy push path** into `amberpack.Writer.AddRecord` | packstore.go:507 |
| `StoredSize` | post-compression payload size from index only — for byte-balanced push batching | packstore.go:538 |
| `Missing` | `([]key.Key) ([]key.Key, error)` — batch absence check, concurrent, preserves order/multiplicity | missing.go:20 |
| `SortByLocation` | reorder keys to on-disk layout for near-sequential read sweeps | packstore.go:578 |
| `WriteBatch` | `iter.Seq2[Object, error]` → store all, one fsync at end; durable-on-return, NOT atomic (valid prefix on crash, harmless under CAS) | packstore.go:410 |
| `WriteParallel` | `(seq, WriteOpts{Writers, BatchSize, Verify}) (WriteStats, error)` — parallel compress+verify+append; `Verify` re-hashes payload vs key (**the authoritative identity gate for received data**) | parallel.go:45, verify.go:98 |
| `Verify` | `(ctx) error` — full scrub of sealed segments (framing, CRC, BLAKE3, footer index, filter) | verify.go:26 |
| `Wipe` | delete all objects, store stays open/empty | packstore.go:640 |
| `Close` | fsync+close active (unsealed), munmap sealed, release flock | packstore.go:689 |

`packstore.Object{Key key.Key; Data []byte}` (segment.go:34-37). Errors: `ErrNotFound`,
`ErrClosed` (packstore.go:22-25), `ErrCorrupt` (= amberpack.ErrCorrupt, segment.go:31),
`ErrVerify` (verify.go:16).

---

## 2. Key type (`key` package)

- `key.Key = [32]byte`, value type, directly comparable → usable as Go map key
  (key/key.go:13-18).
- Layout: header byte = 4-bit type | 1 reserved bit | 3-bit (length-field-size − 1);
  then 1–8 byte big-endian logical length (no leading zero); then BLAKE3-256 of the
  serialized bytes truncated to the remaining 23–30 bytes
  (architecture/keys.md:5-39, key/key.go:100-112).
- Types: `Blob=0, FileNode=1, DirLeaf=2, DirNode=3, XattrSet=4` (key/type.go:12-18);
  5–15 reserved.
- **Length field is logical, not serialized size**: Blob/XattrSet = own byte length;
  FileNode = total file content bytes (**file size readable from the key alone → O(1)
  stat**); DirLeaf/DirNode = own bytes + cumulative subtree footprint (**O(1) du**)
  (architecture/types.md:46-71).
- Constructors/accessors: `New(t, length, serialized)`, `NewFromHash(t, length, [32]byte)`,
  `Parse([]byte)` (validates canonical form), `Validate()`, `Type()`, `Length()`, `Hash()`,
  `String()` = lowercase hex (key/key.go:55-112). Sentinels in key/errors.go:8-16.
- Identical to draganm/amber-store's key package (verified diff) — **keys are portable**.

---

## 3. fstree model

Serialization: RFC 8949 §4.2 core-deterministic CBOR via
`cbor.CoreDetEncOptions()` + `NilContainers: NilContainerAsEmpty` (fstree/encode.go:17-25).
Blob = raw bytes, no framing.

Object layouts (architecture/fstree.md:28-97):
- `FileNode`: CBOR array of 32-byte child keys, file order; offsets implicit (running sum
  of child `key.Length()`).
- `DirLeaf`: CBOR array of entry maps, sorted bytewise by name.
- `DirNode`: CBOR array of `[sepName, childKey]` pairs; sepName = greatest entry name in
  child subtree.
- `XattrSet`: canonical CBOR bstr→bstr map (spilled xattrs).

`fstree.Entry` — the directory entry, integer CBOR keys 0-9 (fstree/encode.go:32-43):
`Name []byte(0)`, `Mode uint64(1)` = **raw POSIX st_mode, type+perm bits incl. exec/setuid**,
`UID(2)`, `GID(3)`, `Mtime int64 ns(4)`, then exactly one payload by `Mode&S_IFMT`:
`ContentKey(5)` for REG/DIR, `LinkTarget(6)` for LNK (inline), `Rdev [major,minor](7)` for
CHR/BLK, nothing for FIFO/SOCK; optional `XattrsIn(8)` (inline, pre-encoded map) xor
`XattrsKey(9)` (spilled). File size is NOT stored in the entry — read
`ContentKey.Length()`. Root directory has no metadata of its own
(architecture/types.md:123-129).

### Encode/decode

`EncodeBlob` (encode.go:54), `EncodeFileNode` (:64), `EncodeDirLeaf` (:86),
`EncodeDirNode` (:118), `EncodeXattrSet` (:140); inverses `DecodeFileNode`
(decode.go:12), `DecodeDirLeaf` (:30), `DecodeDirNode` (:40).
`fstree.Object{Key; Bytes}` + `Emit func(Object) error` — builders emit
children-before-parents (fstree/object.go:10-17).

### Builders (streaming, bottom-up)

- `NewDirBuilder(ic chunkers.ItemChunker)`; `AddEntry(emit, e)` (entries must arrive
  sorted); `Finish(emit) (key.Key, error)` — empty dir emits an empty DirLeaf
  (fstree/dir_builder.go:22-65).
- `NewFileIndexBuilder(ic)`; `AddChild(emit, blobKey, nil)`; `Finish(emit)`
  (fstree/index_builder.go:28, :50, :115). Memory O(levels × MaxRun).

### Read paths (all take `get func(key.Key) ([]byte, error)` — plug in `packstore.Get`)

- `LookupEntry(dir, name, get) (Entry, error)` — O(log n) descent, `ErrNotFound`
  (fstree/lookup.go:16).
- `ResolvePath(root, "a/b/c", get) (key.Key, error)` — dir key at path; rejects `..`;
  `ErrNotDir` (fstree/collect.go:23).
- `ResolveEntry(root, path, get) (*Entry, error)` — final entry of any kind; nil for
  root itself (fstree/collect.go:55).
- `ListEntries(dir, after, limit, get) ([]Entry, more, error)` — **paged readdir**,
  O(log n + limit) (fstree/list.go:18).
- `CollectEntries(dir, get) ([]Entry, error)` — whole directory, name order
  (fstree/collect.go:88).
- `WriteContent(w io.Writer, fileKey, get) error` — stream whole file content (Blob or
  FileNode root) to a writer (fstree/content.go:13).
- `ChildKeys(k, data) ([]key.Key, error)` — direct children of any object
  (fstree/children.go:12).
- `ReachableKeys(root, get) ([]key.Key, error)` — **closure walk**, parallel BFS
  (GOMAXPROCS), root first, deduped; Blob/XattrSet never fetched
  (fstree/reachable.go:23).
- `CheckComplete(root, get, has, jobs) error` — closure existence check;
  `*MissingObjectError{Key}` for absent leaves (fstree/checkcomplete.go:27).

**Gap**: no random-access file reader. O(log n) seek is documented
(architecture/fstree.md:150-154) and the length fields make it trivial (walk FileNode
children summing `Length()`), but the library exposes only whole-file `WriteContent`.
jobs' amberfuse implements its own random reads; if jobs-iroh needs `io.ReaderAt`
over a file key (e.g. serving ranges, dev-mode), it must write the ~40-line walker itself.

### Chunking determinism (identity-critical)

Two chunkers (chunkers package):
- Byte chunker: ultracdc via PlakarKorp/go-cdc-chunkers; `SplitBytes(r, *ByteOpts, fn)`;
  **nil opts = ultracdc defaults min 2 KiB / normal 10 KiB / max 64 KiB**
  (chunkers/byte.go:17-19).
- `ItemChunker` for FileNode/DirLeaf/DirNode streams: boundary when low `bits` of
  BLAKE3(item) are zero; `NewItemChunker(bits)` → MinRun=2^bits/4 (≥2), MaxRun=2^bits*4
  (chunkers/item.go:24-32). Default bits = 7 (ingest/ingest.go:26).

**Every key depends on these parameters** (ingest/ingest.go:44-49). Three different
defaults exist in the wild:
- library nil-ByteOpts: 2K/10K/64K (chunkers/byte.go:17);
- amber-store CLI flags: **32 KiB / 128 KiB / 256 KiB** (cmd/amber-store/main.go:70-87);
- jobs pins the CLI values explicitly: `defByteOpts = {32<<10, 128<<10, 256<<10}` +
  ItemBits 7 (jobs/amber/build.go:24-26).

**jobs-iroh must pin `chunkers.ByteOpts{MinSize: 32<<10, NormalSize: 128<<10,
MaxSize: 256<<10}` and ItemBits 7 everywhere** (server, runner, client) or content keys
(and thus cache identity and dedup against existing stores) diverge.

---

## 4. Ingest (dir → key)

- `ingest.Objects(path, Opts) (iter.Seq2[fstree.Object, error], *key.Key, error)` —
  path may be a directory (root = DirLeaf/DirNode) or single regular file (root =
  Blob/FileNode). Root pointer is filled only after the stream is fully consumed without
  error (ingest/ingest.go:100-132). Use this to route objects somewhere other than a
  local packstore (e.g. straight onto an iroh stream as an amberpack).
- `ingest.Dir(st *packstore.Store, path, Opts) (key.Key, packstore.WriteStats, error)` —
  ingest straight into a packstore via WriteParallel (ingest/ingest.go:137-153).
- `ingest.Scan(dir, noIgnore, jobs) (files, bytes, error)` — cheap pre-scan for progress
  totals (ingest/scan.go:26).
- `Opts{Jobs, Chunk ChunkOpts{Byte *ByteOpts, ItemBits, XattrInlineMax}, NoIgnore,
  Progress}` (ingest/ingest.go:52-62); `Progress` interface = `FileDone()`, `AddBytes(n)`,
  must be concurrency-safe (ingest/ingest.go:35-40). Xattr spill threshold default 256 B
  (ingest/ingest.go:30).
- Parallel walk: bounded pool, non-blocking semaphore (inline fallback → no deadlock);
  per-directory entry order and per-file chunk order preserved → deterministic keys
  (ingest/parallel.go:19-80).
- Metadata captured: raw st_mode/uid/gid/mtime-ns via Lstat (ingest/meta.go:19-27),
  symlink targets, device major/minor, xattrs (skipped on symlinks)
  (ingest/driver.go:81-135). Empty file = empty Blob (ingest/driver.go:166-176).
- `.amberignore`: gitignore semantics (negation, `**`, dir-only, anchored, last match
  wins, per-subtree composition); ignored dirs pruned unread; the `.amberignore` files
  themselves always stored so restored trees re-ingest to the same root
  (amberignore/amberignore.go:32-57; README.md:125-131). Nil `*Matcher` is valid =
  ignore nothing.

**Gap**: no tar-stream ingest. jobs' `amber.Store.Ingest(ctx, body io.Reader)` ingests a
tar body (jobs/amber/storeapi.go:24-26). Port options: (a) `tarextract.Extract` to a temp
dir + `ingest.Dir` (two disk passes), or (b) write a tar→fstree ingester using
DirBuilder/FileIndexBuilder directly — note tar entry order is not sorted-by-name, so (b)
needs buffering per directory; (a) is the low-risk first cut.

---

## 5. Materialize / restore (key → dir)

**There is no direct restore function in the library.** The CLI restores by piping export
into extract (cmd/amber-store/restore.go:41-48):

```go
pr, pw := io.Pipe()
go func() { pw.CloseWithError(tarexport.Write(pw, dirKey, objects.Get)) }()
err := tarextract.Extract(pr, destDir)
```

- `tarexport.Write(w, root, get)` — root must be DirLeaf/DirNode; PAX format (nanosecond
  mtimes, SCHILY.xattr.*, long names, devices); sockets skipped; root dir's own metadata
  not emitted; file size taken from `ContentKey.Length()` — no content pre-read
  (tarexport/tarexport.go:27-37, :96, :124-126).
- `tarextract.Extract(r, destDir)` — creates destDir; **path-traversal-safe** (`safeJoin`
  rejects `..` and absolute names, tarextract/tarextract.go:110-122); dir perms+mtimes
  applied after all children (read-only dirs don't block, tarextract.go:93-104);
  chown only as root; xattrs best-effort (EPERM/ENOTSUP skipped with stderr warning);
  device nodes skipped without privilege (tarextract.go:79-84, :140-167).
  Note tarextract.go:81 and :159 write warnings prefixed `amber-store:` to os.Stderr —
  cosmetic leak jobs-iroh may want to route through slog when vendoring the pattern.

### Build-store flow: restore + bind-mount RO vs jobs' amberfuse

jobs *already defaults to materialize*: `JOBS_STORE_MOUNT` unset/`materialize` extracts
the store tree to disk + RO bind mount; FUSE is opt-in because it "has proven flaky under
concurrent load" (jobs/runner/storemount.go:29-49). `materializeStore` is just
`st.Tar(ctx, storeKey, "")` → extract (jobs/runner/storemount.go:52-60). With
amber-store-core embedded, the identical flow is the export|extract pipe above, with no
network hop — the store is in-process. So **jobs-iroh can drop amberfuse entirely** and
ship materialize-only:
1. runner pulls missing objects for the store root (see §8),
2. `tarexport.Write` → `tarextract.Extract` into `<work>/store/<BOK>/…`,
3. RO bind mount into the sandbox (unchanged jobs logic).
Costs vs FUSE: full tree bytes on disk per build + extract latency; wins: no fuse deps,
no kernel round trips, no flakiness. A content-keyed materialization cache
(dir-per-root-key, hardlink or bind from it) is the natural optimization and needs no
library support beyond `Has`/`CheckComplete`.

---

## 6. References (refstore + reference)

- `refstore.Open(dir, sync) (*Store, error)` — Pebble; dumb name→bytes KV
  (refstore/refstore.go:39-49).
- `Put(name, record)` — **unconditional overwrite; no compare-and-swap, no history**
  (refstore/refstore.go:52-56; architecture/references.md:44-46).
- `Get(name)` → bytes or `ErrNotFound` (:59); `Delete(name)` linearizable existence check
  (:78); `All()` → `[]Record{Name, Data}` lexicographic (:94); `Wipe()` (:116); `Close()`.
- **No namespaces**: names are opaque UTF-8 strings, 1–1024 bytes, `/` allowed but has no
  structural meaning, `@` and control chars banned (reference/reference.go:58-77).
  Namespacing (e.g. `builds/<K>`, `images/<name>`) is a caller convention — prefix scans
  are not exposed (All() then filter, or add a range iterator when porting).
- `reference.Reference{Name(0), Key(1) []byte 32, User(2), CreatedAt(3) int64 ns,
  Signature(4) opt, PublicKey(5) opt}` — canonical CBOR, integer keys
  (reference/reference.go:46-53). `Encode()` validates; `Decode(b)` **rejects any
  non-canonical byte encoding** (re-encode + bytes.Equal, reference.go:139-155).
  `SignaturePayload()` = encoding without key 4 (reference.go:160-163). Core stores
  signature fields opaquely — SSHSIG signing/verification is a consumer concern
  (architecture/references.md:22-31); jobs-iroh gets auth from iroh node identity +
  ALPN instead, so signatures can stay unused.
- If jobs-iroh needs atomic ref CAS (e.g. "set builds/K only if unchanged"), layer a
  mutex + read-compare-put above refstore; writes are already serialized by `writeMu` so
  a single wrapping lock in the owning process is sufficient.

---

## 7. amberpack (wire format) + inbox (durable receive)

### amberpack

Record (shared by disk segments and wire packs, architecture/amberpack.md:16-53):
46-byte header `tag 0x01 | key[32] | flags | ulen u32 | slen u32 | crc32c` + payload;
zstd per-record only when strictly smaller (amberpack/record.go:67-87); CRC over whole
record with crc field zeroed; u32 lengths cap one object at 4 GiB.

Wire pack = `"AMBERPK\x03"` + records + `0x00` end marker — **rootless, unordered,
possibly-partial set**; truncation detected (no clean-EOF ambiguity)
(amberpack/pack.go:37-51; architecture/amberpack.md:74-89).

API:
- `NewWriter(w)`; `Add(fstree.Object)`; **`AddRecord(rec)`** — zero-copy from
  `packstore.GetRecord`, no decompress/recompress (pack.go:96-102); `Close()` writes end
  marker + flush, does not close w (pack.go:106).
- `NewReader(r)`; `All() iter.Seq2[fstree.Object, error]` — one-shot; validates framing,
  CRC, key canonicality, 256 MiB/record bound; does **NOT** verify payload hash — that is
  `WriteParallel{Verify:true}`'s job at the storage gate (pack.go:130-190, :51).
- Sentinels: `ErrMalformed` (stream) pack.go:46, `ErrCorrupt` (record) record.go:36.

This is the natural payload framing for the `jobs-runner-amber` iroh ALPN: an iroh stream
IS a byte stream; write a pack on one side, `Reader.All()` on the other.

### inbox

Durable receive of authenticated packs before they hit the packstore (inbox/entry.go:1-9):
- `inbox.Open(dir, store *packstore.Store, workers, *slog.Logger) (*Inbox, error)` —
  recovers committed entries on restart, sweeps tmp (inbox/inbox.go:46-75, :169-195).
- `Stage(meta Meta{Ref, Root, ReceivedAt}, body io.Reader) (tmpPath, blake3, n, err)` —
  spool to tmp file; caller authorizes against hash then `Commit` or `Discard`
  (inbox/inbox.go:81-110).
- `Commit(tmpPath, bodyHash, root)` — content-addressed by body hash → idempotent
  re-receive (inbox/inbox.go:121-143).
- Worker pool drains entries into the store via `WriteParallel{Verify:true}`; bad packs
  quarantined under `failed/` (inbox/inbox.go:243-276).
- `WaitFor(root)` — barrier: block until every pack tagged with root is processed —
  exactly the "all objects landed before the ref/result is published" gate the server
  needs when a runner pushes build outputs (inbox/inbox.go:147-153).

Server-side push flow: accept pack stream on jobs-runner-amber → `Stage` → `Commit(root)`
→ `WaitFor(root)` → `CheckComplete(root, Get, Has, 0)` → publish ref/result.

---

## 8. Pull/push sync logic (must be reimplemented — not in core)

draganm/amber-store's `remotesync` (jobs uses it via client HTTP) is absent from core, but
its building blocks are all present. The algorithm to port onto iroh streams
(reference impl: `$GOMODCACHE/github.com/draganm/amber-store@v0.0.0-20260625100352-a621bbdb2cf3/remotesync/{push,pull,batch}.go`):

- **Pull** (runner ← server): resolve ref → root key; walk locally:
  `fstree.ReachableKeys` stopping at objects already present (draganm's `localMissing`
  walks top-down, skipping subtrees whose root is present — cheap because interior nodes
  imply children under CAS… it checks each node with `Has` before descending); batch the
  missing keys; request each batch; receive amberpack; `WriteParallel{Verify:true}`.
- **Push** (runner → server / client → server): `ReachableKeys(root, store.Get)` →
  ask remote which are `Missing` (needs a "missing" RPC on the ALPN) → batch by
  `StoredSize` bytes → `GetRecord` + `amberpack.Writer.AddRecord` (zero-copy) → send →
  finally set ref. Order batches with `SortByLocation` for sequential disk reads
  (packstore/packstore.go:578).
- Wire needs only three RPCs per ALPN: `missing(keys) → keys`, `pack(keys) → amberpack`
  (or push: `pack stream + root`), `ref get/set`. Everything else is local.

---

## 9. cborx — can it serve jobs' canonical-CBOR identity needs?

**No — it is deliberately minimal.** cborx only encodes/decodes byte-string-keyed maps
(xattrs), which fxamacker cannot emit from Go maps (cborx/cborx.go:1-13):
`EncodeXattrs(map[string][]byte) []byte` (:45), `DecodeXattrs` (:67).

jobs' build identity (`builddef.Definition.Key()` = amber FileKey of canonical CBOR,
jobs/builddef/definition.go:82-88) uses fxamacker with a canonical enc-mode. The recipe
used uniformly across core (fstree/encode.go:17-25, reference/reference.go:32-40,
inbox/entry.go:32-40) is:

```go
opts := cbor.CoreDetEncOptions()
opts.NilContainers = cbor.NilContainerAsEmpty
encMode, _ := opts.EncMode()
```

jobs-iroh should adopt this exact recipe for job/build identity CBOR (string-keyed structs
work fine with fxamacker; cborx is only needed if byte-string map keys appear). Identity
keys can then be `key.New(key.Blob, uint64(len(canon)), canon)` — same as jobs' FileKey
for single-chunk payloads, so existing K values are preserved (definitions are far below
the 256 KiB max chunk, hence always single-Blob).

---

## 10. CLI (cmd/amber-store) — reusable patterns

Commands: `ingest [--ref NAME] [--jobs] [--min/--avg/--max/--item-bits/--xattr-inline-max]
[--no-ignore]`, `ls`, `export`, `restore`, `ref list/get/set/rm`
(cmd/amber-store/main.go:33-40). Spec parsing worth copying:
`KEY[/PATH]` and `ref:NAME[@PATH]` resolution (cmd/amber-store/spec.go:19-65), subpath
descent to a content key (spec.go:70-87), ingest+ref+progress wiring
(cmd/amber-store/ingest.go:60-140 — note the LIFO cancel/wait teardown of the progress
goroutine, :82-98). Store open/close pair (cmd/amber-store/store.go:17-37).

---

## 11. Cut-points: where draganm/amber-store leaks into jobs today

Import census over `~/fables-for-robots/jobs` (non-test): `key` ×84,
`client` ×21, `reference` ×11, `fstree` ×8, `remotesync` ×7, `refstore`/`packstore`/
`amberpack` ×3 each, plus `embedded`, `daemon`, `server`, `remoteclient`, `remotes`,
`sshsign`, `allowlist`, `grant`, `inbox`, `tarexport`, `tarextract`, `chunkers`,
`amberignore` (jobs/go.mod:9).

- **Mechanical re-import** (byte-identical in core): `key`, `fstree`, `chunkers`,
  `amberignore`, `amberpack`, `packstore`, `refstore`, `reference`, `tarexport`,
  `tarextract`, `inbox`. Every jobs file importing these just changes the import path.
- **`jobs/amber/storeapi.go:24-38` — the real seam.** jobs already defines
  `amber.Store`, the exact interface the engine/runner/bootstrap consume
  (`Ingest(io.Reader)/Ls/Tar/File/PutRef/GetRef/ListRefs/Remote*`). Reimplement this
  interface directly over core (`packstore`+`refstore`+`fstree`+`tarexport`) and the
  vocabulary types now borrowed from `client` (`client.Stats/Entry/RefInfo/Push
  Stats/PullStats/ErrRefNotFound/...`, jobs/amber/storeapi.go:7-38) must be redeclared
  as jobs-iroh's own types.
- **`jobs/amber/embedded.go`** wraps draganm's `embedded.Store` (Signer/Grant/Sync config,
  jobs/amber/embedded.go:36-56) — replace with a thin struct owning
  `*packstore.Store`+`*refstore.Store`; drop Signer/Grant (iroh node keys replace SSH
  identity; allowlist/grant auth is dead in the iroh design).
- **`remotesync`/`remoteclient`/`remotes`** (jobs/amber/remotesync.go,
  jobs/runner/pushprogress.go, refwriter.go) — reimplement per §8 over iroh ALPNs.
- **`amberfuse`** (jobs/amberfuse/*) — drop; materialize path already default
  (jobs/runner/storemount.go:29-49).
- **`daemon`/`server`/`allowlist`/`grant`/`sshsign`** — used by test harnesses
  (jobs/ambertest) and CLI store plumbing; all replaced by the jobs-server iroh surface.

---

## 12. What's missing vs what a build system needs

| Need | Status in core | Note |
|---|---|---|
| existence check | `Has` (packstore.go:615), batch `Missing` (missing.go:20) | ✓ |
| closure walk | `ReachableKeys` (reachable.go:23) | ✓ root-first, parallel |
| closure completeness | `CheckComplete` (checkcomplete.go:27) | ✓ |
| tree diff | **absent** | for incremental sync, `Has`-pruned walk substitutes; a proper sepName-aware prolly-diff would be a new feature |
| random-access file read | **absent** (only whole-file WriteContent, content.go:13) | hand-roll via FileNode length walk if needed |
| tar-stream ingest | **absent** | extract-to-tmp + ingest.Dir, or new tar ingester (§4) |
| ref conditional-put / watch | **absent** (Put overwrites, refstore.go:52) | layer in owning process; watch belongs to NATS KV in jobs-iroh anyway |
| GC / delete | **absent** (tagDelete "reserved for v2", packstore/segment.go:19) | only `Wipe` (all-or-nothing); plan server GC as wipe+re-push or wait for upstream v2 |
| quota/size accounting | key.Length gives O(1) logical du; `StoredSize` per object | ✓ enough for metrics |
| multi-process store | **hard-blocked by flock** (packstore.go:104) | by design; one owner process |
| networked sync | **absent** | §8 recipe over iroh |

---

## 13. Minimal embedding sketch for jobs-iroh

```go
// open (server, runner, and client-local all identical):
objects, _ := packstore.Open(filepath.Join(dir, "packstore"))   // packstore.go:92
refs, _    := refstore.Open(filepath.Join(dir, "refs"), true)   // refstore.go:39
inb, _     := inbox.Open(filepath.Join(dir, "inbox"), objects, 0, log) // inbox.go:46  (server only)

// jobs-iroh chunking convention — matches jobs today (jobs/amber/build.go:24-26):
chunk := ingest.ChunkOpts{Byte: &chunkers.ByteOpts{MinSize: 32<<10, NormalSize: 128<<10, MaxSize: 256<<10}, ItemBits: 7}

// ingest a build output dir:
root, stats, _ := ingest.Dir(objects, outDir, ingest.Opts{Chunk: chunk})   // ingest.go:137

// materialize a store tree for the sandbox:
pr, pw := io.Pipe()
go func() { pw.CloseWithError(tarexport.Write(pw, storeRoot, objects.Get)) }()
_ = tarextract.Extract(pr, sandboxStoreDir)                                 // restore.go:41-48 pattern

// push over an iroh stream:
keys, _ := fstree.ReachableKeys(root, objects.Get)
missing := rpcMissing(keys)             // remote packstore.Missing
objects.SortByLocation(missing)
w := amberpack.NewWriter(irohStream)
for _, k := range missing { rec, _ := objects.GetRecord(k); _ = w.AddRecord(rec) } // zero-copy
_ = w.Close()

// receive on the server:
tmp, h, _, _ := inb.Stage(inbox.Meta{Ref: name, Root: root[:], ReceivedAt: now}, irohStream)
inb.Commit(tmp, h, root); inb.WaitFor(root)
_ = fstree.CheckComplete(root, objects.Get, objects.Has, 0)
// then refs.Put(name, referenceRecordBytes)
```
