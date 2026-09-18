# jobs-iroh

<p align="center">
  <img src="docs/assets/jobs-logo.jpg" alt="iroh JOBS — Jonas' Own Build System" width="520">
</p>

A small, self-contained build system: one server, N runners, and a client,
connected only by iroh QUIC. Builds are hermetic (rootless Linux namespace
sandbox, no network inside), identified by content (canonical-CBOR
definitions and a content-addressed amber store), and described by Starlark
recipes. The server embeds NATS JetStream for scheduling — there is no other
infrastructure: no HTTP, no database, no container runtime.

Full design: [`docs/architecture/architecture.md`](docs/architecture/architecture.md).

## Binaries & ALPNs

Three core binaries: `jobs-server`, `jobs-runner`, `jobs-client` — plus an
optional `jobs-registry` (an OCI registry serving build outputs as pullable
images). One server endpoint, five ALPNs:

| ALPN | Who | What |
|---|---|---|
| `jobs-build/1.0` | client | submit builds, watch, logs, cancel |
| `jobs-runner-nats/3.0` | runner | NATS tunnel to the embedded scheduler |
| `jobs-runner-amber/1.0` | runner | store sync (objects + refs) |
| `jobs-admin/1.0` | client (TUI) | observe builds, stats, fleet, refs, cancel/delete, diagnose |
| `jobs-amber-admin/1.0` | client / registry | push source trees up, pull outputs home |

## A build file

A build is a `BUILD.jobs` Starlark file next to your source. Minimal shape:

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

Imports (fetches) are the only steps with network access; the script runs in
a sandbox with its inputs mounted read-only, the source at `$SRC`, and the
output tree collected from `$out`. Language plugins (Go, npm, PyPI, cargo,
RubyGems, …) expand lockfiles into per-dependency cached imports.

## Quickstart: local build

No server needed — builds run hermetically against an embedded store under
`--data-dir` (default `~/.local/share/jobs-iroh`):

```sh
cd myapp                                            # or any subdirectory of it
jobs-client build                                   # build → prints F + output key
jobs-client run -- arg1                             # build, then exec JOBS.entrypoint
jobs-client develop                                 # interactive shell in the build sandbox
jobs-client image -o app.tar --tag myapp:dev
docker load -i app.tar
```

Omitting `--source` builds where you stand: the nearest ancestor of the
current directory holding a `BUILD.jobs`, searched no higher than the
repository root. The whole repository becomes the ingest context — that is
what makes sibling references (`../lib`, `//lib/common`) resolvable — and the
build root is the directory the recipe was found in. Each command prints what
it resolved:

```
context: /home/you/myrepo  (dir services/api, recipe BUILD.jobs)
```

Pass `--source <dir>` to name the tree explicitly; `--source-root` moves the
ceiling, `--no-repo-root` pins it to the source itself.

## Quickstart: server, runner, remote build

```sh
# 1. Server (prints its endpoint ID on startup):
jobs-server --data-dir /var/lib/jobs-iroh

# 2. Runner(s), anywhere that can reach the server over iroh:
jobs-runner --server <endpoint-id> [--addr host:port] [--size c1-m2]

# 3. Client: push source, build remotely, pull the output home
#    (--source omitted → resolved from the cwd, exactly as for local builds):
cd myapp && jobs-client remote-build --server <endpoint-id>
jobs-client watch  --server <endpoint-id> --request-id <id>   # re-attach
jobs-client status --server <endpoint-id>                     # one-shot tables
jobs-client tui    --server <endpoint-id>                     # interactive admin
```

`remote-build` streams the running steps' build output alongside the
progress block by default (`--no-logs` opts out). When something fails,
`jobs-client diagnose --server <id> --request <id>` prints the durable
failure trail — every failed attempt with its captured output, surviving
retries and server restarts (`--json` for the machine-friendly shape).

Local and remote builds share the same canonical definitions, so the same
source yields the same build identity K/F everywhere — remote outputs pulled
home join the local cache and vice versa.

## Registry: docker pull a build

`jobs-registry` serves build outputs straight to docker/containerd/k8s — no
export/load/push hop:

```sh
jobs-registry --server <endpoint-id> [--addr host:port] --listen :5000
docker pull my-registry-host:5000/jobs:<build-K>
```

Images live in a single `jobs` repository whose tags are build keys K (the
value `remote-build` prints); `/v2/jobs/tags/list` enumerates every
submitted build (a local build's bare F key also pulls, but is not listed).
Images have two layers — the runtime closure under `/jobs/store/<key>` plus
the platform shell (script entrypoints need their `#!/bin/sh` shebang;
`--no-shell` opts out), and the build artifact at the root — assembled on
the fly from the registry's own store, which syncs from the jobs-server on
demand. Layers are **uncompressed** (`…image.layer.v1.tar`) and generated
straight from that store on every request rather than stored as blobs, so
serving costs CPU per pull instead of disk, and `--cache-ttl` (default 24h)
now expires only the small manifest/config blobs. Size the volume for the
store, which holds every synced build and is never reclaimed.
The registry is pull-only and speaks plain HTTP:
terminate TLS at an ingress or mark it insecure in the container runtime.
A sample k8s Deployment lives in
[`deploy/jobs-registry/`](deploy/jobs-registry/).

## Images

Every release publishes three multi-arch (amd64 + arm64) images to the
GitHub Container Registry, tagged `v<version>` and `latest`:

| Image | Binary | Runs as |
|---|---|---|
| `ghcr.io/jobs-build/jobs-iroh-server` | `jobs-server` | distroless nonroot (65532), `/data` = `--data-dir` |
| `ghcr.io/jobs-build/jobs-iroh-runner` | `jobs-runner` | root — its sandboxes need user/mount/pid namespaces and cgroups (privileged in k8s) |
| `ghcr.io/jobs-build/jobs-registry` | `jobs-registry` | distroless nonroot (65532), `/data` = `--data-dir` |

They hold the static binary and CA certificates, nothing else — builds and
imports run in hermetic roots assembled from the store, so the runner image
needs no userland. Dockerfiles under [`deploy/`](deploy/), built and pushed
by the [Release images](.github/workflows/release-images.yml) workflow when
a release tag is pushed. Releases up to v0.35.0 are on Docker Hub as
`dmilhdef/…` instead.

## Dev setup

Go toolchain comes from the Nix devShell:

```sh
direnv allow          # sets up the flake shell + GOPRIVATE
# or without direnv:
nix develop -c go test ./...
```

`GOPRIVATE=github.com/jobs-build/*` is required for module fetches
(exported by `.envrc`).

## Tests

```sh
nix develop -c go build ./... && nix develop -c go vet ./...
nix develop -c go test ./...

# macOS is a dev-fallback target (no hermetic sandbox); build tags mean a
# Linux-only `go build` never type-checks the !linux files, so cross-check:
nix develop -c env CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go vet ./...
```

## License

[AGPL-3.0-or-later](LICENSE).
