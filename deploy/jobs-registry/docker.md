# Running jobs-registry in Docker

The registry is a client of the jobs-server over iroh: give it the server's
endpoint ID and it fetches builds P2P, caching them under `--data-dir`. That
makes a local instance self-contained — a laptop registry talks to the same
jobs-server as a cluster one, no tunnels or ingress needed.

## Quickstart (macOS / Docker Desktop)

```sh
docker volume create jobs-registry-data
docker run --rm -v jobs-registry-data:/data alpine chown -R 65532:65532 /data

docker run -d --name jobs-registry --restart unless-stopped \
  --net host \
  -v jobs-registry-data:/data \
  ghcr.io/jobs-build/jobs-registry:latest \
  --server=<jobs-server endpoint id> \
  --data-dir=/data --listen=:5000 --cache-ttl=24h

docker pull localhost:5000/jobs:<build hash>
```

On Linux, replace `--net host` with `-p 127.0.0.1:5000:5000`. The sections
below explain each piece.

## Prepare the data volume

The image is distroless `nonroot` (uid 65532); a fresh named volume is
root-owned, so chown it once or the registry fails with
`mkdir /data/repos: permission denied`:

```sh
docker volume create jobs-registry-data
docker run --rm -v jobs-registry-data:/data alpine chown -R 65532:65532 /data
```

## Run

```sh
docker run -d --name jobs-registry --restart unless-stopped \
  -p 127.0.0.1:5000:5000 \
  -v jobs-registry-data:/data \
  ghcr.io/jobs-build/jobs-registry:latest \
  --server=<jobs-server endpoint id> \
  --data-dir=/data --listen=:5000 --cache-ttl=24h
```

Then pull with the same names the cluster uses:

```sh
docker pull localhost:5000/jobs:<build hash>
```

Docker treats localhost registries as insecure HTTP automatically — no
daemon config needed. Publish on `127.0.0.1` only (as above): the registry
itself has no auth, so an all-interfaces binding would let anyone on the
network pull images.

## Docker Desktop (macOS)

Two platform quirks:

- `docker pull` runs inside Docker Desktop's Linux VM, so `localhost` at
  pull time is the VM's localhost — the listener must exist in the VM, which
  a published port provides (port publishing binds inside the VM too).
- macOS's AirPlay Receiver squats on port 5000 of the host, which makes
  `-p 127.0.0.1:5000:5000` fail. Either disable it (System Settings →
  General → AirDrop & Handoff), or — since only the VM-side binding matters
  for pulls — sidestep the host entirely with host networking:

```sh
docker run -d --name jobs-registry --restart unless-stopped \
  --net host \
  -v jobs-registry-data:/data \
  ghcr.io/jobs-build/jobs-registry:latest \
  --server=<jobs-server endpoint id> \
  --data-dir=/data --listen=:5000 --cache-ttl=24h
```

`--net host` binds port 5000 inside the VM only: pulls resolve it, the
host's port 5000 stays with AirPlay, and nothing is reachable from the LAN
(the VM sits behind NAT).
