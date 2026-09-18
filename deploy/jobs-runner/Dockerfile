# Containerize jobs-runner. Built like deploy/jobs-registry: binary compiled
# OUTSIDE the image and copied in — see .github/workflows/release-images.yml.
#
# Root, not nonroot: the runner creates user/mount/pid namespaces for its
# build and import sandboxes and parks jobs in their own cgroups for memory
# limits, which in a container means running privileged as uid 0. The image
# holds nothing but the static binary and CA certs: since v0.25.0 both builds
# and imports run in hermetic roots assembled from the store (the embedded
# shell, the fetchers' runtime closures), so no host userland is needed.
FROM gcr.io/distroless/static-debian12:latest
ARG TARGETARCH
COPY jobs-runner-${TARGETARCH} /usr/bin/jobs-runner
# /data is the runner's --data-dir: private store, fetcher cache, work trees.
ENV JOBS_DATA_DIR=/data
ENTRYPOINT ["/usr/bin/jobs-runner"]
