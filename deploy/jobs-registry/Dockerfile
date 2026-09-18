# Containerize jobs-registry. The binary is built OUTSIDE the image and copied
# in, which keeps the image COPY-only — the Release images workflow
# (.github/workflows/release-images.yml) does that for all three images at
# release time; by hand:
#
#   CGO_ENABLED=0 GOOS=linux nix develop -c \
#     go build -trimpath -o deploy/jobs-registry/jobs-registry-amd64 ./cmd/jobs-registry
#   docker build --build-arg TARGETARCH=amd64 -t jobs-registry deploy/jobs-registry
#
# distroless/static ships CA certs (needed for pkarr/DNS discovery when no
# --addr is given) and nothing else.
FROM gcr.io/distroless/static-debian12:nonroot
# Multi-arch: the release builds jobs-registry-{amd64,arm64} into the
# context and one buildx --platform invocation assembles the manifest list
# (COPY-only image — no qemu needed).
ARG TARGETARCH
COPY jobs-registry-${TARGETARCH} /usr/bin/jobs-registry
# /data must be a writable volume: the amber store, blob cache and image
# records live there (see k8s.yaml's PVC).
ENV JOBS_DATA_DIR=/data
EXPOSE 5000
ENTRYPOINT ["/usr/bin/jobs-registry"]
