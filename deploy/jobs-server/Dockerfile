# Containerize jobs-server. Built the same way as deploy/jobs-registry: the
# binary is compiled OUTSIDE the image and copied in — see
# .github/workflows/release-images.yml, which builds all three images from a
# clean tag checkout.
#
# distroless/static ships CA certs (relay/pkarr discovery) and nothing else; the
# nonroot variant runs the server as 65532 — a deployment gives /data to that
# uid (fsGroup on Kubernetes, chown for a plain volume).
FROM gcr.io/distroless/static-debian12:nonroot
ARG TARGETARCH
COPY jobs-server-${TARGETARCH} /usr/bin/jobs-server
# /data is the server's --data-dir: store, NATS state, endpoint key (server.key).
ENV JOBS_DATA_DIR=/data
EXPOSE 4433/udp
ENTRYPOINT ["/usr/bin/jobs-server"]
