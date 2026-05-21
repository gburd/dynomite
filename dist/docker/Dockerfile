# syntax=docker/dockerfile:1.6
#
# Multi-stage Dockerfile for the Rust port of dynomite.
#
# Stage 1 (`builder`): pin to rust:1.90-bookworm to match
# rust-toolchain.toml. We build a release dynomited binary against
# a debian:bookworm sysroot so the runtime image can use the same
# glibc.
#
# Stage 2 (`runtime`): debian:bookworm-slim with a non-root
# dynomite user. We copy the dynomited binary, the bundled config,
# and the systemd-friendly entrypoint that supervises an embedded
# redis-server when DYNOMITE_BACKEND=redis.

ARG RUST_VERSION=1.90
ARG DEBIAN_RELEASE=bookworm

FROM rust:${RUST_VERSION}-${DEBIAN_RELEASE} AS builder
WORKDIR /src

# Cache the dependency layer.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY docs ./docs

# Build only the dynomited binary in release mode. We pass
# --locked to enforce the committed Cargo.lock; the build fails
# if the resolver wants to update the graph.
RUN cargo build \
        --release \
        --locked \
        --workspace \
        --exclude dyn-hash-tool \
        --bin dynomited

# Sanity check: the binary must come up under -h.
RUN /src/target/release/dynomited -h >/dev/null


FROM debian:${DEBIAN_RELEASE}-slim AS runtime

# Install only the runtime dependencies: a TLS trust store for
# outbound connections (when an embedder enables it) and
# redis-server so the image is self-contained for evaluation. The
# C reference Dockerfile also bundled redis-server; we keep the
# shape so a `docker run` works identically.
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        redis-server \
        tini \
 && rm -rf /var/lib/apt/lists/*

# Non-root runtime user.
RUN groupadd --system dynomite \
 && useradd --system --gid dynomite --home /var/lib/dynomite \
        --shell /usr/sbin/nologin --create-home dynomite \
 && mkdir -p /etc/dynomite /var/run/dynomite /var/log/dynomite \
 && chown -R dynomite:dynomite /etc/dynomite /var/run/dynomite /var/log/dynomite

COPY --from=builder /src/target/release/dynomited /usr/local/sbin/dynomited
COPY dist/docker/dynomite.yml /etc/dynomite/dynomite.yml
COPY dist/docker/entrypoint.sh /usr/local/bin/dynomite-entrypoint

RUN chmod 0755 /usr/local/sbin/dynomited /usr/local/bin/dynomite-entrypoint

# Peer port (DNODE).
EXPOSE 8101
# Client (proxy) port.
EXPOSE 8102
# Stats / admin endpoint.
EXPOSE 22222
# Bundled Redis backend.
EXPOSE 22122

USER dynomite
ENV DYNOMITED_CONF=/etc/dynomite/dynomite.yml \
    DYNOMITE_BACKEND=redis

# tini gives us a real PID 1 so signals propagate to dynomited
# and the supervised redis-server.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/dynomite-entrypoint"]
CMD []
