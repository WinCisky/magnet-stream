# syntax=docker/dockerfile:1

# ---- Build stage: static musl binary -----------------------------------
FROM rust:1-slim-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends musl-tools pkg-config \
    && rm -rf /var/lib/apt/lists/* \
    && rustup target add x86_64-unknown-linux-musl

WORKDIR /app

# Build context is the monorepo root (the Cargo workspace and its lockfile
# live there):  docker build -f services/seeders/Dockerfile -t magnet-seeders .
# Cache dependency compilation separately from application code.
COPY Cargo.toml Cargo.lock ./
COPY services/seeders/Cargo.toml services/seeders/
RUN mkdir services/seeders/src \
    && echo "fn main() {}" > services/seeders/src/main.rs \
    && cargo build --release --target x86_64-unknown-linux-musl \
    && rm -rf services/seeders/src

COPY services/seeders/src ./services/seeders/src
RUN touch services/seeders/src/main.rs \
    && cargo build --release --target x86_64-unknown-linux-musl \
    && cp target/x86_64-unknown-linux-musl/release/magnet-seeders /magnet-seeders

# ---- Runtime stage: nothing but the static binary ----------------------
FROM scratch

COPY --from=builder /magnet-seeders /magnet-seeders

ENV BIND_ADDR=0.0.0.0:8080

# HTTP API.
EXPOSE 8080/tcp
# Mainline DHT node (UDP). Must be published for DHT peer discovery to work;
# match this to DHT_PORT if you override it.
EXPOSE 6881/udp
# Tunnel inbound peers (TUNNEL_INBOUND_ADDR, unset by default): TCP and UDP
# (uTP) on one port that differs from DHT_PORT; publish both if you set it,
# e.g. -p 6883:6883/tcp -p 6883:6883/udp with TUNNEL_INBOUND_ADDR=0.0.0.0:6883.

ENTRYPOINT ["/magnet-seeders"]
