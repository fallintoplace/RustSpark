# syntax=docker/dockerfile:1
FROM rust:1.95-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --locked --release --bin rustspark

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home --shell /usr/sbin/nologin rustspark
COPY --from=builder /build/target/release/rustspark /usr/local/bin/rustspark
USER rustspark
WORKDIR /data
ENTRYPOINT ["/usr/local/bin/rustspark"]
CMD ["--help"]
