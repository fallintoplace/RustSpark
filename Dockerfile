# syntax=docker/dockerfile:1
FROM rust:1.95-bookworm AS builder
WORKDIR /build
# The root package depends on the local native-engine crate.
# No root Cargo.lock is currently committed, so do not require one here.
COPY Cargo.toml ./
COPY src/ src/
COPY native-engine/ native-engine/
RUN cargo build --release --bin rustspark

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --user-group --create-home --shell /usr/sbin/nologin rustspark \
    && install -d -m 0750 -o rustspark -g rustspark /data
COPY --from=builder /build/target/release/rustspark /usr/local/bin/rustspark
USER rustspark
WORKDIR /data
ENTRYPOINT ["/usr/local/bin/rustspark"]
CMD ["--help"]
