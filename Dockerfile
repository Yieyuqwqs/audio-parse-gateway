# ---------- build ----------
FROM rust:1.88-bookworm AS builder
WORKDIR /src

# 先只编依赖：空 main + 完整 Cargo.toml，改业务代码时这层缓存不失效
COPY Cargo.toml ./
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release \
    && rm -rf src

COPY src ./src
RUN touch src/main.rs && cargo build --release

# ---------- runtime ----------
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/audio-parse-gateway /usr/local/bin/audio-parse-gateway
ENV BIND_ADDR=0.0.0.0:8080 \
    RUST_LOG=info,audio_parse_gateway=debug
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8080/health || exit 1
CMD ["/usr/local/bin/audio-parse-gateway"]
