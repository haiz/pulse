# Build stage
FROM rust:1.75-slim AS builder
WORKDIR /build
COPY . .
RUN cargo build --release -p pulse-broker

# Runtime stage
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/pulse-broker /usr/local/bin/
COPY config/ /etc/pulse/
EXPOSE 4222 9090
VOLUME /var/lib/pulse
ENTRYPOINT ["pulse-broker"]
