# Rust Project Structure

## 1. Workspace Layout

```
pulse/
├── Cargo.toml                    # workspace root
├── README.md
├── LICENSE
├── .github/
│   └── workflows/
│       ├── ci.yml                # test + clippy + fmt on every PR
│       └── release.yml           # build binaries + publish crates
│
├── crates/
│   ├── pulse-protocol/           # Wire protocol (shared by broker + SDK)
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── frame.rs          # Frame encode/decode
│   │       ├── codec.rs          # tokio Encoder/Decoder impl
│   │       ├── types.rs          # MessageType enum, payload structs
│   │       ├── message_id.rs     # UUIDv7 wrapper
│   │       └── crc.rs            # CRC32C helpers
│   │
│   ├── pulse-cluster/            # Gossip, replication, topology
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs            # Public API re-exports
│   │       ├── gossip.rs         # SWIM protocol implementation
│   │       ├── topology.rs       # Cluster state, node roles, health
│   │       ├── replication.rs    # WAL replication (none/async/sync)
│   │       ├── consistent_hash.rs # Topic-to-node ownership ring
│   │       └── peer.rs           # Peer connection + frame forwarding
│   │
│   ├── pulse-broker/             # Broker binary
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── main.rs           # CLI, config load, signal handling
│   │       ├── server/
│   │       │   ├── mod.rs
│   │       │   ├── listener.rs   # TCP + TLS accept loop
│   │       │   ├── connection.rs # Per-connection handler
│   │       │   └── session.rs    # Authenticated session state
│   │       ├── pipeline/
│   │       │   ├── mod.rs
│   │       │   ├── ingest.rs     # Receive + validate PUB frames
│   │       │   ├── dedup.rs      # Bloom filter + sled dedup
│   │       │   ├── wal.rs        # WAL writer + group commit
│   │       │   └── dispatcher.rs # Orchestrate ingest → route
│   │       ├── routing/
│   │       │   ├── mod.rs
│   │       │   ├── engine.rs     # Topic trie + resolution
│   │       │   ├── filter.rs     # Expression parser + evaluator
│   │       │   ├── transform.rs  # Payload transform ops
│   │       │   └── config.rs     # routes.yaml loader + hot reload
│   │       ├── delivery/
│   │       │   ├── mod.rs
│   │       │   ├── manager.rs    # Per-consumer queue orchestration
│   │       │   ├── queue.rs      # Memory + disk overflow queue
│   │       │   ├── retry.rs      # Exponential backoff scheduler
│   │       │   ├── ack_tracker.rs# In-flight tracking + timeout
│   │       │   └── dlq.rs        # Dead Letter Queue
│   │       ├── storage/
│   │       │   ├── mod.rs
│   │       │   ├── wal_segment.rs# WAL segment file format
│   │       │   ├── compaction.rs # Segment compaction
│   │       │   ├── state_db.rs   # sled wrapper
│   │       │   └── recovery.rs   # Crash recovery (WAL replay)
│   │       ├── auth/
│   │       │   ├── mod.rs
│   │       │   ├── authenticator.rs
│   │       │   └── permissions.rs
│   │       ├── namespace/
│   │       │   ├── mod.rs
│   │       │   └── registry.rs
│   │       ├── metrics/
│   │       │   ├── mod.rs
│   │       │   └── exporter.rs   # Prometheus /metrics endpoint
│   │       └── config/
│   │           ├── mod.rs
│   │           ├── broker.rs     # broker.yaml schema
│   │           ├── services.rs   # services.yaml schema
│   │           ├── routes.rs     # routes.yaml schema
│   │           └── loader.rs     # File watcher + hot reload
│   │
│   ├── pulse-sdk/                # Rust SDK library
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs            # Public API re-exports
│   │       ├── client.rs         # Pulse struct (main entry point)
│   │       ├── builder.rs        # PulseBuilder
│   │       ├── connection.rs     # Connection manager + reconnect
│   │       ├── publish.rs        # Publish API + retry logic
│   │       ├── subscribe.rs      # Subscribe API + handler dispatch
│   │       ├── request_reply.rs  # Request-reply pattern
│   │       ├── stream.rs         # Stream API
│   │       ├── discovery.rs      # Multi-node discovery via seed list
│   │       ├── dedup.rs          # Consumer-side dedup (LRU + sled)
│   │       ├── error.rs          # PulseError enum
│   │       ├── types.rs          # Event, EventMeta, Headers, etc.
│   │       ├── url.rs            # Connection URL parser
│   │       └── mock.rs           # MockPulse for testing
│   │
│   ├── pulse-ffi/                # C ABI for foreign language SDKs
│   │   ├── Cargo.toml
│   │   ├── cbindgen.toml         # C header generation config
│   │   └── src/
│   │       ├── lib.rs            # #[no_mangle] extern "C" functions
│   │       ├── handle.rs         # Opaque pointer management
│   │       └── error.rs          # Error codes for C callers
│   │
│   └── pulse-admin/              # Admin CLI tool
│       ├── Cargo.toml
│       └── src/
│           ├── main.rs           # CLI (clap)
│           ├── commands/
│           │   ├── status.rs     # Show broker/cluster status
│           │   ├── topics.rs     # List topics, subscriptions
│           │   ├── dlq.rs        # Inspect/replay DLQ events
│           │   ├── services.rs   # List connected services
│           │   ├── cluster.rs    # Show mesh topology, node health
│           │   └── config.rs     # Validate config files
│           └── api_client.rs     # HTTP client for admin API
│
├── sdks/                         # Foreign language SDK wrappers
│   ├── python/
│   │   ├── pyproject.toml
│   │   ├── src/
│   │   │   └── lib.rs           # PyO3 bindings
│   │   ├── pulse/__init__.py    # Python package
│   │   └── tests/
│   │
│   ├── typescript/
│   │   ├── package.json
│   │   ├── src/
│   │   │   ├── native.rs        # napi-rs bindings
│   │   │   └── index.ts         # TypeScript wrapper
│   │   └── tests/
│   │
│   └── go/
│       ├── go.mod
│       ├── pulse.go             # Go wrapper over C FFI
│       └── pulse_test.go
│
├── config/                       # Example configuration files
│   ├── broker.yaml
│   ├── services.yaml
│   └── routes.yaml
│
├── tests/                        # Integration tests
│   ├── integration/
│   │   ├── publish_subscribe.rs
│   │   ├── exactly_once.rs
│   │   ├── reconnect.rs
│   │   ├── consumer_groups.rs
│   │   ├── content_filter.rs
│   │   ├── wal_recovery.rs
│   │   ├── backpressure.rs
│   │   ├── cluster_join.rs       # Node join/leave, gossip convergence
│   │   ├── cluster_failover.rs   # Node failure, topic rebalancing
│   │   └── tiered_durability.rs  # Memory/balanced/durable mode switching
│   └── benches/
│       ├── throughput.rs         # Events/sec benchmark
│       ├── latency.rs           # P50/P99 latency benchmark
│       ├── wal_write.rs         # WAL fsync benchmark
│       └── nsq_comparison.rs    # Head-to-head benchmark vs NSQ
│
└── docker/
    ├── Dockerfile                # Multi-stage build for broker
    ├── Dockerfile.dev            # Dev image with tools
    └── docker-compose.yml        # 3-node cluster + observability
```

## 2. Cargo Workspace Configuration

```toml
# pulse/Cargo.toml
[workspace]
resolver = "2"
members = [
    "crates/pulse-protocol",
    "crates/pulse-cluster",
    "crates/pulse-broker",
    "crates/pulse-sdk",
    "crates/pulse-ffi",
    "crates/pulse-admin",
]

[workspace.package]
version = "0.1.0"
edition = "2021"
rust-version = "1.75"
license = "Apache-2.0"
repository = "https://github.com/yourorg/pulse"

[workspace.dependencies]
# Async
tokio = { version = "1", features = ["full"] }
tokio-util = { version = "0.7", features = ["codec"] }
tokio-uring = { version = "0.5", optional = true }  # Linux-only io_uring

# TLS
rustls = "0.23"
tokio-rustls = "0.26"
rustls-pemfile = "2"

# Serialization
serde = { version = "1", features = ["derive"] }
serde_json = "1"
serde_yaml = "0.9"
rmp-serde = "1"

# Storage
sled = "0.34"

# Concurrency
dashmap = "5"
arc-swap = "1"

# Protocol
uuid = { version = "1", features = ["v7"] }
crc32c = "1"
bytes = "1"
lz4_flex = "0.11"
rand = "0.8"

# Clustering
hashring = "0.3"

# Crypto
hmac = "0.12"
sha2 = "0.10"

# Observability
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["json", "env-filter"] }
metrics = "0.22"
metrics-exporter-prometheus = "0.13"

# CLI
clap = { version = "4", features = ["derive"] }

# Error handling
thiserror = "1"
anyhow = "1"

# Testing
criterion = "0.5"
```

## 3. Crate Dependency Graph

```
pulse-protocol    (no internal deps — pure protocol logic)
       ↑
       │
  ┌────┼──────────────────┐
  │    │                   │
  │  pulse-cluster         │
  │    │                   │
  │    ├── depends on      │
  │    │   pulse-protocol  │
  │    │                   │
  │    ↑                   │
  │    │                   │
pulse-sdk          pulse-broker
  │                    │
  │                    ├── depends on pulse-protocol
  ├── depends on       ├── depends on pulse-cluster
  │   pulse-protocol   │
  │                    │
pulse-ffi          pulse-admin
  │                    │
  ├── depends on       ├── HTTP client only
  │   pulse-sdk        │   (no internal deps)
  │                    │
```

**Key principles:**
- `pulse-protocol` has zero internal dependencies. It's shared between broker, cluster, and SDK, containing only frame encoding, message types, and CRC logic.
- `pulse-cluster` depends only on `pulse-protocol`. It handles gossip, topology, consistent hashing, and WAL replication. Kept separate so embedded/single-node deployments can skip it.
- `pulse-broker` depends on both `pulse-protocol` and `pulse-cluster`. The cluster module is optional — single-node mode doesn't activate it.

## 4. Implementation Order

### Phase 1: Foundation — DONE

```
1. pulse-protocol
   ├── types.rs         — Define all message types, enums
   ├── message_id.rs    — UUIDv7 generation
   ├── frame.rs         — Frame serialize/deserialize
   ├── crc.rs           — CRC32C helpers
   └── codec.rs         — tokio Encoder/Decoder

   Status: Complete. 85+ tests passing.

2. pulse-broker (skeleton)
   ├── main.rs           — CLI + config loading
   ├── config/           — Parse broker.yaml, services.yaml
   ├── server/listener   — TCP accept (no TLS yet)
   └── server/connection — Read/write frames (no auth yet)

   Status: Complete. Broker starts, accepts connections.
```

### Phase 2: Core Pipeline

```
3. WAL
   ├── wal_segment.rs    — Segment file format, read/write
   ├── wal.rs            — WAL writer with fsync + group commit
   └── recovery.rs       — WAL replay on startup

   Test: write events, kill process, recover, verify all events present.

4. Dedup
   ├── dedup.rs          — Bloom filter + sled dedup

   Test: publish same msg_id twice → second returns "duplicate".

5. Pipeline integration
   ├── dispatcher.rs     — Wire ingest → dedup → WAL → ACK

   Test: end-to-end PUB → ACK with WAL durability.
```

### Phase 3: Tiered Durability

```
6. Memory mode
   ├── Skip WAL entirely, in-memory dispatch only
   ├── Configurable per-topic and per-namespace

   Test: 800K+ msg/sec throughput, ≤5μs P99 latency.

7. Balanced mode
   ├── Group commit (batch fsync every 5ms)
   ├── Async WAL writer with bounded channel

   Test: 100K+ msg/sec, ≤5ms data loss window on crash.

8. Mode selection
   ├── Per-topic durability override
   ├── CLI flag: --durability memory|balanced|durable
   ├── YAML config: durability.default

   Test: mixed modes within same broker, correct guarantees per topic.
```

### Phase 4: Routing & Delivery

```
9. Routing
   ├── engine.rs         — Topic trie
   ├── filter.rs         — Expression parser + evaluator
   ├── config.rs         — routes.yaml loader

   Test: wildcard matching, content filters, fan-out.

10. Delivery
    ├── manager.rs        — Per-consumer queues
    ├── ack_tracker.rs    — Track in-flight + timeout
    ├── retry.rs          — Exponential backoff
    └── dlq.rs            — Dead letter queue

    Test: publish → route → deliver → ACK. Retry on NACK. DLQ after max retries.

11. Consumer groups
    ├── Partition key support (pin publisher → consumer)
    ├── Round-robin fallback without partition key

    Test: partition key preserves ordering. Round-robin distributes load.
```

### Phase 5: Clustering (Pulse Mesh)

```
12. pulse-cluster crate
    ├── gossip.rs          — SWIM protocol (join, leave, suspect, dead)
    ├── topology.rs        — Cluster state machine, node health
    ├── consistent_hash.rs — Topic ownership ring

    Test: 3-node cluster forms, nodes discover each other, topology converges.

13. Topic ownership
    ├── Consistent hashing assigns topics to nodes
    ├── Zero-copy frame forwarding for misrouted messages

    Test: publish to non-owner node → forwarded → delivered correctly.

14. WAL replication
    ├── replication.rs     — none/async/sync replication modes
    ├── peer.rs            — Peer-to-peer frame transport

    Test: node failure → replica takes over → zero data loss (sync mode).
```

### Phase 6: Security & SDK

```
15. Authentication
    ├── TLS (rustls)
    ├── API key + HMAC verification
    └── Topic-level permissions

    Test: invalid key → reject. Publish to unauthorized topic → ERR.

16. Rust SDK
    ├── connection.rs     — Auto reconnect
    ├── discovery.rs      — Multi-node seed list, failover
    ├── publish.rs        — Retry with same msg_id
    ├── subscribe.rs      — Handler dispatch + dedup
    └── mock.rs           — MockPulse

    Test: full integration with broker. Reconnect to different node on failure.
```

### Phase 7: Performance

```
17. io_uring (Linux)
    ├── tokio-uring for WAL writes
    ├── Feature-gated, automatic fallback on non-Linux

18. Zero-copy forwarding
    ├── Forward raw frame bytes between nodes without deserialization

19. Per-core sharding
    ├── Pin listener threads to cores
    ├── Connection affinity to reduce cross-core traffic

20. SIMD CRC32C
    ├── Hardware-accelerated CRC on x86_64 (SSE4.2) and aarch64

21. Benchmarks vs NSQ
    ├── nsq_comparison.rs  — Automated head-to-head
    ├── Throughput, latency, memory, recovery time

    Target: beat NSQ on throughput (memory mode), offer durability NSQ lacks.
```

### Phase 8: Polish

```
22. Namespace isolation (cluster-wide)
23. Content-based filtering (SUB filter on structured payloads)
24. Transform pipeline
25. Backpressure (FLOW frame)
26. WAL compaction
27. Metrics + Prometheus exporter
28. Admin CLI (cluster topology, node health)
29. Hot config reload
30. FFI layer (C ABI)
31. Python SDK (PyO3)
32. TypeScript SDK (napi-rs)
33. Go SDK (CGo)
34. Docker packaging (single node + 3-node cluster)
35. Documentation
```

## 5. Build & Test Commands

```bash
# Build everything
cargo build --workspace

# Build broker only (release)
cargo build --release -p pulse-broker

# Run all tests
cargo test --workspace

# Run specific crate tests
cargo test -p pulse-protocol
cargo test -p pulse-broker
cargo test -p pulse-cluster

# Run integration tests (requires broker)
cargo test --test '*' -- --test-threads=1

# Benchmarks
cargo bench -p pulse-broker

# Benchmark vs NSQ (requires NSQ installed locally)
# Runs identical workload against Pulse and NSQ, reports comparison
cargo bench -p pulse-broker -- nsq_comparison
# Or run the standalone comparison script:
cargo run --release --bin bench-vs-nsq

# Clippy (CI enforced)
cargo clippy --workspace -- -D warnings

# Format check
cargo fmt --all -- --check

# Build Docker image
docker build -f docker/Dockerfile -t pulse-broker:latest .

# Run 3-node cluster locally
docker compose -f docker/docker-compose.yml up
```

## 6. Docker

### Dockerfile (multi-stage)

```dockerfile
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
EXPOSE 4222 9090 8080
VOLUME /var/lib/pulse
CMD ["pulse-broker"]
```

### docker-compose.yml (3-node mesh cluster)

```yaml
version: "3.8"
services:
  node-1:
    build:
      context: .
      dockerfile: docker/Dockerfile
    hostname: node-1
    ports:
      - "4222:4222"   # Pulse protocol
      - "9090:9090"   # Metrics
      - "8080:8080"   # Health + Admin API
    volumes:
      - node1-data:/var/lib/pulse
    command:
      - pulse-broker
      - --node-id=node-1
      - --bind=0.0.0.0:4222
      - --seeds=node-2:4222,node-3:4222
      - --durability=balanced

  node-2:
    build:
      context: .
      dockerfile: docker/Dockerfile
    hostname: node-2
    ports:
      - "4223:4222"
      - "9091:9090"
      - "8081:8080"
    volumes:
      - node2-data:/var/lib/pulse
    command:
      - pulse-broker
      - --node-id=node-2
      - --bind=0.0.0.0:4222
      - --seeds=node-1:4222,node-3:4222
      - --durability=balanced

  node-3:
    build:
      context: .
      dockerfile: docker/Dockerfile
    hostname: node-3
    ports:
      - "4224:4222"
      - "9092:9090"
      - "8082:8080"
    volumes:
      - node3-data:/var/lib/pulse
    command:
      - pulse-broker
      - --node-id=node-3
      - --bind=0.0.0.0:4222
      - --seeds=node-1:4222,node-2:4222
      - --durability=balanced

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9093:9090"
    volumes:
      - ./config/prometheus.yml:/etc/prometheus/prometheus.yml

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    volumes:
      - ./config/grafana/:/etc/grafana/provisioning/

volumes:
  node1-data:
  node2-data:
  node3-data:
```
