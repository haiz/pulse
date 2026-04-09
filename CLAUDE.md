# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
# Build
cargo build --workspace
cargo build --release -p pulse-broker

# Test
cargo test --workspace
cargo test -p pulse-protocol
cargo test -p pulse-broker
cargo test -p pulse-protocol -- frame::tests::test_name   # single test

# Lint & Format
cargo clippy --workspace -- -D warnings
cargo fmt --all -- --check
cargo fmt --all                                            # auto-fix

# Benchmarks
cargo bench -p pulse-broker
```

## Architecture

Pulse is a **high-performance event broker** for reliable, low-latency inter-service communication — a lightweight alternative to RabbitMQ/Kafka, written in Rust.

### Workspace Crates

- **`pulse-protocol`** — Wire protocol library (zero internal deps). Shared by broker, cluster, and SDK. Contains frame encoding/decoding, all 10 message types, CRC32C integrity, and tokio codec. Fully implemented with 85+ tests.
- **`pulse-cluster`** — Gossip (SWIM), consistent hashing, WAL replication, topology management. Depends only on pulse-protocol.
- **`pulse-broker`** — Broker server binary. Currently a skeleton (CLI + tracing setup). Depends on pulse-protocol and pulse-cluster. Planned modules: server, pipeline, routing, delivery, storage, auth, namespace, metrics, config.

### Wire Protocol

Custom binary protocol over TCP+TLS (default port 4222):
- 29-byte fixed header + variable MessagePack payload (optional LZ4 compression)
- 10 message types: CONNECT, CONNACK, PUB, ACK, SUB, UNSUB, PING, PONG, FLOW, ERR
- UUIDv7 message IDs (time-sortable, 16 bytes)
- CRC32C on every frame

### Core Design Guarantees

- **Tiered durability** — 3 modes: `memory` (~800K msg/sec, no persistence), `balanced` (async WAL, group fsync every 5ms, ~100K msg/sec), `durable` (fsync every write, exactly-once, ~10K msg/sec)
- **Distributed mesh (Pulse Mesh)** — gossip (SWIM) discovery, consistent hashing for topic ownership, WAL replication (none/async/sync)
- **Zero-config mode** — `pulse-broker` with no args starts a single node; all YAML optional; every config has CLI flag equivalent
- **Data format agnostic** — payload default is opaque bytes; optional structured encoding (MsgPack/JSON) for content filtering
- **Ordering** — per-topic, per-publisher within namespace
- **Low latency** — in-memory hot path, binary protocol, io_uring on Linux, zero-copy forwarding

### Crate Dependency Graph

```
pulse-protocol  (no internal deps)
       ↑
  ┌────┼────────────┐
  │  pulse-cluster  │
  │    ↑            │
  ┌────┴────────────┐
pulse-sdk       pulse-broker
  ↑
pulse-ffi       pulse-admin (HTTP only)
```

### Key Technology Choices

| Component | Library | Notes |
|-----------|---------|-------|
| Async | tokio (full) | Runtime for all I/O |
| TLS | rustls + tokio-rustls | Pure-Rust, TLS 1.3 |
| Storage | sled | Embedded DB for dedup/state/offsets |
| Framing | tokio-util codec | Streaming encode/decode |
| Serialization | rmp-serde (MessagePack) | Payload encoding |
| Concurrency | dashmap, arc-swap | Lock-free routing, config hot-reload |
| Observability | tracing + metrics | JSON logs, Prometheus export |
| io_uring | tokio-uring (Linux, optional) | Kernel-bypassed disk I/O for WAL |
| Gossip | Custom SWIM impl | Cluster discovery, failure detection |
| Consistent hashing | hashring | Topic-to-node ownership |

### Configuration (planned, not yet implemented)

Three optional YAML files: `broker.yaml` (network/storage/delivery), `services.yaml` (API keys/permissions), `routes.yaml` (topic routing, hot-reloadable). Supports `${VAR_NAME:-default}` env substitution. Every setting has a CLI flag equivalent — zero-config mode works with no files.

### Implementation Status

The protocol crate is complete (Phase 1 done). The broker is at skeleton stage. See `docs/technical/07-project-structure.md` for the 8-phase implementation plan (foundation → core pipeline → tiered durability → routing → clustering → security/SDK → performance → polish).

## Documentation

Extensive design specs in `docs/technical/`:
- `00-overview.md` — Architecture & guarantees
- `01-protocol.md` — Wire protocol spec
- `02-broker.md` — Broker internals & concurrency model
- `03-data-flow.md` — Event lifecycle
- `04-wal-storage.md` — WAL design
- `05-routing.md` — Routing pipeline
- `06-sdk.md` — SDK architecture
- `07-project-structure.md` — Workspace layout & implementation order
- `08-operations.md` — Deployment, monitoring, troubleshooting
