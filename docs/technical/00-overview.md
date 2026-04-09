# Pulse — Architecture Overview

## 1. What is Pulse?

Pulse is a lightweight, high-performance event broker written in Rust. It enables services within an organization to communicate through events — reliably, quickly, and with minimal integration effort.

**Design philosophy:** Simple by default, powerful when needed. Beat NSQ in every dimension.

## 2. Core Guarantees

Pulse offers **tiered durability** — guarantees depend on the chosen mode per topic or namespace.

| Guarantee | Memory Mode | Balanced Mode | Durable Mode |
|-----------|-------------|---------------|--------------|
| **Delivery** | At-most-once | At-least-once | Exactly-once (dedup at broker + consumer) |
| **Durability** | None (in-memory only) | Async WAL (group fsync every 5ms) | Sync WAL (fsync every write) |
| **Ordering** | Per-topic, per-publisher | Per-topic, per-publisher | Per-topic, per-publisher |
| **Throughput** | ~800K msg/sec | ~100K msg/sec | ~10K msg/sec |
| **P99 Latency** | ~5 μs | ~500 μs | ~2 ms |
| **Crash recovery** | Data lost on crash | ≤5ms of data loss | Zero data loss |

All modes share the same binary protocol, connection management, and routing engine. Mode is selectable per topic or per namespace, and can be mixed within a single cluster.

**Universal guarantees (all modes):**

| Guarantee | Implementation |
|-----------|---------------|
| **Ordering** | Per-topic ordering within a single publisher |
| **Low latency** | In-memory hot path, persistent TCP connections, binary protocol |
| **Backpressure** | FLOW frames for per-connection flow control |
| **Data format agnostic** | Payload default is opaque bytes; optional structured encoding (MsgPack/JSON) for content filtering |

## 3. High-Level Architecture

### Mesh Topology

Pulse nodes form a self-organizing mesh cluster using a gossip protocol (SWIM). No separate discovery service is required (unlike NSQ's nsqlookupd).

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  Service A  │  │  Service B  │  │  Service C  │  │  Service D  │
│ (publisher) │  │ (subscriber)│  │  (both)     │  │ (subscriber)│
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │                │
       │         Pulse Protocol (TCP + TLS)               │
       │              Binary frames                       │
       ▼                ▼                ▼                ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│  Pulse Node  │ │  Pulse Node  │ │  Pulse Node  │
│    (node-1)  │ │    (node-2)  │ │    (node-3)  │
│              │ │              │ │              │
│  Topics:     │ │  Topics:     │ │  Topics:     │
│  order.*     │ │  payment.*   │ │  user.*      │
│  cart.*      │ │  invoice.*   │ │  audit.*     │
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘
       │                │                │
       └────── gossip ──┼── gossip ──────┘
               (SWIM)   │   (SWIM)
                        │
              consistent hashing
              for topic ownership
```

Clients connect to any node. If a message arrives at a node that does not own the target topic, it is forwarded internally to the owning node (zero-copy frame forwarding).

### Per-Node Internal Pipeline

Each node in the mesh runs the same internal pipeline:

```
┌─────────────────────────────────────────────────────┐
│                  PULSE NODE                          │
│                                                      │
│  ┌────────────────────────────────────────────────┐  │
│  │            Connection Manager                  │  │
│  │  - TLS termination (rustls)                    │  │
│  │  - Session management                          │  │
│  │  - Authentication (API Key + HMAC)             │  │
│  │  - Backpressure / flow control                 │  │
│  └───────────────────┬────────────────────────────┘  │
│                      │                               │
│  ┌───────────────────▼────────────────────────────┐  │
│  │             Message Pipeline                    │  │
│  │                                                 │  │
│  │  Ingest → Dedup → WAL Write → Route →          │  │
│  │  Filter → Transform → Fan-out → Deliver        │  │
│  │                                                 │  │
│  │  (WAL step skipped in memory mode)              │  │
│  └───────────────────┬────────────────────────────┘  │
│                      │                               │
│  ┌───────────────────▼────────────────────────────┐  │
│  │            Delivery Manager                     │  │
│  │  - Per-consumer queues                          │  │
│  │  - Retry scheduler (exponential backoff)        │  │
│  │  - ACK tracker                                  │  │
│  │  - Dead Letter Queue (DLQ)                      │  │
│  └────────────────────────────────────────────────┘  │
│                                                      │
│  ┌───────────┐ ┌───────────┐ ┌───────────┐          │
│  │ WAL Disk  │ │ State DB  │ │  Metrics  │          │
│  │ (append)  │ │ (sled)    │ │ (tracing) │          │
│  └───────────┘ └───────────┘ └───────────┘          │
│                                                      │
│  ┌────────────────────────────────────────────────┐  │
│  │            Cluster Module                       │  │
│  │  - Gossip (SWIM protocol)                       │  │
│  │  - Consistent hash ring (topic ownership)       │  │
│  │  - WAL replication (none/async/sync)            │  │
│  │  - Peer-to-peer frame forwarding                │  │
│  └────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

## 4. Technology Stack

| Component | Crate | Purpose |
|-----------|-------|---------|
| Async runtime | `tokio` | All async I/O, timers, task spawning |
| io_uring (Linux) | `tokio-uring` | Kernel-bypassed disk I/O for WAL (Linux only, optional) |
| TLS | `rustls` | Pure-Rust TLS (no OpenSSL dependency) |
| Embedded DB | `sled` | Event state, dedup index, consumer offsets |
| Serialization | `serde` + `rmp-serde` | MessagePack codec for payloads |
| Concurrent maps | `dashmap` | Lock-free routing table, subscription registry |
| Integrity | `crc32c` | Frame-level CRC32C (Castagnoli) checksum, SIMD-accelerated |
| Message IDs | `uuid` (v7) | Time-sortable unique identifiers |
| Gossip | Custom SWIM impl | Failure detection, membership, cluster state dissemination |
| Consistent hashing | `hashring` | Topic-to-node ownership mapping |
| Observability | `tracing` + `tracing-subscriber` | Structured logging and spans |
| Metrics | `metrics` + `metrics-exporter-prometheus` | Prometheus-compatible metrics |

## 5. Namespace Isolation

```
pulse://service:key@node:4222/namespace
                               ─────────
                               isolation boundary

Namespace "ecommerce"          Namespace "internal-tools"
├── topic: order.*             ├── topic: ticket.*
├── topic: payment.*           ├── topic: deploy.*
├── services: order-svc,       ├── services: jira-bot,
│             payment-svc      │             deploy-svc
└── independent routing rules  └── independent routing rules
```

Namespaces are fully isolated: topics, services, routing rules, quotas. A service in namespace A cannot see or interact with namespace B.

**Mesh behavior:** Namespaces work across the entire mesh. A namespace's topics may be distributed across multiple nodes via consistent hashing, but isolation guarantees hold cluster-wide.

## 6. Capacity Planning

### Single Node

| Metric | Memory Mode | Balanced Mode | Durable Mode |
|--------|-------------|---------------|--------------|
| Throughput | ~800K msg/sec | ~100K msg/sec | ~10K msg/sec |
| P50 latency | ~3 μs | ~200 μs | ~1 ms |
| P99 latency | ~5 μs | ~500 μs | ~2 ms |
| Concurrent connections | ~50,000 | ~50,000 | ~10,000 |
| Memory (10K pending msgs) | ~200 MB | ~200 MB | ~200 MB |
| WAL write speed | N/A | ~200 MB/s (group fsync) | ~50 MB/s (per-write fsync) |

### 3-Node Mesh Cluster

| Metric | Memory Mode | Balanced Mode | Durable Mode |
|--------|-------------|---------------|--------------|
| Aggregate throughput | ~2.2M msg/sec | ~280K msg/sec | ~28K msg/sec |
| Concurrent connections | ~150,000 | ~150,000 | ~30,000 |
| Replication overhead | None | ~15% (async) | ~30% (sync) |
| Failover time | ~2s (gossip detection) | ~2s + WAL replay | ~2s + WAL replay |

Performance targets assume modern hardware (8+ cores, NVMe SSD, 10GbE). Memory mode numbers are competitive with NSQ; balanced and durable modes trade throughput for stronger guarantees that NSQ does not offer.

## 7. Deployment Modes

| Mode | Use Case | Config Required | Nodes |
|------|----------|-----------------|-------|
| **Single node (zero-config)** | Development, testing, small workloads | None — run `pulse-broker` with no args | 1 |
| **Mesh cluster** | Production, high availability | Seed node list (CLI flags or YAML) | 2+ |
| **Embedded (library)** | Edge/IoT, in-process messaging | Programmatic via `pulse-broker` as library crate | 1 (in-process) |

**Zero-config mode:** Running `pulse-broker` with no arguments starts a single node with memory mode, default port 4222, and no TLS. Every configuration option has a CLI flag equivalent — YAML files are never required.

**Mesh cluster:** Provide one or more seed nodes via `--seeds node1:4222,node2:4222` or `broker.yaml`. Nodes discover the full cluster topology automatically via gossip. New nodes join by contacting any existing member.

**Embedded mode:** Import `pulse-broker` as a library crate for in-process event routing. Useful for edge deployments, IoT gateways, or integration tests that need a real broker without a separate process.

## 8. Known Limitations (v1)

| Limitation | Details | Mitigation |
|-----------|---------|------------|
| **No schema registry** | No built-in payload schema validation or evolution tracking. | Recommend versioned payloads (`{ "v": 2, ... }`). See SDK docs. |
| **Consumer group ordering** | Round-robin delivery within consumer groups breaks per-publisher ordering guarantee. | Use partition key to pin a publisher's messages to a single consumer within the group. Single-consumer subscriptions also preserve ordering. |
| **No multi-region** | Mesh clustering operates within a single network region. Cross-region adds unacceptable gossip latency. | Cross-region replication planned for v2. Deploy independent clusters per region with application-level bridging if needed. |
| **io_uring Linux only** | `tokio-uring` is Linux-only. macOS/Windows fall back to standard `tokio` file I/O. | Fallback is automatic and transparent. Production deployments on Linux get the performance benefit. |

**What v1 does support:** Mesh clustering with automatic discovery (no separate coordinator), tiered durability, zero-copy frame forwarding, and per-core sharding.

## 9. Document Index

| Document | Contents |
|----------|----------|
| `01-protocol.md` | Wire protocol specification — frame format, message types, handshake |
| `02-broker.md` | Broker internals — module architecture, internal channels, concurrency model |
| `03-data-flow.md` | Event lifecycle — publish to ACK, failure handling, exactly-once mechanics |
| `04-wal-storage.md` | WAL design, segment management, compaction, crash recovery |
| `05-routing.md` | Routing pipeline — topic matching, content filters, transforms, DLQ |
| `06-sdk.md` | SDK architecture — public API, internals, multi-language strategy |
| `07-project-structure.md` | Rust workspace layout, crate boundaries, build & test |
| `08-operations.md` | Deployment, configuration, monitoring, troubleshooting |
