# Architecture

System design reference for contributors and developers evaluating Pulse.

## System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Clients                                   │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────────┐   │
│  │ Rust SDK │ │ Python   │ │ Go/Node  │ │ Any (curl/HTTP)  │   │
│  │ TCP:4222 │ │ TCP:4222 │ │ HTTP:8080│ │ HTTP:8080        │   │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────────┬─────────┘   │
└───────┼────────────┼────────────┼─────────────────┼─────────────┘
        │            │            │                 │
        ▼            ▼            ▼                 ▼
┌─────────────┐  ┌──────────────────────────────────────┐
│ Pulse Broker│  │         HTTP/WS Gateway               │
│ TCP :4222   │◄─┤  REST /v1/publish    WS /v1/subscribe │
│             │  │  Translates JSON ↔ binary protocol    │
└──────┬──────┘  └──────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────────────┐
│                  Broker Pipeline                      │
│                                                       │
│  ┌─────────┐  ┌───────┐  ┌─────┐  ┌───────┐        │
│  │ Receive │→ │ Dedup │→ │ WAL │→ │ Route │→ Deliver│
│  │ + CRC   │  │ Bloom │  │Write│  │ Trie  │  to     │
│  │ verify  │  │ +Sled │  │+Sync│  │+Filter│  subs   │
│  └─────────┘  └───────┘  └─────┘  └───────┘        │
│                                                       │
│  ┌────────────┐  ┌──────────┐  ┌──────────────────┐ │
│  │ Delivery   │  │ Storage  │  │ Cluster (opt)    │ │
│  │ ACK track  │  │ WAL segs │  │ Gossip (SWIM)    │ │
│  │ Retry+DLQ  │  │ StateDB  │  │ Hash ring        │ │
│  │ Con.Groups │  │ Compact  │  │ WAL replication  │ │
│  └────────────┘  └──────────┘  └──────────────────┘ │
└──────────────────────────────────────────────────────┘
```

## Crate Dependency Graph

```
pulse-protocol        (zero deps — shared wire format)
       ↑
  ┌────┼──────────────┐
  │    │               │
pulse-cluster    pulse-sdk
  │                │
  ↑                ↑
pulse-broker    pulse-ffi
  ↑                
pulse-gateway   pulse-admin   pulse-demo
```

| Crate | LOC | Purpose |
|-------|-----|---------|
| `pulse-protocol` | ~2,300 | Wire protocol: 10 frame types, CRC32C, tokio codec, UUIDv7 |
| `pulse-broker` | ~7,000 | Server: TCP/TLS listener, pipeline, routing, delivery, WAL, auth, metrics |
| `pulse-cluster` | ~1,000 | Gossip (SWIM), consistent hash ring, topology, WAL replication |
| `pulse-sdk` | ~750 | Rust client: connect, publish, subscribe, auto-reconnect, consumer dedup |
| `pulse-gateway` | ~600 | HTTP/WS gateway: REST publish, WebSocket subscribe, JSON↔binary |
| `pulse-admin` | ~320 | CLI tool: status, pub, sub, ping, config-check |
| `pulse-ffi` | ~230 | C ABI: opaque handles for foreign language bindings |
| `pulse-demo` | ~200 | E2E demo: starts broker + gateway + subscribers in-process |

## Wire Protocol

Custom binary over TCP, port 4222:

```
┌────────┬─────────┬──────┬───────┬───────────┬─────────────┬───────┐
│ Magic  │ Version │ Type │ Flags │ MessageID │ PayloadLen  │  CRC  │
│ 2 bytes│ 1 byte  │1 byte│1 byte │ 16 bytes  │  4 bytes    │4 bytes│
└────────┴─────────┴──────┴───────┴───────────┴─────────────┴───────┘
                                                │
                                     ┌──────────┘
                                     ▼
                              MessagePack payload
                              (variable length)
```

10 message types: CONNECT, CONNACK, PUB, ACK, SUB, UNSUB, PING, PONG, FLOW, ERR.

Message IDs are UUIDv7 (time-sortable, 16 bytes). CRC32C on every frame.

## Pipeline Flow

### Publish Path

```
Connection Handler
  → receive frame, verify CRC
  → decode, extract topic + msg_id
  → send to Dispatcher via mpsc channel

Dispatcher
  → Dedup check (bloom filter → sled if durable mode)
  → Serialize payload to MessagePack
  → WAL append (per-event fsync or group commit)
  → Dedup insert (bloom + sled if durable)
  → Send ACK to publisher via oneshot channel

Router
  → Resolve topic against TopicTrie (wildcard matching)
  → Apply content filters per subscriber
  → Fan-out to matched subscriber channels

Delivery
  → Push frame to subscriber's mpsc channel
  → Track in-flight (ACK tracker)
  → On timeout/NACK: retry with exponential backoff
  → After max retries: move to DLQ
```

### Subscribe Path

```
Connection Handler receives SUB frame
  → Register subscription in shared Router (Arc<Router>)
  → TopicTrie.insert(pattern, SubscriptionTarget)
  → SubscriptionTarget contains mpsc::Sender<Frame>

Events delivered by:
  → Dispatcher routes after WAL write
  → Router.resolve() finds matching targets
  → Frame pushed to target's mpsc channel
  → Connection handler's select loop reads from channel
  → Frame sent to client over TCP
```

## Tiered Durability

| Component | Memory Mode | Balanced Mode | Durable Mode |
|-----------|-------------|---------------|--------------|
| Dedup check | Bloom only (29ns) | Bloom only (29ns) | Bloom + sled (1-8µs) |
| WAL write | Skipped | Group commit (5ms batch) | Per-event fsync |
| Dedup insert | Bloom only | Bloom only | Bloom + sled |
| Delivery guarantee | At-most-once | At-least-once | Exactly-once |

Mode is selectable per-topic via config. Default: balanced.

## Routing Engine

### TopicTrie

Trie-based structure for O(segments) wildcard matching:

```rust
struct TopicTrie {
    children: HashMap<String, TopicTrie>,      // exact segment children
    subscribers: Vec<SubscriptionTarget>,       // leaf subscribers
    single_wildcard: Vec<SubscriptionTarget>,   // "*" at this level
    multi_wildcard: Vec<SubscriptionTarget>,    // ">" at this level
}
```

Performance: <250ns per resolve (4M lookups/sec).

### Content Filters

Compiled once at SUB time, evaluated per-event:

```
FilterExpr::Compare { left: FieldPath, op: CompareOp, right: Value }
FilterExpr::Logic { op: And|Or, left: Box<Expr>, right: Box<Expr> }
FilterExpr::Not(Box<Expr>)
FilterExpr::Function { name: Contains|StartsWith|..., field, args }
```

Performance: 9ns simple, 57ns complex (100M+ evals/sec).

## Concurrency Model

All async, tokio-based. Key concurrent structures:

| Structure | Type | Purpose |
|-----------|------|---------|
| Routing table | `Arc<Router>` (RwLock<TopicTrie>) | Shared between connections + dispatcher |
| Sessions | `DashMap<SessionId, SessionHandle>` | Lock-free concurrent session map |
| Config | `ArcSwap<BrokerConfig>` | Hot-reloadable, zero-copy reads |
| Dispatch channel | `mpsc::channel(4096)` | Connection handlers → dispatcher |
| Delivery channels | `mpsc::channel(256)` per subscriber | Dispatcher → connection handlers |
| WAL | `Mutex<WalWriter>` | Single writer, serialized WAL access |

## Cluster Architecture

```
┌──────────┐  gossip  ┌──────────┐  gossip  ┌──────────┐
│  Node 1  │◄────────►│  Node 2  │◄────────►│  Node 3  │
│          │          │          │          │          │
│ Topics:  │          │ Topics:  │          │ Topics:  │
│ order.*  │          │ payment.*│          │ user.*   │
└──────────┘          └──────────┘          └──────────┘
```

- **Discovery**: SWIM gossip protocol (200ms probe interval, 3 indirect probes)
- **Topic ownership**: Consistent hash ring (128 virtual nodes per physical node)
- **Replication**: None, async, or sync WAL replication per config
- **Forwarding**: PUB to non-owner node → forwarded to owner (zero-copy)

## Key Files

| File | What it does |
|------|--------------|
| `pulse-broker/src/main.rs` | Entry point: config, WAL recovery, start listener |
| `pulse-broker/src/broker.rs` | `BrokerHandle`: shared state (config, router, sessions) |
| `pulse-broker/src/server/connection.rs` | Per-connection bidirectional frame loop |
| `pulse-broker/src/pipeline/dispatcher.rs` | Ingest pipeline: dedup → WAL → route |
| `pulse-broker/src/pipeline/batch.rs` | Batch pipeline: group dedup + WAL + single fsync |
| `pulse-broker/src/routing/engine.rs` | `TopicTrie`: wildcard topic matching |
| `pulse-broker/src/routing/filter.rs` | Content filter DSL: parser + evaluator |
| `pulse-broker/src/delivery/manager.rs` | Delivery: ACK tracking, retry, DLQ |
| `pulse-broker/src/storage/wal.rs` | WAL: segment files, append, recovery |
| `pulse-gateway/src/rest.rs` | HTTP handlers: publish, batch, health |
| `pulse-gateway/src/websocket.rs` | WebSocket handler: sub/unsub/ack |
