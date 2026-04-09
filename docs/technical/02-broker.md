# Pulse Broker — Internal Design

## 1. Module Architecture

```
pulse-broker binary
│
├── main.rs                    — CLI, config loading, signal handling
│
├── server/
│   ├── listener.rs            — TCP + TLS accept loop
│   ├── connection.rs          — Per-connection read/write loop
│   └── session.rs             — Authenticated session state
│
├── pipeline/
│   ├── ingest.rs              — Receive PUB, validate, assign to pipeline
│   ├── dedup.rs               — Bloom filter + sled dedup engine
│   ├── wal.rs                 — Write-Ahead Log manager
│   └── dispatcher.rs          — Coordinate ingest → dedup → wal → route
│
├── routing/
│   ├── engine.rs              — Topic matching + content filter evaluation
│   ├── filter.rs              — Expression parser and evaluator
│   ├── transform.rs           — Payload transform operations
│   └── config.rs              — Route config hot-reload
│
├── delivery/
│   ├── manager.rs             — Per-consumer queue management
│   ├── queue.rs               — In-memory queue with disk overflow
│   ├── retry.rs               — Retry scheduler (exponential backoff)
│   ├── ack_tracker.rs         — Track in-flight deliveries, detect timeouts
│   └── dlq.rs                 — Dead Letter Queue handler
│
├── storage/
│   ├── wal_segment.rs         — WAL segment file read/write
│   ├── compaction.rs          — Segment compaction and cleanup
│   ├── state_db.rs            — sled wrapper for event state, subscriptions
│   └── recovery.rs            — Crash recovery: WAL replay logic
│
├── auth/
│   ├── authenticator.rs       — API Key + HMAC verification
│   ├── permissions.rs         — Topic-level ACL checks
│   └── config.rs              — services.yaml parser, hot-reload
│
├── namespace/
│   ├── registry.rs            — Namespace lifecycle management
│   └── isolation.rs           — Topic/service scoping per namespace
│
├── protocol/
│   ├── frame.rs               — Frame encode/decode, CRC verify
│   ├── codec.rs               — tokio codec for framing TCP stream
│   └── types.rs               — Message type enums and payload structs
│
├── cluster/
│   ├── gossip.rs              — SWIM protocol, failure detection
│   ├── topology.rs            — Consistent hashing, topic ownership
│   ├── replication.rs         — WAL replication (leader → follower)
│   ├── peer.rs                — Peer connection management
│   └── discovery.rs           — Built-in peer discovery
│
├── metrics/
│   ├── counters.rs            — Event counters, error rates
│   ├── histograms.rs          — Latency distributions
│   └── exporter.rs            — Prometheus HTTP endpoint
│
└── config/
    ├── broker.rs              — Broker-level config (ports, limits, paths)
    ├── routes.rs              — Route rules config
    └── loader.rs              — YAML/TOML loader with hot-reload
```

## 2. Concurrency Model

The broker uses `tokio` for all async operations. No thread-per-connection. Key concurrency primitives:

### Task Layout

```
┌─────────────────────────────────────────────────────────┐
│                    tokio Runtime                         │
│                                                         │
│  Task: TLS Listener                                     │
│    └─ accept() loop → spawn Connection task per client  │
│                                                         │
│  Task: Connection(service_a)                             │
│    └─ read frames → send to dispatch_tx channel         │
│    └─ read from delivery_rx channel → write frames      │
│                                                         │
│  Task: Connection(service_b)                             │
│    └─ (same pattern)                                    │
│                                                         │
│  Task: Namespace Dispatcher                              │
│    └─ recv from dispatch_tx → route to per-ns pipeline  │
│    └─ lazy-spawn Pipeline task per namespace             │
│                                                         │
│  Task: Pipeline(ns=ecommerce)                            │
│    └─ recv from ns_pipeline_rx → dedup → WAL → route    │
│    └─ fan-out to per-consumer delivery_tx channels      │
│                                                         │
│  Task: Pipeline(ns=internal-tools)                       │
│    └─ (same pattern, independent ordering domain)       │
│                                                         │
│  Task: Delivery(consumer_1)                              │
│    └─ recv from delivery_tx → send frame → track ACK    │
│    └─ timeout check → retry                             │
│                                                         │
│  Task: WAL Compactor                                     │
│    └─ periodic: compact completed segments              │
│                                                         │
│  Task: Config Watcher                                    │
│    └─ inotify on config files → reload routes/auth      │
│                                                         │
│  Task: Metrics Server                                    │
│    └─ HTTP /metrics endpoint for Prometheus              │
│                                                         │
│  Task: Health Check                                      │
│    └─ HTTP /health and /ready endpoints                 │
│                                                         │
│  Task: Gossip Protocol                                   │
│    └─ periodic SWIM probes (every 200ms)                │
│    └─ failure detection: suspect → dead transitions     │
│    └─ disseminate membership changes to peers           │
│                                                         │
│  Task: Replication Writer                                │
│    └─ stream WAL entries to follower nodes              │
│    └─ one sub-task per follower per owned topic         │
│    └─ track per-follower replication watermark          │
│                                                         │
│  Task: Replication Reader                                │
│    └─ receive WAL entries from leader via PEER_SYNC     │
│    └─ write to local WAL (follower replica)             │
│    └─ send PEER_ACK back to leader                      │
│                                                         │
│  Task: Topology Manager                                  │
│    └─ maintain consistent hash ring                     │
│    └─ handle ring changes on PEER_JOIN / PEER_LEAVE     │
│    └─ coordinate topic ownership rebalancing            │
│    └─ notify Pipeline tasks of ownership transfers      │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### Internal Channels

All inter-task communication uses `tokio::mpsc` bounded channels. Sizes are configurable.

```
Connection task(s) ──mpsc(1024)──> Namespace Dispatcher
                                         │
                          ┌───────────────┼───────────────┐
                          ▼               ▼               ▼
                   mpsc(1024)      mpsc(1024)      mpsc(1024)
                          │               │               │
                  Pipeline(ns_a)  Pipeline(ns_b)  Pipeline(ns_n)
                          │               │               │
                          ├─mpsc(4096)─>Delivery    ├─...─>Delivery
                          ├─mpsc(4096)─>Delivery    └─...─>Delivery
                          └─mpsc(4096)─>Delivery

Delivery task ──mpsc(1024)──> Connection task (write path)
```

**Backpressure**: when a channel is full, the sender `.send().await` blocks. This propagates backpressure naturally:
- If consumer is slow → delivery channel fills → pipeline blocks on fan-out → ingest slows → TCP read slows → publisher sees write backpressure

### Shared State (concurrent access)

| Data | Structure | Access Pattern |
|------|-----------|----------------|
| Routing table | `DashMap<TopicPattern, Vec<ConsumerRef>>` | Read-heavy, write on SUB/UNSUB |
| Active sessions | `DashMap<ServiceId, SessionHandle>` | Read on route, write on connect/disconnect |
| Subscription registry | `DashMap<SubId, Subscription>` | Read on deliver, write on SUB/UNSUB |
| Dedup bloom filter | `RwLock<BloomFilter>` | Read on every PUB, rebuild periodically |
| Config (routes, auth) | `ArcSwap<Config>` | Read on every operation, write on reload |
| Cluster topology | `ArcSwap<HashRing>` | Read on route/proxy, write on ring change |
| Peer connections | `DashMap<NodeId, PeerHandle>` | Read on replicate, write on join/leave |

## 3. Connection Lifecycle (Detailed)

```rust
// Pseudocode for connection handling
async fn handle_connection(tls_stream: TlsStream, broker: BrokerHandle) {
    let codec = PulseCodec::new();
    let (mut reader, mut writer) = codec.framed(tls_stream).split();

    // Step 1: Await CONNECT frame (5s timeout)
    let connect_frame = timeout(5.sec(), reader.next()).await??;
    let session = broker.authenticate(connect_frame)?;

    // Step 2: Send CONNACK
    writer.send(Frame::connack(&session)).await?;

    // Step 3: Create delivery channel for this session
    let (deliver_tx, mut deliver_rx) = mpsc::channel(4096);
    broker.register_session(session.id(), deliver_tx);

    // Step 4: Bidirectional loop
    loop {
        tokio::select! {
            // Inbound: frames from client
            frame = reader.next() => {
                match frame??.msg_type() {
                    Type::PUB => broker.dispatch_tx.send(Ingest { frame, session }).await?,
                    Type::ACK => broker.delivery.ack(frame.msg_id(), session.id()),
                    Type::SUB => broker.subscribe(frame, &session).await?,
                    Type::UNSUB => broker.unsubscribe(frame, &session).await?,
                    Type::PING => writer.send(Frame::pong(frame.msg_id())).await?,
                    Type::FLOW => broker.delivery.flow_control(frame, &session),
                    _ => writer.send(Frame::err(4000, "unexpected type")).await?,
                }
            }
            // Outbound: events to deliver to this client
            event = deliver_rx.recv() => {
                writer.send(event?.to_frame()).await?;
            }
            // Keepalive: send PING if idle
            _ = tokio::time::sleep(keepalive_interval) => {
                writer.send(Frame::ping()).await?;
            }
        }
    }

    // Cleanup on disconnect
    broker.unregister_session(session.id());
}
```

## 4. Pipeline Processor (Core Loop)

All ingest messages flow through a central `NamespaceDispatcher` which fans out to per-namespace pipeline tasks. Each namespace gets its own serial `pipeline_loop`, guaranteeing event ordering within a namespace while allowing namespaces to process concurrently.

### 4.1 Namespace Dispatcher

```rust
// Pseudocode
struct NamespaceDispatcher {
    pipelines: HashMap<Namespace, mpsc::Sender<IngestMessage>>,
    dedup: DedupEngine,
    wal: WalWriter,
    router: Router,
    delivery: DeliveryManager,
    durability: DurabilityMode,
}

async fn dispatcher_loop(
    mut rx: mpsc::Receiver<IngestMessage>,
    state: Arc<Mutex<NamespaceDispatcher>>,
) {
    while let Some(msg) = rx.recv().await {
        let ns = msg.session.namespace().to_owned();

        let ns_tx = {
            let mut dispatcher = state.lock().await;

            if let Some(tx) = dispatcher.pipelines.get(&ns) {
                tx.clone()
            } else {
                // First message for this namespace — spawn a dedicated pipeline task
                let (ns_tx, ns_rx) = mpsc::channel::<IngestMessage>(1024);
                tokio::spawn(pipeline_loop(
                    ns_rx,
                    dispatcher.dedup.clone(),
                    dispatcher.wal.clone(),
                    dispatcher.router.clone(),
                    dispatcher.delivery.clone(),
                    dispatcher.durability,
                ));
                dispatcher.pipelines.insert(ns.clone(), ns_tx.clone());
                ns_tx
            }
        };

        if let Err(e) = ns_tx.send(msg).await {
            metrics::counter!("dispatcher_send_errors", "namespace" => ns).increment(1);
        }
    }
}
```

Connection tasks send to the single `dispatch_tx` channel. The dispatcher looks up (or lazily creates) the per-namespace pipeline task and forwards the message.

### 4.2 Per-Namespace Pipeline Loop

Each namespace runs its own `pipeline_loop`. The persistence path branches on the broker's configured durability mode.

```rust
// Pseudocode — spawned once per namespace by the dispatcher
async fn pipeline_loop(
    mut rx: mpsc::Receiver<IngestMessage>,
    dedup: DedupEngine,
    wal: WalWriter,
    router: Router,
    delivery: DeliveryManager,
    durability: DurabilityMode,
) {
    while let Some(msg) = rx.recv().await {
        let frame = &msg.frame;
        let session = &msg.session;
        let msg_id = frame.msg_id();

        // 1. Permission check
        if !session.can_publish(frame.topic()) {
            msg.reply(Frame::err(4030, "forbidden")).await;
            continue;
        }

        // 2. Dedup
        match dedup.check(msg_id).await {
            DedupResult::New => { /* continue */ }
            DedupResult::Duplicate => {
                msg.reply(Frame::ack(msg_id, "duplicate")).await;
                continue;
            }
        }

        // 3. Persist based on durability mode
        match durability {
            DurabilityMode::Memory => {
                // Skip WAL entirely — insert directly to memory buffer
                // Lowest latency, no disk I/O, data lost on crash
            }

            DurabilityMode::Balanced => {
                // WAL write with group commit — batch fsync every 5ms
                // Amortizes fsync cost across multiple events
                if let Err(e) = wal.append_buffered(frame).await {
                    metrics::counter!("wal_write_errors").increment(1);
                    msg.reply(Frame::err(5000, "wal write failed")).await;
                    continue;
                }
                // Note: fsync happens asynchronously in the WAL group commit task.
                // In the worst case, up to 5ms of events may be lost on crash.
            }

            DurabilityMode::Durable => {
                // WAL write with per-event fsync — strongest guarantee
                if let Err(e) = wal.append(frame).await {
                    metrics::counter!("wal_write_errors").increment(1);
                    msg.reply(Frame::err(5000, "wal write failed")).await;
                    continue;
                }
            }
        }

        // 4. Register in dedup (after successful persist or memory insert)
        dedup.insert(msg_id).await;

        // 5. Insert into memory buffer
        // (used for late subscribers with position: "earliest" within buffer window)

        // 6. ACK to publisher — event is now stored per durability guarantee
        msg.reply(Frame::ack(msg_id, "stored")).await;

        // 7. Route
        let targets = router.resolve(frame.topic(), frame.payload());

        // 8. Fan-out to delivery queues
        for target in targets {
            let event = DeliveryEvent {
                msg_id,
                topic: frame.topic().to_owned(),
                payload: frame.payload().clone(),
                headers: frame.headers().clone(),
                attempt: 1,
            };
            if let Err(_) = delivery.enqueue(target, event).await {
                metrics::counter!("delivery_enqueue_overflow").increment(1);
                // Overflow to disk — see delivery/queue.rs
            }
        }
    }
}
```

**Durability modes summary:**

| Mode | WAL | fsync | Latency | Data safety |
|------|-----|-------|---------|-------------|
| `memory` | None | N/A | ~10us | Lost on crash |
| `balanced` | Group commit | Every 5ms | ~50us | Up to 5ms lost on crash |
| `durable` | Per-event | Every write | ~200us | Fully durable |

**Ordering guarantee**: events from the same publisher to the same topic are processed in order because:
1. TCP guarantees ordered delivery from a single connection
2. The dispatcher preserves send order when forwarding to a namespace channel
3. Each namespace's `pipeline_loop` processes messages sequentially
4. Per-consumer delivery queue preserves insertion order

**Namespace isolation**: a slow namespace (e.g. heavy WAL writes) cannot block processing in other namespaces, since each runs in its own tokio task.

## 5. Session State

```rust
pub struct Session {
    pub id: SessionId,             // unique per connection
    pub service_id: String,        // "order-service"
    pub namespace: String,         // "ecommerce"
    pub connected_at: Instant,
    pub permissions: Permissions,  // publish/subscribe ACLs
    pub subscriptions: Vec<SubId>, // active subscriptions
    pub codec: Codec,              // msgpack or json
    pub max_inflight: u32,         // flow control limit
    pub deliver_tx: mpsc::Sender<DeliveryEvent>, // channel to this connection's write loop
}

pub struct Permissions {
    pub publish_topics: Vec<TopicPattern>,   // ["order.*"]
    pub subscribe_topics: Vec<TopicPattern>, // ["payment.*", "inventory.*"]
}
```

## 6. Graceful Shutdown

```
SIGTERM received
  │
  ▼
1. Notify peers via PEER_LEAVE (reason: "shutdown")
  │
  ▼
2. Transfer topic ownership to replicas
   └─ Topology Manager selects most up-to-date follower per topic
   └─ Waits for replication to catch up (bounded by 10s timeout)
  │
  ▼
3. Stop accepting new connections
  │
  ▼
4. Send ERR(5030, "shutting down") to all connected clients
  │
  ▼
5. Wait up to 30s for in-flight ACKs
  │
  ▼
6. Flush WAL (fsync)
  │
  ▼
7. Close sled DB
  │
  ▼
8. Exit 0
```

In-flight events that haven't been ACKed will be re-delivered on next startup (WAL recovery). In a cluster, followers that received replicated WAL entries can serve these events immediately after ownership transfer completes.

## 7. Configuration

### 7.1 Zero-Config Defaults

Pulse is designed to run with zero configuration. A bare `pulse-broker` invocation starts a fully functional single-node broker:

```bash
# Start with all defaults — no config files required
pulse-broker

# Equivalent to:
#   listen_addr:    0.0.0.0:4222
#   data_dir:       ./pulse-data
#   durability:     balanced
#   max_payload:    1 MB
#   cluster:        disabled (single-node)
```

Every setting has a sensible default. Config files and CLI flags override defaults when needed.

### 7.2 CLI Flags

Every configuration option has a corresponding CLI flag. Flags take precedence over config files, which take precedence over defaults.

| Flag | Config equivalent | Default | Description |
|------|-------------------|---------|-------------|
| `--listen-addr` | `listen_addr` | `0.0.0.0:4222` | Client listen address |
| `--data-dir` | `data_dir` | `./pulse-data` | Data directory for WAL and state |
| `--durability` | `durability.mode` | `balanced` | Durability mode: `memory`, `balanced`, `durable` |
| `--tls-cert` | `tls.cert_path` | None (TLS disabled in dev) | TLS certificate path |
| `--tls-key` | `tls.key_path` | None | TLS private key path |
| `--max-payload` | `max_payload_bytes` | `1048576` (1 MB) | Max payload size in bytes |
| `--max-connections` | `max_connections` | `5000` | Max concurrent connections |
| `--cluster-listen` | `cluster.listen_addr` | `0.0.0.0:4223` | Inter-node listen address |
| `--cluster-seeds` | `cluster.seeds` | `[]` (no cluster) | Seed node addresses |
| `--cluster-node-id` | `cluster.node_id` | Auto-generated | Unique node identifier |
| `--config` | N/A | None | Path to `broker.yaml` |
| `--services-config` | N/A | None | Path to `services.yaml` |
| `--routes-config` | N/A | None | Path to `routes.yaml` |
| `--log-level` | `log_level` | `info` | Log verbosity: `trace`, `debug`, `info`, `warn`, `error` |
| `--log-format` | `log_format` | `json` | Log format: `json`, `pretty` |

### 7.3 broker.yaml

```yaml
# Network
listen_addr: "0.0.0.0:4222"
tls:
  cert_path: "/etc/pulse/cert.pem"
  key_path: "/etc/pulse/key.pem"

# Limits
max_connections: 5000
max_payload_bytes: 1048576           # 1 MB
max_pending_per_consumer: 10000

# Keepalive
keepalive_interval_secs: 10
keepalive_timeout_secs: 30
connect_timeout_secs: 5

# Durability
durability:
  mode: "balanced"                   # "memory" | "balanced" | "durable"
  group_commit_interval_ms: 5        # only applies to "balanced" mode

# Storage
data_dir: "/var/lib/pulse"
wal:
  segment_size_bytes: 67108864       # 64 MB
  sync_mode: "fsync"                 # "fsync" | "fdatasync" | "none" (testing only)
  retention_hours: 168               # 7 days

# Delivery
delivery:
  ack_timeout_secs: 30
  max_redeliveries: 5
  backoff:
    initial_secs: 1
    max_secs: 60
    multiplier: 2.0

# Compaction
compaction:
  interval_secs: 3600                # run every hour
  min_completed_ratio: 0.8           # compact when 80% events done

# Cluster
cluster:
  enabled: true
  node_id: "node-1"                  # unique identifier; auto-generated if omitted
  listen_addr: "0.0.0.0:4223"       # inter-node listen address
  seeds: ["10.0.1.2:4223", "10.0.1.3:4223"]  # seed nodes for initial discovery
  replication: "async"               # "async" (default, ~1ms lag) | "sync" (majority quorum)
  gossip_interval_ms: 200            # SWIM protocol period
  failure_timeout_ms: 5000           # time before suspect → dead

# Metrics
metrics:
  enabled: true
  listen_addr: "0.0.0.0:9090"
  path: "/metrics"

# Health
health:
  listen_addr: "0.0.0.0:8080"
```

### 7.4 services.yaml (hot-reloadable)

```yaml
namespaces:
  ecommerce:
    services:
      order-service:
        key: "psk_live_7f3a8b2c4d5e6f7a8b9c0d1e2f3a4b5c"
        permissions:
          publish: ["order.*"]
          subscribe: ["payment.*", "inventory.*"]

      payment-service:
        key: "psk_live_9d4e1f5a6b7c8d9e0f1a2b3c4d5e6f7a"
        permissions:
          publish: ["payment.*"]
          subscribe: ["order.created"]

      analytics-service:
        key: "psk_live_1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d"
        permissions:
          publish: []
          subscribe: ["*"]   # can subscribe to everything

  internal-tools:
    services:
      deploy-service:
        key: "psk_live_abc123..."
        permissions:
          publish: ["deploy.*"]
          subscribe: ["deploy.*"]
```

### 7.5 Hot Reload Mechanism

```
Config Watcher task:
  1. inotify watch on services.yaml and routes.yaml
  2. On file change → parse new config
  3. Validate: all keys unique, topics valid, no syntax errors
  4. If valid → ArcSwap::store(new_config)
     All subsequent operations read new config immediately
  5. If invalid → log error, keep old config, emit metric
  6. No connections are dropped during reload
```

## 8. Topic Lifecycle

Topics in Pulse are **implicit** — they are created automatically and require no upfront declaration.

### 8.1 Creation

A topic comes into existence when:
1. **First PUB**: a publisher sends an event to a topic → topic is tracked in state DB
2. **First SUB**: a subscriber registers interest in a topic pattern → pattern is added to routing trie

No `CREATE TOPIC` command exists. This keeps the protocol simple and avoids a coordination step.

### 8.2 Metadata

While a topic has active publishers or subscribers, the broker tracks:
- Last event timestamp
- Message count (approximate, from metrics)
- Active subscriber count
- Active publisher count

This metadata is available via the Admin API (`GET /api/v1/topics`).

### 8.3 Cleanup

Topics are never explicitly deleted. They become inactive when:
- No subscribers are registered for the topic pattern
- No events have been published in the retention window

Inactive topic metadata is cleaned up during sled maintenance (hourly). WAL segments containing only completed events for inactive topics are compacted normally.

## 9. Secret Management

### 9.1 Environment Variable Substitution

`services.yaml` supports environment variable references to avoid storing secrets in plaintext:

```yaml
namespaces:
  ecommerce:
    services:
      order-service:
        key: "${ORDER_SVC_API_KEY}"        # resolved from environment
        permissions:
          publish: ["order.*"]
          subscribe: ["payment.*"]
```

**Syntax:**
- `${VAR_NAME}` — required, broker fails to start if not set
- `${VAR_NAME:-default}` — optional, uses default if not set

### 9.2 Future: External Secret Providers

v2 will support pluggable secret backends:
- HashiCorp Vault
- AWS Secrets Manager
- Kubernetes Secrets (via mounted volumes — already works with env var substitution)

## 10. Cluster Internals

When `cluster.enabled` is `true`, the broker participates in a distributed mesh. This section describes the internal mechanisms.

### 10.1 Gossip Protocol

Failure detection uses the SWIM protocol (Scalable Weakly-consistent Infection-style Membership):

- **Protocol period**: 200ms (configurable via `gossip_interval_ms`)
- **Probe cycle**: each period, the node selects a random peer and sends PEER_PING
- **Indirect probe**: if direct probe fails, the node asks `k` random peers (default `k=3`) to probe the target on its behalf
- **State transitions**: `alive` → `suspect` (direct + indirect probe failed) → `dead` (suspect for `failure_timeout_ms`)
- **Dissemination**: membership changes piggyback on PEER_PING messages, providing O(log N) convergence

Dead nodes are removed from the topology after a configurable grace period (default 30s) to avoid flapping.

### 10.2 Topic Ownership

Topics are assigned to nodes using a consistent hash ring:

- **Hash function**: xxHash64 on topic name
- **Virtual nodes**: each physical node contributes 128 virtual nodes to the ring for balanced distribution
- **Owner**: the node whose virtual node is the first clockwise match for the topic hash
- **Ring version**: monotonically increasing counter, incremented on any topology change; included in PEER_PING for consistency detection

When a node joins or leaves:
1. The ring is recalculated
2. Affected topics are identified (only topics that fall between the departing/joining node and its predecessor)
3. Ownership transfers are initiated — the new owner pulls WAL state from the old owner (or a replica)
4. Clients publishing to a transferred topic are transparently proxied (see 10.6)

### 10.3 Replication

The topic leader streams WAL entries to follower nodes for redundancy:

- **Replication factor**: configurable per topic (default 2, meaning 1 leader + 1 follower)
- **Async mode** (default): leader ACKs the publisher immediately after local WAL write, then streams to followers asynchronously. Typical replication lag is ~1ms under normal load.
- **Sync mode**: leader waits for a majority of replicas (including itself) to acknowledge before ACKing the publisher. Provides stronger durability at the cost of higher latency (~2-5ms).
- **Catch-up**: when a follower falls behind (e.g., after a restart), it requests WAL entries from the leader starting at its last known offset. The leader streams the gap before resuming live replication.

Replication uses the PEER_SYNC / PEER_ACK frame types defined in the protocol spec (01-protocol.md, 4.10).

### 10.4 Failover

When a topic leader fails:

1. **Detection**: gossip protocol marks the node as `dead` (typically within 3-5 seconds, bounded by `failure_timeout_ms`)
2. **Election**: the follower with the highest replication watermark (most up-to-date WAL offset) for that topic is promoted to leader
3. **Ring update**: the consistent hash ring is updated, and the new ring version is disseminated via gossip
4. **Client impact**: clients connected to the failed node reconnect to a peer (using the `peers` list from CONNACK). The new node proxies requests to the new topic leader transparently.
5. **Data gap**: in async replication mode, events that were ACKed by the old leader but not yet replicated may be lost. The `balanced` durability mode accepts this trade-off for lower latency. `durable` + `sync` replication provides zero data loss.

### 10.5 Split-Brain Prevention

Ownership changes require a majority quorum to prevent split-brain scenarios:

- A topology change (join, leave, failover) is only committed if a majority of known nodes (N/2 + 1) agree on the new ring version
- If the cluster partitions and neither side has a majority, the minority partition stops accepting writes (returns ERR 5000) and operates in read-only mode for existing consumers
- When the partition heals, the minority side reconciles with the majority's ring version and replays any missed WAL entries

### 10.6 Client Routing

Clients connect to any node in the cluster. The connected node handles the request transparently:

- **PUB**: if the connected node is the topic leader, it processes normally. If not, it proxies the PUB frame to the leader and relays the ACK back. The client is unaware of the proxy hop.
- **SUB**: the connected node registers the subscription locally and subscribes to the topic leader on behalf of the client. Events are relayed from the leader to the client through the connected node.
- **Latency**: proxy adds ~0.1ms per hop on a local network. For latency-sensitive workloads, the SDK can use the CONNACK `peers` list and topic-to-node mapping to connect directly to topic leaders (planned SDK optimization).
