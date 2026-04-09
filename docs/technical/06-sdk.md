# SDK Design

## 1. Multi-Language Strategy

**FFI-first architecture**: One Rust core, exposed via C ABI, wrapping every language SDK. This guarantees consistent behavior — reconnect logic, dedup, flow control, retry, and protocol handling are identical regardless of which language you use.

This is a deliberate departure from the NSQ model where each client library reimplements the protocol independently, leading to subtle behavioral differences (different reconnect strategies, inconsistent dedup, varying flow control implementations). With Pulse, the Rust core is the single source of truth.

```
┌──────────────────────────────────────┐
│       pulse-core (Rust library)       │
│  - Protocol codec (frame encode/decode)│
│  - Connection management + reconnect  │
│  - Cluster topology + leader routing  │
│  - Retry logic + dedup cache          │
│  - Backpressure / flow control        │
│  - TLS (rustls)                       │
│  - HMAC authentication                │
└────────────────┬─────────────────────┘
                 │ C ABI (libpulse.so / libpulse.dylib / pulse.dll)
                 │
    ┌────────────┼────────────┬────────────┬──────────┐
    ▼            ▼            ▼            ▼          ▼
  Rust SDK    Python SDK   TypeScript   Go SDK     Swift SDK
  (native)    (PyO3)       SDK          (cgo)      (future)
                           (napi-rs)
```

**Phase 1**: Rust SDK (native, full-featured) — implement first
**Phase 2**: Python + TypeScript SDKs (via FFI) — most common service languages
**Phase 3**: Go SDK — for teams using Go
**Phase 4**: Other languages as needed

> **Community implementations**: The wire protocol spec (§01-protocol.md) is public. Community-maintained native implementations in any language are welcome — the FFI approach is the official path for guaranteed behavioral consistency, but native implementations may be preferable for environments where C FFI is impractical.

## 2. Rust SDK — Public API

### 2.1 Connection

```rust
use pulse_sdk::{Pulse, PulseError, Result};

// ─── Simple connect (one line) ───
// Connect to one node — SDK auto-discovers peers from CONNACK
// ⚠️ Security: URL contains API key in plaintext. Use builder pattern in production
//    to avoid key leakage via logs, error messages, or process listings.
let pulse = Pulse::connect(
    "pulse://order-service:psk_live_7f3a@broker.company.com:4222/ecommerce"
).await?;

// ─── Explicit multi-node (seed list) ───
// SDK connects to first available seed, receives peer list from CONNACK,
// and maintains connections to all nodes hosting subscribed topics.
let pulse = Pulse::builder()
    .brokers(["node1:4222", "node2:4222", "node3:4222"])
    .namespace("ecommerce")
    .service_id("order-service")
    .api_key("psk_live_7f3a8b2c4d5e6f7a8b9c0d1e2f3a4b5c")
    // ... rest of builder
    .build().await?;

// ─── Builder for customization (recommended for production) ───
let pulse = Pulse::builder()
    // Required
    .broker("broker.company.com:4222")
    .namespace("ecommerce")
    .service_id("order-service")
    .api_key("psk_live_7f3a8b2c4d5e6f7a8b9c0d1e2f3a4b5c")

    // Connection behavior
    .reconnect(ReconnectPolicy::exponential(
        Duration::from_secs(1),    // initial delay
        Duration::from_secs(30),   // max delay
    ))
    .connect_timeout(Duration::from_secs(5))
    .keepalive(Duration::from_secs(10))

    // TLS
    .tls(TlsConfig::default())                    // verify broker cert (default)
    // .tls(TlsConfig::insecure())                // skip verify (dev only)
    // .tls(TlsConfig::custom_ca("ca.pem"))       // custom CA

    // Publish defaults
    .publish_timeout(Duration::from_secs(5))
    .publish_retries(3)

    // Subscribe defaults
    .max_in_flight(10)
    .ack_timeout(Duration::from_secs(30))

    // Codec — controls payload encoding for all publish operations
    .codec(Codec::Raw)          // opaque bytes, maximum performance
    // .codec(Codec::MessagePack)  // structured, enables content filtering (default)
    // .codec(Codec::Json)         // structured, human-readable

    .build()
    .await?;
```

**Codec selection**: The codec determines the ENCODING flags in the frame header. `Codec::Raw` sends opaque bytes — the broker cannot inspect the payload, so content filters and transforms are skipped (maximum throughput). `Codec::MessagePack` (default) and `Codec::Json` produce structured payloads that enable broker-side content filtering and transforms.

**Multi-node connection**: When connecting to a cluster, the SDK only needs one reachable seed node. The CONNACK response includes the full peer list (all nodes, their addresses, and topic ownership). The SDK then opens connections to nodes hosting topics it subscribes to or publishes to, enabling direct-to-leader routing.

**Connection URL format:**

```
pulse://service_id:api_key@host:port/namespace
│       │          │       │    │    │
│       │          │       │    │    └── namespace (required)
│       │          │       │    └─── port (default: 4222)
│       │          │       └──── broker hostname
│       │          └──────── API key
│       └─────────────── service identifier
└───────────────────── scheme (always "pulse")
```

### 2.2 Publishing

```rust
use pulse_sdk::Pulse;
use serde::Serialize;

#[derive(Serialize)]
struct Order {
    id: String,
    amount: u64,
    region: String,
}

// ─── Basic publish ───
// Blocks until broker ACKs STORED. Returns error only on auth/permission/timeout.
pulse.publish("order.created", &Order {
    id: "ord_123".into(),
    amount: 500_000,
    region: "VN".into(),
}).await?;

// ─── Publish with headers ───
pulse.publish_with_headers(
    "order.created",
    &order,
    Headers::new()
        .insert("trace_id", "abc123")
        .insert("source", "web-checkout"),
).await?;

// ─── Publish with options (including per-publish durability) ───
pulse.publish_opts("order.created", &order, PublishOpts {
    timeout: Duration::from_secs(10),
    priority: Priority::High,
    durability: Durability::Balanced,   // override broker default per-publish
    headers: None,
}).await?;

// ─── Durability modes ───
// Fire fast, no WAL — 800K+ msg/sec
pulse.publish_opts("analytics.event", &data, PublishOpts {
    durability: Durability::Memory,
    ..Default::default()
}).await?;

// WAL + async fsync — 100K+ msg/sec (default)
pulse.publish_opts("order.created", &order, PublishOpts {
    durability: Durability::Balanced,
    ..Default::default()
}).await?;

// WAL + sync fsync — strongest guarantee, lower throughput
pulse.publish_opts("payment.processed", &payment, PublishOpts {
    durability: Durability::Durable,
    ..Default::default()
}).await?;

// ─── Batch publish (atomic) ───
pulse.publish_batch(&[
    ("order.created", &order as &dyn Serialize),
    ("inventory.reserved", &reservation as &dyn Serialize),
    ("notification.queue", &notification as &dyn Serialize),
]).await?;
// All 3 events stored atomically or none.

// ─── Fire-and-forget (no durability guarantee) ───
pulse.publish_async("analytics.pageview", &pageview);
// Returns immediately, buffered internally.
// Eventual delivery, no ACK waited.
```

### 2.3 Subscribing

```rust
use pulse_sdk::{Event, Pulse, SubscribeOpts};
use serde::Deserialize;

#[derive(Deserialize)]
struct Payment {
    id: String,
    order_id: String,
    amount: u64,
}

// ─── Basic subscribe (callback) ───
pulse.subscribe("payment.completed", |event: Event<Payment>| async move {
    println!("Payment {} for order {}", event.payload.id, event.payload.order_id);
    // Returning Ok = auto ACK to broker
    // Returning Err = auto NACK → broker retries
    Ok(())
}).await?;

// ─── Subscribe with raw payload (no deserialization) ───
pulse.subscribe_raw("payment.*", |event: RawEvent| async move {
    println!("Topic: {}, Payload bytes: {}", event.topic(), event.payload().len());
    Ok(())
}).await?;

// ─── Subscribe with options ───
pulse.subscribe_opts(
    "order.created",
    SubscribeOpts {
        group: Some("payment-processors".into()),  // consumer group
        filter: Some("payload.amount > 1000".into()),
        position: Position::Latest,                  // or Earliest
        max_in_flight: 5,
        ack_timeout: Duration::from_secs(60),
        dedup: Dedup::InMemory(10_000),              // LRU cache size
        // dedup: Dedup::Persistent("/var/lib/myapp/pulse-dedup"),
    },
    |event: Event<Order>| async move {
        process_large_order(&event.payload).await?;
        Ok(())
    },
).await?;

// ─── Stream API (manual control) ───
let mut stream = pulse.stream("order.*").await?;
loop {
    match stream.next().await {
        Some(event) => {
            match event.topic() {
                "order.created" => {
                    let order: Order = event.deserialize()?;
                    handle_created(order).await?;
                }
                "order.cancelled" => {
                    let cancel: Cancel = event.deserialize()?;
                    handle_cancel(cancel).await?;
                }
                _ => {} // ignore unknown subtopics
            }
            event.ack().await?;  // manual ACK
        }
        None => break, // stream closed (disconnect)
    }
}

// ─── Consumer group ───
pulse.subscribe_group(
    "order.created",
    "payment-processors",    // group name
    |event: Event<Order>| async move {
        charge_payment(&event.payload).await?;
        Ok(())
    },
).await?;
// Only ONE instance in the group receives each event.
```

### 2.4 Request-Reply

```rust
// ─── Requester side ───
let response: InventoryStatus = pulse
    .request("inventory.check", &CheckRequest { sku: "ABC123" })
    .timeout(Duration::from_secs(5))
    .await?;

println!("Stock: {}", response.quantity);

// ─── Responder side ───
pulse.serve("inventory.check", |req: Event<CheckRequest>| async move {
    let stock = db.check_stock(&req.payload.sku).await?;
    Ok(InventoryStatus { sku: req.payload.sku.clone(), quantity: stock })
}).await?;
```

**Under the hood**:
1. Requester publishes to "inventory.check" with `reply_to: "inbox.order-svc.{random}"`
2. Requester creates temp subscription on "inbox.order-svc.{random}"
3. Responder processes, publishes response to the `reply_to` topic
4. Requester receives response, removes temp subscription

### 2.5 Event Object

```rust
pub struct Event<T> {
    payload: T,          // deserialized from MessagePack
    metadata: EventMeta,
}

pub struct EventMeta {
    pub msg_id: MessageId,      // UUIDv7
    pub topic: String,
    pub headers: Headers,
    pub produced_at: Option<u64>,  // millis, set by publisher
    pub received_at: u64,          // millis, when SDK received it
    pub delivery_attempt: u32,     // 1 = first delivery
}

pub struct RawEvent {
    metadata: EventMeta,
    payload_bytes: Bytes,   // raw MessagePack bytes
}

impl<T: DeserializeOwned> Event<T> {
    pub fn payload(&self) -> &T { &self.payload }
    pub fn topic(&self) -> &str { &self.metadata.topic }
    pub fn msg_id(&self) -> &MessageId { &self.metadata.msg_id }
    pub fn headers(&self) -> &Headers { &self.metadata.headers }
    pub fn attempt(&self) -> u32 { self.metadata.delivery_attempt }
}

impl RawEvent {
    pub fn deserialize<T: DeserializeOwned>(&self) -> Result<T> {
        rmp_serde::from_slice(&self.payload_bytes).map_err(Into::into)
    }
}
```

### 2.6 Lifecycle Management

```rust
// ─── Run forever (typical for services) ───
pulse.run_forever().await;
// Blocks. Handles reconnect, keepalive, etc.
// Exits only on unrecoverable error or SIGTERM.

// ─── Graceful shutdown ───
let pulse = Pulse::connect("pulse://...").await?;
let shutdown_handle = pulse.shutdown_handle();

// In signal handler or elsewhere:
tokio::spawn(async move {
    tokio::signal::ctrl_c().await.unwrap();
    shutdown_handle.shutdown().await;
    // Waits for in-flight handlers to complete (up to 30s)
    // Unsubscribes from all topics
    // Closes connection gracefully
});

pulse.run_forever().await;

// ─── Event hooks ───
pulse.on_connect(|| async {
    tracing::info!("Connected to Pulse broker");
});
pulse.on_disconnect(|reason| async move {
    tracing::warn!("Disconnected: {}", reason);
});
pulse.on_reconnect(|attempt| async move {
    tracing::info!("Reconnected after {} attempts", attempt);
});
```

## 3. SDK Internals

### 3.1 Internal Architecture

```
┌──────────────────────────────────────────────────┐
│                    Pulse SDK                      │
│                                                   │
│  ┌──────────────────────────────────────────┐     │
│  │ Public API Layer                          │     │
│  │  connect(), publish(), subscribe(),       │     │
│  │  request(), stream()                      │     │
│  └─────────────────┬────────────────────────┘     │
│                    │                              │
│  ┌─────────────────▼────────────────────────┐     │
│  │ Session Manager                           │     │
│  │  - Owns tokio tasks                       │     │
│  │  - Coordinates publish/subscribe          │     │
│  │  - Manages subscriptions registry         │     │
│  └─────────────────┬────────────────────────┘     │
│                    │                              │
│  ┌─────────────────▼────────────────────────┐     │
│  │ Connection Manager                        │     │
│  │  - TCP + TLS connect                      │     │
│  │  - Auto reconnect (exponential backoff)   │     │
│  │  - PING/PONG keepalive                    │     │
│  │  - CONNECT + auth handshake               │     │
│  │  - On reconnect: re-SUB all topics,       │     │
│  │    replay pending PUBs                    │     │
│  └─────────────────┬────────────────────────┘     │
│                    │                              │
│  ┌─────────────────▼────────────────────────┐     │
│  │ Message Pipeline                          │     │
│  │                                           │     │
│  │  Outbound path:                           │     │
│  │    serialize → build frame → CRC →        │     │
│  │    enqueue → TCP write                    │     │
│  │    └── pending_acks: HashMap<MsgId, Tx>   │     │
│  │                                           │     │
│  │  Inbound path:                            │     │
│  │    TCP read → deframe → CRC verify →      │     │
│  │    dispatch by type:                      │     │
│  │      ACK → resolve pending_ack oneshot    │     │
│  │      PUB → dedup → invoke handler         │     │
│  │      PONG → reset keepalive timer         │     │
│  │      ERR → log or propagate              │     │
│  └─────────────────┬────────────────────────┘     │
│                    │                              │
│  ┌─────────────────▼────────────────────────┐     │
│  │ Protocol Codec                            │     │
│  │  - Frame encode/decode                    │     │
│  │  - CRC32 compute/verify                   │     │
│  │  - MessagePack / JSON codec               │     │
│  │  - Implements tokio Encoder/Decoder       │     │
│  └──────────────────────────────────────────┘     │
└──────────────────────────────────────────────────┘
```

### 3.2 Reconnect State Machine

```
                    ┌─────────────┐
     start ───────> │ CONNECTING  │
                    └──────┬──────┘
                           │
                    ┌──────┴──────┐
                    │             │
                 success       failure
                    │             │
                    ▼             ▼
             ┌───────────┐  ┌──────────────┐
             │ CONNECTED │  │ BACKOFF_WAIT │
             └─────┬─────┘  └──────┬───────┘
                   │               │ timer expires
              TCP error            │
                   │               ▼
                   │        ┌─────────────┐
                   └──────> │ RECONNECTING│ ──success──> CONNECTED
                            └──────┬──────┘
                                   │ failure
                                   ▼
                            ┌──────────────┐
                            │ BACKOFF_WAIT │ (delay *= 2, cap at max)
                            └──────────────┘
                                   │
                                   └──> RECONNECTING (loop)

During BACKOFF_WAIT and RECONNECTING:
  - publish() calls: buffered in local queue (up to 1000 messages)
  - subscribe callbacks: paused (broker holds events)
  - All transparent to user code
```

**Multi-node failover**: In a cluster, the SDK maintains connections to multiple nodes. If one node becomes unreachable:

1. The SDK seamlessly fails over to another connected node for in-flight operations
2. If the failed node owned topics the client subscribes to, the cluster rebalances topic ownership
3. The SDK receives an updated peer list reflecting the new topology
4. Publish retries automatically route to the new topic leader
5. Subscriptions are re-registered on the new topic owner (transparent to user code)

The reconnect state machine above applies per-connection. The SDK manages multiple concurrent connection state machines — one per peer node.

### 3.3 Publish Flow (Internal)

```rust
// Internal publish implementation
async fn publish_internal(&self, topic: &str, payload: &[u8], opts: &PublishOpts) -> Result<Ack> {
    // 1. Generate or reuse Message ID
    let msg_id = MessageId::new_v7();

    // 2. Build frame
    let frame = Frame::pub_frame(msg_id, topic, payload, &opts.headers);

    // 3. Create oneshot for ACK
    let (ack_tx, ack_rx) = oneshot::channel();
    self.pending_acks.insert(msg_id, ack_tx);

    // 4. Send frame (may block if TCP buffer full = backpressure)
    self.write_tx.send(frame).await
        .map_err(|_| PulseError::ConnectionLost)?;

    // 5. Wait for ACK with timeout
    let result = timeout(opts.timeout, ack_rx).await;

    match result {
        Ok(Ok(ack)) => {
            match ack.status {
                AckStatus::Stored | AckStatus::Duplicate => Ok(ack),
                AckStatus::Rejected { reason } => Err(PulseError::Rejected(reason)),
            }
        }
        Ok(Err(_)) => {
            // oneshot dropped — connection lost during wait
            // Buffer for retry on reconnect
            self.retry_buffer.push(RetryEntry { msg_id, topic, payload, opts });
            Err(PulseError::ConnectionLost)
        }
        Err(_) => {
            // Timeout — will be retried (same msg_id)
            self.pending_acks.remove(&msg_id);
            if opts.retries_left > 0 {
                self.publish_internal(topic, payload, &opts.decrement_retry()).await
            } else {
                Err(PulseError::Timeout)
            }
        }
    }
}
```

### 3.4 Subscribe Flow (Internal)

```rust
// Internal subscription handler
async fn handle_delivery(&self, frame: Frame) {
    let msg_id = frame.msg_id();
    let topic = frame.topic();

    // 1. Consumer-side dedup
    if self.dedup_cache.contains(&msg_id) {
        // Already processed — just ACK
        self.send_ack(msg_id, AckStatus::Done).await;
        return;
    }

    // 2. Find matching subscription
    let sub = match self.subscriptions.find_match(topic) {
        Some(sub) => sub,
        None => {
            // No subscription matches (should not happen normally)
            tracing::warn!("Received event for unsubscribed topic: {}", topic);
            return;
        }
    };

    // 3. Deserialize payload
    let event = match sub.deserialize_event(&frame) {
        Ok(event) => event,
        Err(e) => {
            tracing::error!("Failed to deserialize event: {}", e);
            self.send_ack(msg_id, AckStatus::Rejected("deserialization error")).await;
            return;
        }
    };

    // 4. Invoke user handler (with panic catch)
    let result = std::panic::AssertUnwindSafe((sub.handler)(event))
        .catch_unwind()
        .await;

    match result {
        Ok(Ok(())) => {
            // Success — ACK and record in dedup
            self.dedup_cache.insert(msg_id);
            self.send_ack(msg_id, AckStatus::Done).await;
        }
        Ok(Err(user_error)) => {
            // User returned Err — NACK, broker will retry
            tracing::warn!("Handler error for {}: {}", msg_id, user_error);
            self.send_nack(msg_id, &user_error.to_string()).await;
        }
        Err(panic_info) => {
            // Handler panicked — NACK
            tracing::error!("Handler panicked for {}: {:?}", msg_id, panic_info);
            self.send_nack(msg_id, "handler panicked").await;
        }
    }
}
```

### 3.5 Client-Side Topology

The SDK maintains a local view of the cluster topology, enabling intelligent routing decisions without relying on the broker to proxy requests.

**Topology acquisition:**
1. On initial CONNACK, the broker returns the full peer list: `{ node_id, addr, topics_owned[] }` for every node in the cluster
2. Periodically (and on cluster membership changes), the broker pushes updated topology via a control frame
3. The SDK caches this topology in memory (`ArcSwap<ClusterTopology>`)

**Routing behavior:**

```rust
pub struct ClusterTopology {
    pub nodes: Vec<NodeInfo>,
    pub topic_owners: HashMap<TopicPattern, NodeId>,
}

impl ClusterTopology {
    /// Find the node that owns this topic.
    /// Returns None if topic ownership is unknown (falls back to any connected node).
    pub fn topic_leader(&self, topic: &str) -> Option<&NodeInfo> {
        self.topic_owners.get(topic)
            .and_then(|node_id| self.nodes.iter().find(|n| &n.id == node_id))
    }
}
```

- **Publish**: SDK looks up the topic leader and sends directly to that node. If the leader is unknown or unreachable, falls back to any connected node (that node will proxy the PUB to the correct owner).
- **Subscribe**: SDK registers the subscription on the topic owner node. If the topic owner changes, the SDK re-registers transparently.
- **Fallback**: For new/unknown topics where ownership has not yet been observed, the SDK sends to any connected node. The broker handles forwarding.

This direct-to-leader routing eliminates the proxy hop for the common case, matching single-node latency even in a distributed cluster.

## 4. Error Types

```rust
#[derive(Debug, thiserror::Error)]
pub enum PulseError {
    // ─── Connection errors ───
    #[error("failed to connect: {0}")]
    ConnectionFailed(String),

    #[error("connection lost")]
    ConnectionLost,

    #[error("TLS handshake failed: {0}")]
    TlsFailed(String),

    // ─── Authentication errors ───
    #[error("authentication failed: invalid API key or HMAC")]
    AuthFailed,

    #[error("forbidden: no permission for topic '{topic}'")]
    Forbidden { topic: String },

    // ─── Publish errors ───
    #[error("publish timeout after {0:?}")]
    Timeout(Duration),

    #[error("payload too large: {size} bytes (max: {max})")]
    PayloadTooLarge { size: usize, max: usize },

    #[error("publish rejected: {0}")]
    Rejected(String),

    // ─── Subscribe errors ───
    #[error("invalid filter expression: {0}")]
    InvalidFilter(String),

    #[error("deserialization error: {0}")]
    DeserializeError(String),

    // ─── Protocol errors ───
    #[error("frame CRC mismatch")]
    CrcMismatch,

    #[error("invalid frame: {0}")]
    InvalidFrame(String),

    #[error("broker error ({code}): {message}")]
    BrokerError { code: u32, message: String },

    // ─── Internal ───
    #[error("internal SDK error: {0}")]
    Internal(String),
}
```

## 5. Testing Support

### 5.1 MockPulse

```rust
use pulse_sdk::mock::MockPulse;

#[tokio::test]
async fn test_order_creates_payment() {
    // Create mock — no network, no broker needed
    let mock = MockPulse::new();

    // Register the handler being tested
    let service = OrderService::new(mock.client());
    service.start().await;

    // Simulate incoming event
    mock.simulate("order.created", &Order {
        id: "ord_123".into(),
        amount: 500_000,
        region: "VN".into(),
    }).await;

    // Assert what was published in response
    let published = mock.published("payment.request").await;
    assert_eq!(published.len(), 1);
    let payment: PaymentRequest = published[0].deserialize().unwrap();
    assert_eq!(payment.order_id, "ord_123");
    assert_eq!(payment.amount, 500_000);
}

#[tokio::test]
async fn test_handler_error_triggers_nack() {
    let mock = MockPulse::new();

    mock.client().subscribe("order.created", |_event: Event<Order>| async {
        Err(PulseError::Internal("db unavailable".into()))
    }).await.unwrap();

    mock.simulate("order.created", &order).await;

    // Assert NACK was sent
    assert_eq!(mock.nack_count(), 1);
    assert_eq!(mock.ack_count(), 0);
}

#[tokio::test]
async fn test_dedup_prevents_duplicate_processing() {
    let mock = MockPulse::new();
    let counter = Arc::new(AtomicU32::new(0));
    let c = counter.clone();

    mock.client().subscribe("order.created", move |_event: Event<Order>| {
        let c = c.clone();
        async move {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }).await.unwrap();

    // Simulate same event twice (same msg_id)
    let msg_id = MessageId::new_v7();
    mock.simulate_with_id(msg_id, "order.created", &order).await;
    mock.simulate_with_id(msg_id, "order.created", &order).await;

    // Handler called only once
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}
```

### 5.2 Integration Test Helper

```rust
use pulse_sdk::test::TestBroker;

#[tokio::test]
async fn integration_test_full_flow() {
    // Start an in-process broker (no external process needed)
    let broker = TestBroker::start().await;

    // Connect publisher
    let publisher = Pulse::connect(&broker.url("pub-service", "test-ns")).await.unwrap();

    // Connect subscriber
    let subscriber = Pulse::connect(&broker.url("sub-service", "test-ns")).await.unwrap();

    let (tx, rx) = oneshot::channel();
    subscriber.subscribe("order.created", move |event: Event<Order>| {
        let tx = Some(tx); // move into closure
        async move {
            if let Some(tx) = tx {
                tx.send(event.payload.id.clone()).ok();
            }
            Ok(())
        }
    }).await.unwrap();

    // Publish
    publisher.publish("order.created", &Order { id: "ord_1".into(), .. }).await.unwrap();

    // Wait for subscriber to receive
    let received_id = timeout(Duration::from_secs(5), rx).await.unwrap().unwrap();
    assert_eq!(received_id, "ord_1");

    broker.shutdown().await;
}
```

## 6. Python SDK (via PyO3)

```python
import asyncio
from pulse import Pulse, Event

async def main():
    # Connect
    pulse = await Pulse.connect("pulse://order-svc:key@broker:4222/ecommerce")

    # Publish
    await pulse.publish("order.created", {
        "id": "ord_123",
        "amount": 500_000,
        "region": "VN",
    })

    # Subscribe (decorator style)
    @pulse.on("payment.completed")
    async def handle_payment(event: Event):
        print(f"Payment {event.payload['id']} completed")
        # return normally = ACK
        # raise Exception = NACK

    # Subscribe (function style)
    async def handle_order(event: Event):
        await process_order(event.payload)

    await pulse.subscribe("order.created", handle_order, group="processors")

    # Request-reply
    response = await pulse.request("inventory.check", {"sku": "ABC123"}, timeout=5.0)
    print(f"Stock: {response['quantity']}")

    await pulse.run_forever()

asyncio.run(main())
```

## 7. TypeScript SDK (via napi-rs)

```typescript
import { Pulse, Event } from '@pulse-mq/sdk';

async function main() {
  // Connect
  const pulse = await Pulse.connect('pulse://order-svc:key@broker:4222/ecommerce');

  // Publish
  await pulse.publish('order.created', {
    id: 'ord_123',
    amount: 500_000,
    region: 'VN',
  });

  // Subscribe (typed)
  interface Payment {
    id: string;
    orderId: string;
    amount: number;
  }

  await pulse.subscribe<Payment>('payment.completed', async (event) => {
    console.log(`Payment ${event.payload.id} completed`);
    // return = ACK
    // throw = NACK
  });

  // Subscribe with options
  await pulse.subscribe<Order>('order.created', handler, {
    group: 'payment-processors',
    filter: 'payload.amount > 1000',
    maxInFlight: 5,
  });

  // Request-reply
  const stock = await pulse.request<StockResponse>('inventory.check', { sku: 'ABC123' }, {
    timeout: 5000,
  });

  await pulse.runForever();
}

main().catch(console.error);
```

## 8. Go SDK

```go
package main

import (
    "context"
    "fmt"
    "github.com/pulsemq/pulse-go"
)

func main() {
    ctx := context.Background()

    // Connect
    p, err := pulse.Connect(ctx, "pulse://order-svc:key@broker:4222/ecommerce")
    if err != nil { panic(err) }
    defer p.Close()

    // Publish
    err = p.Publish(ctx, "order.created", Order{
        ID:     "ord_123",
        Amount: 500000,
        Region: "VN",
    })

    // Subscribe
    p.Subscribe("payment.completed", func(event pulse.Event[Payment]) error {
        fmt.Printf("Payment %s completed\n", event.Payload.ID)
        return nil // ACK
        // return err // NACK
    })

    // Consumer group
    p.SubscribeGroup("order.created", "processors", func(event pulse.Event[Order]) error {
        return processOrder(event.Payload)
    })

    p.RunForever(ctx)
}

## 9. Schema Evolution

Pulse does not include a built-in schema registry (v1). Payload schema management is the responsibility of publishers and consumers. The following patterns are recommended.

### 9.1 Versioned Payloads

Include a version field in every event payload:

```rust
#[derive(Serialize)]
struct OrderCreated {
    v: u32,               // schema version — always first field
    id: String,
    amount: u64,
    // v2 addition:
    currency: Option<String>,  // Option for backward compatibility
}

// Publish with version
pulse.publish("order.created", &OrderCreated {
    v: 2,
    id: "ord_123".into(),
    amount: 500_000,
    currency: Some("VND".into()),
}).await?;
```

### 9.2 Consumer Version Handling

Consumers should handle multiple payload versions gracefully:

```rust
pulse.subscribe_raw("order.created", |event: RawEvent| async move {
    let version = event.payload_field::<u32>("v").unwrap_or(1);

    match version {
        1 => {
            let order: OrderCreatedV1 = event.deserialize()?;
            handle_v1(order).await
        }
        2 => {
            let order: OrderCreatedV2 = event.deserialize()?;
            handle_v2(order).await
        }
        v => {
            tracing::warn!("Unknown order.created version: {}", v);
            Ok(())  // ACK and skip — don't block the queue
        }
    }
}).await?;
```

### 9.3 Best Practices

| Practice | Rationale |
|----------|-----------|
| Always add fields as `Option<T>` | Old consumers won't break on new fields |
| Never remove or rename fields | Existing consumers depend on field names |
| Increment version on breaking changes | Consumers can branch on version |
| Use `subscribe_raw` for multi-version handling | Avoids deserialization failure on unknown versions |
| ACK unknown versions (don't NACK) | Prevents DLQ pollution from schema upgrades |
| Coordinate deployment order | Deploy consumers before publishers when adding required fields |

### 9.4 Future: Schema Registry

A schema registry is planned for v2, providing:
- Centralized schema storage and validation
- Compatibility checks (backward, forward, full)
- Auto-generated serialization code
- Broker-side payload validation (optional)
```
