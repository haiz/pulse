# Pulse Protocol Specification v1.0

## 1. Design Goals

- **Minimal overhead**: binary framing, no HTTP layer, no schema compilation step
- **Self-describing**: every frame carries enough info to decode without external state
- **Integrity**: CRC32 on every frame to detect corruption
- **Sortable IDs**: UUIDv7 message IDs encode creation timestamp
- **Simple to implement**: 10 client message types + 5 inter-node types cover all use cases
- **Data-agnostic**: payloads are opaque bytes by default — no forced serialization format

## 2. Transport Layer

- **Transport**: TCP with mandatory TLS 1.3 (via `rustls`)
- **Default port**: 4222 (client), 4223 (inter-node)
- **Byte order**: Big-endian (network byte order) for all multi-byte integers
- **Connection**: persistent, full-duplex — both sides can send frames at any time
- **Keepalive**: PING/PONG at configurable interval (default: 10 seconds)

## 3. Frame Format

Every message on the wire is a single frame:

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|         Magic (0x50 0x4C)     |   Version     |     Type      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     Flags     |                                               |
+-+-+-+-+-+-+-+-+                                               +
|                                                               |
+                       Message ID (16 bytes)                   +
|                          UUIDv7                                |
+                                               +-+-+-+-+-+-+-+-+
|                                               |               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+               +
|                    Payload Length (4 bytes)                    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
|                     Payload (variable)                         |
|          Encoding determined by ENCODING flags                 |
|                 (default: raw bytes)                            |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       CRC32 (4 bytes)                         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### Field Reference

| Field | Offset | Size | Description |
|-------|--------|------|-------------|
| Magic | 0 | 2 bytes | Always `0x504C` (ASCII "PL" for Pulse). Used to detect stream misalignment. |
| Version | 2 | 1 byte | Protocol version. Currently `0x01`. |
| Type | 3 | 1 byte | Message type (see §4). |
| Flags | 4 | 1 byte | Bitfield (see §3.1). |
| Message ID | 5 | 16 bytes | UUIDv7. For PUB frames, this is the event ID used for dedup. For other frames, used for request-response correlation. |
| Payload Length | 21 | 4 bytes | Length of payload in bytes. Max: 16 MB (`0x01000000`). 0 for frames with no payload (PING, PONG, UNSUB). |
| Payload | 25 | variable | Encoding determined by ENCODING flags (default: raw bytes). Structure depends on message type. |
| CRC32 | 25 + len | 4 bytes | CRC32C (Castagnoli) over bytes [0, 25+len). Covers header + payload. |

**Total header size**: 25 bytes (fixed) + payload + 4 bytes CRC = 29 bytes overhead minimum.

### 3.1 Flags Bitfield

```
Bit 0:   COMPRESSED    — payload is LZ4-compressed (0 = raw, 1 = compressed)
Bit 1:   BATCH         — payload contains multiple messages (0 = single, 1 = batch)
Bit 2:   REPLY_TO      — payload includes a reply_to field for request-reply pattern
Bit 3:   PRIORITY      — high-priority message, skip to front of delivery queue
Bit 4-5: ENCODING      — payload encoding format:
                           00 = raw bytes (opaque, default — no deserialization needed)
                           01 = MessagePack
                           10 = JSON
                           11 = Reserved
Bits 6-7: RESERVED     — must be 0, ignored by receiver
```

When ENCODING is `00` (raw bytes), the broker treats the payload as an opaque blob. It is passed through without inspection, matching the NSQ model where the broker has no knowledge of payload structure. Content-based filtering (§4.5) requires ENCODING `01` (MessagePack) or `10` (JSON) so the broker can evaluate filter expressions.

### 3.2 Maximum Frame Size

- Maximum payload: 16 MB (configurable, default 1 MB)
- Maximum total frame: 16 MB + 29 bytes
- Frames exceeding the configured max are rejected with ERR frame

## 4. Message Types

### 4.1 CONNECT (0x01) — Client → Broker

Sent immediately after TLS handshake. Must be the first frame on a new connection.

**Payload (MessagePack map):**

```
{
  "service_id":  "order-service",          // string, required
  "namespace":   "ecommerce",              // string, required
  "timestamp":   1700000000,               // uint64, Unix epoch seconds, required
  "hmac":        <32 bytes>,               // bytes, HMAC-SHA256(api_key, timestamp), required
  "client_ver":  "pulse-sdk-rust/0.1.0",   // string, optional
  "max_inflight": 10,                      // uint32, optional, default 100
  "codec":       "msgpack"                 // string, optional, "msgpack" | "json"
}
```

**Node-to-node CONNECT** (mesh mode):

When a peer node connects to another node, it sends a CONNECT with additional fields:

```
{
  "service_id":  "pulse-node-2",           // string, required — the connecting node's ID
  "namespace":   "_cluster",               // string, required — reserved namespace for inter-node
  "timestamp":   1700000000,               // uint64, required
  "hmac":        <32 bytes>,               // bytes, HMAC-SHA256(cluster_secret, timestamp)
  "peers":       ["10.0.1.2:4223",         // string array, optional — node's known peer list
                  "10.0.1.3:4223"],         //   (used for peer discovery protocol)
  "node_id":     "pulse-node-2",           // string, required — unique node identifier
  "node_addr":   "10.0.1.4:4223"           // string, required — this node's inter-node listen address
}
```

The `_cluster` namespace is reserved and cannot be used by clients.

**Authentication flow:**
1. Client computes `HMAC-SHA256(api_key, timestamp_as_string)`
2. Broker looks up `api_key` by `service_id` + `namespace`
3. Broker verifies HMAC
4. Broker checks `|timestamp - server_time| < 30 seconds` (anti-replay)
5. If valid → CONNACK. If invalid → ERR + close connection.

For node-to-node connections, step 2 uses the shared `cluster_secret` instead of a per-service API key.

### 4.2 CONNACK (0x02) — Broker → Client

**Payload:**

```
{
  "status":      "ok",                     // "ok" | "error"
  "broker_id":   "pulse-broker-01",        // string
  "node_id":     "pulse-node-1",           // string — unique node identifier in the cluster
  "server_time": 1700000001,               // uint64, for client clock sync
  "max_payload": 1048576,                  // uint32, max payload bytes
  "features":    ["batch", "compress"],    // string array, supported features
  "durability":  "balanced",               // string — current durability mode: "memory" | "balanced" | "durable"
  "peers":       ["10.0.1.2:4222",         // string array, optional — peer nodes for client-side discovery
                  "10.0.1.3:4222"]          //   (client port, not inter-node port)
}
```

The `peers` field enables client-side discovery: if the current node becomes unavailable, the SDK can reconnect to an alternative peer without requiring external service discovery. The `durability` field informs the client of the broker's current durability guarantee so the SDK can adjust retry and confirmation behavior accordingly.

### 4.3 PUB (0x03) — Client → Broker

Publish an event to a topic.

**Payload:**

```
{
  "topic":      "order.created",           // string, required, max 255 chars
  "data":       <arbitrary msgpack>,       // any, required — the event payload
  "headers":    {                          // map, optional — metadata
    "trace_id":    "abc123",
    "reply_to":    "inbox.svc.abc123",     // only if REPLY_TO flag set
    "priority":    "high"
  },
  "produced_at": 1700000000000             // uint64, optional, millis since epoch
}
```

**Message ID**: the frame's Message ID field is the unique event ID. Publishers must reuse the same Message ID when retrying a failed publish to enable dedup.

### 4.4 ACK (0x04) — Bidirectional

Used for three purposes:
1. **Broker → Publisher**: acknowledges event stored in WAL (ACK STORED)
2. **Consumer → Broker**: acknowledges event processed (ACK DONE)
3. **Consumer → Broker**: rejects event processing (NACK = ACK with `status: "rejected"`)

> **Note on NACK**: There is no separate NACK frame type. A "NACK" is an ACK frame with `status: "rejected"` and an optional `reason` field. Other documents may use the term "NACK" as shorthand for this.

**Payload:**

```
{
  "status":   "stored" | "done" | "rejected" | "duplicate",
  "msg_id":   <16 bytes>,                 // echoes the Message ID being ACKed
  "reason":   "payload_too_large"          // string, only present if "rejected"
}
```

**The frame's Message ID**: matches the original PUB frame's Message ID.

### 4.5 SUB (0x05) — Client → Broker

Subscribe to a topic pattern.

**Payload:**

```
{
  "topic":      "order.*",                 // string, required, supports wildcards
  "group":      "payment-processors",      // string, optional — consumer group name
  "filter":     "payload.amount > 1000",   // string, optional — content-based filter
  "position":   "latest" | "earliest",     // string, optional, default "latest"
  "sub_id":     "sub_001"                  // string, required — client-assigned subscription ID
}
```

**Wildcard rules:**
- `*` matches exactly one segment: `order.*` matches `order.created`, not `order.us.created`
- `>` matches one or more segments: `order.>` matches `order.created` and `order.us.created`
- Exact match has highest priority, then `*`, then `>`

**Broker responds with ACK** (status "ok") or ERR if topic pattern is invalid or permission denied.

### 4.6 UNSUB (0x06) — Client → Broker

**Payload:**

```
{
  "sub_id": "sub_001"                      // string, required — matches the SUB's sub_id
}
```

Broker responds with ACK.

### 4.7 PING (0x07) / PONG (0x08) — Bidirectional

No payload (payload length = 0). Either side can initiate.

- Client sends PING, broker responds PONG (and vice versa)
- If no PONG received within `keepalive_timeout` (default 30s), connection is considered dead
- Message ID field is echoed in PONG for correlation

### 4.8 FLOW (0x09) — Client → Broker

Flow control signal from consumer.

**Payload:**

```
{
  "max_inflight": 5,                       // uint32, required — max unACKed messages
  "sub_id":       "sub_001"                // string, optional — apply to specific sub, or all if omitted
}
```

Broker will not deliver more than `max_inflight` unACKed messages to this consumer (per subscription or globally).

### 4.9 ERR (0x0A) — Broker → Client

**Payload:**

```
{
  "code":    4010,                          // uint32, error code
  "message": "authentication failed"       // string, human-readable
}
```

**Error codes:**

| Code | Meaning |
|------|---------|
| 4000 | Bad request — malformed frame |
| 4001 | Invalid CRC — frame corrupted |
| 4010 | Authentication failed |
| 4030 | Forbidden — no permission for topic |
| 4040 | Namespace not found |
| 4090 | Payload too large |
| 4290 | Rate limited |
| 5000 | Internal broker error |
| 5030 | Broker shutting down |

### 4.10 Inter-Node Mesh Messages

These message types are used exclusively for node-to-node communication within the cluster mesh. They are sent on the inter-node port (default 4223) and are never exposed to clients.

#### PEER_JOIN (0x0B) — Node → Mesh

A node announces itself to the mesh when it starts up or discovers new peers.

**Payload:**

```
{
  "node_id":     "pulse-node-3",           // string, required — unique node identifier
  "listen_addr": "10.0.1.4:4223",         // string, required — inter-node listen address
  "client_addr": "10.0.1.4:4222",         // string, required — client-facing listen address
  "topics":      ["order.*", "payment.*"], // string array — topics this node currently owns
  "generation":  42,                       // uint64 — monotonic counter, incremented on restart
  "joined_at":   1700000000                // uint64 — Unix epoch seconds
}
```

On receiving PEER_JOIN, each node updates its local topology view and relays the join to other known peers (with TTL to prevent infinite propagation).

#### PEER_LEAVE (0x0C) — Node → Mesh

A node announces graceful departure from the mesh before shutting down.

**Payload:**

```
{
  "node_id":     "pulse-node-3",           // string, required
  "reason":      "shutdown",               // string — "shutdown" | "maintenance" | "rebalance"
  "transfer_to": "pulse-node-1"           // string, optional — preferred node to take ownership
}
```

On receiving PEER_LEAVE, the remaining nodes redistribute topic ownership. If `transfer_to` is specified and that node is healthy, it is preferred for ownership transfer.

#### PEER_SYNC (0x0D) — Leader → Follower

WAL replication frame. The topic leader streams WAL entries to follower nodes for redundancy.

**Payload:**

```
{
  "topic":       "order.created",          // string, required — topic being replicated
  "wal_offset":  123456,                   // uint64, required — WAL segment offset
  "segment_id":  7,                        // uint32, required — WAL segment number
  "entries":     [                         // array, required — one or more WAL entries
    {
      "msg_id":  <16 bytes>,               // UUIDv7 — original message ID
      "data":    <bytes>,                  // raw frame bytes (header + payload)
      "ts":      1700000000123             // uint64 — millis since epoch
    }
  ],
  "commit":      true                      // bool — if true, entries are committed (fsync'd on leader)
}
```

Followers write received entries to their local WAL. In async replication mode, the leader does not wait for follower acknowledgment before ACKing the publisher. In sync mode, the leader waits for a majority of followers to acknowledge.

#### PEER_ACK (0x0E) — Follower → Leader

Replication acknowledgment. The follower confirms it has persisted WAL entries from a PEER_SYNC.

**Payload:**

```
{
  "node_id":     "pulse-node-2",           // string, required — the acknowledging follower
  "topic":       "order.created",          // string, required — topic being acknowledged
  "wal_offset":  123456,                   // uint64, required — highest contiguous offset persisted
  "segment_id":  7                         // uint32, required — WAL segment number
}
```

The leader tracks the replication watermark per follower per topic. This is used for leader election (most up-to-date follower wins) and for determining when it is safe to compact WAL segments.

#### PEER_PING (0x0F) — Node → Node (Gossip)

Gossip protocol health check. Unlike client PING/PONG (which are simple keepalives), PEER_PING carries cluster state for dissemination.

**Payload:**

```
{
  "node_id":     "pulse-node-1",           // string, required — sender
  "generation":  42,                       // uint64 — sender's generation counter
  "members":     {                         // map — sender's view of cluster membership
    "pulse-node-1": { "state": "alive", "gen": 42, "addr": "10.0.1.1:4223" },
    "pulse-node-2": { "state": "alive", "gen": 15, "addr": "10.0.1.2:4223" },
    "pulse-node-3": { "state": "suspect", "gen": 8, "addr": "10.0.1.3:4223" }
  },
  "ring_version": 5                        // uint64 — hash ring version for consistency check
}
```

**Member states**: `alive`, `suspect` (missed probes, not yet confirmed dead), `dead` (confirmed failed, pending removal). The receiver merges the sender's membership view with its own, keeping the highest generation per node. This is the SWIM protocol dissemination mechanism.

The response is also a PEER_PING (bidirectional exchange), allowing both nodes to converge on the same cluster view in a single round trip.

## 5. Connection Lifecycle

### 5.1 Client Connection

```
Client                                    Broker
  │                                         │
  │ ──── TLS Handshake ──────────────────>  │
  │ <─── TLS Established ────────────────   │
  │                                         │
  │ ──── CONNECT { service_id, hmac } ──>   │
  │ <─── CONNACK { ok, features,            │
  │                 node_id, durability,     │
  │                 peers } ──────────────   │
  │                                         │
  │ ──── SUB { topic, sub_id } ──────────>  │
  │ <─── ACK { ok } ────────────────────    │
  │                                         │
  │         ... normal operation ...         │
  │                                         │
  │ ──── PING ───────────────────────────>  │
  │ <─── PONG ──────────────────────────    │
  │                                         │
  │    (graceful shutdown)                  │
  │ ──── UNSUB { sub_id } ──────────────>   │
  │ <─── ACK ───────────────────────────    │
  │ ──── TCP FIN ────────────────────────>  │
```

### 5.2 Node-to-Node (Peer) Connection

```
Node A                                    Node B
  │                                         │
  │ ──── TLS Handshake (mutual) ─────────>  │
  │ <─── TLS Established ────────────────   │
  │                                         │
  │ ──── CONNECT { node_id, peers,          │
  │                _cluster, hmac } ──────>  │
  │ <─── CONNACK { ok, node_id, peers } ──  │
  │                                         │
  │ ──── PEER_JOIN { topics, gen } ───────>  │
  │ <─── PEER_JOIN { topics, gen } ────────  │
  │                                         │
  │     ... gossip protocol (periodic) ...  │
  │                                         │
  │ ──── PEER_PING { members, ring_ver } ─>  │
  │ <─── PEER_PING { members, ring_ver } ──  │
  │                                         │
  │     ... WAL replication (continuous) ... │
  │                                         │
  │ ──── PEER_SYNC { entries } ───────────>  │
  │ <─── PEER_ACK { wal_offset } ─────────  │
  │                                         │
  │    (graceful departure)                 │
  │ ──── PEER_LEAVE { reason } ───────────>  │
  │ ──── TCP FIN ────────────────────────>   │
```

**Timeout rules:**
- CONNECT must arrive within 5 seconds of TLS completion, else broker closes
- PING/PONG timeout: 30 seconds (configurable)
- Idle connection without PING: broker sends PING after `keepalive` interval
- Peer connections: PEER_PING interval defaults to 200ms (gossip protocol period)

## 6. Delivery Semantics on the Wire

### Publisher → Broker (PUB/ACK)

```
Publisher                      Broker
  │                              │
  │ ── PUB(id=X, topic, data) ──>│
  │                              │  1. Dedup check
  │                              │  2. WAL write + fsync
  │                              │  3. Memory buffer insert
  │ <── ACK(id=X, "stored") ──── │
  │                              │
  │  (if no ACK within 5s)       │
  │ ── PUB(id=X, same) ────────> │  (dedup: already stored)
  │ <── ACK(id=X, "duplicate") ──│
```

### Broker → Consumer (DELIVER/ACK)

Broker reuses PUB frame type (0x03) for delivery with an additional header:

```
{
  "topic":       "order.created",
  "data":        <original payload>,
  "headers":     { ...original headers... },
  "delivery": {                            // added by broker
    "attempt":     1,                      // retry count
    "first_sent":  1700000000000,          // millis, first delivery attempt
    "msg_id":      <original msg ID>
  }
}
```

Consumer sends ACK(id=X, "done") after processing.

**Retry policy:**
- No ACK within `ack_timeout` (default 30s) → redeliver
- Backoff: 1s, 2s, 4s, 8s, 16s, 32s, 60s, 60s... (exponential, capped at 60s)
- After `max_redeliveries` (default 5) → move to DLQ

## 7. Batch Mode

When BATCH flag (bit 1) is set, the payload contains an array of messages:

```
[
  { "topic": "order.created", "data": {...} },
  { "topic": "order.updated", "data": {...} },
  { "topic": "inventory.reserved", "data": {...} }
]
```

Broker treats batch atomically:
- All messages written to WAL in a single fsync
- Single ACK for the entire batch (references the batch frame's Message ID)
- If any message fails validation → entire batch rejected

## 8. Topic Naming Convention

```
{namespace}/{segment}.{segment}.{segment}
            └──── topic within namespace ────┘

Segment rules:
- Lowercase alphanumeric + hyphens: [a-z0-9-]
- Max 63 chars per segment
- Max 10 segments
- Max 255 chars total topic length
- No leading/trailing dots

Examples:
  order.created
  payment.completed
  user.profile.updated
  inventory.warehouse-hanoi.stock-changed
```

## 9. Compression

When COMPRESSED flag is set:
1. Payload is LZ4-compressed before framing
2. `Payload Length` field reflects compressed size
3. Receiver decompresses after CRC verification
4. Compression recommended for payloads > 1KB

## 10. Protocol Negotiation

The CONNACK `features` array tells the client what the broker supports:

| Feature string | Meaning |
|----------------|---------|
| `"batch"` | Batch publish supported |
| `"compress"` | LZ4 compression supported |
| `"filter"` | Content-based filtering supported |
| `"groups"` | Consumer groups supported |
| `"request_reply"` | Request-reply pattern supported |
| `"mesh"` | Cluster mesh is active — `peers` field in CONNACK is populated |
| `"memory_mode"` | Broker is running in memory-only durability (no WAL) |
| `"raw_payload"` | Broker accepts raw byte payloads (ENCODING=00) without deserialization |

Client must not use features not listed in CONNACK.

## 11. Rate Limiting

The broker enforces rate limits to protect against misbehaving or misconfigured publishers.

### 11.1 Rate Limit Scopes

| Scope | Config location | Behavior |
|-------|----------------|----------|
| **Per-service** | `routes.yaml` | Limits publish rate for a specific service. Excess → ERR(4290). |
| **Per-namespace** | `routes.yaml` | Aggregate limit across all services in a namespace. |
| **Global** | `broker.yaml` | Broker-wide safety limit. |

### 11.2 Rate Limit Response

When a publisher exceeds its rate limit, the broker responds with:

```
ERR {
  "code":    4290,
  "message": "rate limited: 1000 events/sec exceeded for service 'order-service'"
}
```

The SDK handles rate limit errors by backing off automatically (exponential, starting at 100ms). Publishers can also check `Pulse::rate_limit_remaining()` proactively.

### 11.3 Token Bucket Algorithm

Rate limiting uses a token bucket per scope:
- Bucket refills at `publish_rate` tokens/sec
- Burst capacity: `burst` tokens (allows short spikes)
- When bucket is empty → reject with ERR(4290)

## 12. Security Considerations

### 12.1 HMAC Replay Window

The authentication HMAC includes a timestamp checked within a ±30 second window (§4.1). This means a captured HMAC can be replayed within that window.

**Mitigations:**
- TLS 1.3 encrypts the HMAC on the wire, preventing passive capture
- The 30s window is a trade-off between clock drift tolerance and replay risk
- For environments requiring stronger replay protection, a connection-level nonce/challenge mechanism is planned for protocol v2

### 12.2 API Key in Connection URL

Connection URLs (e.g., `pulse://service:key@broker:4222/ns`) embed the API key in plaintext. This key can leak via:
- Application logs
- Error messages and stack traces
- Process listing (`/proc/*/cmdline`)
- Monitoring and APM tools

**Recommendation:** Use the SDK builder pattern in production instead of URL strings. Reserve URL format for development and CLI tools.

### 12.3 Inter-Node TLS and Cluster Authentication

All inter-node connections (port 4223) use mutual TLS 1.3. Both sides present certificates, and certificates must be signed by the same CA or be in a shared trust store.

In addition to TLS, peer authentication uses a shared cluster secret:
- Each node is configured with the same `cluster_secret` value
- CONNECT frames on the inter-node port use `HMAC-SHA256(cluster_secret, timestamp)` for authentication (same flow as client auth, §4.1)
- The cluster secret should be rotated periodically; during rotation, both old and new secrets are accepted for a configurable grace period (default 60 seconds)

**Recommendation:** Distribute the cluster secret via a secrets manager (Vault, Kubernetes Secrets) rather than embedding it in config files. For development, a plaintext value in `broker.yaml` is acceptable.
