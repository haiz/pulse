# Getting Started

This guide takes you from zero to publishing and subscribing to events in under 5 minutes.

## Prerequisites

- Rust 1.75+ (`rustup update stable`)
- For non-Rust integration: Python 3.8+, Node.js 18+, or Go 1.21+ (any one is enough)

## 1. Build

```bash
git clone <repository-url> pulse
cd pulse
cargo build --workspace
```

## 2. Start the System

The fastest way to get a running broker + HTTP gateway:

```bash
cargo run -p pulse-demo
```

This starts:
- **Broker** on `127.0.0.1:4222` (binary TCP protocol)
- **HTTP Gateway** on `127.0.0.1:8080` (REST + WebSocket)
- **Analytics subscriber** (Rust, native TCP — listens to all events)
- **Payment subscriber** (Rust, native TCP — listens to `order.*`)

For production, start components separately:

```bash
# Broker only (zero-config mode)
cargo run -p pulse-broker

# Broker with config
cargo run -p pulse-broker -- --config config/broker.yaml

# Gateway (connects to broker)
cargo run -p pulse-gateway -- --broker 127.0.0.1:4222 --listen 0.0.0.0:8080
```

## 3. Publish Your First Event

```bash
curl -X POST http://localhost:8080/v1/publish \
  -H 'Content-Type: application/json' \
  -d '{
    "topic": "order.created",
    "data": {
      "order_id": "ORD-001",
      "customer": "John",
      "total": 99.99
    }
  }'
```

Response:
```json
{"msg_id": "019d7198-23da-7a12-...", "status": "stored"}
```

## 4. Subscribe to Events (WebSocket)

Open a WebSocket connection and send a subscribe message:

```javascript
// Browser or Node.js
const ws = new WebSocket("ws://localhost:8080/v1/subscribe");

ws.onopen = () => {
  ws.send(JSON.stringify({
    type: "sub",
    topic: "order.*",
    sub_id: "my-sub-1"
  }));
};

ws.onmessage = (e) => {
  const msg = JSON.parse(e.data);
  if (msg.type === "event") {
    console.log(`Received: ${msg.topic}`, msg.data);
    // ACK the event
    ws.send(JSON.stringify({ type: "ack", msg_id: msg.msg_id }));
  }
};
```

## 5. Verify Everything Works

```bash
# Health check
curl http://localhost:8080/v1/health
# → {"status":"ok"}

# Broker info
curl http://localhost:8080/v1/info
# → {"version":"0.1.0","broker_id":"pulse-1","gateway_mode":"sidecar"}
```

## Core Concepts

### Topics

Topics are dot-delimited strings that categorize events:

```
order.created
order.updated
payment.completed
notification.email
```

### Topic Patterns (Wildcards)

Subscribers use patterns to match topics:

| Pattern | Matches | Example |
|---------|---------|---------|
| `order.created` | Exact match only | `order.created` |
| `order.*` | One segment after `order.` | `order.created`, `order.updated` |
| `order.>` | One or more segments | `order.created`, `order.us.created` |
| `>` | Everything | All topics |

### Durability Modes

| Mode | Guarantee | Throughput | Use Case |
|------|-----------|------------|----------|
| **Memory** | At-most-once | ~800K/s | Metrics, logs, real-time analytics |
| **Balanced** | At-least-once | ~100K/s | General production (default) |
| **Durable** | Exactly-once | ~10K/s | Financial transactions, audit trails |

Set via CLI flag:
```bash
cargo run -p pulse-broker -- --durability balanced
```

### Events

An event has:
- **topic** — routing key (`order.created`)
- **data** — any JSON payload
- **msg_id** — auto-generated UUIDv7 (time-sortable)
- **headers** — optional string key-value pairs

### Consumer Groups

Multiple subscribers with the same `group` name receive events round-robin (load balancing):

```javascript
// These 3 instances share the workload
ws.send(JSON.stringify({
  type: "sub",
  topic: "order.created",
  sub_id: "s1",
  group: "order-processors"
}));
```

## Next Steps

- [Integration Guide](integration-guide.md) — per-language SDK setup and examples
- [HTTP API Reference](api-reference.md) — full gateway endpoint documentation
- [Deployment Guide](deployment.md) — production configuration, Docker, monitoring
