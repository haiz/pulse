# Integration Guide

How to integrate Pulse into your services, with examples in every supported language.

## Choose Your Integration Path

| Path | Best For | Latency | Setup |
|------|----------|---------|-------|
| **HTTP REST** | Any language, simplest | ~2-10ms | Zero — just HTTP calls |
| **WebSocket** | Real-time consumers | ~2-10ms | WebSocket library |
| **Rust SDK** | Rust services, max performance | ~27µs | `cargo add pulse-sdk` |
| **Python SDK** | Python services (PyO3) | ~50µs | `pip install pulse-py` |
| **C FFI** | C/C++/Zig, embedding | ~50µs | Link `libpulse_ffi` |

**Rule of thumb**: Use HTTP for publishing (stateless, simple). Use WebSocket or native SDK for subscribing (needs persistent connection for event delivery).

---

## HTTP REST (All Languages)

The HTTP gateway translates JSON requests to the Pulse binary protocol. Works with any language that has an HTTP client.

### Publish

```
POST http://localhost:8080/v1/publish
Content-Type: application/json
Authorization: Bearer <api_key>  (optional)

{
  "topic": "order.created",
  "data": {"order_id": "ORD-001", "total": 99.99},
  "headers": {"trace_id": "abc123"}
}
```

Response:
```json
{"msg_id": "019d7198-23da-...", "status": "stored"}
```

### Batch Publish

```
POST http://localhost:8080/v1/publish/batch
Content-Type: application/json

{
  "events": [
    {"topic": "inventory.updated", "data": {"sku": "A", "qty": -1}},
    {"topic": "inventory.updated", "data": {"sku": "B", "qty": -2}}
  ]
}
```

Response:
```json
{
  "results": [
    {"msg_id": "...", "status": "stored"},
    {"msg_id": "...", "status": "stored"}
  ]
}
```

### Error Handling

| Status | Meaning | Action |
|--------|---------|--------|
| 200 | Success | Event stored |
| 400 | Bad request | Fix request body |
| 401 | Unauthorized | Check API key |
| 413 | Payload too large | Reduce payload size (max 1MB default) |
| 429 | Rate limited | Back off and retry |
| 500 | Internal error | Retry with exponential backoff |

---

## Python

### Option A: HTTP Gateway (simplest, no native deps)

```python
import json
import urllib.request

GATEWAY = "http://localhost:8080"

def publish(topic: str, data: dict, headers: dict = None) -> dict:
    body = json.dumps({
        "topic": topic,
        "data": data,
        "headers": headers or {},
    }).encode()
    req = urllib.request.Request(
        f"{GATEWAY}/v1/publish",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())

# Publish events
result = publish("order.created", {"order_id": "ORD-001", "total": 99.99})
print(f"Published: {result['msg_id']}")

# With requests library (even simpler)
import requests

r = requests.post(f"{GATEWAY}/v1/publish", json={
    "topic": "payment.completed",
    "data": {"order_id": "ORD-001", "amount": 99.99}
})
print(r.json())
```

### Option B: Native SDK (PyO3 — higher performance)

```bash
cd sdks/python
pip install maturin
maturin develop
```

```python
from pulse_py import Pulse

client = Pulse.connect("127.0.0.1:4222", "payment-service", "default")

# Publish
msg_id = client.publish("payment.completed", {
    "order_id": "ORD-001",
    "amount": 99.99,
    "method": "credit_card",
})
print(f"Published: {msg_id}")

# Subscribe
client.subscribe("order.*")

# With headers
msg_id = client.publish("audit.log", {"action": "payment"},
    headers={"trace_id": "abc123"})
```

---

## Node.js / TypeScript

### Option A: HTTP Gateway (no deps beyond fetch)

```javascript
const GATEWAY = "http://localhost:8080";

// Publish
async function publish(topic, data, headers = {}) {
  const resp = await fetch(`${GATEWAY}/v1/publish`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ topic, data, headers }),
  });
  return resp.json();
}

await publish("order.created", { id: 42, total: 99.99 });
```

### Option B: WebSocket Subscribe

```javascript
const ws = new WebSocket("ws://localhost:8080/v1/subscribe");

ws.onopen = () => {
  // Subscribe to topic patterns
  ws.send(JSON.stringify({
    type: "sub",
    topic: "order.*",
    sub_id: "order-handler",
  }));

  // Subscribe to another pattern with consumer group
  ws.send(JSON.stringify({
    type: "sub",
    topic: "payment.>",
    sub_id: "payment-handler",
    group: "payment-workers",  // load balance across group members
  }));
};

ws.onmessage = (e) => {
  const msg = JSON.parse(e.data);

  switch (msg.type) {
    case "subscribed":
      console.log(`Subscribed to ${msg.topic} (${msg.sub_id})`);
      break;

    case "event":
      console.log(`[${msg.topic}] ${JSON.stringify(msg.data)}`);
      // ACK the event (success)
      ws.send(JSON.stringify({ type: "ack", msg_id: msg.msg_id }));
      break;

    case "error":
      console.error(`Error ${msg.code}: ${msg.message}`);
      break;
  }
};
```

### Option C: SDK Package (typed, auto-reconnect)

```bash
cd sdks/typescript
npm install
npm run build
```

```typescript
import { Pulse } from 'pulse-client';

const client = new Pulse({
  url: 'http://localhost:8080',
  apiKey: 'psk_live_abc',
  autoReconnect: true,
});

// Publish via HTTP
const result = await client.publish('order.created', { id: 42 });
console.log(result.msgId);

// Subscribe via WebSocket
client.subscribe('order.*', async (event) => {
  console.log(event.topic, event.data);
  event.ack();
});

// Batch publish
await client.publishBatch([
  { topic: 'inventory.updated', data: { sku: 'A', qty: -1 } },
  { topic: 'inventory.updated', data: { sku: 'B', qty: -2 } },
]);
```

---

## Go

### HTTP Gateway

```go
package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "net/http"
)

const gateway = "http://localhost:8080"

func publish(topic string, data any) (map[string]any, error) {
    body, _ := json.Marshal(map[string]any{
        "topic": topic,
        "data":  data,
    })
    resp, err := http.Post(gateway+"/v1/publish", "application/json", bytes.NewReader(body))
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    var result map[string]any
    json.NewDecoder(resp.Body).Decode(&result)
    return result, nil
}

func main() {
    result, err := publish("order.created", map[string]any{
        "order_id": "ORD-001",
        "total":    99.99,
    })
    if err != nil {
        panic(err)
    }
    fmt.Printf("Published: %s\n", result["msg_id"])
}
```

### SDK Package (typed, health checks)

```go
import "github.com/pulse/pulse-go"

client := pulse.NewClient("http://localhost:8080", pulse.Options{
    APIKey: "psk_live_abc",
})

// Publish
result, _ := client.Publish("order.created", map[string]any{
    "order_id": "ORD-001",
    "total":    99.99,
})
fmt.Println(result.MsgID)

// Batch
client.PublishBatch([]struct{...}{
    {Topic: "a", Data: map[string]any{"x": 1}},
    {Topic: "b", Data: map[string]any{"x": 2}},
})

// Health check
healthy, _ := client.Health()
```

---

## Rust (Native SDK)

The highest performance option — communicates directly via binary TCP protocol.

```toml
# Cargo.toml
[dependencies]
pulse-sdk = { path = "crates/pulse-sdk" }
tokio = { version = "1", features = ["full"] }
rmpv = "1"
```

```rust
use pulse_sdk::{PulseBuilder, PulseError};

#[tokio::main]
async fn main() -> Result<(), PulseError> {
    // Connect
    let mut client = PulseBuilder::new("order-service", "ecommerce")
        .addr("127.0.0.1:4222".parse().unwrap())
        .api_key("psk_live_abc")
        .auto_reconnect(true)       // reconnect on disconnect
        .dedup_capacity(10_000)     // consumer-side dedup cache
        .connect()
        .await?;

    // Publish
    let msg_id = client.publish(
        "order.created",
        rmpv::Value::Map(vec![
            (rmpv::Value::String("order_id".into()), rmpv::Value::String("ORD-001".into())),
            (rmpv::Value::String("total".into()), rmpv::Value::F64(99.99)),
        ]),
        None,
    ).await?;
    println!("Published: {msg_id}");

    // Publish with retry (same msg_id preserved across retries = dedup safe)
    client.publish_with_retry("payment.completed",
        rmpv::Value::Map(vec![]),
        None,
        3,  // max retries
    ).await?;

    // Subscribe
    client.subscribe("order.*", None).await?;

    // Consume events
    client.consume(|event| async move {
        println!("[{}] {:?}", event.topic, event.data);
        Ok(())  // ACK — return Err to NACK (triggers retry)
    }).await?;

    Ok(())
}
```

---

## Common Patterns

### Retry with Exponential Backoff

```python
import time

def publish_with_retry(topic, data, max_retries=3):
    for attempt in range(max_retries + 1):
        try:
            return publish(topic, data)
        except Exception as e:
            if attempt == max_retries:
                raise
            time.sleep(min(2 ** attempt, 30))
```

### Service Health Check

```bash
# Quick health check for monitoring/readiness probes
curl -sf http://localhost:8080/v1/health || exit 1
```

### Multiple Topic Subscriptions

```javascript
// Subscribe to multiple patterns on the same WebSocket
ws.send(JSON.stringify({ type: "sub", topic: "order.*", sub_id: "s1" }));
ws.send(JSON.stringify({ type: "sub", topic: "payment.*", sub_id: "s2" }));
ws.send(JSON.stringify({ type: "sub", topic: "notification.>", sub_id: "s3" }));
```

### Content Filtering (Server-side)

Subscribe only to events matching a filter expression:

```javascript
ws.send(JSON.stringify({
  type: "sub",
  topic: "order.created",
  sub_id: "large-orders",
  filter: "amount > 1000 AND region == 'VN'",
}));
```

Filter syntax: `field > value`, `field == "string"`, `AND`, `OR`, `NOT`, `contains()`, `starts_with()`, `in()`.

---

## Configuration

### Authentication (optional)

Create `config/services.yaml`:

```yaml
namespaces:
  ecommerce:
    services:
      order-service:
        key: "psk_live_order_xxx"
        permissions:
          publish: ["order.*"]
          subscribe: ["payment.*"]
```

Use the key in requests:
```bash
# HTTP
curl -H "Authorization: Bearer psk_live_order_xxx" ...

# WebSocket
ws://localhost:8080/v1/subscribe?token=psk_live_order_xxx

# Rust SDK
PulseBuilder::new("order-service", "ecommerce")
    .api_key("psk_live_order_xxx")

# Python SDK
Pulse.connect("127.0.0.1:4222", "order-service", "ecommerce", "psk_live_order_xxx")
```

### Server-side Routing Rules

Create `config/routes.yaml`:

```yaml
routes:
  - name: "fraud-check"
    match:
      topic: "order.created"
      where: "payload.amount > 1000"
    deliver:
      - service: "fraud-detection"

  - name: "all-to-analytics"
    match:
      topic: ">"
    deliver:
      - service: "analytics"
```

---

## Testing Your Integration

```bash
# Start the system
cargo run -p pulse-demo

# Test publish
curl -X POST http://localhost:8080/v1/publish \
  -H 'Content-Type: application/json' \
  -d '{"topic":"test.integration","data":{"from":"your-service"}}'

# Run multi-language demo
./demo/run.sh

# Run load test (concurrent stress)
python3 demo/load_test.py
```
