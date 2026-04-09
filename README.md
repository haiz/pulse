# Pulse

High-performance event broker for reliable, low-latency inter-service communication. A lightweight alternative to RabbitMQ/Kafka, written in Rust.

```
Publisher (any language)          Pulse Broker           Subscriber (any language)
         │                            │                           │
         │  POST /v1/publish          │                           │
         │  {"topic":"order.created", │                           │
         │   "data":{"id": 42}}       │                           │
         │ ─────────────────────────► │                           │
         │                            │  route via TopicTrie      │
         │                            │  match "order.*"          │
         │   200 {"status":"stored"}  │ ─────────────────────────►│
         │ ◄───────────────────────── │   deliver event           │
```

## Features

- **Tiered durability** — Memory (~800K msg/sec), Balanced (async WAL, ~100K), Durable (fsync, exactly-once, ~10K)
- **Content-based routing** — Wildcard topics (`order.*`, `payment.>`), filter expressions (`amount > 1000 AND region == "VN"`)
- **Any language** — HTTP gateway (REST + WebSocket), native Rust SDK, Python SDK, Go/Node clients
- **Zero-config** — `pulse-broker` with no args starts a working single node
- **Clustering** — Gossip discovery (SWIM), consistent hashing, WAL replication

## Quickstart

```bash
# Build
cargo build --workspace

# Start broker + HTTP gateway (demo mode)
cargo run -p pulse-demo

# Publish from any language
curl -X POST http://localhost:8080/v1/publish \
  -H 'Content-Type: application/json' \
  -d '{"topic":"order.created","data":{"id":42,"amount":99.99}}'
```

## Integration

Pulse supports 5 integration paths — use whichever fits your stack:

### HTTP REST (any language — zero dependencies)

```bash
# Publish
curl -X POST http://localhost:8080/v1/publish \
  -H 'Content-Type: application/json' \
  -d '{"topic":"order.created","data":{"id":42}}'

# Batch publish
curl -X POST http://localhost:8080/v1/publish/batch \
  -H 'Content-Type: application/json' \
  -d '{"events":[{"topic":"a","data":{}},{"topic":"b","data":{}}]}'

# Health check
curl http://localhost:8080/v1/health
```

### Python

```python
import urllib.request, json

def publish(topic, data):
    body = json.dumps({"topic": topic, "data": data}).encode()
    req = urllib.request.Request("http://localhost:8080/v1/publish",
        data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

publish("order.created", {"id": 42, "amount": 99.99})
```

### Node.js / TypeScript

```javascript
const result = await fetch("http://localhost:8080/v1/publish", {
  method: "POST",
  headers: {"Content-Type": "application/json"},
  body: JSON.stringify({topic: "order.created", data: {id: 42}})
}).then(r => r.json());
```

### Go

```go
body, _ := json.Marshal(map[string]any{
    "topic": "order.created",
    "data":  map[string]any{"id": 42},
})
http.Post("http://localhost:8080/v1/publish", "application/json", bytes.NewReader(body))
```

### Rust (native SDK — highest performance)

```rust
let mut client = PulseBuilder::new("my-service", "default")
    .addr("127.0.0.1:4222".parse().unwrap())
    .connect().await?;

client.publish("order.created", rmpv::Value::Map(vec![
    ("id".into(), 42.into()),
]), None).await?;
```

## Architecture

```
crates/
  pulse-protocol   Wire protocol (frame encode/decode, 10 message types, CRC32C)
  pulse-broker     Broker server (pipeline, routing, delivery, WAL, auth, metrics)
  pulse-cluster    Clustering (gossip/SWIM, consistent hash, WAL replication)
  pulse-sdk        Rust SDK (client, auto-reconnect, consumer dedup)
  pulse-gateway    HTTP/WebSocket gateway (REST publish, WS subscribe)
  pulse-admin      Admin CLI (status, pub, sub, ping, config-check)
  pulse-ffi        C ABI for foreign language bindings
  pulse-demo       End-to-end demo system

sdks/
  python/          Python SDK (PyO3 bindings)
  typescript/      TypeScript SDK (HTTP/WS client)
  go/              Go SDK (HTTP client)
```

## Documentation

| Document | Contents |
|----------|----------|
| [Getting Started](docs/getting-started.md) | Installation, first pub/sub, concepts |
| [Integration Guide](docs/integration-guide.md) | Per-language setup, SDK reference, examples |
| [HTTP API Reference](docs/api-reference.md) | Gateway REST + WebSocket endpoints |
| [Deployment Guide](docs/deployment.md) | Production setup, Docker, configuration, monitoring |
| [Protocol Spec](docs/technical/01-protocol.md) | Wire protocol specification |
| [Broker Internals](docs/technical/02-broker.md) | Concurrency model, pipeline architecture |
| [Routing Design](docs/technical/05-routing.md) | Topic matching, content filters, transforms |

## Running Tests

```bash
cargo test --workspace           # 270 unit + integration tests
cargo bench -p pulse-broker      # Performance benchmarks
cargo run -p pulse-demo          # Start demo system
python3 demo/load_test.py        # Concurrent stress test
./demo/run.sh                    # Multi-language service demo
```

## License

Apache-2.0
