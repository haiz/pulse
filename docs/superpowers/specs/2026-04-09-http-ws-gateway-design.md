# Pulse HTTP/WebSocket Gateway — Design Spec

**Date**: 2026-04-09  
**Status**: Approved  
**Goal**: Make Pulse accessible from any language via standard HTTP and WebSocket, eliminating the need for custom binary protocol implementations.

## Problem

Pulse only speaks a custom binary TCP protocol. Non-Rust services must either implement the full protocol (frame encoding, MessagePack, CRC32C, connection management) or use the limited C FFI. This makes integration impractical for most teams.

## Solution

Add an HTTP/WebSocket gateway layer that translates standard HTTP requests and WebSocket messages into Pulse's internal protocol. Two deployment modes from the same codebase:

- **Embedded**: runs inside `pulse-broker`, zero-copy internal calls, enabled via `--http-addr`
- **Sidecar**: standalone `pulse-gateway` binary, connects to broker via `pulse-sdk` over TCP

## API Design

### REST Endpoints (publish + admin)

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/v1/publish` | Publish a single event |
| `POST` | `/v1/publish/batch` | Publish multiple events atomically |
| `GET` | `/v1/topics` | List active topics |
| `GET` | `/v1/health` | Health check |
| `GET` | `/v1/info` | Broker info (version, uptime, config) |

#### POST /v1/publish

Request:
```json
{
  "topic": "order.created",
  "data": {"id": 42, "amount": 1500},
  "headers": {"trace_id": "abc123"}
}
```

Response (200):
```json
{"msg_id": "0192a3b4-...", "status": "stored"}
```

Errors: 400 (bad request), 401 (unauthorized), 403 (forbidden), 413 (payload too large), 429 (rate limited), 500 (internal error).

#### POST /v1/publish/batch

Request:
```json
{
  "events": [
    {"topic": "order.created", "data": {"id": 1}},
    {"topic": "order.created", "data": {"id": 2}}
  ]
}
```

Response (200):
```json
{
  "results": [
    {"msg_id": "...", "status": "stored"},
    {"msg_id": "...", "status": "stored"}
  ]
}
```

### WebSocket Endpoint (subscribe + consume)

Path: `WS /v1/subscribe`

Authentication: `?token=<api_key>` query parameter.

#### Client → Gateway Messages

```json
{"type": "sub", "topic": "order.*", "sub_id": "s1", "group": "workers"}
{"type": "unsub", "sub_id": "s1"}
{"type": "ack", "msg_id": "0192a3b4-..."}
{"type": "ping"}
```

#### Gateway → Client Messages

```json
{"type": "event", "msg_id": "...", "topic": "order.created", "data": {...}, "headers": {}, "attempt": 1}
{"type": "subscribed", "sub_id": "s1", "topic": "order.*"}
{"type": "error", "code": 4030, "message": "forbidden"}
{"type": "pong"}
```

### Authentication

- REST: `Authorization: Bearer <api_key>` header
- WebSocket: `?token=<api_key>` query param on connect
- Both validate against `services.yaml` (same auth as binary protocol)
- Anonymous mode (no auth) when running without `services.yaml`

## Architecture

### Crate Structure

```
crates/pulse-gateway/
  Cargo.toml
  src/
    lib.rs          — GatewayServer, shared config
    rest.rs         — REST handler (publish, batch, topics, health, info)
    websocket.rs    — WebSocket handler (sub/unsub/ack/ping)
    auth.rs         — Extract + validate Bearer token / query param
    types.rs        — JSON request/response types (serde)
    main.rs         — Sidecar binary (connects via pulse-sdk)
```

### Embedded Mode

`pulse-broker` adds `pulse-gateway` as an optional dependency. When `--http-addr` is provided, the broker spawns the gateway HTTP server in the same tokio runtime. The gateway calls broker internals directly (dispatcher, router) without TCP serialization.

### Sidecar Mode

`pulse-gateway` binary connects to the broker via `pulse-sdk` (TCP). Each HTTP request translates to SDK calls. WebSocket connections maintain their own `pulse-sdk` client with subscriptions.

### Dependencies

- `axum` — HTTP framework (built on hyper + tower, WebSocket support included)
- `serde_json` — JSON serialization
- `pulse-sdk` — for sidecar mode
- `pulse-broker` — for embedded mode (optional dependency)

## Data Flow

### Publish (REST)

```
HTTP POST /v1/publish
  → axum handler extracts JSON + auth
  → Embedded: create PubPayload, send to dispatcher channel, await ACK
  → Sidecar: sdk.publish(topic, data)
  → Return JSON {msg_id, status}
```

### Subscribe (WebSocket)

```
WS /v1/subscribe?token=...
  → axum WebSocket upgrade
  → Spawn per-connection task
  → Client sends {"type":"sub","topic":"order.*","sub_id":"s1"}
  → Embedded: register subscription in router, receive via mpsc channel
  → Sidecar: sdk.subscribe("order.*")
  → Events pushed as JSON over WS
  → Client sends {"type":"ack","msg_id":"..."}
  → Forward ACK to broker
```

## Configuration

```yaml
# broker.yaml (embedded mode)
http:
  enabled: true
  listen_addr: "0.0.0.0:8080"
  cors_origins: ["*"]
  max_body_bytes: 1048576
```

CLI flags:
- `--http-addr 0.0.0.0:8080` — enable embedded gateway
- Sidecar: `pulse-gateway --broker 127.0.0.1:4222 --listen 0.0.0.0:8080`

## Testing

- Unit tests: JSON parsing, auth extraction, request/response types
- Integration tests: start broker with embedded gateway, publish via HTTP, subscribe via WS, verify delivery
- Sidecar test: start broker + gateway, same flow over network
