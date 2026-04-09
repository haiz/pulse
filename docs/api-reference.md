# HTTP Gateway API Reference

Base URL: `http://localhost:8080`

## REST Endpoints

### POST /v1/publish

Publish a single event.

**Request:**
```
POST /v1/publish
Content-Type: application/json
Authorization: Bearer <api_key>  (optional)
```

```json
{
  "topic": "order.created",
  "data": {"order_id": "ORD-001", "total": 99.99},
  "headers": {"trace_id": "abc123"}
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `topic` | string | yes | Dot-delimited topic name |
| `data` | any | yes | Event payload (any JSON value) |
| `headers` | object | no | String key-value metadata |

**Response (200):**
```json
{
  "msg_id": "019d7198-23da-7a12-949a-5e15ffe0d18f",
  "status": "stored"
}
```

**Errors:**

| Status | Body | Cause |
|--------|------|-------|
| 400 | `{"error": "...", "code": 4000}` | Invalid JSON or missing required field |
| 401 | `{"error": "unauthorized", "code": 4010}` | Invalid or missing API key |
| 500 | `{"error": "...", "code": 5000}` | Broker internal error |

---

### POST /v1/publish/batch

Publish multiple events in one request. Each event is processed independently — partial success is possible.

**Request:**
```json
{
  "events": [
    {"topic": "inventory.updated", "data": {"sku": "A", "qty": -1}},
    {"topic": "inventory.updated", "data": {"sku": "B", "qty": -2}},
    {"topic": "shipping.requested", "data": {"order_id": "ORD-001"}}
  ]
}
```

**Response (200):**
```json
{
  "results": [
    {"msg_id": "019d...", "status": "stored"},
    {"msg_id": "019d...", "status": "stored"},
    {"msg_id": "019d...", "status": "stored"}
  ]
}
```

Results are returned in the same order as the input events. Check each result's `status` — individual events can fail while others succeed.

---

### GET /v1/health

Health check endpoint. Returns 200 if the gateway is operational.

**Response (200):**
```json
{"status": "ok"}
```

Use for Kubernetes readiness/liveness probes:
```yaml
livenessProbe:
  httpGet:
    path: /v1/health
    port: 8080
  periodSeconds: 10
```

---

### GET /v1/info

Gateway and broker metadata.

**Response (200):**
```json
{
  "version": "0.1.0",
  "broker_id": "pulse-1",
  "gateway_mode": "sidecar"
}
```

---

### GET /v1/topics

List active topics. (Currently returns an empty list — full implementation in future release.)

**Response (200):**
```json
{"topics": []}
```

---

## WebSocket Endpoint

### WS /v1/subscribe

Persistent WebSocket connection for subscribing to events. Events are pushed from the server as they match subscriptions.

**Connect:**
```
ws://localhost:8080/v1/subscribe
ws://localhost:8080/v1/subscribe?token=<api_key>
```

All messages are JSON. The `type` field determines the message kind.

---

### Client Messages (Client → Server)

#### Subscribe

```json
{
  "type": "sub",
  "topic": "order.*",
  "sub_id": "unique-subscription-id",
  "group": "order-processors",
  "filter": "amount > 100"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `topic` | string | yes | Topic pattern (supports `*` and `>` wildcards) |
| `sub_id` | string | yes | Client-chosen unique ID for this subscription |
| `group` | string | no | Consumer group name (load balances across members) |
| `filter` | string | no | Content filter expression |

#### Unsubscribe

```json
{
  "type": "unsub",
  "sub_id": "unique-subscription-id"
}
```

#### Acknowledge

```json
{
  "type": "ack",
  "msg_id": "019d7198-23da-..."
}
```

Send after successfully processing an event. If not sent within the ACK timeout (default 30s), the broker re-delivers the event.

#### Ping

```json
{"type": "ping"}
```

---

### Server Messages (Server → Client)

#### Event Delivery

```json
{
  "type": "event",
  "msg_id": "019d7198-23da-7a12-949a-5e15ffe0d18f",
  "topic": "order.created",
  "data": {"order_id": "ORD-001", "total": 99.99},
  "headers": {"trace_id": "abc123"},
  "attempt": 1
}
```

| Field | Type | Description |
|-------|------|-------------|
| `msg_id` | string | Unique message ID (UUIDv7) |
| `topic` | string | The actual topic (not the pattern) |
| `data` | any | Event payload |
| `headers` | object | Event metadata headers |
| `attempt` | number | Delivery attempt (1 = first delivery) |

#### Subscription Confirmed

```json
{
  "type": "subscribed",
  "sub_id": "unique-subscription-id",
  "topic": "order.*"
}
```

#### Error

```json
{
  "type": "error",
  "code": 4030,
  "message": "forbidden: cannot subscribe to order.*"
}
```

#### Pong

```json
{"type": "pong"}
```

---

## Filter Expression Syntax

Used in WebSocket `sub` messages and in `routes.yaml`.

### Operators

```
field == "value"          String equality
field != "value"          String inequality
field > 100               Numeric comparison
field >= 100              Numeric comparison
field < 100               Numeric comparison
field <= 100              Numeric comparison
```

### Logic

```
expr AND expr             Both must be true
expr OR expr              Either can be true
NOT expr                  Negate
(expr)                    Grouping
```

### Functions

```
contains(field, "substr")         String contains
starts_with(field, "prefix")      String prefix
ends_with(field, "suffix")        String suffix
in(field, ["a", "b", "c"])        Value in set
```

### Examples

```
amount > 1000
status == "active" AND region == "VN"
NOT status == "deleted"
contains(name, "test") OR starts_with(email, "admin")
in(status, ["active", "pending"]) AND amount >= 50
```

### Field Paths

Fields use dot notation to access nested values:
```
payload.amount
payload.customer.email
payload.items
```

---

## Topic Pattern Matching

| Pattern | Description | Example Matches |
|---------|-------------|-----------------|
| `order.created` | Exact match | `order.created` only |
| `order.*` | Single segment wildcard | `order.created`, `order.updated` |
| `order.>` | Multi-segment wildcard | `order.created`, `order.us.created` |
| `>` | Match everything | All topics |

Wildcards apply at the segment level (segments separated by `.`). `*` matches exactly one segment, `>` matches one or more.
