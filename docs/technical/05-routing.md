# Routing Pipeline Design

## 1. Overview

The routing pipeline determines which consumers receive which events. It evaluates in order:

```
Event arrives
  │
  ▼
┌──────────────┐     ┌───────────────┐     ┌─────────────┐     ┌──────────┐
│ Topic Match  │────>│ Content Filter │────>│  Transform  │────>│ Fan-out  │
│              │     │  (optional)    │     │ (optional)  │     │          │
└──────────────┘     └───────────────┘     └─────────────┘     └──────────┘
```

## 2. Topic Matching

### 2.1 Subscription-Based Matching

Every SUB frame registers a topic pattern. The routing engine maintains a concurrent map:

```rust
type RoutingTable = DashMap<TopicPattern, Vec<SubscriptionTarget>>;

pub struct SubscriptionTarget {
    pub consumer_id: ConsumerId,
    pub sub_id: SubId,
    pub group: Option<String>,         // consumer group name
    pub filter: Option<CompiledFilter>, // content-based filter
    pub deliver_tx: mpsc::Sender<DeliveryEvent>,
}
```

### 2.2 Wildcard Matching Algorithm

Three levels of matching, checked in priority order:

| Pattern | Matches | Example |
|---------|---------|---------|
| Exact: `order.created` | Only `order.created` | `order.created` ✓, `order.updated` ✗ |
| Single wildcard: `order.*` | Any single segment after `order.` | `order.created` ✓, `order.us.created` ✗ |
| Multi wildcard: `order.>` | One or more segments after `order.` | `order.created` ✓, `order.us.created` ✓ |
| Global: `*` | All topics | Everything ✓ |

**Implementation**: trie-based lookup for O(segments) matching:

```rust
pub struct TopicTrie {
    exact: HashMap<String, Vec<SubscriptionTarget>>,
    children: HashMap<String, TopicTrie>,
    single_wildcard: Vec<SubscriptionTarget>,    // "*" at this level
    multi_wildcard: Vec<SubscriptionTarget>,      // ">" at this level
}

impl TopicTrie {
    pub fn resolve(&self, topic: &str) -> Vec<&SubscriptionTarget> {
        let segments: Vec<&str> = topic.split('.').collect();
        let mut results = Vec::new();
        self.resolve_recursive(&segments, 0, &mut results);
        results
    }

    fn resolve_recursive(
        &self,
        segments: &[&str],
        depth: usize,
        results: &mut Vec<&SubscriptionTarget>,
    ) {
        // Multi-wildcard ">" matches everything from here down
        results.extend(&self.multi_wildcard);

        if depth >= segments.len() {
            // Reached end of topic — collect exact matches at this node
            if let Some(targets) = self.exact.get("") {
                results.extend(targets);
            }
            // Single wildcard at final position also matches
            results.extend(&self.single_wildcard);
            return;
        }

        let segment = segments[depth];
        let is_last = depth == segments.len() - 1;

        // Exact segment match: descend into child
        if let Some(child) = self.children.get(segment) {
            child.resolve_recursive(segments, depth + 1, results);
        }

        // Single wildcard "*": matches exactly this one segment
        // Must recurse into wildcard child for deeper matching (e.g., "*.created")
        if let Some(wildcard_child) = self.children.get("*") {
            wildcard_child.resolve_recursive(segments, depth + 1, results);
        }
        // Also collect single wildcard subscriptions at this level if this is the last segment
        if is_last {
            results.extend(&self.single_wildcard);
        }
    }
}
```

### 2.3 Consumer Groups

When multiple consumers subscribe with the same `group` name, each event is delivered to exactly ONE member of the group (load balancing):

```
SUB: { topic: "order.created", group: "payment-processors" }
  → Service payment-1 subscribes
  → Service payment-2 subscribes
  → Service payment-3 subscribes

Event "order.created" arrives:
  → Router finds group "payment-processors" has 3 members
  → Select one via round-robin (or least-loaded)
  → Deliver to ONLY that member

If that member NACKs or times out:
  → Retry to SAME member first (preserve ordering)
  → After 2 consecutive failures: rebalance to another group member
```

**Partition key support**: SUB frames can include a `partition_key` field to guarantee that events with the same key value always route to the same group member. This preserves ordering within a partition key while allowing parallel processing across different keys.

```
SUB: { topic: "order.created", group: "payment-processors", partition_key: "payload.user_id" }
  → Service payment-1 subscribes
  → Service payment-2 subscribes
  → Service payment-3 subscribes

Event "order.created" { user_id: "u_42", ... } arrives:
  → hash("u_42") % 3 = 1  → always delivered to payment-2
  → All events for user_id "u_42" go to the same member
  → Events for user_id "u_99" may go to a different member
```

Hash-based assignment: `member = members[hash(partition_key_value) % members.len()]`. The partition key is a dot-delimited field path resolved against the deserialized payload (same path syntax as content filters). If the field is missing or the payload is raw bytes, the event falls back to round-robin.

**Implementation**:

```rust
pub struct ConsumerGroup {
    name: String,
    members: Vec<SubscriptionTarget>,
    next_index: AtomicU64,           // round-robin counter
    partition_key: Option<FieldPath>, // e.g., "payload.user_id"
}

impl ConsumerGroup {
    pub fn select(&self, payload: Option<&msgpack::Value>) -> &SubscriptionTarget {
        // If partition key is configured and payload is available, use hash-based assignment
        if let (Some(key_path), Some(payload)) = (&self.partition_key, payload) {
            if let Some(key_value) = resolve_field_path(key_path, payload) {
                let hash = hash_value(&key_value);
                return &self.members[hash as usize % self.members.len()];
            }
        }
        // Fallback: round-robin
        let idx = self.next_index.fetch_add(1, Ordering::Relaxed);
        &self.members[idx as usize % self.members.len()]
    }
}
```

### 2.4 Consumer Groups & Ordering Trade-off

> **Important**: Consumer groups break the per-publisher ordering guarantee described in the overview — unless a partition key is used.

When events are distributed across group members via round-robin (no partition key), the processing order depends on each member's speed. Two events published in order (A, B) may be processed as (B, A) if member-1 (handling A) is slower than member-2 (handling B).

**Ordering behavior depends on partition key configuration:**

| Mode | Ordering | Use Case |
|------|----------|----------|
| No partition key (default) | Round-robin, ordering **not** guaranteed | Independent events, max throughput |
| With partition key | Ordering guaranteed for events with the **same key value** | User-scoped workflows, order processing |

**When ordering matters within a group:**
- Use a partition key (§2.3) — events with the same key always route to the same group member, preserving order for that key
- Use a single group member (simple but defeats parallelism)
- Design idempotent handlers that don't depend on ordering

**When to use consumer groups without partition key:**
- Event processing is independent (no causal relationship between events)
- Throughput is more important than ordering
- Processing is idempotent and commutative

## 3. Content-Based Filtering

> **Encoding requirement**: Content-based filtering only works when the payload encoding is MsgPack or JSON (ENCODING flags in the frame header). If encoding is raw bytes (`0x00`), content filters are skipped — events pass through to all matching subscribers. This is by design: raw bytes = maximum performance, structured = filtering capability.

### 3.1 Filter Expression Language

Filters operate on the deserialized event payload. Syntax:

```
Expression = Comparison | Logic | Function

Comparison:
  payload.field.nested > 1000
  payload.status == "active"
  payload.tags != null

Logic:
  expr AND expr
  expr OR expr
  NOT expr
  (expr)

Functions:
  contains(payload.name, "test")
  starts_with(payload.region, "VN")
  ends_with(payload.email, "@company.com")
  len(payload.items) > 5
  in(payload.status, ["active", "pending"])

Operators:
  ==  !=  >  <  >=  <=

Types:
  String:  "hello"
  Number:  42, 3.14
  Bool:    true, false
  Null:    null
  Array:   ["a", "b"]  (only in `in()` function)
```

### 3.2 Filter Compilation

Filters in SUB frames and routes.yaml are compiled once into an AST at registration time, then evaluated per-event:

```rust
pub enum FilterExpr {
    Compare {
        left: FieldPath,       // e.g., "payload.amount"
        op: CompareOp,         // Gt, Lt, Eq, Neq, Gte, Lte
        right: Value,          // literal value
    },
    Logic {
        op: LogicOp,           // And, Or
        left: Box<FilterExpr>,
        right: Box<FilterExpr>,
    },
    Not(Box<FilterExpr>),
    Function {
        name: FunctionName,    // Contains, StartsWith, EndsWith, Len, In
        args: Vec<FilterArg>,
    },
}

pub struct CompiledFilter {
    ast: FilterExpr,
}

impl CompiledFilter {
    pub fn evaluate(&self, payload: &msgpack::Value) -> bool {
        self.eval_expr(&self.ast, payload)
    }

    fn eval_expr(&self, expr: &FilterExpr, payload: &msgpack::Value) -> bool {
        match expr {
            FilterExpr::Compare { left, op, right } => {
                let field_value = self.resolve_path(left, payload);
                self.compare(field_value, op, right)
            }
            FilterExpr::Logic { op, left, right } => {
                match op {
                    LogicOp::And => self.eval_expr(left, payload) && self.eval_expr(right, payload),
                    LogicOp::Or => self.eval_expr(left, payload) || self.eval_expr(right, payload),
                }
            }
            FilterExpr::Not(inner) => !self.eval_expr(inner, payload),
            FilterExpr::Function { name, args } => self.eval_function(name, args, payload),
        }
    }
}
```

### 3.3 Filter Performance

- Filter compilation: ~1μs (done once at SUB time)
- Filter evaluation: ~1-10μs per event (depends on expression complexity)
- Field path resolution: uses MessagePack in-place traversal (no full deserialization)

## 4. Transform Pipeline

> **Encoding requirement**: Transforms only work on structured payloads (MsgPack or JSON encoding). If the payload encoding is raw bytes (`0x00`), transforms are skipped and the payload is delivered as-is. Same rationale as content filtering: raw bytes are opaque to the broker.

### 4.1 Transform Operations

Transforms modify the event payload before delivery. Defined in routes.yaml:

```yaml
transform:
  - set: "payload.warehouse = 'hanoi-wh-01'"      # add/overwrite field
  - remove: "payload.customer.credit_card"          # remove sensitive field
  - rename: "payload.old_name -> payload.new_name"  # rename field
  - copy: "payload.id -> headers.order_id"          # copy value to headers
  - default: "payload.priority = 'normal'"           # set only if field missing
```

### 4.2 Transform Implementation

```rust
pub enum TransformOp {
    Set { path: FieldPath, value: Value },
    Remove { path: FieldPath },
    Rename { from: FieldPath, to: FieldPath },
    Copy { from: FieldPath, to: FieldPath },
    Default { path: FieldPath, value: Value },
}

pub struct TransformPipeline {
    ops: Vec<TransformOp>,
}

impl TransformPipeline {
    /// Apply transforms to a CLONE of the payload.
    /// Original event in WAL is never modified.
    pub fn apply(&self, payload: &mut msgpack::Value) {
        for op in &self.ops {
            match op {
                TransformOp::Set { path, value } => {
                    set_field(payload, path, value.clone());
                }
                TransformOp::Remove { path } => {
                    remove_field(payload, path);
                }
                TransformOp::Rename { from, to } => {
                    if let Some(val) = remove_field(payload, from) {
                        set_field(payload, to, val);
                    }
                }
                TransformOp::Copy { from, to } => {
                    if let Some(val) = get_field(payload, from) {
                        set_field(payload, to, val.clone());
                    }
                }
                TransformOp::Default { path, value } => {
                    if get_field(payload, path).is_none() {
                        set_field(payload, path, value.clone());
                    }
                }
            }
        }
    }
}
```

**Important**: transforms operate on a clone of the payload. The original event stored in WAL is immutable. Different consumers may receive different transformed payloads from the same event.

## 5. Route Configuration (routes.yaml)

### 5.1 Full Configuration Reference

```yaml
routes:
  # Simple topic subscription — mostly handled by SUB frames.
  # Routes in YAML are for broker-side logic that doesn't depend on consumers.

  # ─── Content-based routing ───
  - name: "large-orders-fraud-check"
    description: "Route orders > $1000 to fraud detection"
    match:
      topic: "order.created"
      where: "payload.amount > 1000"
    deliver:
      - service: "fraud-detection"
    enabled: true

  # ─── Transform + filter ───
  - name: "order-to-warehouse-vn"
    match:
      topic: "order.created"
      where: "payload.region == 'VN'"
    transform:
      - set: "payload.warehouse = 'hanoi-wh-01'"
      - remove: "payload.customer.credit_card"
      - remove: "payload.customer.phone"
    deliver:
      - service: "vn-warehouse"

  # ─── Wildcard fan-out ───
  - name: "all-events-to-analytics"
    match:
      topic: "*"
    deliver:
      - service: "analytics"
        mode: "best-effort"    # no retry on failure

  # ─── Multi-target ───
  - name: "payment-notifications"
    match:
      topic: "payment.completed"
    deliver:
      - service: "email-service"
      - service: "sms-service"
        where: "payload.amount > 5000"   # per-target filter
      - service: "accounting-service"

  # ─── Consumer group ───
  - name: "order-processing-pool"
    match:
      topic: "order.created"
    deliver:
      - group: "order-processors"
        balance: "round-robin"            # or "least-loaded"

# ─── DLQ / failure handling ───
failure_policy:
  default:
    max_retries: 5
    backoff:
      type: "exponential"
      initial_secs: 1
      max_secs: 60
      multiplier: 2.0
    dead_letter:
      topic_prefix: "dlq"                # events go to "dlq.{original_topic}"
      retention_hours: 168                # 7 days
    alert:
      webhook: "https://hooks.slack.com/services/T.../B.../xxx"
      on: ["dlq"]                         # alert when event enters DLQ

  overrides:
    - match_topic: "payment.*"
      max_retries: 10                     # more retries for payments
      alert:
        on: ["dlq", "retry_3"]           # also alert on 3rd retry
```

### 5.2 Hot Reload

```
File watcher (inotify) on routes.yaml
  │
  ▼
Parse new YAML
  │
  ├── Syntax error → log error, keep old config, emit metric
  │
  ├── Semantic error (unknown service, invalid filter) → same
  │
  └── Valid → 
        1. Compile all new filter expressions
        2. Build new RoutingConfig struct
        3. ArcSwap::store(new_config)
        4. Log: "Routes reloaded: 5 rules active"
        5. Emit metric: pulse_config_reload_total{status="success"}

In-flight events: processed with the config that was active when they entered the pipeline. No mid-event config switch.
```

## 6. Routing Resolution Algorithm

When an event enters the routing stage:

```rust
pub fn resolve(&self, topic: &str, payload: &Value) -> Vec<DeliveryTarget> {
    let mut targets = Vec::new();

    // 1. Subscription-based targets (from SUB frames)
    let sub_targets = self.trie.resolve(topic);
    for target in sub_targets {
        // Apply per-subscription content filter
        if let Some(filter) = &target.filter {
            if !filter.evaluate(payload) {
                continue; // filtered out
            }
        }
        targets.push(target.to_delivery_target());
    }

    // 2. Route-config targets (from routes.yaml)
    for route in self.config.load().routes.iter() {
        if !route.enabled { continue; }

        // Topic match
        if !route.match_topic.matches(topic) { continue; }

        // Content filter
        if let Some(filter) = &route.compiled_filter {
            if !filter.evaluate(payload) { continue; }
        }

        // Apply transforms (clone payload)
        let mut transformed_payload = payload.clone();
        if let Some(pipeline) = &route.transform {
            pipeline.apply(&mut transformed_payload);
        }

        // Add delivery targets
        for deliver in &route.deliver {
            // Per-target filter — intentionally evaluates against ORIGINAL payload,
            // not the transformed version. This ensures filters are predictable and
            // independent of transform ordering. Transforms are cosmetic (for delivery),
            // while filters are semantic (for routing decisions).
            if let Some(target_filter) = &deliver.compiled_filter {
                if !target_filter.evaluate(payload) { continue; }
            }

            targets.push(DeliveryTarget {
                consumer_id: deliver.service.clone(),
                group: deliver.group.clone(),
                payload: transformed_payload.clone(),
                mode: deliver.mode,
            });
        }
    }

    // 3. Deduplicate targets (same consumer shouldn't receive twice)
    targets.dedup_by_key(|t| t.consumer_id.clone());

    // 4. Handle consumer groups — pick one member per group
    self.resolve_groups(&mut targets);

    targets
}
```

## 7. Unrouted Events

If an event matches no subscription and no route:

```yaml
# broker.yaml
unrouted_policy: "log"    # "log" | "reject" | "store"

# "log":    WAL stores event, log warning, no delivery. Event stays in WAL until compacted.
# "reject": Send ERR to publisher. Event NOT written to WAL.
# "store":  WAL stores, tag as unrouted. Queryable via admin API.
#           If a matching subscription arrives later, event is NOT retroactively delivered.
```

Default: `"log"` — safest option. Event is durable (if a subscription appears during the event's buffer window, it may be deliverable).

## 8. Performance Expectations

| Operation | Complexity | Latency |
|-----------|-----------|---------|
| Topic trie lookup | O(number of topic segments) | <1μs |
| Content filter evaluation | O(expression depth × field depth) | 1-10μs |
| Transform pipeline | O(number of ops) | 1-5μs |
| Full route resolution — local (topic owned by this node) | — | 5-20μs |
| Full route resolution — remote (forward to topic owner) | — | 5-20μs + 0.1-1ms network hop |
| Route config reload | O(number of rules × compilation) | 1-10ms (background) |

Local routing (topic owned by the receiving node) has the same performance as single-node operation. When a PUB arrives at a node that does not own the topic, the event is forwarded to the owner node, adding one network hop (typically 0.1-1ms on the same network). The SDK minimizes this by publishing directly to the topic leader when the topology is known (see SDK §3.5).

## 9. Rate Limiting Configuration

Rate limits are defined in `routes.yaml` and enforced at the pipeline ingest stage, before dedup or WAL write.

```yaml
# routes.yaml
rate_limits:
  per_service:
    order-service:
      publish_rate: 1000    # events/sec
      burst: 2000           # token bucket burst capacity
    analytics-service:
      publish_rate: 100
      burst: 200

  per_namespace:
    ecommerce:
      total_rate: 5000      # aggregate across all services
      burst: 10000
    internal-tools:
      total_rate: 1000
      burst: 2000
```

Rate limits use a **token bucket** algorithm (see protocol doc §11.3). When a service exceeds its limit, the broker returns ERR(4290) without writing to WAL — no resources wasted on rate-limited events.

Rate limit config is hot-reloadable alongside other route config.

## 10. Distributed Routing

In a multi-node Pulse cluster, topics are distributed across nodes using consistent hashing. The routing pipeline extends to handle cross-node event delivery.

### 10.1 Topic Ownership

Each topic has a "home" node determined by consistent hashing over the topic name:

```
home_node = consistent_hash(topic_name) → node in ring
```

The home node is the authoritative owner for that topic. It stores the topic's subscription list, manages consumer groups, and handles delivery for all subscribers (local and remote).

### 10.2 Subscription Registration

When a consumer on node A subscribes to a topic owned by node B:

```
Consumer → SUB("order.created") → Node A (local)
  → Node A looks up topic owner: Node B
  → Node A forwards SUB to Node B
  → Node B registers the subscription with a remote delivery target (node A)
  → Events for "order.created" are delivered by Node B → forwarded to Node A → delivered to consumer
```

Subscriptions are always stored on the home node for the topic, regardless of where the consumer is connected. This ensures a single source of truth for routing decisions.

### 10.3 Cross-Node Publish Flow

```
Publisher → PUB("order.created") → Node A
  │
  ├── Topic owned by Node A? → route locally (same as single-node)
  │
  └── Topic owned by Node B? → forward PUB to Node B
        → Node B writes to WAL
        → Node B runs routing pipeline
        → Node B delivers to local subscribers directly
        → Node B forwards to remote subscribers (e.g., consumer on Node A)
```

The SDK optimizes this by maintaining a local topology view and publishing directly to the topic leader when possible (see SDK §3.5), avoiding the proxy hop.

### 10.4 Subscription Fan-Out

The home node tracks all subscribers for its owned topics, both local and remote:

```rust
pub struct SubscriptionTarget {
    pub consumer_id: ConsumerId,
    pub sub_id: SubId,
    pub group: Option<String>,
    pub partition_key: Option<FieldPath>,
    pub filter: Option<CompiledFilter>,
    pub deliver_tx: mpsc::Sender<DeliveryEvent>,
    pub location: TargetLocation,  // Local or Remote(node_id)
}

pub enum TargetLocation {
    Local,                        // consumer connected to this node
    Remote { node_id: NodeId },   // consumer connected to another node
}
```

For remote targets, the home node sends the event over the inter-node connection. The receiving node handles final delivery to the consumer (ACK tracking, retry, etc.).

### 10.5 Route Config Replication

Route configuration (`routes.yaml`) is replicated to all nodes in the cluster. Each node stores the config via ArcSwap with the same hot-reload mechanism used for single-node operation. When any node detects a config file change, it propagates the update to all peers via gossip. All nodes converge to the same route config within one gossip cycle (typically <1s).

### 10.6 Failure & Rebalancing

When a node leaves the cluster (graceful shutdown or failure), its owned topics are redistributed to remaining nodes via consistent hashing. The new owner node:

1. Receives topic ownership from the consistent hash ring update
2. Accepts new subscriptions for those topics
3. Existing subscribers are notified to re-subscribe (SDK handles this transparently)

During rebalancing, in-flight events are safe — the WAL is replicated, so the new owner can resume delivery.
