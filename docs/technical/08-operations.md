# Operations & Observability

## 1. Deployment

### 1.1 System Requirements

**Single node:**

| Resource | Minimum | Recommended | Notes |
|----------|---------|-------------|-------|
| CPU | 1 core | 2 cores | tokio uses all available cores |
| RAM | 512 MB | 2 GB | Depends on buffer size + connections |
| Disk | 1 GB SSD | 10 GB SSD | WAL + sled. HDD not recommended (fsync latency) |
| OS | Linux 5.4+ | Ubuntu 22.04+ | Also works on macOS for dev |
| Network | 10 Mbps | 100 Mbps | Per-event overhead: ~200 bytes + payload |

**Mesh cluster (recommended for production):**

| Resource | Minimum | Recommended | Notes |
|----------|---------|-------------|-------|
| Nodes | 3 | 3-5 | 3-node minimum for HA (tolerates 1 node failure) |
| CPU | 2 cores/node | 4 cores/node | Gossip + replication + routing overhead |
| RAM | 1 GB/node | 4 GB/node | Replication buffers + topic state per node |
| Disk | 5 GB SSD/node | 20 GB SSD/node | WAL replicated across nodes |
| Network | 100 Mbps | 1 Gbps | Inter-node replication + gossip traffic |

### 1.2 Installation

**Zero-config quick start** — no config file needed:

```bash
# Zero config — just run it
pulse-broker
# Listens on :4222, memory mode, no auth, no TLS

# With peers (instant mesh)
pulse-broker --peers 10.0.1.2:4223,10.0.1.3:4223

# Production with config
pulse-broker --config /etc/pulse/broker.yaml
```

**CLI flags reference:**

| Flag | Default | Description |
|------|---------|-------------|
| `--listen` | `:4222` | TCP listen address |
| `--durability` | `balanced` | `memory` \| `balanced` \| `durable` |
| `--data-dir` | `/var/lib/pulse` | Data directory (WAL, state) |
| `--peers` | (none) | Comma-separated peer addresses for mesh |
| `--tls-cert` | (none) | TLS certificate path |
| `--tls-key` | (none) | TLS key path |
| `--max-connections` | `10000` | Max client connections |
| `--max-payload` | `1048576` | Max message payload bytes |
| `--mem-queue-size` | `100000` | Memory ring buffer size |
| `--no-auth` | `false` | Disable authentication |
| `--admin-addr` | `:8080` | Admin/health API address |
| `--metrics-addr` | `:9090` | Prometheus metrics address |
| `--config` | (none) | Path to broker.yaml (overrides all flags) |

**Full installation options:**

```bash
# Option 1: Binary download
curl -fsSL https://github.com/pulsemq/pulse/releases/latest/download/pulse-broker-linux-amd64 \
  -o /usr/local/bin/pulse-broker
chmod +x /usr/local/bin/pulse-broker

# Option 2: Docker
docker run -d \
  --name pulse \
  -p 4222:4222 \
  -p 9090:9090 \
  -p 8080:8080 \
  -v /var/lib/pulse:/var/lib/pulse \
  -v /etc/pulse:/etc/pulse \
  ghcr.io/pulsemq/pulse-broker:latest

# Option 3: systemd service
cat > /etc/systemd/system/pulse-broker.service << 'EOF'
[Unit]
Description=Pulse Event Broker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=pulse
Group=pulse
ExecStart=/usr/local/bin/pulse-broker --config /etc/pulse/broker.yaml
Restart=always
RestartSec=5
LimitNOFILE=65536

# Security hardening
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
ReadWritePaths=/var/lib/pulse /var/log/pulse

[Install]
WantedBy=multi-user.target
EOF

systemctl enable --now pulse-broker
```

### 1.3 TLS Setup

```bash
# Using Let's Encrypt (recommended for internet-facing)
certbot certonly --standalone -d pulse.company.com
# Cert: /etc/letsencrypt/live/pulse.company.com/fullchain.pem
# Key:  /etc/letsencrypt/live/pulse.company.com/privkey.pem

# Self-signed (dev/internal only)
openssl req -x509 -newkey rsa:4096 -nodes \
  -keyout /etc/pulse/key.pem \
  -out /etc/pulse/cert.pem \
  -days 365 -subj '/CN=pulse.internal'
```

Update `broker.yaml`:

```yaml
tls:
  cert_path: "/etc/letsencrypt/live/pulse.company.com/fullchain.pem"
  key_path: "/etc/letsencrypt/live/pulse.company.com/privkey.pem"
```

Broker watches cert files and auto-reloads on change (supports certbot auto-renewal).

## 2. Health Checks

### 2.1 Endpoints

```
GET /health     → 200 OK if broker process is running
GET /ready      → 200 OK if broker is accepting connections
                   503 if shutting down or recovering
GET /status     → JSON with detailed status
```

**`/status` response:**

```json
{
  "broker_id": "pulse-broker-01",
  "version": "0.1.0",
  "uptime_secs": 86400,
  "state": "running",
  "connections": {
    "active": 12,
    "max": 5000
  },
  "namespaces": ["ecommerce", "internal-tools"],
  "wal": {
    "active_segment": 42,
    "total_segments": 5,
    "total_size_bytes": 134217728,
    "pending_events": 23
  },
  "delivery": {
    "in_flight": 5,
    "total_delivered": 1234567,
    "total_acked": 1234560,
    "dlq_count": 7
  },
  "cluster": {
    "node_id": "node-1",
    "state": "active",
    "peers": [
      {"id": "node-2", "addr": "10.0.1.2:4223", "state": "alive", "topics_owned": 15},
      {"id": "node-3", "addr": "10.0.1.3:4223", "state": "alive", "topics_owned": 12}
    ],
    "replication_mode": "async",
    "replication_lag_ms": 0.8
  }
}
```

> **Note**: The `cluster` field is omitted when the broker is running in single-node mode (no `--peers` configured).

### 2.2 Kubernetes Probes

```yaml
# Deployment snippet
containers:
  - name: pulse-broker
    livenessProbe:
      httpGet:
        path: /health
        port: 8080
      initialDelaySeconds: 5
      periodSeconds: 10
    readinessProbe:
      httpGet:
        path: /ready
        port: 8080
      initialDelaySeconds: 10
      periodSeconds: 5
    startupProbe:
      httpGet:
        path: /ready
        port: 8080
      failureThreshold: 30
      periodSeconds: 2
```

## 3. Metrics (Prometheus)

### 3.1 Exposed Metrics

All metrics prefixed with `pulse_`.

**Connection metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `pulse_connections_active` | Gauge | Currently connected clients |
| `pulse_connections_total` | Counter | Total connections since start |
| `pulse_auth_failures_total` | Counter | Failed authentication attempts |
| `pulse_auth_failures_total{reason="..."}` | Counter | By reason: invalid_key, expired_hmac, forbidden |

**Event metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `pulse_events_received_total` | Counter | PUB frames received |
| `pulse_events_stored_total` | Counter | Events written to WAL |
| `pulse_events_delivered_total` | Counter | Events sent to consumers |
| `pulse_events_acked_total` | Counter | Consumer ACKs received |
| `pulse_events_nacked_total` | Counter | Consumer NACKs received |
| `pulse_events_dlq_total` | Counter | Events moved to DLQ |
| `pulse_events_dedup_total` | Counter | Duplicate events detected |

**Latency metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `pulse_publish_latency_seconds` | Histogram | PUB → ACK(stored) latency |
| `pulse_delivery_latency_seconds` | Histogram | Stored → delivered to consumer |
| `pulse_end_to_end_latency_seconds` | Histogram | PUB → consumer ACK(done) |
| `pulse_wal_write_latency_seconds` | Histogram | WAL append + fsync time |

**Queue metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `pulse_consumer_queue_depth{consumer="..."}` | Gauge | Pending events per consumer |
| `pulse_consumer_inflight{consumer="..."}` | Gauge | In-flight (delivered, awaiting ACK) |
| `pulse_consumer_overflow{consumer="..."}` | Gauge | Events in disk overflow |
| `pulse_retry_queue_depth` | Gauge | Events waiting for retry |

**WAL metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `pulse_wal_segments_active` | Gauge | Number of WAL segments |
| `pulse_wal_size_bytes` | Gauge | Total WAL disk usage |
| `pulse_wal_writes_total` | Counter | WAL write operations |
| `pulse_wal_compactions_total` | Counter | Compaction runs |
| `pulse_wal_recovery_duration_seconds` | Gauge | Last recovery time |

**Cluster metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `pulse_cluster_peers_active` | Gauge | Currently connected peer nodes |
| `pulse_cluster_peers_total` | Counter | Total peer connections since start |
| `pulse_replication_lag_seconds` | Histogram | WAL replication lag to peers |
| `pulse_replication_bytes_total` | Counter | Total bytes replicated to peers |
| `pulse_gossip_messages_total` | Counter | Gossip protocol messages sent/received |
| `pulse_topic_ownership_changes_total` | Counter | Topic ownership rebalance events |
| `pulse_cluster_failover_total` | Counter | Node failover events |

**System metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `pulse_config_reload_total{status="success|error"}` | Counter | Config hot-reload events |
| `pulse_uptime_seconds` | Gauge | Broker uptime |

### 3.2 Prometheus Configuration

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'pulse-broker'
    static_configs:
      - targets: ['broker:9090']
    scrape_interval: 15s
```

### 3.3 Recommended Alerts

```yaml
# alerts.yml (Prometheus AlertManager)
groups:
  - name: pulse
    rules:
      # DLQ growing — events failing permanently
      - alert: PulseDlqGrowing
        expr: rate(pulse_events_dlq_total[5m]) > 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Events entering DLQ"
          description: "{{ $value }} events/sec entering dead letter queue"

      # Consumer queue backing up
      - alert: PulseConsumerQueueHigh
        expr: pulse_consumer_queue_depth > 5000
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Consumer {{ $labels.consumer }} queue depth: {{ $value }}"

      # High publish latency
      - alert: PulsePublishLatencyHigh
        expr: histogram_quantile(0.99, rate(pulse_publish_latency_seconds_bucket[5m])) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "P99 publish latency > 100ms"

      # WAL disk usage
      - alert: PulseWalDiskHigh
        expr: pulse_wal_size_bytes > 5e9  # 5 GB
        labels:
          severity: warning
        annotations:
          summary: "WAL disk usage: {{ $value | humanize1024 }}"

      # No connections (broker might be unreachable)
      - alert: PulseNoConnections
        expr: pulse_connections_active == 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "No active connections to Pulse broker"

      # Broker down
      - alert: PulseBrokerDown
        expr: up{job="pulse-broker"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Pulse broker is down"

      # ─── Cluster alerts ───

      # Peer node unreachable
      - alert: PulsePeerUnreachable
        expr: pulse_cluster_peers_active < (pulse_cluster_peers_total - 1)
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "Cluster peer unreachable — {{ $value }} active peers"

      # Replication lag too high
      - alert: PulseReplicationLagHigh
        expr: histogram_quantile(0.99, rate(pulse_replication_lag_seconds_bucket[5m])) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "P99 replication lag > 100ms"

      # Topic ownership rebalancing (informational, but sustained rebalancing is a problem)
      - alert: PulseTopicRebalancing
        expr: rate(pulse_topic_ownership_changes_total[5m]) > 1
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Sustained topic ownership rebalancing — possible node instability"

      # Split-brain detected (nodes disagree on cluster membership)
      - alert: PulseSplitBrain
        expr: count(pulse_cluster_peers_active) by (job) > 1 and stddev(pulse_cluster_peers_active) by (job) > 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Possible split-brain — nodes report different peer counts"
```

## 4. Logging

### 4.1 Structured Logging Format

```json
{
  "timestamp": "2024-01-15T10:30:00.000Z",
  "level": "INFO",
  "target": "pulse_broker::pipeline::dispatcher",
  "message": "Event stored",
  "msg_id": "01945a2b-3c4d-7e5f-8a9b-c0d1e2f3a4b5",
  "topic": "order.created",
  "namespace": "ecommerce",
  "publisher": "order-service",
  "wal_segment": 42,
  "wal_offset": 1048576,
  "latency_us": 1234
}
```

### 4.2 Log Levels

| Level | Usage |
|-------|-------|
| ERROR | Unrecoverable: WAL write failure, sled corruption, bind failure |
| WARN | Recoverable: consumer NACK, auth failure, dedup collision, DLQ entry |
| INFO | Operational: connection open/close, config reload, compaction |
| DEBUG | Diagnostic: individual frame processing, routing decisions |
| TRACE | Wire-level: raw frame bytes, CRC details |

### 4.3 Configuration

```yaml
# broker.yaml
logging:
  level: "info"                    # global default
  format: "json"                   # "json" | "pretty" (dev)
  output: "stdout"                 # "stdout" | "file"
  file_path: "/var/log/pulse/broker.log"  # if output: "file"
  file_rotation: "daily"
  file_max_size_mb: 100
  file_max_backups: 7

  # Per-module overrides
  overrides:
    pulse_broker::pipeline: "debug"
    pulse_broker::server::connection: "warn"
```

Or via environment:

```bash
RUST_LOG="info,pulse_broker::pipeline=debug" pulse-broker
```

## 5. Admin API

HTTP API on the health port (8080) for operational tasks.

### 5.1 Authentication

The Admin API requires a bearer token for all mutating operations (POST, DELETE). Read-only operations (GET) can optionally require authentication.

```yaml
# broker.yaml
admin:
  listen_addr: "127.0.0.1:8080"    # bind to localhost by default
  auth:
    enabled: true
    bearer_token: "${PULSE_ADMIN_TOKEN}"  # env var substitution
    read_requires_auth: false              # GET endpoints open by default
```

**Request format:**

```
POST /api/v1/dlq/{msg_id}/replay
Authorization: Bearer <token>
```

When `auth.enabled: false` (development only), all endpoints are open without authentication. The broker logs a warning on startup if auth is disabled.

### 5.2 Endpoints

```
# Status
GET  /api/v1/status                     → Broker status (same as /status)

# Topics
GET  /api/v1/topics                     → List all active topics
GET  /api/v1/topics/{topic}/subscribers → List subscribers for a topic

# Services
GET  /api/v1/services                   → List connected services
GET  /api/v1/services/{id}              → Service detail (subscriptions, queue depth)

# DLQ
GET  /api/v1/dlq                        → List DLQ events (paginated)
GET  /api/v1/dlq/{msg_id}               → DLQ event detail
POST /api/v1/dlq/{msg_id}/replay        → Re-inject event into original topic
POST /api/v1/dlq/replay-all             → Replay all DLQ events
DELETE /api/v1/dlq/{msg_id}             → Purge single DLQ event
DELETE /api/v1/dlq                      → Purge all DLQ events

# Config
POST /api/v1/config/reload              → Force config reload
GET  /api/v1/config/validate            → Validate current config files

# WAL
GET  /api/v1/wal/status                 → WAL segments, sizes, pending count
POST /api/v1/wal/compact                → Trigger manual compaction
```

### 5.3 Admin CLI

```bash
# Status
pulse-admin status --broker broker:8080

# List topics
pulse-admin topics list

# Inspect DLQ
pulse-admin dlq list --limit 20
pulse-admin dlq show <msg_id>
pulse-admin dlq replay <msg_id>
pulse-admin dlq replay-all --topic "order.created"
pulse-admin dlq purge --older-than 7d

# Service info
pulse-admin services list
pulse-admin services show order-service

# Config validation
pulse-admin config validate ./config/

# Force compaction
pulse-admin wal compact
```

## 6. Troubleshooting

### 6.1 Common Issues

**Publisher gets Timeout error**

```
Check:
1. Broker is reachable? → pulse-admin status
2. WAL disk full?       → pulse-admin wal status → check disk space
3. Network latency?     → ping broker host
4. Broker overloaded?   → check pulse_publish_latency_seconds metric

Fix:
- Increase publish_timeout in SDK config
- If WAL disk full: increase disk, run compaction, reduce retention
- If broker CPU high: check for hot consumer (slow handler causing retry storms)
```

**Consumer not receiving events**

```
Check:
1. Consumer connected?  → pulse-admin services show <consumer_id>
2. Subscription active? → pulse-admin topics <topic> subscribers
3. Filter too strict?   → check filter expression in SUB or routes.yaml
4. Consumer group?      → another group member may be receiving

Fix:
- Verify topic name matches exactly (including namespace)
- Test filter with simpler expression
- Check consumer logs for deserialization errors
```

**DLQ accumulating events**

```
Check:
1. What errors?          → pulse-admin dlq list → inspect last_error
2. Consumer healthy?     → check consumer logs
3. Payload changed?      → schema mismatch between publisher and consumer

Fix:
- Fix consumer handler bug → pulse-admin dlq replay-all
- If schema issue: update consumer, then replay
- If events are stale/irrelevant: pulse-admin dlq purge
```

**High memory usage**

```
Check:
1. Ring buffer size?     → config: buffer_capacity
2. Consumer queue depth? → pulse_consumer_queue_depth metric
3. Many offline consumers holding queues?

Fix:
- Reduce ring_buffer capacity in broker.yaml
- Investigate slow/offline consumers
- Lower max_pending_per_consumer
- Set stricter retention on consumer queues
```

**WAL growing too large**

```
Check:
1. Compaction running?   → pulse_wal_compactions_total metric
2. Pending events?       → pulse-admin wal status → pending count
3. Consumers keeping events pending?

Fix:
- Force compaction: pulse-admin wal compact
- Investigate consumers holding events (slow/offline)
- Reduce retention_hours
- Increase compaction frequency (compaction.interval_secs)
```

### 6.2 Cluster Issues

**Node not joining cluster**

```
Check:
1. Peers reachable?      → telnet <peer_addr> 4223 from the new node
2. Firewall?             → port 4223 (gossip) must be open between all nodes
3. Version mismatch?     → all nodes must run compatible versions
4. Node ID conflict?     → check logs for "duplicate node_id" errors

Fix:
- Ensure --peers addresses are correct and reachable
- Open port 4223 (gossip port) between all cluster nodes
- Verify all nodes are on the same major version
- Each node must have a unique node_id (auto-generated from hostname by default)
```

**Replication lag growing**

```
Check:
1. Network bandwidth?    → pulse_replication_bytes_total rate vs available bandwidth
2. Disk I/O on follower? → WAL write latency on the lagging node
3. CPU saturation?       → check system metrics on lagging node

Fix:
- Upgrade inter-node network (1 Gbps recommended)
- Move to faster storage (NVMe SSD)
- Reduce event volume or increase node count to distribute load
- Check for noisy neighbors if running on shared infrastructure
```

**Split-brain symptoms**

```
Symptoms:
- Different clients see different cluster state
- Events published to one partition not visible on another
- pulse_cluster_peers_active differs across nodes

Check:
1. Network partition?    → can all nodes reach all other nodes?
2. Gossip working?       → pulse_gossip_messages_total should be non-zero on all nodes

Fix:
- Resolve network partition (most common cause)
- Restart isolated nodes — they will rejoin and reconcile state
- If data diverged: the WAL with the highest sequence wins during reconciliation
```

**Topic ownership not rebalancing**

```
Check:
1. New node joined?      → check pulse_cluster_peers_active on existing nodes
2. Gossip converged?     → wait for 1-2 gossip cycles (~5-10s)
3. Rebalance in progress? → pulse_topic_ownership_changes_total should be increasing

Fix:
- Verify new node appears in /status cluster.peers on all existing nodes
- If stuck: restart the node that should be receiving ownership
- Check logs for "rebalance" entries on all nodes
```

### 6.3 Debug Mode

For deep investigation, enable trace logging for specific modules:

```bash
RUST_LOG="pulse_broker::pipeline=trace,pulse_broker::delivery=debug" pulse-broker

# This logs every frame, every routing decision, every ACK
# WARNING: extremely verbose. Use only for short debugging sessions.
```

## 7. Backup & Disaster Recovery

### 7.1 What to Back Up

| Data | Location | Method | Frequency |
|------|----------|--------|-----------|
| WAL segments | `/var/lib/pulse/wal/` | Filesystem snapshot or rsync | Continuous or hourly |
| State DB | `/var/lib/pulse/state/` | sled export or filesystem copy | Hourly |
| Config | `/etc/pulse/` | Version control (git) | On every change |

### 7.2 Recovery Procedure

```bash
# Full recovery from backup (single node)
1. Stop broker
2. Restore WAL segments to /var/lib/pulse/wal/
3. Restore state DB to /var/lib/pulse/state/
4. Restore config to /etc/pulse/
5. Start broker
   → Broker automatically replays WAL and recovers pending events
```

**Important**: WAL replay handles inconsistency between WAL and state DB. If state DB is older than WAL, replay will re-process missing events. This is safe because all operations are idempotent.

### 7.3 Cluster Backup

In a cluster deployment, the WAL is replicated across nodes. You can back up from **any single node** — the replicated WAL contains all events for topics owned by that node, and the cluster state is reconstructable from any node's gossip history.

**Recommended approach**: Back up one node per cluster (rotate which node). If a node is lost entirely, the remaining nodes hold replicated WAL data and can reconstruct the lost node's state when a replacement joins.

For complete disaster recovery (all nodes lost), restore from the most recent backup of any single node — it will bootstrap a new cluster with full WAL history for its owned topics. Other topics' history can be recovered from backups of other nodes.

## 8. Security Checklist

```
[ ] TLS enabled with valid certificate
[ ] API keys generated with sufficient entropy (32+ bytes)
[ ] services.yaml has minimal permissions per service
[ ] services.yaml uses env var substitution for API keys (not plaintext)
[ ] Admin API protected with bearer token or bound to localhost only
[ ] Metrics endpoint bound to internal network
[ ] Firewall: port 4222 open only to known service IPs
[ ] Firewall: ports 8080, 9090 open only to monitoring infra
[ ] Config files readable only by pulse user (chmod 600)
[ ] API keys not committed to version control
[ ] Connection URLs not logged in application code (key leakage risk)
[ ] Log level set to INFO (not DEBUG/TRACE in production)
[ ] HMAC timestamp validation enabled (anti-replay)
[ ] Firewall: port 4223 open only between cluster nodes (gossip)
```

## 9. Cluster Deployment

### 9.1 Minimum Topology

A production cluster requires **3 nodes minimum** to tolerate 1 node failure. Each node runs an identical `pulse-broker` binary — there is no leader election or special coordinator role. All nodes are peers.

```
                  ┌─────────────────┐
                  │  TCP Load       │
                  │  Balancer       │
                  │  (:4222)        │
                  └──┬────┬────┬───┘
                     │    │    │
              ┌──────┘    │    └──────┐
              ▼           ▼           ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │  Node 1  │ │  Node 2  │ │  Node 3  │
        │  :4222   │ │  :4222   │ │  :4222   │
        │  :4223   │ │  :4223   │ │  :4223   │
        └────┬─────┘ └────┬─────┘ └────┬─────┘
             │             │             │
             └─────────────┼─────────────┘
                     gossip mesh
                     (port 4223)
```

- **Port 4222**: Client connections (publishers, subscribers). Fronted by a TCP load balancer.
- **Port 4223**: Inter-node gossip and replication. Direct mesh, no load balancer.

### 9.2 Load Balancer Configuration

Use a **TCP (L4) load balancer** — not HTTP. Pulse uses a binary protocol over persistent TCP connections.

```
# HAProxy example
frontend pulse_front
    bind *:4222
    mode tcp
    default_backend pulse_nodes

backend pulse_nodes
    mode tcp
    balance roundrobin
    option tcp-check
    server node1 10.0.1.1:4222 check inter 5s fall 3 rise 2
    server node2 10.0.1.2:4222 check inter 5s fall 3 rise 2
    server node3 10.0.1.3:4222 check inter 5s fall 3 rise 2
```

The SDK discovers all peers from CONNACK and routes directly to topic leaders, so the load balancer is primarily used for initial connection bootstrapping. Once connected, clients maintain direct connections to relevant nodes.

### 9.3 Rolling Upgrades

Nodes can be upgraded one at a time with zero downtime:

```bash
# 1. Drain node (stop accepting new connections, finish in-flight)
curl -X POST http://node1:8080/api/v1/admin/drain

# 2. Wait for drain to complete (in-flight events finish)
#    Node will report state: "draining" in /status
#    Topics owned by this node are transferred to remaining peers

# 3. Stop the node
systemctl stop pulse-broker

# 4. Upgrade binary
cp pulse-broker-new /usr/local/bin/pulse-broker

# 5. Start the node
systemctl start pulse-broker
#    Node rejoins cluster, receives topic ownership via consistent hashing

# 6. Repeat for next node
```

Wait for the upgraded node to appear as `alive` in all peers' `/status` before proceeding to the next node.

### 9.4 Scaling Up / Down

**Scaling up:**

```bash
# Start new node with existing peers
pulse-broker --peers 10.0.1.1:4223,10.0.1.2:4223

# The new node:
# 1. Joins the gossip mesh
# 2. Receives topic ownership for a portion of topics (consistent hashing rebalance)
# 3. Receives WAL data for newly owned topics from previous owners
# 4. Begins accepting client connections for owned topics
```

**Scaling down:**

```bash
# 1. Drain the node
curl -X POST http://node-to-remove:8080/api/v1/admin/drain

# 2. Wait for drain + topic transfer
# 3. Stop the node
systemctl stop pulse-broker

# Remaining nodes absorb the departed node's topics automatically
```

### 9.5 Docker Compose (3-Node Cluster)

```yaml
# docker-compose.yml
version: "3.8"

services:
  pulse-1:
    image: ghcr.io/pulsemq/pulse-broker:latest
    command: >
      --listen 0.0.0.0:4222
      --peers pulse-2:4223,pulse-3:4223
      --data-dir /data
      --durability balanced
    ports:
      - "4222:4222"
      - "9090:9090"
      - "8080:8080"
    volumes:
      - pulse-1-data:/data

  pulse-2:
    image: ghcr.io/pulsemq/pulse-broker:latest
    command: >
      --listen 0.0.0.0:4222
      --peers pulse-1:4223,pulse-3:4223
      --data-dir /data
      --durability balanced
    ports:
      - "4223:4222"
      - "9091:9090"
      - "8081:8080"
    volumes:
      - pulse-2-data:/data

  pulse-3:
    image: ghcr.io/pulsemq/pulse-broker:latest
    command: >
      --listen 0.0.0.0:4222
      --peers pulse-1:4223,pulse-2:4223
      --data-dir /data
      --durability balanced
    ports:
      - "4224:4222"
      - "9092:9090"
      - "8082:8080"
    volumes:
      - pulse-3-data:/data

volumes:
  pulse-1-data:
  pulse-2-data:
  pulse-3-data:
```

```bash
# Start the cluster
docker compose up -d

# Verify all nodes see each other
curl -s http://localhost:8080/status | jq '.cluster.peers'
curl -s http://localhost:8081/status | jq '.cluster.peers'
curl -s http://localhost:8082/status | jq '.cluster.peers'
```
