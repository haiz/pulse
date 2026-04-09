# Deployment Guide

Production deployment, configuration, Docker, and monitoring.

## Deployment Modes

### Single Node (Development / Small Workloads)

```bash
# Zero-config — just run the binary
pulse-broker

# With options
pulse-broker --listen 0.0.0.0:4222 --durability balanced

# With gateway
pulse-gateway --broker 127.0.0.1:4222 --listen 0.0.0.0:8080
```

### Docker (Single Node)

```bash
# Build
docker build -f docker/Dockerfile -t pulse-broker:latest .

# Run
docker run -p 4222:4222 -p 9090:9090 -v pulse-data:/var/lib/pulse pulse-broker:latest
```

### Docker Compose (3-Node Cluster)

```bash
docker compose -f docker/docker-compose.yml up
```

This starts:
- 3 broker nodes (ports 4222, 4223, 4224)
- Metrics on 9090, 9091, 9092

---

## Configuration

All configuration is optional. Every setting has a CLI flag equivalent.

### broker.yaml

```yaml
# Network
listen_addr: "0.0.0.0:4222"

# TLS (optional)
tls:
  cert_path: "/etc/pulse/cert.pem"
  key_path: "/etc/pulse/key.pem"

# Limits
max_connections: 5000
max_payload_bytes: 1048576    # 1 MB

# Keepalive
keepalive_interval_secs: 10
keepalive_timeout_secs: 30
connect_timeout_secs: 5

# Durability: "memory" | "balanced" | "durable"
durability:
  mode: "balanced"
  group_commit_interval_ms: 5  # fsync batch interval
  group_commit_max_batch: 100  # max events per fsync batch

# Storage
data_dir: "/var/lib/pulse"
wal:
  segment_size_bytes: 67108864  # 64 MB per WAL segment
  sync_mode: "fsync"            # fsync | fdatasync | none

# Delivery
delivery:
  ack_timeout_secs: 30
  max_redeliveries: 5
  backoff:
    initial_secs: 1
    max_secs: 60
    multiplier: 2.0

# Metrics
metrics:
  enabled: true
  listen_addr: "0.0.0.0:9090"
```

### services.yaml (Authentication)

```yaml
namespaces:
  production:
    services:
      order-service:
        key: "${ORDER_SVC_KEY}"
        permissions:
          publish: ["order.*"]
          subscribe: ["payment.*", "inventory.*"]

      payment-service:
        key: "${PAYMENT_SVC_KEY}"
        permissions:
          publish: ["payment.*"]
          subscribe: ["order.created"]

      analytics:
        key: "${ANALYTICS_KEY}"
        permissions:
          publish: []
          subscribe: [">"]        # read-only, all topics
```

Supports `${ENV_VAR}` and `${ENV_VAR:-default}` substitution.

### routes.yaml (Server-side Routing)

```yaml
routes:
  - name: "large-orders-fraud-check"
    match:
      topic: "order.created"
      where: "payload.amount > 1000"
    deliver:
      - service: "fraud-detection"
    enabled: true

  - name: "all-to-analytics"
    match:
      topic: ">"
    deliver:
      - service: "analytics"

failure_policy:
  default:
    max_retries: 5
    backoff:
      type: "exponential"
      initial_secs: 1
      max_secs: 60
      multiplier: 2.0
    dead_letter:
      topic_prefix: "dlq"
      retention_hours: 168
```

### CLI Flags

Every config option has a CLI equivalent:

```bash
pulse-broker \
  --listen 0.0.0.0:4222 \
  --durability balanced \
  --config /etc/pulse/broker.yaml
```

```bash
pulse-gateway \
  --broker 127.0.0.1:4222 \
  --listen 0.0.0.0:8080 \
  --service-id pulse-gateway \
  --namespace default
```

---

## Monitoring

### Prometheus Metrics

Broker exposes Prometheus metrics on `:9090/metrics`:

```bash
curl http://localhost:9090/metrics
```

Scrape config:
```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'pulse'
    static_configs:
      - targets: ['localhost:9090']
```

### Health Checks

```bash
# Broker metrics endpoint
curl http://localhost:9090/health

# Gateway endpoint
curl http://localhost:8080/v1/health
```

### Admin CLI

```bash
# Check broker status
pulse-admin status

# Ping broker
pulse-admin ping

# Publish test event
pulse-admin pub order.test '{"test": true}'

# Subscribe and monitor events
pulse-admin sub "order.*"

# Validate config
pulse-admin config-check config/broker.yaml
```

---

## Production Checklist

### Before Deployment

- [ ] Choose durability mode per topic/namespace (memory, balanced, or durable)
- [ ] Configure TLS (`tls.cert_path`, `tls.key_path`)
- [ ] Set API keys in `services.yaml` (don't use anonymous in production)
- [ ] Set appropriate `max_connections` and `max_payload_bytes`
- [ ] Configure WAL `data_dir` on fast storage (SSD/NVMe)
- [ ] Set up Prometheus scraping for `:9090/metrics`
- [ ] Configure health check probes for orchestrator

### Capacity Planning (Single Node)

| Mode | Throughput | P99 Latency | Connections | WAL Disk |
|------|-----------|------------|-------------|----------|
| Memory | ~800K/s | ~5µs | ~50,000 | None |
| Balanced | ~100K/s | ~500µs | ~50,000 | ~200 MB/s |
| Durable | ~10K/s | ~2ms | ~10,000 | ~50 MB/s |

### WAL Storage Sizing

```
Events/day × avg_event_size × retention_days = WAL storage needed

Example: 100K events/sec × 500 bytes × 86400 sec/day × 7 days
       = ~30 TB (before compaction)
       ≈ ~6 TB (with 80% compaction ratio)
```

### Resource Recommendations

| Workload | CPU | RAM | Disk | Network |
|----------|-----|-----|------|---------|
| Dev/Test | 2 cores | 1 GB | SSD | 1 GbE |
| Small (10K/s) | 4 cores | 4 GB | NVMe | 1 GbE |
| Medium (100K/s) | 8 cores | 8 GB | NVMe | 10 GbE |
| Large (500K+/s) | 16 cores | 16 GB | NVMe RAID | 25 GbE |

---

## Troubleshooting

### Broker won't start

```bash
# Check port in use
lsof -i :4222

# Check config syntax
pulse-admin config-check config/broker.yaml

# Run with debug logging
RUST_LOG=debug pulse-broker
```

### Events not being delivered

```bash
# Check broker is healthy
curl http://localhost:9090/health

# Check gateway is connected
curl http://localhost:8080/v1/info

# Publish a test event and verify
curl -X POST http://localhost:8080/v1/publish \
  -H 'Content-Type: application/json' \
  -d '{"topic":"test.debug","data":{"debug":true}}'

# Monitor all events
pulse-admin sub ">"
```

### High latency

- Check durability mode (`balanced` vs `durable`)
- Check WAL disk I/O (`iostat -x 1`)
- Check connection count vs `max_connections`
- Use batch publish for high-volume producers
- Consider memory mode for non-critical events
