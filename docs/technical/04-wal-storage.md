# WAL & Storage Design

## 1. Storage Architecture Overview

```
/var/lib/pulse/
├── wal/                        # Write-Ahead Log segments
│   ├── segment-000001.wal      # 64 MB max per segment
│   ├── segment-000002.wal
│   └── segment-000003.wal      # active (currently being written)
│
├── state/                      # sled embedded database
│   └── (sled internal files)   # event state, dedup index, consumer offsets
│
├── overflow/                   # disk-backed consumer queue overflow
│   ├── consumer-order-svc/
│   └── consumer-payment-svc/
│
├── cluster/                    # replication and cluster data
│   ├── node-id                 # persistent node identity (UUIDv4)
│   ├── peers/                  # cached peer state from gossip
│   └── replication/            # incoming WAL streams from leaders
│       ├── topic-abc/          # replicated WAL segments per topic
│       └── topic-xyz/
│
└── archive/                    # compressed old segments (optional)
    └── segment-000001.wal.zst
```

**Note on memory mode**: When running in `memory` durability mode, the `wal/` directory is not created. Events are stored exclusively in the memory ring buffer. The `overflow/` directory is still used for slow consumer disk spill. The `cluster/` directory is still used if the node participates in a distributed mesh.

## 2. Write-Ahead Log (WAL)

### 2.0 Durability Modes

Pulse supports three durability modes that control how (and whether) events are persisted to the WAL. The mode is configured per-broker and applies to all topics on that node.

| Mode | WAL Behavior | fsync Strategy | Dedup Layers | Target Use Case |
|------|-------------|----------------|--------------|-----------------|
| **memory** | No WAL. Events stored in memory ring buffer only. | N/A | Optional bloom (no sled) | Ephemeral events, metrics, logs, real-time analytics |
| **balanced** | WAL with group commit. Writes batched for up to 5ms, single fsync per batch. | `fdatasync` every 5ms (group commit) | Bloom only (no sled) | General production use. Default mode. |
| **durable** | WAL with per-event fsync. Every event individually fsynced before ACK. | `fsync` or `fdatasync` per write | Bloom + sled (full two-layer) | Financial transactions, audit trails, payment processing |

**Configuration:**

```yaml
# broker.yaml
durability:
  mode: "balanced"                  # "memory" | "balanced" | "durable"
  group_commit_interval_ms: 5       # balanced mode only: max wait before fsync
  group_commit_max_batch: 100       # balanced mode only: max events per batch
  sync_mode: "fdatasync"            # "fsync" | "fdatasync" | "none"
```

**Memory mode** provides the highest throughput (800K+ msg/sec) by eliminating all disk I/O from the hot path. Events exist only in the memory ring buffer. If the ring buffer fills, oldest events are evicted. Disk overflow is used only for slow consumer queues, not for event persistence. On crash, all in-memory events are lost.

**Balanced mode** (default) batches WAL writes and issues a single `fdatasync` every 5ms (or when the batch reaches 100 events, whichever comes first). This amortizes the cost of fsync across many events, achieving 100K+ msg/sec. On crash, up to 5ms of uncommitted writes may be lost.

**Durable mode** calls fsync after every individual WAL write. This guarantees zero data loss on crash at the cost of throughput (10K+ msg/sec). Combined with the full bloom + sled dedup engine, this is the only mode that provides exactly-once semantics.

### 2.1 Purpose

The WAL is the source of truth for event durability in balanced and durable modes. In durable mode, every event must be written to WAL and fsynced before the broker sends ACK STORED to the publisher. In balanced mode, the event is written to WAL but the fsync is deferred to the next group commit boundary. On crash recovery, the WAL is replayed to reconstruct in-flight state.

### 2.2 Segment File Format

Each segment is an append-only file with sequential records:

```
Segment File Layout:
┌──────────────────────────────────────────────┐
│ Segment Header (32 bytes)                     │
├──────────────────────────────────────────────┤
│ Record 0                                      │
├──────────────────────────────────────────────┤
│ Record 1                                      │
├──────────────────────────────────────────────┤
│ ...                                           │
├──────────────────────────────────────────────┤
│ Record N                                      │
└──────────────────────────────────────────────┘
```

**Segment Header:**

```
Offset  Size  Field
0       4     Magic: 0x50_4C_57_4C ("PLWL" = Pulse WAL)
4       1     Version: 0x01
5       4     Segment Number (uint32)
9       8     Created At (uint64, millis since epoch)
17      15    Reserved (zeros)
```

**Record Format:**

```
Offset  Size      Field
0       4         Record Length (uint32, total bytes including this field)
4       1         Record Type
5       16        Message ID (UUIDv7)
21      variable  Record Data (depends on type)
-4      4         CRC32C over bytes [0, len-4)
```

**Record Types:**

| Type | Value | Data Contents | Description |
|------|-------|---------------|-------------|
| EVENT_WRITE | 0x01 | Full serialized event (topic + payload + headers) | Event ingested and stored |
| COMPLETION | 0x02 | Consumer ID (string) | Consumer acknowledged processing |
| BATCH_WRITE | 0x03 | Array of events (same as PUB batch) | Atomic batch write |
| BATCH_COMPLETION | 0x04 | Batch ID + Consumer ID | Entire batch completed |
| CHECKPOINT | 0x05 | Snapshot of pending event IDs | Optimization for faster recovery |

### 2.3 Write Path

```rust
pub struct WalWriter {
    active_segment: File,
    segment_number: u32,
    segment_offset: u64,
    segment_max_size: u64,       // 64 MB default
    sync_mode: SyncMode,         // Fsync | Fdatasync | None
    write_buffer: Vec<u8>,       // reusable buffer to avoid allocs
}

impl WalWriter {
    pub async fn append(&mut self, event: &Event) -> Result<WalPosition> {
        // 1. Serialize record
        let record = WalRecord::event_write(event);
        let bytes = record.serialize(&mut self.write_buffer);

        // 2. Check segment rotation
        if self.segment_offset + bytes.len() as u64 > self.segment_max_size {
            self.rotate_segment().await?;
        }

        // 3. Write to file
        let position = WalPosition {
            segment: self.segment_number,
            offset: self.segment_offset,
        };
        self.active_segment.write_all(bytes).await?;

        // 4. fsync — THIS IS THE DURABILITY BOUNDARY
        match self.sync_mode {
            SyncMode::Fsync => self.active_segment.sync_all().await?,
            SyncMode::Fdatasync => self.active_segment.sync_data().await?,
            SyncMode::None => { /* no sync — testing only */ }
        }

        // 5. Update offset
        self.segment_offset += bytes.len() as u64;

        Ok(position)
    }

    async fn rotate_segment(&mut self) -> Result<()> {
        // Sync current segment
        self.active_segment.sync_all().await?;

        // Create new segment
        self.segment_number += 1;
        let path = format!("wal/segment-{:06}.wal", self.segment_number);
        self.active_segment = File::create(&path).await?;

        // Write segment header
        let header = SegmentHeader::new(self.segment_number);
        self.active_segment.write_all(&header.serialize()).await?;
        self.segment_offset = 32; // header size

        Ok(())
    }
}
```

### 2.4 fsync Strategy

| Mode | Config value | Behavior | Durability | Performance |
|------|-------------|----------|------------|-------------|
| fsync | `"fsync"` | `File::sync_all()` after each write | Highest — metadata + data flushed | ~1-5ms per write |
| fdatasync | `"fdatasync"` | `File::sync_data()` after each write | High — data flushed, metadata lazy | ~0.5-2ms per write |
| none | `"none"` | No explicit sync (OS page cache) | Low — up to 30s of data loss on crash | ~0.01ms per write |

**Durable mode recommendation**: Use `"fsync"` in production. With 10 events/sec, fsync overhead is negligible. Use `"none"` only in testing/development.

**Balanced mode**: Uses group commit (see below) regardless of the `sync_mode` setting. The `sync_mode` value determines which syscall the group commit uses for the batched fsync.

**Group commit details (balanced mode)**:

The group commit writer collects WAL writes for up to `group_commit_interval_ms` (default: 5ms) or until `group_commit_max_batch` events accumulate (default: 100), whichever comes first. Then it writes all records sequentially and issues a single fsync for the entire batch. This amortizes the ~1ms fsync cost across dozens or hundreds of events.

```rust
// Group commit: collect writes for up to 5ms, then single fsync
pub struct GroupCommitWriter {
    pending: Vec<(Event, oneshot::Sender<Result<WalPosition>>)>,
    flush_interval: Duration,  // 5ms (configurable)
    max_batch: usize,          // 100 (configurable)
}

impl GroupCommitWriter {
    async fn run(&mut self) {
        loop {
            // Collect writes until flush interval or batch cap
            let flush_deadline = tokio::time::sleep(self.flush_interval);
            tokio::pin!(flush_deadline);

            loop {
                tokio::select! {
                    item = self.rx.recv() => {
                        match item {
                            Some(item) => {
                                self.pending.push(item);
                                if self.pending.len() >= self.max_batch { break; }
                            }
                            None => return, // channel closed
                        }
                    }
                    _ = &mut flush_deadline => break,
                }
            }

            if self.pending.is_empty() { continue; }

            // Write all, single fsync
            let mut positions = Vec::new();
            for (event, _) in &self.pending {
                let pos = self.wal.write_no_sync(event).await.unwrap();
                positions.push(pos);
            }
            self.wal.fsync().await.unwrap();

            // Notify all waiters
            for ((_, tx), pos) in self.pending.drain(..).zip(positions) {
                let _ = tx.send(Ok(pos));
            }
        }
    }
}
```

**Throughput impact**: At 100K msg/sec with 5ms group commit, each fsync batch contains ~500 events. The per-event amortized fsync cost drops from ~1ms to ~0.002ms. This is why balanced mode achieves 10x the throughput of durable mode.

### 2.5 io_uring Optimization (Linux Only)

On Linux kernels 5.6+, Pulse uses `io_uring` via `tokio-uring` for asynchronous disk I/O. This reduces syscall overhead and enables the kernel to batch multiple I/O operations.

**How it works:**

```
Standard I/O path (non-Linux / fallback):
  write() syscall → kernel copy → page cache → fsync() syscall → disk flush
  Each operation = 1 syscall, 1 context switch

io_uring path (Linux):
  Submit linked SQEs to ring buffer:
    SQE 1: write(record_1)
    SQE 2: write(record_2)
    ...
    SQE N: write(record_N)
    SQE N+1: fsync() ← linked to previous, executes after all writes complete
  Single submit() call → kernel processes all SQEs → single completion event
```

**Key benefits:**

| Aspect | Standard I/O | io_uring |
|--------|-------------|----------|
| Syscalls per batch | 2N+1 (N writes + N possible copies + 1 fsync) | 1 submit + 1 completion poll |
| Context switches | 2 per syscall | 0 (kernel-side processing) |
| Memory copies | Kernel buffer copy | Zero-copy with registered buffers |
| Batch fsync | Manual group commit logic | Linked SQEs (hardware-level ordering) |

**Expected improvement**: 2-3x throughput for balanced mode on Linux. The improvement is most pronounced under high write concurrency where syscall overhead dominates.

**Configuration:**

```yaml
# broker.yaml
storage:
  io_engine: "auto"       # "auto" | "io_uring" | "tokio"
  # "auto" uses io_uring on Linux 5.6+, falls back to tokio elsewhere
  io_uring_queue_depth: 256   # SQE ring size
  io_uring_registered_buffers: 64  # pre-registered write buffers
```

**Fallback**: On macOS, Windows, and older Linux kernels, Pulse automatically falls back to standard `tokio::fs` I/O with no configuration change required. The `io_engine: "auto"` setting (default) handles platform detection at startup.

## 3. Crash Recovery

### 3.0 Recovery Behavior by Durability Mode

| Mode | Recovery Behavior |
|------|-------------------|
| **memory** | No WAL recovery. Broker state is rebuilt from zero. All in-memory events from the previous run are lost. Subscriptions and consumer offsets are restored from sled (if available). Consumers will reconnect and resume from the latest available event. |
| **balanced** | WAL replay with up to 5ms of uncommitted data lost. Events in the last group commit batch that had not been fsynced are irrecoverable. Publishers of those events will timeout and retry. All fsynced events are fully recovered. |
| **durable** | Full WAL recovery. Zero data loss. Every event that was ACKed to a publisher is guaranteed to be in the WAL. Complete state reconstruction. |

### 3.1 Recovery Algorithm

```
fn recover(data_dir: &Path, mode: DurabilityMode) -> BrokerState {
    // Memory mode: no WAL to replay
    if mode == DurabilityMode::Memory {
        return BrokerState {
            pending_events: vec![],
            dedup_index: HashSet::new(),
            next_segment: 1,
        };
    }

    let segments = list_wal_segments(data_dir.join("wal"))
        .sort_by_segment_number();

    let mut written: HashMap<MessageId, Event> = HashMap::new();
    let mut completed: HashSet<(MessageId, ConsumerId)> = HashSet::new();

    for segment in &segments {
        let mut reader = SegmentReader::open(segment)?;

        loop {
            match reader.next_record() {
                Ok(Record::EventWrite { msg_id, event }) => {
                    written.insert(msg_id, event);
                }
                Ok(Record::Completion { msg_id, consumer_id }) => {
                    completed.insert((msg_id, consumer_id));
                }
                Ok(Record::Checkpoint { pending_ids }) => {
                    // Optimization: only process events in pending_ids
                    // Skip events that were already completed before checkpoint
                }
                Ok(Record::BatchWrite { batch_id, events }) => {
                    for event in events {
                        written.insert(event.msg_id, event);
                    }
                }
                Err(WalError::CorruptRecord) => {
                    // Truncate segment at this point
                    // All subsequent records in this segment are lost
                    // (they were never ACKed, so publisher will retry)
                    truncate_at_current_position(segment, reader.position());
                    break;
                }
                Err(WalError::Eof) => break,
            }
        }
    }

    // Determine pending events
    let route_config = load_routes_config();
    let pending_events = Vec::new();

    for (msg_id, event) in &written {
        let targets = route_config.resolve(&event.topic);
        for target in targets {
            if !completed.contains(&(msg_id, target.consumer_id)) {
                pending_events.push(PendingEvent {
                    msg_id: *msg_id,
                    event: event.clone(),
                    target: target.clone(),
                });
            }
        }
    }

    // Rebuild dedup index
    let dedup_index: HashSet<MessageId> = written.keys().cloned().collect();

    BrokerState {
        pending_events,
        dedup_index,
        next_segment: segments.last().number + 1,
    }
}
```

### 3.2 Recovery Time Estimates

| Events in WAL | Segments | Recovery Time |
|--------------|----------|---------------|
| 10,000 | 1 | <0.5 seconds |
| 100,000 | 2-3 | ~2 seconds |
| 1,000,000 | ~15 | ~15 seconds |
| 10,000,000 | ~150 | ~2 minutes |

At 10 events/sec, 7 days retention = ~6M events. Recovery ~1.5 minutes worst case.

### 3.3 Checkpointing (Optimization)

To speed up recovery, the broker periodically writes CHECKPOINT records:

```
Every 10,000 events (configurable):
  1. Snapshot current pending event IDs
  2. Write CHECKPOINT record to WAL
  3. On recovery: find last CHECKPOINT, only process records after it
     (events in checkpoint that are still pending are pre-loaded)

Effect: Recovery only processes events since last checkpoint.
  With checkpoint every 10K events at 10 evt/s:
  Checkpoint interval: ~17 minutes
  Max events to replay: 10,000
  Recovery time: <1 second
```

## 4. Segment Compaction

### 4.1 When to Compact

```
Compaction Trigger (checked hourly by default):
  For each segment (not the active one):
    count completed events vs total events in segment
    if completed / total >= 0.8 (80%):
      → segment is eligible for compaction
```

### 4.2 Compaction Process

```
Before compaction:
  segment-000005.wal:
    [Event A: COMPLETED]
    [Event B: COMPLETED]
    [Event C: PENDING]
    [Event D: COMPLETED]
    [Event E: COMPLETED]
    [Event F: PENDING]

Compaction:
  1. Create new segment: segment-000005.wal.compact
  2. Copy only PENDING events (C, F) to new segment
  3. Add CHECKPOINT with pending event IDs
  4. fsync new segment
  5. Atomic rename: segment-000005.wal.compact → segment-000005.wal
  6. fsync parent directory for rename durability on Linux
     let dir = File::open("wal/")?;
     dir.sync_all()?;
  7. (Optional) Archive old segment as segment-000005.wal.zst

After compaction:
  segment-000005.wal:
    [CHECKPOINT: {C, F}]
    [Event C]
    [Event F]

Size reduction: 6 records → 3 records (50% in this example, typically 80%+)
```

### 4.3 Retention Policy

```yaml
# broker.yaml
wal:
  retention_hours: 168     # 7 days
  retention_action: "archive"  # "delete" | "archive"
  archive_path: "/var/lib/pulse/archive"
  archive_compression: "zstd"
```

Segments older than retention_hours where ALL events are COMPLETED are eligible for deletion/archival.

## 5. State Database (sled)

### 5.1 Key Spaces

sled is used as a fast, embedded key-value store for metadata that needs indexed access. The WAL stores raw event data; sled stores state and indexes.

```
Tree: "dedup"
  Key:   msg_id (16 bytes)
  Value: DedupEntry { stored_at: u64, topic: String }
  TTL:   7 days (cleaned up by background task)

Tree: "event_state"
  Key:   msg_id (16 bytes) + consumer_id_hash (8 bytes)
  Value: EventState { state, attempt, timestamps, last_error }

Tree: "subscriptions"
  Key:   namespace + "/" + sub_id
  Value: Subscription { topic_pattern, group, filter, consumer_id }

Tree: "consumer_offsets"
  Key:   consumer_id
  Value: ConsumerOffset { last_acked_wal_position, pending_count }

Tree: "dlq"
  Key:   msg_id (16 bytes) + consumer_id_hash (8 bytes)
  Value: DlqEntry { event, original_topic, attempts, first_error_at, last_error }
```

### 5.2 Why sled, not SQLite?

| Factor | sled | SQLite |
|--------|------|--------|
| Concurrency | Lock-free reads, designed for concurrent Rust | Single-writer, readers can block |
| Embedded | Pure Rust, no C dependency | C library, needs bindings |
| Performance | Optimized for point lookups and scans | Better for complex queries |
| Transactions | Atomic batches | Full ACID transactions |
| Fit for Pulse | Perfect: simple KV lookups on msg_id | Overkill: we don't need SQL |

> **Stability note**: sled 0.34.x is technically pre-1.0 and has had reported data corruption issues under certain crash scenarios. The sled project's development pace has been inconsistent. For production deployments, consider:
> - Enabling `sled::Config::flush_every_ms(Some(1000))` for more frequent flushing
> - Monitoring sled's internal metrics (tree size, flush latency)
> - Evaluating alternatives like `redb` or `fjall` if stability issues arise in practice
> - WAL remains the source of truth — sled state can always be rebuilt from WAL replay

### 5.3 sled Maintenance

```
Background task (every hour):
  1. Scan "dedup" tree
  2. Delete entries where stored_at + TTL < now
  3. Trigger sled compaction to reclaim space
  4. Rebuild bloom filter from remaining dedup entries
```

## 6. Memory Ring Buffer

### 6.1 Purpose

The ring buffer serves as the primary event storage in memory mode, and as a fast delivery cache in balanced and durable modes:

1. **Primary storage (memory mode)**: all events live here. No WAL backing. Eviction on capacity is permanent data loss for that event.
2. **Fast delivery (balanced + durable modes)**: newly routed events are served from memory, not WAL disk reads.
3. **Catchup**: when a new subscriber joins with `position: "earliest"`, serve recent events from buffer instead of WAL replay.

### 6.2 Design

```rust
use crossbeam::queue::ArrayQueue;

pub struct RingBuffer {
    queue: ArrayQueue<BufferEntry>,
    recent: DashMap<MessageId, BufferEntry>,  // for msg_id lookups
    capacity: usize,
}

pub struct BufferEntry {
    msg_id: MessageId,
    topic: String,
    payload: Bytes,
    headers: Headers,
    wal_position: Option<WalPosition>,  // None in memory mode
    inserted_at: Instant,
}

impl RingBuffer {
    pub fn push(&self, entry: BufferEntry) {
        self.recent.insert(entry.msg_id, entry.clone());
        if self.queue.push(entry).is_err() {
            // Queue full — pop oldest, then push
            if let Some(old) = self.queue.pop() {
                self.recent.remove(&old.msg_id);
            }
            let _ = self.queue.push(entry);
        }
    }

    pub fn get(&self, msg_id: &MessageId) -> Option<&BufferEntry> {
        self.recent.get(msg_id).map(|r| r.value())
    }
}
```

> **Design note**: `ArrayQueue` (from crossbeam) provides lock-free MPMC (multi-producer, multi-consumer)
> operations, making concurrent pushes and pops safe without mutexes. The `DashMap` index enables O(1)
> lookups by message ID, which is critical for catchup scenarios where a reconnecting consumer needs to
> resume from a specific message rather than scanning the entire buffer.

### 6.3 Memory Budget

| Buffer Size | Entry Size (avg) | Total Memory | Recommended Mode |
|-------------|-----------------|--------------|-----------------|
| 10,000 | 1 KB | ~10 MB | Testing / low-volume durable |
| 100,000 | 1 KB | ~100 MB | Default (balanced + durable modes) |
| 1,000,000 | 1 KB | ~1 GB | Default for memory mode |
| 10,000,000 | 1 KB | ~10 GB | High-volume memory mode |

Default capacity: 100,000 for balanced and durable modes, 1,000,000 for memory mode. In memory mode, this buffer is the only storage — size it according to your throughput and acceptable event retention window. At 800K msg/sec, a 1M buffer holds ~1.25 seconds of events.

## 7. Disk Overflow Queue

When a consumer's in-memory delivery queue exceeds `max_pending_per_consumer`, events spill to disk:

```
/var/lib/pulse/overflow/consumer-{consumer_id}/
├── queue-000001.dat    # sequential event files
├── queue-000002.dat
└── meta.json           # { head: 1, tail: 2, count: 1500 }
```

Format is identical to WAL records (EVENT_WRITE). Events are read back sequentially when consumer catches up.

Overflow is transparent to the pipeline — the delivery queue abstraction handles memory/disk tiering automatically.

## 8. WAL Replication

### 8.1 Purpose

WAL replication provides durability across nodes and enables automatic failover in a distributed Pulse cluster. When a node fails, a follower with a replicated copy of the WAL can take over as the new leader with minimal or zero data loss.

### 8.2 Replication Modes

| Mode | Behavior | Data Loss on Leader Failure | Latency Impact |
|------|----------|---------------------------|----------------|
| **none** | Single-node, no replication. | Full WAL on failed node (unless disk survives). | None |
| **async** (default) | Leader streams WAL records to followers. Does not wait for PEER_ACK before ACKing publisher. | Up to ~1ms of replication lag. | None (replication is fire-and-forget from publisher's perspective) |
| **sync** | Leader waits for all replicas to send PEER_ACK before ACKing publisher. | Zero data loss (all replicas have the event). | +1-2ms per event (network RTT to replicas) |

**Configuration:**

```yaml
# broker.yaml
cluster:
  replication_mode: "async"     # "none" | "async" | "sync"
  replication_factor: 2         # number of follower copies
  replication_timeout_ms: 50    # sync mode: max wait for PEER_ACK
```

### 8.3 Replication Flow

```
Leader Node                          Follower Node
===========                          =============

1. Event passes pipeline
   (CRC, dedup, WAL append)

2. WAL record written to local
   segment file

3. Stream WAL record ──────────────> 4. Receive WAL record
   (TCP connection, framed with         via replication stream
   length prefix + CRC32)

                                     5. Write record to local WAL
                                        (in cluster/replication/topic-xxx/)

                                     6. Send PEER_ACK ──────────> 7. Leader records
                                        { msg_id, wal_position }     replication status

   (async mode: ACK to publisher                                  (sync mode: ACK to
    already sent at step 2)                                        publisher sent now)
```

### 8.4 Catch-Up Replication

When a new node joins the cluster or a recovering node comes back online, it needs to catch up on WAL entries it missed:

```
Catch-up flow:
  1. Follower connects to leader, sends last known WAL position
  2. Leader begins streaming from that position forward
  3. Follower writes records to local WAL sequentially
  4. Once caught up (lag < 1 batch), follower switches to real-time streaming
  5. Follower reports ready to gossip protocol

During catch-up:
  - Follower is NOT eligible for leader election
  - Follower does NOT serve reads for the affected topics
  - Progress metric: pulse_replication_catchup_progress{node="..."} (0.0 → 1.0)
```

### 8.5 Monitoring

| Metric | Description |
|--------|-------------|
| `pulse_replication_lag_ms` | Time delta between leader WAL write and follower PEER_ACK. Should be <5ms for async mode. |
| `pulse_replication_lag_events` | Number of WAL events the follower is behind the leader. |
| `pulse_replication_catchup_progress` | 0.0-1.0 progress indicator during catch-up replication. |
| `pulse_replication_peer_ack_latency_ms` | Histogram of PEER_ACK round-trip times. |

## 9. State DB Schema Migration

### 9.1 Versioning Strategy

The state DB stores a schema version in a dedicated sled tree:

```
Tree: "meta"
  Key:   "schema_version"
  Value: u32 (current: 1)
```

On startup, the broker reads the schema version and runs migrations if needed:

```rust
fn migrate_state_db(db: &sled::Db) -> Result<()> {
    let meta = db.open_tree("meta")?;
    let version: u32 = meta.get("schema_version")?
        .map(|v| u32::from_be_bytes(v.as_ref().try_into().unwrap()))
        .unwrap_or(0);

    match version {
        0 => {
            // Initial setup: create all trees
            db.open_tree("dedup")?;
            db.open_tree("event_state")?;
            db.open_tree("subscriptions")?;
            db.open_tree("consumer_offsets")?;
            db.open_tree("dlq")?;
            meta.insert("schema_version", &1u32.to_be_bytes())?;
        }
        1 => { /* current version, no migration needed */ }
        v => return Err(anyhow!("unknown schema version: {}", v)),
    }

    db.flush()?;
    Ok(())
}
```

### 9.2 Migration Safety

- Migrations run **before** the broker accepts connections
- Each migration is idempotent (safe to re-run after crash during migration)
- Unknown future versions cause the broker to refuse startup (prevents data corruption from running old broker against new schema)
- Backup state DB before major version upgrades: `cp -r /var/lib/pulse/state/ /var/lib/pulse/state.backup/`

## 10. Recovery with Changed Route Configuration

### 10.1 The Problem

During WAL recovery, events are re-routed to determine pending deliveries. If the route configuration has changed between the crash and recovery, the routing results may differ:

| Scenario | Effect |
|----------|--------|
| New route added | Events may be delivered to a consumer that wasn't a target at publish time |
| Route removed | Events may not be delivered to a consumer that was originally targeted |
| Filter changed | Events may be filtered in or out differently than originally |
| Transform changed | Delivered payload may differ from what was originally intended |

### 10.2 Broker Behavior

The broker uses the **current** route config during recovery, not a historical snapshot. This is a deliberate trade-off:
- Storing per-event route snapshots in WAL would significantly increase WAL size
- Route changes are infrequent and typically intentional
- The COMPLETION records in WAL accurately reflect what was already delivered

### 10.3 Recommendations

- **Avoid route config changes during planned maintenance windows** where a crash/restart is possible
- After changing routes, allow the broker to process all pending events before restarting
- Monitor `pulse_wal_recovery_duration_seconds` after restarts to detect large recovery replays
- If incorrect deliveries occur after recovery, use the Admin API to inspect and manually manage affected events
