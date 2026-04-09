# Data Flow & Exactly-Once Delivery

## 1. Event Lifecycle — Per Durability Mode

Pulse supports three durability modes. Each mode follows a different flow path through the broker pipeline, trading durability for throughput.

### 1.1 Memory Mode (Fastest)

No WAL write. Events live in memory only. Disk overflow for slow consumers.

```
    PUBLISHER SIDE              BROKER                         CONSUMER SIDE
    ==============              ======                         =============

 1. SDK serializes             
    payload (opaque bytes      
    or user-chosen format)     
                               
 2. SDK generates UUIDv7       
    message ID (or reuses      
    on retry)                  
                               
 3. SDK builds PUB frame       
    with CRC32                 
                               
 4. SDK sends over TCP/TLS ──> 5. Receive frame              
                                  Verify CRC32               
                               
                               6. Decode frame               
                                  Extract topic, msg_id      
                               
                               7. Permission check           
                                  session.can_publish(topic)  
                                  → FAIL: send ERR(4030)     
                               
                               8. Dedup check (optional)     
                                  bloom_filter.may_contain(id)
                                  → YES: send ACK            
                                    (status:"duplicate")     
                                  → NO: continue             
                                  (no sled lookup)           
                               
                               9. Memory buffer insert        
                                  ring_buffer.push(event)    
                               
                              10. Send ACK(status:"stored")   
 11. SDK receives ACK ←────── 
     Publisher done ✓          
                              11. Route evaluation            
                                  match topic against rules  
                                  evaluate content filters   
                                  apply transforms           
                               
                              12. Fan-out to consumer queues  
                                  For each matched consumer: 
                                  delivery_queue.push(event) 
                               
                              13. Delivery: send PUB frame ──> 14. SDK receives frame
                                  to consumer via TCP/TLS         Verify CRC32
                                  Set state: DELIVERED            Decode payload
                                  Start ack_timeout timer    
                                                              15. Call user's handler
                                                              
                                                              16a. Handler returns Ok
                                                                   → SDK sends ACK
                              17. Receive ACK ←──────────────      (status:"done")
                                  Mark event: COMPLETED       
                                  Remove from in-flight      
                                                              
                                                              16b. Handler returns Err
                                                                   → SDK sends ACK
                              17. Receive ACK                      (status:"rejected")
                                  (a.k.a. NACK)              
                                  Schedule retry             
                                  increment attempt counter  
                                  → if attempt > max_retries 
                                    → move to DLQ            
                                                              
                                                              16c. No response (timeout)
                              17. ack_timeout fires           
                                  Schedule retry             
                                  (same as rejected path)        
```

**Key differences from other modes**: No WAL write at step 9. No sled dedup lookup. No COMPLETION record on consumer ACK. Fastest possible path.

### 1.2 Balanced Mode (Default for Production)

WAL writes are batched via group commit (single fsync every 5ms). Bloom-only dedup (no sled confirmation).

```
    PUBLISHER SIDE              BROKER                         CONSUMER SIDE
    ==============              ======                         =============

 1. SDK serializes             
    payload (opaque bytes      
    or user-chosen format)     
                               
 2. SDK generates UUIDv7       
    message ID (or reuses      
    on retry)                  
                               
 3. SDK builds PUB frame       
    with CRC32                 
                               
 4. SDK sends over TCP/TLS ──> 5. Receive frame              
                                  Verify CRC32               
                               
                               6. Decode frame               
                                  Extract topic, msg_id      
                               
                               7. Permission check           
                                  session.can_publish(topic)  
                                  → FAIL: send ERR(4030)     
                               
                               8. Dedup check                
                                  bloom_filter.may_contain(id)
                                  → YES: send ACK            
                                    (status:"duplicate")     
                                  → NO: continue (new event) 
                                  (bloom-only, no sled)      
                               
                               9. WAL append (batched)       
                                  Record: EVENT_WRITE        
                                  Queued for group commit    
                                  (fsync every 5ms)          
                                  → FAIL: send ERR(5000)     
                               
                              10. Dedup insert               
                                  bloom_filter.insert(id)    
                                  (no sled insert)           
                               
                              11. Memory buffer insert        
                                  ring_buffer.push(event)    
                               
                              12. Send ACK(status:"stored")   
 13. SDK receives ACK ←────── 
     Publisher done ✓          
                              13. Route evaluation            
                                  match topic against rules  
                                  evaluate content filters   
                                  apply transforms           
                               
                              14. Fan-out to consumer queues  
                                  For each matched consumer: 
                                  delivery_queue.push(event) 
                               
                              15. Delivery: send PUB frame ──> 16. SDK receives frame
                                  to consumer via TCP/TLS         Verify CRC32
                                  Set state: DELIVERED            Decode payload
                                  Start ack_timeout timer    
                                                              17. Call user's handler
                                                                  function
                                                              
                                                              18a. Handler returns Ok
                                                                   → SDK sends ACK
                              19. Receive ACK ←──────────────      (status:"done")
                                  Mark event: COMPLETED       
                                  WAL: append COMPLETION     
                                  Remove from in-flight      
                                                              
                                                              18b. Handler returns Err
                                                                   → SDK sends ACK
                              19. Receive ACK                      (status:"rejected")
                                  (a.k.a. NACK)              
                                  Schedule retry             
                                  increment attempt counter  
                                  → if attempt > max_retries 
                                    → move to DLQ            
                                                              
                                                              18c. No response (timeout)
                              19. ack_timeout fires           
                                  Schedule retry             
                                  (same as rejected path)        
```

**Key differences from durable mode**: WAL writes are batched (not per-event fsync). Bloom filter is the only dedup layer (no sled confirmation on publish path). Up to 5ms of writes may be lost on crash.

### 1.3 Durable Mode (Zero Data Loss)

Per-event fsync. Full two-layer dedup (bloom + sled). For financial/audit workloads.

```
    PUBLISHER SIDE              BROKER                         CONSUMER SIDE
    ==============              ======                         =============

 1. SDK serializes             
    payload (opaque bytes      
    or user-chosen format)     
                               
 2. SDK generates UUIDv7       
    message ID (or reuses      
    on retry)                  
                               
 3. SDK builds PUB frame       
    with CRC32                 
                               
 4. SDK sends over TCP/TLS ──> 5. Receive frame              
                                  Verify CRC32               
                               
                               6. Decode frame               
                                  Extract topic, msg_id      
                               
                               7. Permission check           
                                  session.can_publish(topic)  
                                  → FAIL: send ERR(4030)     
                               
                               8. Dedup check                
                                  bloom_filter.may_contain(id)
                                  → YES: sled.get(id)        
                                    → exists: send ACK       
                                      (status:"duplicate")   
                                  → NO: continue (new event) 
                               
                               9. WAL append + fsync         
                                  Record: EVENT_WRITE        
                                  → FAIL: send ERR(5000)     
                               
                              10. Dedup insert               
                                  bloom_filter.insert(id)    
                                  sled.insert(id, metadata)  
                               
                              11. Memory buffer insert        
                                  ring_buffer.push(event)    
                               
                              12. Send ACK(status:"stored")   
 13. SDK receives ACK ←────── 
     Publisher done ✓          
                              13. Route evaluation            
                                  match topic against rules  
                                  evaluate content filters   
                                  apply transforms           
                               
                              14. Fan-out to consumer queues  
                                  For each matched consumer: 
                                  delivery_queue.push(event) 
                               
                              15. Delivery: send PUB frame ──> 16. SDK receives frame
                                  to consumer via TCP/TLS         Verify CRC32
                                  Set state: DELIVERED            Decode payload
                                  Start ack_timeout timer    
                                                              17. Call user's handler
                                                                  function
                                                              
                                                              18a. Handler returns Ok
                                                                   → SDK sends ACK
                              19. Receive ACK ←──────────────      (status:"done")
                                  Mark event: COMPLETED       
                                  WAL: append COMPLETION     
                                  Remove from in-flight      
                                                              
                                                              18b. Handler returns Err
                                                                   → SDK sends ACK
                              19. Receive ACK                      (status:"rejected")
                                  (a.k.a. NACK)              
                                  Schedule retry             
                                  increment attempt counter  
                                  → if attempt > max_retries 
                                    → move to DLQ            
                                                              
                                                              18c. No response (timeout)
                              19. ack_timeout fires           
                                  Schedule retry             
                                  (same as rejected path)        
```

## 2. Exactly-Once Mechanics

### 2.0 Delivery Guarantees by Durability Mode

Exactly-once delivery is **only available in `durable` mode**, which uses the full two-layer dedup engine (bloom + sled) and per-event fsync.

| Durability Mode | Publisher → Broker | Broker → Consumer | End-to-End Guarantee |
|-----------------|-------------------|-------------------|---------------------|
| **memory** | at-most-once (no WAL) | at-least-once (with redelivery) | at-most-once, or at-least-once with optional bloom dedup |
| **balanced** | at-least-once (bloom dedup, async WAL) | at-least-once (with redelivery) | at-least-once with broker dedup (bloom only, no sled confirmation) |
| **durable** | exactly-once (bloom + sled dedup, per-event fsync) | at-least-once + consumer dedup = exactly-once processing | exactly-once |

**Memory mode** is fire-and-forget: events are not persisted and will be lost on crash. Optional bloom dedup catches obvious retries but without sled backing, false positives cannot be resolved deterministically.

**Balanced mode** provides strong at-least-once guarantees. The bloom filter catches most duplicates, but without sled confirmation, a bloom false positive will incorrectly reject a new event (~0.1% rate). For most workloads this is an acceptable trade-off.

**Durable mode** is the only mode that guarantees exactly-once semantics end-to-end, using the full dedup pipeline described below.

### 2.1 Publisher-Side Deduplication

**Problem**: Publisher sends PUB, broker writes WAL, but ACK is lost on the network. Publisher retries, creating a potential duplicate.

**Solution**: Publisher always retries with the same Message ID. Broker deduplicates.

```
Timeline (durable mode):
  t=0   Publisher sends PUB(id=abc)
  t=1   Broker writes WAL ✓
  t=2   Broker sends ACK → lost on network
  t=5   Publisher timeout, retries PUB(id=abc)  ← SAME ID
  t=6   Broker dedup: id=abc already in WAL
  t=7   Broker sends ACK(status:"duplicate")
  t=8   Publisher receives ACK ✓

Result: Event abc exists exactly once in WAL.
```

**SDK responsibility**: The SDK generates Message ID once and caches it until ACK is received. On reconnect, pending (unACKed) publishes are replayed with original IDs.

### 2.2 Broker-Side Deduplication Engine

Two-layer design for performance (durable mode uses both layers; balanced mode uses Layer 1 only):

```
Layer 1: Bloom Filter (in-memory) — used in balanced + durable modes
  ├── Capacity: 1,000,000 entries
  ├── False positive rate: 0.1%
  ├── Memory: ~1.2 MB
  ├── Lookup time: O(k) where k = number of hash functions (~7)
  └── Purpose: fast negative check — if bloom says NO, it's definitely new

Layer 2: sled Database (on disk, cached in memory by sled) — durable mode only
  ├── Key: msg_id (16 bytes)
  ├── Value: { timestamp, topic, state } (~64 bytes)
  ├── Purpose: exact check when bloom filter says MAYBE
  └── TTL: entries expire after 7 days (configurable)
```

**Dedup flow**:

```rust
pub async fn check(&self, msg_id: &MessageId, mode: DurabilityMode) -> DedupResult {
    // Memory mode with dedup disabled: skip entirely
    if mode == DurabilityMode::Memory && !self.bloom_enabled {
        return DedupResult::New;
    }

    // Fast path: bloom filter says definitely not seen
    if !self.bloom.may_contain(msg_id) {
        return DedupResult::New;
    }

    // Balanced mode: bloom says maybe → treat as duplicate (no sled to confirm)
    if mode == DurabilityMode::Balanced {
        return DedupResult::Duplicate;
    }

    // Durable mode: bloom says maybe → check sled for certainty
    match self.db.get(msg_id)? {
        Some(_) => DedupResult::Duplicate,
        None => DedupResult::New,  // bloom false positive
    }
}

pub async fn insert(&self, msg_id: &MessageId, metadata: &EventMeta, mode: DurabilityMode) {
    self.bloom.insert(msg_id);
    if mode == DurabilityMode::Durable {
        self.db.insert(msg_id, metadata)?;
    }
}
```

**Bloom filter maintenance**:
- Rebuilt every 24 hours from sled (removes expired entries) in durable mode
- In balanced mode, rebuilt from WAL replay or reset on rotation schedule
- During rebuild: old filter still serves reads; new filter swaps in atomically via `ArcSwap`

### 2.3 Consumer-Side Deduplication

**Problem**: Broker delivers event to consumer. Consumer processes it, sends ACK, but ACK is lost. Broker retries delivery. Consumer processes again — duplicate processing.

**Solution**: Consumer SDK tracks recently processed Message IDs.

```
Consumer SDK internal:
  processed_ids: LruCache<MessageId, ()>  // capacity: 10,000

On event received:
  if processed_ids.contains(msg_id):
    → send ACK immediately (already processed)
    → do NOT call user's handler
  else:
    → call user's handler
    → if Ok: insert msg_id into processed_ids, send ACK
    → if Err: send NACK (do not insert)
```

**Edge case**: Consumer restarts, LRU cache is lost. Broker re-delivers an event that was already processed before restart.

**Mitigation**: For consumers that cannot tolerate this, SDK provides optional persistent dedup:

```rust
pulse.subscribe_opts("payment.completed", SubscribeOpts {
    dedup: Dedup::Persistent("/var/lib/myservice/pulse-dedup"),
    ..Default::default()
}, |event| async move {
    // guaranteed exactly-once even across restarts
    Ok(())
}).await?;
```

This uses a local sled database on the consumer side. Default (in-memory LRU) is sufficient for most cases.

## 3. Failure Scenarios — Detailed

### 3.0 Failure Impact by Durability Mode

| Failure | Memory Mode | Balanced Mode | Durable Mode |
|---------|-------------|---------------|--------------|
| Broker crash | In-memory events lost. Disk overflow data survives. | Up to 5ms of writes lost (group commit window). WAL-committed data survives. | Zero data loss. Full WAL recovery. |
| Power loss | Same as crash | Same as crash | Same as crash (fsync guarantees) |
| Node failure (distributed) | Gossip detects in ~2s. Failover to replica. In-flight events on failed node lost. | Gossip detects in ~2s. Failover to replica. Up to 5ms + replication lag lost. | Gossip detects in ~2s. Failover to replica. Sync replication: zero loss. Async: up to replication lag lost. |
| Disk failure | No impact (no WAL). Memory continues serving. | Fatal for node. Failover to replica required. | Fatal for node. Failover to replica required. |

### 3.1 Broker Crash During WAL Write (Balanced + Durable)

```
Scenario: Broker crashes after partial write to WAL segment.

WAL segment on disk:
  [Record 1: complete, valid CRC] ✓
  [Record 2: complete, valid CRC] ✓
  [Record 3: partial, truncated]  ✗

Recovery:
  1. Open segment file
  2. Read records sequentially
  3. For each record: verify length field matches actual data, verify CRC32
  4. Record 3 fails CRC → truncate file at Record 2 boundary
  5. Record 3's event was never ACKed to publisher (crash happened before ACK)
  6. Publisher will timeout and retry → event written cleanly

Result (durable mode): No data corruption. No duplicate. No loss.
Result (balanced mode): Records in the uncommitted group commit batch (~5ms window)
  are also lost. Publishers of those events will timeout and retry.
```

### 3.2 Broker Crash After ACK, Before Delivery (Balanced + Durable)

```
Scenario: Event abc written to WAL, ACKed to publisher, broker crashes before delivery.

Recovery:
  1. WAL replay finds: abc has EVENT_WRITE record, no COMPLETION record
  2. abc is "pending" → re-enter routing pipeline
  3. Route evaluation produces delivery targets
  4. Events enqueued for delivery

Result: Event delivered, possibly with delay. No loss.
```

### 3.3 Broker Crash After Delivery, Before Consumer ACK (All Modes)

```
Scenario: Event abc delivered to consumer, consumer is processing, broker crashes.

Recovery (balanced + durable modes):
  1. WAL replay: abc has EVENT_WRITE, no COMPLETION
  2. abc re-enters delivery pipeline
  3. Consumer may receive abc again (if consumer also reconnects)
  4. Consumer-side dedup catches duplicate

Recovery (memory mode):
  1. No WAL to replay. Event abc is gone from broker state.
  2. If consumer already processed it: no issue.
  3. If consumer had not yet ACKed: event is lost.

Result (balanced + durable): No duplicate processing (dedup). No loss.
Result (memory): Event may be lost if not yet processed by consumer.
```

### 3.4 Consumer Crash During Processing

```
Scenario: Consumer receives abc, handler function panics/crashes.

Broker side:
  1. TCP connection drops → broker detects consumer disconnect
  2. All in-flight events for that consumer → state back to PENDING
  3. When consumer reconnects and re-subscribes → events re-delivered

Consumer side:
  1. Process restarts
  2. SDK auto-reconnects (exponential backoff)
  3. SDK re-sends all SUB frames
  4. Broker starts delivering pending events

Result: Event re-processed. Consumer dedup prevents duplicate side-effects.
```

### 3.5 Network Partition Between Broker and Consumer

```
Scenario: Consumer is alive but network path to broker is broken.

Broker side:
  1. PING/PONG fails after keepalive_timeout (30s)
  2. Broker marks consumer as disconnected
  3. In-flight events → state back to PENDING
  4. New events for this consumer → queued (up to max_pending)
  5. If queue exceeds max_pending → overflow to disk

Consumer side:
  1. SDK detects connection loss (PONG timeout or write error)
  2. SDK begins reconnect loop with backoff
  3. Network recovers → TCP reconnect → TLS → CONNECT → SUB
  4. Broker drains queued events to consumer

Result: Events delayed but not lost. Order preserved within topic.
```

### 3.6 Slow Consumer (Backpressure)

```
Scenario: Consumer handler takes 5 seconds per event, broker has 100 events queued.

Flow:
  1. Consumer sets max_inflight: 1 (via FLOW frame or connect config)
  2. Broker delivers 1 event
  3. Broker waits for ACK before delivering next
  4. Events accumulate in consumer's delivery queue (in-memory)
  5. If queue > max_pending_per_consumer:
     → Overflow to disk-backed queue
     → Publisher still receives ACK (event is durable)
     → No one is blocked except the slow consumer

Metrics emitted:
  - pulse_consumer_queue_depth{consumer="payment-svc"} = 100
  - pulse_consumer_queue_overflow{consumer="payment-svc"} = 1
  → Alerts can trigger on these
```

### 3.7 DLQ Flow

```
Scenario: Consumer consistently fails to process event abc.

Timeline:
  Attempt 1: deliver → handler returns Err → NACK
  (wait 1s)
  Attempt 2: deliver → handler returns Err → NACK
  (wait 2s)
  Attempt 3: deliver → handler returns Err → NACK
  (wait 4s)
  Attempt 4: deliver → handler returns Err → NACK
  (wait 8s)
  Attempt 5: deliver → handler returns Err → NACK
  → max_redeliveries reached

DLQ action:
  1. Event moved to DLQ topic: "dlq.{original_topic}"
     e.g., "dlq.order.created"
  2. DLQ event includes:
     - Original event payload
     - All headers
     - Delivery metadata: attempt count, first/last attempt timestamps
     - Last error reason (from NACK payload)
  3. Alert webhook fired (if configured in routes.yaml)
  4. Event state marked as DLQ in state DB
  5. DLQ events can be:
     - Inspected via admin API
     - Replayed (re-injected into original topic)
     - Purged after investigation
```

### 3.8 Distributed Data Flow

In a multi-node Pulse cluster, events are routed to the correct node via consistent hashing on the topic name. The cluster uses gossip for membership and failure detection.

#### Publisher Flow (Distributed)

```
Publisher → Any Node (ingress)
  │
  ├── Is this node the topic leader?
  │     YES → proceed with local pipeline (per durability mode)
  │     NO  → forward PUB frame to topic leader node
  │
  └── Topic Leader:
        1. Run full pipeline (CRC, dedup, WAL, route, etc.)
        2. Replicate WAL entry to follower nodes
        3. Wait for replication ACK (sync mode) or continue (async mode)
        4. Send ACK(status:"stored") back through ingress node to publisher
```

#### Consumer Flow (Distributed)

```
Consumer → Any Node (ingress)
  │
  ├── Consumer sends SUB(topic)
  │
  └── Ingress node:
        1. Discover which node owns the topic (consistent hash ring)
        2. If local → register subscription locally
        3. If remote → proxy subscription to topic owner
           OR redirect consumer to connect directly to topic owner
        4. Events delivered from topic owner to consumer
           (via proxy or direct connection)
```

#### Replication Flow

```
Topic Leader                     Follower Node(s)
============                     ================

1. WAL append (EVENT_WRITE)
2. Stream WAL record ──────────> 3. Receive WAL record
                                 4. Write to local WAL
                                 5. Send PEER_ACK ──────> 6. Leader records
                                                             replication status

Replication modes:
  - none:  single-node, no replication
  - async: leader does not wait for PEER_ACK before ACKing publisher (~1ms lag)
  - sync:  leader waits for all replicas to PEER_ACK before ACKing publisher

On node failure:
  1. Gossip protocol detects failure (~2s)
  2. Consistent hash ring updated
  3. New leader elected from followers
  4. Followers switch replication source
  5. New leader begins serving reads and writes for affected topics
```

## 4. Event State Machine

Every event in the broker has a state tracked in the state DB:

```
                    ┌────────────┐
  PUB received ──>  │  RECEIVED  │  (frame validated, not yet written)
                    └─────┬──────┘
                          │ WAL write success (balanced/durable)
                          │ or buffer insert (memory)
                          ▼
                    ┌────────────┐
                    │   STORED   │  (WAL durable, ACK sent to publisher)
                    └─────┬──────┘
                          │ routing resolved
                          ▼
                    ┌────────────┐
               ┌──> │  PENDING   │  (in consumer delivery queue)
               │    └─────┬──────┘
               │          │ sent to consumer
               │          ▼
               │    ┌────────────┐
  timeout/     │    │ DELIVERED  │  (sent, awaiting ACK)
  NACK         │    └─────┬──────┘
               │          │
               │     ┌────┴────┐
               │     │         │
               │   ACK OK    ACK FAIL or timeout
               │     │         │
               │     ▼         ▼
               │  ┌──────┐  ┌────────┐
               │  │ DONE │  │ RETRY  │  (schedule re-delivery)
               │  └──────┘  └───┬────┘
               │                │
               │    ┌───────────┴──────────┐
               │    │                      │
               │  attempt < max         attempt >= max
               │    │                      │
               └────┘                      ▼
                                     ┌──────────┐
                                     │   DLQ    │
                                     └──────────┘
```

**State storage:**

```rust
// sled key-value for event state
key:   msg_id (16 bytes)
value: EventState {
    state: State,              // enum: Received, Stored, Pending, Delivered, Done, Dlq
    topic: String,
    produced_at: u64,          // millis
    stored_at: u64,
    delivered_at: Option<u64>,
    completed_at: Option<u64>,
    attempt: u32,
    target_consumer: String,
    last_error: Option<String>,
}
```

**Note on fan-out**: If an event routes to N consumers, there are N state entries (keyed by `msg_id + consumer_id`). The event is DONE for the system when all N entries are DONE (or DLQ).

## 5. Delivery Guarantee Summary — Per Mode

```
== MEMORY MODE ==
Publisher → Broker:
  Guarantee: at-most-once (no WAL)
  Mechanism: optional bloom dedup for retry suppression
  Trade-off: events lost on crash; maximum throughput

Broker → Consumer:
  Guarantee: at-least-once delivery (with redelivery on timeout/NACK)
  Mechanism: broker retries on timeout/NACK, consumer SDK dedup cache

End-to-end:
  at-most-once for persistence, at-least-once for delivery if broker stays up.
  Best for: ephemeral events, metrics, logs, real-time analytics.

== BALANCED MODE ==
Publisher → Broker:
  Guarantee: at-least-once write to WAL (with bloom dedup)
  Mechanism: UUIDv7 Message ID + bloom filter dedup (no sled confirmation)
  Trade-off: ~0.1% false positive rate may reject valid new events; up to 5ms data loss on crash

Broker → Consumer:
  Guarantee: at-least-once delivery + consumer dedup
  Mechanism: broker retries on timeout/NACK, consumer SDK dedup cache

End-to-end:
  at-least-once with strong dedup. Sufficient for the vast majority of workloads.
  Best for: general production use, microservice events, notifications.

== DURABLE MODE ==
Publisher → Broker:
  Guarantee: exactly-once write to WAL
  Mechanism: UUIDv7 Message ID + broker dedup engine (bloom + sled)
  SDK role:  retry with same ID, cache ID until ACK received

Broker → Consumer:
  Guarantee: at-least-once delivery + consumer dedup = exactly-once processing
  Mechanism: broker retries on timeout/NACK, consumer SDK dedup cache
  SDK role:  LRU cache of processed IDs (optional: persistent sled)
  User role: design idempotent handlers as defense-in-depth

End-to-end:
  Each event is written once, delivered once (effectively), processed once.
  Edge case: consumer restart without persistent dedup → at-least-once
  Mitigation: enable persistent dedup for critical consumers
  Best for: financial transactions, audit trails, payment processing.
```

## 6. Performance Characteristics

### 6.1 Latency by Durability Mode

| Metric | Memory Mode | Balanced Mode | Durable Mode |
|--------|-------------|---------------|--------------|
| PUB → ACK (stored) | <5 us | 0.5-5 ms | 1-5 ms |
| Route evaluation | <0.1 ms | <0.1 ms | <0.1 ms |
| Delivery (in-flight) | <1 ms | <1 ms | <1 ms |
| End-to-end (PUB → consumer handler) | <20 us | 2-10 ms | 2-10 ms |
| Dedup check (bloom hit: no) | <0.01 ms | <0.01 ms | <0.01 ms |
| Dedup check (bloom hit: yes) | <0.01 ms (bloom only) | <0.01 ms (bloom only) | <0.5 ms (sled lookup) |
| Recovery (100K events) | N/A (no WAL) | ~2 seconds | ~2 seconds |

### 6.2 Throughput by Durability Mode

| Durability Mode | Target Throughput (msg/sec) | Bottleneck |
|-----------------|---------------------------|------------|
| **memory** | 800,000+ | CPU, memory bandwidth |
| **balanced** | 100,000+ | Group commit fsync (amortized over 5ms batches) |
| **durable** | 10,000+ | Per-event fsync + sled write |

Memory mode matches or exceeds NSQ's published benchmarks (~200K msg/sec) by 4x, with lower tail latency due to the zero-disk-write hot path.

### 6.3 General Notes

All latencies assume SSD storage and same-region network. Internet (cross-region) adds network RTT. Distributed mode adds forwarding latency for events that land on a non-leader node (typically <1ms within the same datacenter).

## 7. Consumer Offset & Reconnect Behavior

Understanding what happens to events when a consumer disconnects and reconnects is critical for delivery guarantees.

### 7.1 During Disconnection

When the broker detects a consumer disconnect (TCP drop or PING/PONG timeout):

1. All in-flight events (DELIVERED state) for that consumer → back to **PENDING**
2. New events matching the consumer's subscriptions → **queued** in the consumer's delivery queue
3. Queue grows up to `max_pending_per_consumer` (default: 10,000)
4. Beyond that limit → overflow to disk-backed queue (see WAL docs S7)

The broker holds events for disconnected consumers **indefinitely** as long as the subscription exists in state DB. Subscriptions persist across disconnects.

### 7.2 On Reconnect

| `position` setting | Behavior on reconnect |
|--------------------|----------------------|
| `"latest"` (default) | Resume from broker's queue. Events queued during disconnect are delivered. Events that arrived *after* the queue overflowed and was drained are lost. |
| `"earliest"` | Replay from ring buffer (memory) or WAL if buffer doesn't cover the gap. May cause re-delivery of already-processed events — consumer-side dedup handles this. |

### 7.3 Consumer Group Reconnect

When a consumer group member reconnects:
1. Broker re-adds it to the group's member list
2. New events are load-balanced across all active members (including the reconnected one)
3. Events that were **in-flight** to this member before disconnect were already returned to PENDING and may have been rebalanced to another member

### 7.4 Subscription Expiry

If a consumer does not reconnect within `subscription_ttl` (default: 7 days, configurable):
1. Subscription is removed from state DB
2. Queued events for that consumer are discarded
3. Consumer must re-subscribe on next connect (SDK does this automatically)

This prevents unbounded queue growth for permanently offline consumers.
