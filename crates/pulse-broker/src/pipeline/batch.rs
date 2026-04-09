use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, Mutex};

use pulse_protocol::{MessageId, PubPayload};

use crate::error::BrokerError;
use crate::pipeline::dedup::{DedupEngine, DedupResult};
use crate::pipeline::ingest::IngestResult;
use crate::routing::Router;
use crate::storage::wal::WalWriter;

/// A message in the batch pipeline.
pub struct BatchIngestMessage {
    pub msg_id: MessageId,
    pub pub_payload: PubPayload,
    pub namespace: String,
    pub reply_tx: oneshot::Sender<IngestResult>,
}

/// High-throughput batch pipeline.
///
/// Collects events for up to `batch_interval` (default 5ms) or until
/// `max_batch_size` events accumulate, then processes the entire batch:
///
/// 1. Bloom dedup check (all at once, ~29ns each)
/// 2. Serialize all payloads
/// 3. WAL write all records (no sync between records)
/// 4. Single fsync for entire batch
/// 5. Bloom insert all new IDs
/// 6. Notify all waiters
///
/// This eliminates per-event sled I/O and amortizes fsync across the batch.
pub struct BatchPipeline;

impl BatchPipeline {
    /// Spawn the batch pipeline.
    pub fn spawn(
        dedup: Arc<DedupEngine>,
        wal: WalWriter,
        mut rx: mpsc::Receiver<BatchIngestMessage>,
        router: Option<Arc<Router>>,
        batch_interval_ms: u64,
        max_batch_size: usize,
    ) -> tokio::task::JoinHandle<()> {
        let wal = Arc::new(Mutex::new(wal));
        let batch_interval = Duration::from_millis(batch_interval_ms);

        tokio::spawn(async move {
            let mut pending: Vec<BatchIngestMessage> = Vec::with_capacity(max_batch_size);

            loop {
                // Wait for first message
                match rx.recv().await {
                    Some(msg) => pending.push(msg),
                    None => break,
                }

                // Collect more until interval or batch cap
                let deadline = tokio::time::sleep(batch_interval);
                tokio::pin!(deadline);

                loop {
                    if pending.len() >= max_batch_size {
                        break;
                    }
                    tokio::select! {
                        biased;
                        msg = rx.recv() => {
                            match msg {
                                Some(m) => pending.push(m),
                                None => break,
                            }
                        }
                        _ = &mut deadline => break,
                    }
                }

                // Process batch
                process_batch(&dedup, &wal, &router, &mut pending).await;
            }

            // Flush remaining
            if !pending.is_empty() {
                process_batch(&dedup, &wal, &router, &mut pending).await;
            }

            tracing::info!("batch pipeline shutdown");
        })
    }
}

async fn process_batch(
    dedup: &Arc<DedupEngine>,
    wal: &Arc<Mutex<WalWriter>>,
    router: &Option<Arc<Router>>,
    pending: &mut Vec<BatchIngestMessage>,
) {
    let batch_size = pending.len();
    if batch_size == 0 {
        return;
    }

    // Phase 1: Dedup check (bloom-only for balanced, very fast)
    let mut results: Vec<Option<IngestResult>> = Vec::with_capacity(batch_size);
    let mut new_indices: Vec<usize> = Vec::with_capacity(batch_size);

    for (i, msg) in pending.iter().enumerate() {
        match dedup.check(&msg.msg_id) {
            Ok(DedupResult::Duplicate) => {
                results.push(Some(IngestResult::Duplicate));
            }
            Ok(DedupResult::New) => {
                results.push(None); // will be filled after WAL write
                new_indices.push(i);
            }
            Err(e) => {
                results.push(Some(IngestResult::Failed { error: e }));
            }
        }
    }

    // Phase 2: Serialize + WAL write all new events (no sync between writes)
    if !new_indices.is_empty() {
        let mut wal = wal.lock().await;

        for &idx in &new_indices {
            let msg = &pending[idx];
            let data = match rmp_serde::to_vec_named(&msg.pub_payload) {
                Ok(bytes) => bytes,
                Err(e) => {
                    results[idx] = Some(IngestResult::Failed {
                        error: BrokerError::Serialize(e.to_string()),
                    });
                    continue;
                }
            };

            match wal.append_event_no_sync(msg.msg_id, &data).await {
                Ok(position) => {
                    results[idx] = Some(IngestResult::Stored { position });
                }
                Err(e) => {
                    results[idx] = Some(IngestResult::Failed { error: e });
                }
            }
        }

        // Phase 3: Single fsync for entire batch
        if let Err(e) = wal.sync().await {
            tracing::error!(error = %e, "batch fsync failed");
            // Mark all WAL-written events as failed
            for &idx in &new_indices {
                if matches!(results[idx], Some(IngestResult::Stored { .. })) {
                    results[idx] = Some(IngestResult::Failed {
                        error: BrokerError::Wal(format!("batch fsync failed: {e}")),
                    });
                }
            }
        }
    }

    // Phase 4: Dedup insert for all successfully stored events (bloom update, fast)
    for &idx in &new_indices {
        if matches!(results[idx], Some(IngestResult::Stored { .. })) {
            let msg = &pending[idx];
            let _ = dedup.insert(&msg.msg_id, &msg.pub_payload.topic);
        }
    }

    // Phase 5: Route + deliver + notify
    for (msg, result) in pending.drain(..).zip(results) {
        let result = result.unwrap_or(IngestResult::Failed {
            error: BrokerError::Wal("batch processing error".into()),
        });

        // Route on success
        if matches!(result, IngestResult::Stored { .. }) {
            if let Some(router) = router {
                let targets = router.resolve(&msg.pub_payload.topic);
                for target in &targets {
                    let delivery_frame =
                        pulse_protocol::Frame::publish(msg.msg_id, msg.pub_payload.clone());
                    let _ = target.deliver_tx.try_send(delivery_frame);
                }
            }
        }

        let _ = msg.reply_tx.send(result);
    }

    if batch_size > 1 {
        tracing::trace!(batch_size, "batch processed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BrokerConfig, DurabilityMode};
    use crate::storage::state_db::StateDb;
    use std::collections::HashMap;

    fn test_pub_payload(topic: &str) -> PubPayload {
        PubPayload {
            topic: topic.into(),
            data: rmpv::Value::String("test-data".into()),
            headers: HashMap::new(),
            produced_at: None,
            delivery: None,
        }
    }

    #[tokio::test]
    async fn batch_pipeline_processes_events() {
        let dir = tempfile::tempdir().unwrap();
        let config = BrokerConfig::for_testing(dir.path().to_path_buf());

        let state_db = Arc::new(StateDb::open(config.data_dir.join("state")).unwrap());
        let wal = WalWriter::open(config.data_dir.join("wal"), &config.wal)
            .await
            .unwrap();
        let dedup = Arc::new(DedupEngine::tiered(state_db, DurabilityMode::Balanced));

        let (tx, rx) = mpsc::channel(1024);
        let _handle = BatchPipeline::spawn(dedup, wal, rx, None, 5, 100);

        // Send 10 events
        let mut reply_rxs = Vec::new();
        for i in 0..10 {
            let (reply_tx, reply_rx) = oneshot::channel();
            tx.send(BatchIngestMessage {
                msg_id: MessageId::new(),
                pub_payload: test_pub_payload(&format!("topic.{i}")),
                namespace: "default".into(),
                reply_tx,
            })
            .await
            .unwrap();
            reply_rxs.push(reply_rx);
        }

        // All should succeed
        for rx in reply_rxs {
            let result = rx.await.unwrap();
            assert!(
                matches!(result, IngestResult::Stored { .. }),
                "expected Stored, got {result:?}"
            );
        }
    }

    #[tokio::test]
    async fn batch_pipeline_dedup_within_batch() {
        let dir = tempfile::tempdir().unwrap();
        let config = BrokerConfig::for_testing(dir.path().to_path_buf());

        let state_db = Arc::new(StateDb::open(config.data_dir.join("state")).unwrap());
        let wal = WalWriter::open(config.data_dir.join("wal"), &config.wal)
            .await
            .unwrap();
        let dedup = Arc::new(DedupEngine::tiered(state_db, DurabilityMode::Balanced));

        let (tx, rx) = mpsc::channel(1024);
        let _handle = BatchPipeline::spawn(dedup, wal, rx, None, 50, 100);

        let msg_id = MessageId::new();

        // Send same msg_id twice
        let (reply_tx1, reply_rx1) = oneshot::channel();
        let (reply_tx2, reply_rx2) = oneshot::channel();

        tx.send(BatchIngestMessage {
            msg_id,
            pub_payload: test_pub_payload("t"),
            namespace: "default".into(),
            reply_tx: reply_tx1,
        })
        .await
        .unwrap();

        // Wait for first to be processed before sending duplicate
        let r1 = reply_rx1.await.unwrap();
        assert!(matches!(r1, IngestResult::Stored { .. }));

        tx.send(BatchIngestMessage {
            msg_id,
            pub_payload: test_pub_payload("t"),
            namespace: "default".into(),
            reply_tx: reply_tx2,
        })
        .await
        .unwrap();

        let r2 = reply_rx2.await.unwrap();
        assert!(matches!(r2, IngestResult::Duplicate));
    }
}
