use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use pulse_protocol::{MessageId, PubPayload};

use crate::error::BrokerError;
use crate::pipeline::dedup::{DedupEngine, DedupResult};
use crate::pipeline::ingest::IngestResult;
use crate::routing::Router;
use crate::storage::sharded_wal::ShardedWalWriter;

/// A message sent from a connection handler to the dispatcher.
pub struct IngestMessage {
    pub msg_id: MessageId,
    pub pub_payload: PubPayload,
    pub namespace: String,
    pub reply_tx: oneshot::Sender<IngestResult>,
}

/// Orchestrates the core ingest pipeline: dedup -> WAL -> ACK -> route -> deliver.
pub struct Dispatcher {
    dedup: DedupEngine,
    wal: ShardedWalWriter,
}

impl Dispatcher {
    pub fn new(dedup: DedupEngine, wal: ShardedWalWriter) -> Self {
        Self { dedup, wal }
    }

    /// Spawn a dispatcher task that reads from an mpsc channel.
    /// After successful ingest, routes and delivers events to subscribers.
    pub fn spawn(
        dedup: DedupEngine,
        wal: ShardedWalWriter,
        mut rx: mpsc::Receiver<IngestMessage>,
        router: Option<Arc<Router>>,
    ) -> tokio::task::JoinHandle<()> {
        let dispatcher = Arc::new(Self::new(dedup, wal));

        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let msg_id = msg.msg_id;
                let pub_payload = msg.pub_payload.clone();
                let result = dispatcher.ingest(msg_id, &pub_payload).await;

                // Route on successful ingest
                if matches!(result, IngestResult::Stored { .. }) {
                    if let Some(router) = &router {
                        let targets = router.resolve(&pub_payload.topic);
                        for target in &targets {
                            let delivery_frame =
                                pulse_protocol::Frame::publish(msg_id, pub_payload.clone());
                            // Best-effort delivery to subscriber channel
                            let _ = target.deliver_tx.try_send(delivery_frame);
                        }
                    }
                }

                let _ = msg.reply_tx.send(result);
            }
            tracing::info!("dispatcher shutdown");
        })
    }

    /// Process a PUB event through the full pipeline.
    ///
    /// 1. Dedup check
    /// 2. Serialize payload
    /// 3. WAL append + fsync
    /// 4. Dedup insert
    /// 5. Return IngestResult::Stored
    pub async fn ingest(&self, msg_id: MessageId, pub_payload: &PubPayload) -> IngestResult {
        // 1. Dedup check
        match self.dedup.check(&msg_id) {
            Ok(DedupResult::Duplicate) => return IngestResult::Duplicate,
            Ok(DedupResult::New) => {}
            Err(e) => return IngestResult::Failed { error: e },
        }

        // 2. Serialize payload for WAL
        let data = match rmp_serde::to_vec_named(pub_payload) {
            Ok(bytes) => bytes,
            Err(e) => {
                return IngestResult::Failed {
                    error: BrokerError::Serialize(e.to_string()),
                }
            }
        };

        // 3. WAL append + fsync
        let sharded_pos = match self.wal.append_event(&pub_payload.topic, msg_id, &data).await {
            Ok(pos) => pos,
            Err(e) => return IngestResult::Failed { error: e },
        };
        let position = sharded_pos.position;

        // 4. Dedup insert (after successful WAL write)
        if let Err(e) = self.dedup.insert(&msg_id, &pub_payload.topic) {
            // WAL write succeeded but dedup insert failed.
            // This is not fatal — on recovery, WAL replay rebuilds the dedup index.
            tracing::warn!(
                msg_id = %msg_id,
                error = %e,
                "dedup insert failed after WAL write (will recover)"
            );
        }

        IngestResult::Stored { position }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BrokerConfig;
    use crate::pipeline::dedup::DedupEngine;
    use crate::storage::sharded_wal::ShardedWalWriter;
    use crate::storage::state_db::StateDb;
    use crate::storage::wal;
    use std::collections::HashMap;

    async fn setup() -> (tempfile::TempDir, Dispatcher) {
        let dir = tempfile::tempdir().unwrap();
        let config = BrokerConfig::for_testing(dir.path().to_path_buf());

        let state_db = Arc::new(StateDb::open(config.data_dir.join("state")).unwrap());
        let wal = ShardedWalWriter::open(config.data_dir.join("wal"), &config.wal, 1)
            .await
            .unwrap();
        let dedup = DedupEngine::new(state_db);

        (dir, Dispatcher::new(dedup, wal))
    }

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
    async fn ingest_new_event_returns_stored() {
        let (_dir, dispatcher) = setup().await;
        let msg_id = MessageId::new();
        let payload = test_pub_payload("order.created");

        let result = dispatcher.ingest(msg_id, &payload).await;
        assert!(matches!(result, IngestResult::Stored { .. }));
    }

    #[tokio::test]
    async fn ingest_duplicate_returns_duplicate() {
        let (_dir, dispatcher) = setup().await;
        let msg_id = MessageId::new();
        let payload = test_pub_payload("order.created");

        let r1 = dispatcher.ingest(msg_id, &payload).await;
        assert!(matches!(r1, IngestResult::Stored { .. }));

        let r2 = dispatcher.ingest(msg_id, &payload).await;
        assert!(matches!(r2, IngestResult::Duplicate));
    }

    #[tokio::test]
    async fn ingest_writes_to_wal() {
        let dir = tempfile::tempdir().unwrap();
        let config = BrokerConfig::for_testing(dir.path().to_path_buf());
        let wal_dir = config.data_dir.join("wal");

        let state_db = Arc::new(StateDb::open(config.data_dir.join("state")).unwrap());
        let wal_writer = ShardedWalWriter::open(wal_dir.clone(), &config.wal, 1)
            .await
            .unwrap();
        let dedup = DedupEngine::new(state_db);
        let dispatcher = Dispatcher::new(dedup, wal_writer);

        let msg_id = MessageId::new();
        dispatcher.ingest(msg_id, &test_pub_payload("test")).await;

        // Drop dispatcher to flush
        drop(dispatcher);

        // Replay WAL — should find the event (shard-00 subdir)
        let result = wal::replay_wal(&wal_dir.join("shard-00")).await.unwrap();
        assert!(result.event_ids.contains(&msg_id));
    }

    #[tokio::test]
    async fn ingest_dedup_persists_across_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let config = BrokerConfig::for_testing(dir.path().to_path_buf());
        let msg_id = MessageId::new();

        // Session 1: ingest an event
        {
            let state_db = Arc::new(StateDb::open(config.data_dir.join("state")).unwrap());
            let wal = ShardedWalWriter::open(config.data_dir.join("wal"), &config.wal, 1)
                .await
                .unwrap();
            let dedup = DedupEngine::new(state_db);
            let dispatcher = Dispatcher::new(dedup, wal);

            let result = dispatcher.ingest(msg_id, &test_pub_payload("t")).await;
            assert!(matches!(result, IngestResult::Stored { .. }));
        }

        // Session 2: reopen — same msg_id should be duplicate
        {
            let state_db = Arc::new(StateDb::open(config.data_dir.join("state")).unwrap());
            let wal = ShardedWalWriter::open(config.data_dir.join("wal"), &config.wal, 1)
                .await
                .unwrap();
            let dedup = DedupEngine::new(state_db);
            let dispatcher = Dispatcher::new(dedup, wal);

            let result = dispatcher.ingest(msg_id, &test_pub_payload("t")).await;
            assert!(matches!(result, IngestResult::Duplicate));
        }
    }

    #[tokio::test]
    async fn ingest_multiple_events_positions_increase() {
        let (_dir, dispatcher) = setup().await;

        let mut prev_offset = 0u64;
        for _ in 0..10 {
            let result = dispatcher
                .ingest(MessageId::new(), &test_pub_payload("t"))
                .await;
            match result {
                IngestResult::Stored { position } => {
                    assert!(position.offset > prev_offset);
                    prev_offset = position.offset;
                }
                other => panic!("expected Stored, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn ingest_different_topics() {
        let (_dir, dispatcher) = setup().await;

        for topic in ["order.created", "payment.completed", "user.updated"] {
            let result = dispatcher
                .ingest(MessageId::new(), &test_pub_payload(topic))
                .await;
            assert!(matches!(result, IngestResult::Stored { .. }));
        }
    }

    #[tokio::test]
    async fn ingest_after_wal_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let config = BrokerConfig::for_testing(dir.path().to_path_buf());
        let wal_dir = config.data_dir.join("wal");
        let state_dir = config.data_dir.join("state");

        let ids: Vec<MessageId> = (0..5).map(|_| MessageId::new()).collect();

        // Session 1: ingest 5 events
        {
            let state_db = Arc::new(StateDb::open(state_dir.clone()).unwrap());
            let wal = ShardedWalWriter::open(wal_dir.clone(), &config.wal, 1)
                .await
                .unwrap();
            let dedup = DedupEngine::new(state_db);
            let dispatcher = Dispatcher::new(dedup, wal);

            for &id in &ids {
                dispatcher.ingest(id, &test_pub_payload("t")).await;
            }
        }

        // Simulate crash: delete state DB (but WAL survives)
        tokio::fs::remove_dir_all(&state_dir).await.unwrap();

        // Session 2: rebuild dedup from WAL replay (shard-00 subdir)
        let replay_result = wal::replay_wal(&wal_dir.join("shard-00")).await.unwrap();
        assert_eq!(replay_result.event_ids.len(), 5);

        let state_db = Arc::new(StateDb::open(state_dir).unwrap());
        state_db
            .dedup_bulk_insert(replay_result.event_ids.into_iter())
            .unwrap();

        let wal = ShardedWalWriter::open(wal_dir, &config.wal, 1)
            .await
            .unwrap();
        let dedup = DedupEngine::new(state_db);
        let dispatcher = Dispatcher::new(dedup, wal);

        // All 5 original IDs should be duplicates
        for &id in &ids {
            let result = dispatcher.ingest(id, &test_pub_payload("t")).await;
            assert!(
                matches!(result, IngestResult::Duplicate),
                "expected Duplicate for {id}"
            );
        }

        // New ID should be stored
        let result = dispatcher
            .ingest(MessageId::new(), &test_pub_payload("t"))
            .await;
        assert!(matches!(result, IngestResult::Stored { .. }));
    }

    #[tokio::test]
    async fn ingest_complex_payload() {
        let (_dir, dispatcher) = setup().await;

        let payload = PubPayload {
            topic: "order.created".into(),
            data: rmpv::Value::Map(vec![
                (
                    rmpv::Value::String("id".into()),
                    rmpv::Value::String("ord_123".into()),
                ),
                (
                    rmpv::Value::String("amount".into()),
                    rmpv::Value::Integer(500_000.into()),
                ),
                (
                    rmpv::Value::String("items".into()),
                    rmpv::Value::Array(vec![
                        rmpv::Value::Integer(1.into()),
                        rmpv::Value::Integer(2.into()),
                    ]),
                ),
            ]),
            headers: HashMap::from([
                ("trace_id".into(), "abc123".into()),
                ("source".into(), "web".into()),
            ]),
            produced_at: Some(1700000000000),
            delivery: None,
        };

        let result = dispatcher.ingest(MessageId::new(), &payload).await;
        assert!(matches!(result, IngestResult::Stored { .. }));
    }

    #[tokio::test]
    async fn dispatcher_spawn_processes_messages() {
        let dir = tempfile::tempdir().unwrap();
        let config = BrokerConfig::for_testing(dir.path().to_path_buf());

        let state_db = Arc::new(StateDb::open(config.data_dir.join("state")).unwrap());
        let wal = ShardedWalWriter::open(config.data_dir.join("wal"), &config.wal, 1)
            .await
            .unwrap();
        let dedup = DedupEngine::new(state_db);

        let (tx, rx) = mpsc::channel(64);
        let handle = Dispatcher::spawn(dedup, wal, rx, None);

        // Send a message through the channel
        let msg_id = MessageId::new();
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(IngestMessage {
            msg_id,
            pub_payload: test_pub_payload("test.topic"),
            namespace: "default".into(),
            reply_tx,
        })
        .await
        .unwrap();

        let result = reply_rx.await.unwrap();
        assert!(matches!(result, IngestResult::Stored { .. }));

        drop(tx);
        handle.await.unwrap();
    }
}
