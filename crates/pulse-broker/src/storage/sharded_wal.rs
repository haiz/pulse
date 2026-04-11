use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::time::Duration;

use crate::config::WalConfig;
use crate::error::BrokerError;
use crate::storage::wal::WalPosition;
use crate::storage::wal_thread::WalThreadHandle;
use pulse_protocol::MessageId;

/// Position of a record within a specific WAL shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardedWalPosition {
    pub shard: usize,
    pub position: WalPosition,
}

/// A WAL writer that owns N `WalThreadHandle`s, routing writes by topic hash.
///
/// Each shard has its own segment files in a subdirectory (`shard-00/`, `shard-01/`, etc.)
/// and a dedicated OS thread for all file I/O — no Mutex contention, no tokio::fs overhead.
pub struct ShardedWalWriter {
    shards: Vec<WalThreadHandle>,
    num_shards: usize,
}

impl ShardedWalWriter {
    /// Open or create N shard directories, each with a dedicated writer thread.
    /// This is SYNCHRONOUS (not async) — call during initialization.
    pub fn open(
        wal_dir: PathBuf,
        config: &WalConfig,
        num_shards: usize,
        flush_interval: Duration,
        max_batch: usize,
    ) -> Result<Self, BrokerError> {
        assert!(num_shards > 0, "num_shards must be >= 1");
        let mut shards = Vec::with_capacity(num_shards);
        for i in 0..num_shards {
            let shard_dir = wal_dir.join(format!("shard-{i:02}"));
            let handle = WalThreadHandle::spawn(shard_dir, config, flush_interval, max_batch)?;
            shards.push(handle);
        }
        Ok(Self { shards, num_shards })
    }

    /// Determine which shard a topic maps to via `DefaultHasher` mod `num_shards`.
    pub fn shard_for(&self, topic: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        topic.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_shards
    }

    /// Append an event to the shard determined by `topic`.
    /// NOTE: data is `Vec<u8>` (moved to writer thread), not `&[u8]`.
    pub async fn append_event(
        &self,
        topic: &str,
        msg_id: MessageId,
        data: Vec<u8>,
    ) -> Result<ShardedWalPosition, BrokerError> {
        let shard_idx = self.shard_for(topic);
        let position = self.shards[shard_idx].append_event(msg_id, data).await?;
        Ok(ShardedWalPosition {
            shard: shard_idx,
            position,
        })
    }

    /// Sync a single shard to disk.
    pub async fn sync_shard(&self, shard_idx: usize) -> Result<(), BrokerError> {
        self.shards[shard_idx].sync().await
    }

    /// Sync all shards to disk.
    pub async fn sync_all(&self) -> Result<(), BrokerError> {
        for shard in &self.shards {
            shard.sync().await?;
        }
        Ok(())
    }

    /// Return the number of shards.
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }

    /// Request a graceful shutdown of all writer threads.
    pub fn shutdown(&self) {
        for shard in &self.shards {
            shard.shutdown();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn test_wal_config() -> WalConfig {
        WalConfig {
            segment_size_bytes: 64 * 1024 * 1024,
            sync_mode: "none".into(),
            shards: 1,
        }
    }

    #[tokio::test]
    async fn sharded_wal_distributes_by_topic() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config();

        let sharded = ShardedWalWriter::open(
            wal_dir,
            &config,
            4,
            Duration::from_millis(5),
            100,
        )
        .unwrap();

        // Same topic always maps to the same shard
        let shard_a1 = sharded.shard_for("orders.created");
        let shard_a2 = sharded.shard_for("orders.created");
        assert_eq!(shard_a1, shard_a2);

        // Write to different topics and verify shard assignment is consistent
        let topics = ["orders.created", "users.signup", "payments.processed", "logs.debug"];
        for topic in &topics {
            let expected_shard = sharded.shard_for(topic);
            let pos = sharded
                .append_event(topic, MessageId::new(), b"data".to_vec())
                .await
                .unwrap();
            assert_eq!(pos.shard, expected_shard);
        }

        sharded.shutdown();
    }

    #[tokio::test]
    async fn sharded_wal_creates_shard_directories() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config();

        let sharded = ShardedWalWriter::open(
            wal_dir.clone(),
            &config,
            4,
            Duration::from_millis(5),
            100,
        )
        .unwrap();

        for i in 0..4 {
            let shard_dir = wal_dir.join(format!("shard-{i:02}"));
            assert!(shard_dir.exists(), "shard-{i:02}/ should exist");
            assert!(shard_dir.is_dir(), "shard-{i:02}/ should be a directory");
        }

        sharded.shutdown();
    }

    #[tokio::test]
    async fn sharded_wal_single_shard_is_equivalent() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config();

        let sharded = ShardedWalWriter::open(
            wal_dir,
            &config,
            1,
            Duration::from_millis(5),
            100,
        )
        .unwrap();

        // Every topic maps to shard 0
        assert_eq!(sharded.shard_for("topic-a"), 0);
        assert_eq!(sharded.shard_for("topic-b"), 0);
        assert_eq!(sharded.shard_for("topic-c"), 0);

        let pos = sharded
            .append_event("any-topic", MessageId::new(), b"hello".to_vec())
            .await
            .unwrap();
        assert_eq!(pos.shard, 0);

        sharded.shutdown();
    }

    #[tokio::test]
    async fn sharded_wal_sync_all() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config();

        let sharded = ShardedWalWriter::open(
            wal_dir,
            &config,
            4,
            Duration::from_millis(5),
            100,
        )
        .unwrap();

        // Write events to various shards
        for i in 0..10 {
            let topic = format!("topic-{i}");
            sharded
                .append_event(&topic, MessageId::new(), b"payload".to_vec())
                .await
                .unwrap();
        }

        // sync_all should succeed without errors
        sharded.sync_all().await.unwrap();
        sharded.shutdown();
    }

    #[tokio::test]
    async fn sharded_wal_concurrent_writes() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config();

        let sharded = Arc::new(
            ShardedWalWriter::open(
                wal_dir,
                &config,
                4,
                Duration::from_millis(5),
                100,
            )
            .unwrap(),
        );

        let topics = ["alpha", "beta", "gamma", "delta", "epsilon"];
        let mut handles = Vec::new();

        for i in 0..20 {
            let writer = Arc::clone(&sharded);
            let topic = topics[i % topics.len()].to_string();
            handles.push(tokio::spawn(async move {
                writer
                    .append_event(&topic, MessageId::new(), b"concurrent-data".to_vec())
                    .await
            }));
        }

        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok(), "concurrent write should succeed");
        }

        sharded.shutdown();
    }
}
