use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

use tokio::sync::Mutex;

use crate::config::WalConfig;
use crate::error::BrokerError;
use crate::storage::wal::{WalPosition, WalWriter};
use pulse_protocol::MessageId;

/// Position of a record within a specific WAL shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardedWalPosition {
    pub shard: usize,
    pub position: WalPosition,
}

/// A WAL writer that owns N `WalWriter`s, routing writes by topic hash.
///
/// Each shard has its own segment files in a subdirectory (`shard-00/`, `shard-01/`, etc.).
/// This allows concurrent writes to different topics to avoid contention on a single WAL.
pub struct ShardedWalWriter {
    shards: Vec<Mutex<WalWriter>>,
    num_shards: usize,
}

impl ShardedWalWriter {
    /// Open or create N shard directories under `wal_dir`, each containing a `WalWriter`.
    pub async fn open(
        wal_dir: PathBuf,
        config: &WalConfig,
        num_shards: usize,
    ) -> Result<Self, BrokerError> {
        assert!(num_shards > 0, "num_shards must be >= 1");

        let mut shards = Vec::with_capacity(num_shards);
        for i in 0..num_shards {
            let shard_dir = wal_dir.join(format!("shard-{i:02}"));
            let writer = WalWriter::open(shard_dir, config).await?;
            shards.push(Mutex::new(writer));
        }

        Ok(Self { shards, num_shards })
    }

    /// Determine which shard a topic maps to via `DefaultHasher` mod `num_shards`.
    pub fn shard_for(&self, topic: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        topic.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_shards
    }

    /// Append an event to the shard determined by `topic`, with sync.
    pub async fn append_event(
        &self,
        topic: &str,
        msg_id: MessageId,
        data: &[u8],
    ) -> Result<ShardedWalPosition, BrokerError> {
        let shard = self.shard_for(topic);
        let mut writer = self.shards[shard].lock().await;
        let position = writer.append_event(msg_id, data).await?;
        Ok(ShardedWalPosition { shard, position })
    }

    /// Append an event without sync (for group commit).
    pub async fn append_event_no_sync(
        &self,
        topic: &str,
        msg_id: MessageId,
        data: &[u8],
    ) -> Result<ShardedWalPosition, BrokerError> {
        let shard = self.shard_for(topic);
        let mut writer = self.shards[shard].lock().await;
        let position = writer.append_event_no_sync(msg_id, data).await?;
        Ok(ShardedWalPosition { shard, position })
    }

    /// Sync a single shard to disk.
    pub async fn sync_shard(&self, shard_idx: usize) -> Result<(), BrokerError> {
        let mut writer = self.shards[shard_idx].lock().await;
        writer.sync().await
    }

    /// Sync all shards to disk.
    pub async fn sync_all(&self) -> Result<(), BrokerError> {
        for shard in &self.shards {
            let mut writer = shard.lock().await;
            writer.sync().await?;
        }
        Ok(())
    }

    /// Return the number of shards.
    pub fn num_shards(&self) -> usize {
        self.num_shards
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

        let sharded = ShardedWalWriter::open(wal_dir, &config, 4).await.unwrap();

        // Same topic always maps to the same shard
        let shard_a1 = sharded.shard_for("orders.created");
        let shard_a2 = sharded.shard_for("orders.created");
        assert_eq!(shard_a1, shard_a2);

        // Write to different topics and verify shard assignment is consistent
        let topics = ["orders.created", "users.signup", "payments.processed", "logs.debug"];
        for topic in &topics {
            let expected_shard = sharded.shard_for(topic);
            let pos = sharded
                .append_event(topic, MessageId::new(), b"data")
                .await
                .unwrap();
            assert_eq!(pos.shard, expected_shard);
        }
    }

    #[tokio::test]
    async fn sharded_wal_creates_shard_directories() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config();

        let _sharded = ShardedWalWriter::open(wal_dir.clone(), &config, 4)
            .await
            .unwrap();

        for i in 0..4 {
            let shard_dir = wal_dir.join(format!("shard-{i:02}"));
            assert!(shard_dir.exists(), "shard-{i:02}/ should exist");
            assert!(shard_dir.is_dir(), "shard-{i:02}/ should be a directory");
        }
    }

    #[tokio::test]
    async fn sharded_wal_single_shard_is_equivalent() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config();

        let sharded = ShardedWalWriter::open(wal_dir, &config, 1).await.unwrap();

        // Every topic maps to shard 0
        assert_eq!(sharded.shard_for("topic-a"), 0);
        assert_eq!(sharded.shard_for("topic-b"), 0);
        assert_eq!(sharded.shard_for("topic-c"), 0);

        let pos = sharded
            .append_event("any-topic", MessageId::new(), b"hello")
            .await
            .unwrap();
        assert_eq!(pos.shard, 0);
    }

    #[tokio::test]
    async fn sharded_wal_sync_all() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config();

        let sharded = ShardedWalWriter::open(wal_dir, &config, 4).await.unwrap();

        // Write events to various shards
        for i in 0..10 {
            let topic = format!("topic-{i}");
            sharded
                .append_event_no_sync(&topic, MessageId::new(), b"payload")
                .await
                .unwrap();
        }

        // sync_all should succeed without errors
        sharded.sync_all().await.unwrap();
    }

    #[tokio::test]
    async fn sharded_wal_concurrent_writes() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config();

        let sharded = Arc::new(
            ShardedWalWriter::open(wal_dir, &config, 4).await.unwrap(),
        );

        let topics = ["alpha", "beta", "gamma", "delta", "epsilon"];
        let mut handles = Vec::new();

        for i in 0..20 {
            let writer = Arc::clone(&sharded);
            let topic = topics[i % topics.len()].to_string();
            handles.push(tokio::spawn(async move {
                writer
                    .append_event(&topic, MessageId::new(), b"concurrent-data")
                    .await
            }));
        }

        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok(), "concurrent write should succeed");
        }
    }
}
