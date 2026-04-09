use crate::consistent_hash::NodeId;

/// Replication mode for WAL entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationMode {
    /// Single-node, no replication.
    None,
    /// Leader streams to followers, doesn't wait for ACK before ACKing publisher.
    Async,
    /// Leader waits for majority replica ACK before ACKing publisher.
    Sync,
}

impl Default for ReplicationMode {
    fn default() -> Self {
        Self::Async
    }
}

/// Replication configuration.
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub mode: ReplicationMode,
    /// Number of follower copies (default: 1, meaning leader + 1 follower = 2 total).
    pub replication_factor: usize,
    /// Sync mode: max wait for PEER_ACK in milliseconds.
    pub timeout_ms: u64,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            mode: ReplicationMode::Async,
            replication_factor: 1,
            timeout_ms: 50,
        }
    }
}

/// Tracks replication progress for a follower.
#[derive(Debug, Clone)]
pub struct ReplicationWatermark {
    pub follower_id: NodeId,
    /// Last WAL segment + offset confirmed by follower.
    pub last_segment: u32,
    pub last_offset: u64,
    /// Replication lag in milliseconds.
    pub lag_ms: u64,
}

/// Manages WAL replication from leader to followers.
pub struct ReplicationManager {
    config: ReplicationConfig,
    watermarks: Vec<ReplicationWatermark>,
}

impl ReplicationManager {
    pub fn new(config: ReplicationConfig) -> Self {
        Self {
            config,
            watermarks: Vec::new(),
        }
    }

    /// Add a follower to track.
    pub fn add_follower(&mut self, follower_id: NodeId) {
        self.watermarks.push(ReplicationWatermark {
            follower_id,
            last_segment: 0,
            last_offset: 0,
            lag_ms: 0,
        });
    }

    /// Update a follower's watermark after receiving PEER_ACK.
    pub fn update_watermark(&mut self, follower_id: &str, segment: u32, offset: u64, lag_ms: u64) {
        if let Some(wm) = self
            .watermarks
            .iter_mut()
            .find(|w| w.follower_id == follower_id)
        {
            wm.last_segment = segment;
            wm.last_offset = offset;
            wm.lag_ms = lag_ms;
        }
    }

    /// Check if replication is caught up (lag < threshold).
    pub fn is_caught_up(&self, threshold_ms: u64) -> bool {
        self.watermarks.iter().all(|w| w.lag_ms < threshold_ms)
    }

    /// Get the replication mode.
    pub fn mode(&self) -> ReplicationMode {
        self.config.mode
    }

    /// Number of followers being tracked.
    pub fn follower_count(&self) -> usize {
        self.watermarks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replication_watermarks() {
        let mut mgr = ReplicationManager::new(ReplicationConfig::default());
        mgr.add_follower("node-2".into());
        mgr.add_follower("node-3".into());

        assert_eq!(mgr.follower_count(), 2);
        assert!(mgr.is_caught_up(100));

        mgr.update_watermark("node-2", 5, 1024, 2);
        mgr.update_watermark("node-3", 5, 512, 50);

        assert!(mgr.is_caught_up(100));
        assert!(!mgr.is_caught_up(10)); // node-3 has 50ms lag
    }

    #[test]
    fn replication_mode_default() {
        let mgr = ReplicationManager::new(ReplicationConfig::default());
        assert_eq!(mgr.mode(), ReplicationMode::Async);
    }
}
