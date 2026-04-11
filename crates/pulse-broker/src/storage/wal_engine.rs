use std::path::PathBuf;

use pulse_protocol::MessageId;

use crate::config::WalConfig;
use crate::error::BrokerError;
use crate::storage::wal::{WalPosition, WalWriter};

/// IO engine selection for WAL writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IoEngine {
    /// Standard tokio file I/O (all platforms).
    Tokio,
    /// io_uring via tokio-uring (Linux 5.6+ only).
    IoUring,
    /// Auto-detect: use io_uring on supported Linux, fallback to tokio.
    #[default]
    Auto,
}

impl IoEngine {
    /// Resolve Auto to the actual engine for this platform.
    pub fn resolve(self) -> Self {
        match self {
            Self::Auto => {
                if cfg!(target_os = "linux") {
                    // In a full implementation, we'd check kernel version >= 5.6
                    // For now, default to Tokio since we don't have tokio-uring compiled
                    Self::Tokio
                } else {
                    Self::Tokio
                }
            }
            other => other,
        }
    }
}

/// Abstraction over WAL write engines.
///
/// Currently only the standard tokio engine is implemented.
/// The io_uring engine would be feature-gated behind `#[cfg(target_os = "linux")]`
/// and would use `tokio-uring` for kernel-bypassed disk I/O.
///
/// Key benefits of io_uring (when available):
/// - Single submit + completion poll instead of 2N+1 syscalls per batch
/// - Zero context switches (kernel-side processing)
/// - Zero-copy with registered buffers
/// - Linked SQEs for hardware-level write+fsync ordering
///
/// Expected improvement: 2-3x throughput for balanced mode on Linux.
pub struct WalEngine {
    inner: WalWriter,
    engine: IoEngine,
}

impl WalEngine {
    /// Open a WAL engine with the resolved IO engine.
    pub async fn open(
        wal_dir: PathBuf,
        config: &WalConfig,
        engine: IoEngine,
    ) -> Result<Self, BrokerError> {
        let resolved = engine.resolve();
        let inner = WalWriter::open(wal_dir, config).await?;

        if resolved == IoEngine::IoUring {
            tracing::info!("WAL engine: io_uring (not yet implemented, falling back to tokio)");
        } else {
            tracing::debug!("WAL engine: tokio");
        }

        Ok(Self {
            inner,
            engine: resolved,
        })
    }

    /// Append an event to the WAL.
    pub async fn append_event(
        &mut self,
        msg_id: MessageId,
        data: &[u8],
    ) -> Result<WalPosition, BrokerError> {
        self.inner.append_event(msg_id, data).await
    }

    /// Append without fsync (for group commit).
    pub async fn append_event_no_sync(
        &mut self,
        msg_id: MessageId,
        data: &[u8],
    ) -> Result<WalPosition, BrokerError> {
        self.inner.append_event_no_sync(msg_id, data).await
    }

    /// Explicit sync.
    pub async fn sync(&mut self) -> Result<(), BrokerError> {
        self.inner.sync().await
    }

    /// The active IO engine.
    pub fn engine(&self) -> IoEngine {
        self.engine
    }

    /// Current segment number.
    pub fn segment_number(&self) -> u32 {
        self.inner.segment_number()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_resolves_to_tokio_on_non_linux() {
        let engine = IoEngine::Auto.resolve();
        // On macOS/Windows, should resolve to Tokio
        assert_eq!(engine, IoEngine::Tokio);
    }

    #[test]
    fn explicit_tokio_stays_tokio() {
        assert_eq!(IoEngine::Tokio.resolve(), IoEngine::Tokio);
    }

    #[tokio::test]
    async fn wal_engine_opens() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let config = WalConfig {
            segment_size_bytes: 64 * 1024 * 1024,
            sync_mode: "none".into(),
            shards: 1,
        };

        let engine = WalEngine::open(wal_dir, &config, IoEngine::Auto)
            .await
            .unwrap();
        assert_eq!(engine.engine(), IoEngine::Tokio);
    }

    #[tokio::test]
    async fn wal_engine_writes() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let config = WalConfig {
            segment_size_bytes: 64 * 1024 * 1024,
            sync_mode: "none".into(),
            shards: 1,
        };

        let mut engine = WalEngine::open(wal_dir, &config, IoEngine::Auto)
            .await
            .unwrap();
        let pos = engine
            .append_event(MessageId::new(), b"test-data")
            .await
            .unwrap();
        assert_eq!(pos.segment, 1);
    }
}
