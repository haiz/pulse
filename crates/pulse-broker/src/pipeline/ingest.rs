use crate::error::BrokerError;
use crate::storage::wal::WalPosition;

/// Result of processing a publish event through the pipeline.
#[derive(Debug)]
pub enum IngestResult {
    /// Event written to WAL and dedup index updated.
    Stored { position: WalPosition },
    /// Event was already seen (same message ID).
    Duplicate,
    /// Processing failed.
    Failed { error: BrokerError },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingest_result_debug() {
        let r = IngestResult::Duplicate;
        assert!(format!("{r:?}").contains("Duplicate"));

        let r = IngestResult::Stored {
            position: WalPosition {
                segment: 1,
                offset: 32,
            },
        };
        assert!(format!("{r:?}").contains("Stored"));
    }

    #[test]
    fn ingest_result_failed() {
        let r = IngestResult::Failed {
            error: BrokerError::Wal("test".into()),
        };
        assert!(format!("{r:?}").contains("Failed"));
    }
}
