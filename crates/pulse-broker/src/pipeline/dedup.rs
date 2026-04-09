use std::sync::Arc;

use parking_lot::RwLock;
use pulse_protocol::MessageId;

use crate::config::DurabilityMode;
use crate::error::BrokerError;
use crate::pipeline::bloom::BloomFilter;
use crate::storage::state_db::{DedupEntry, StateDb};

/// Result of a dedup check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DedupResult {
    New,
    Duplicate,
}

/// Tiered deduplication engine.
///
/// - **Memory mode**: optional bloom filter only (fastest, ~29ns)
/// - **Balanced mode**: bloom filter only (fast, no sled on hot path, ~29ns)
/// - **Durable mode**: bloom filter + sled confirmation (~8µs, exact)
pub struct DedupEngine {
    bloom: Option<RwLock<BloomFilter>>,
    state_db: Arc<StateDb>,
    mode: DurabilityMode,
}

impl DedupEngine {
    /// Create a durable-mode dedup engine (sled-only, backward compatible).
    pub fn new(state_db: Arc<StateDb>) -> Self {
        Self {
            bloom: None,
            state_db,
            mode: DurabilityMode::Durable,
        }
    }

    /// Create a tiered dedup engine with bloom filter.
    pub fn tiered(state_db: Arc<StateDb>, mode: DurabilityMode) -> Self {
        let bloom = match mode {
            DurabilityMode::Memory => {
                // Optional bloom for memory mode — catches obvious retries
                Some(RwLock::new(BloomFilter::new(1_000_000, 0.001)))
            }
            DurabilityMode::Balanced => {
                // Bloom-only: fast negative check, no sled on hot path
                Some(RwLock::new(BloomFilter::new(1_000_000, 0.001)))
            }
            DurabilityMode::Durable => {
                // Bloom + sled: bloom as fast negative filter, sled for confirmation
                Some(RwLock::new(BloomFilter::new(1_000_000, 0.001)))
            }
        };

        Self {
            bloom,
            state_db,
            mode,
        }
    }

    /// Check whether a message has already been seen.
    pub fn check(&self, msg_id: &MessageId) -> Result<DedupResult, BrokerError> {
        // Fast path: bloom filter says definitely not seen
        if let Some(bloom) = &self.bloom {
            let bloom = bloom.read();
            if !bloom.may_contain(msg_id) {
                return Ok(DedupResult::New);
            }

            // Bloom says "maybe" — behavior depends on mode
            match self.mode {
                DurabilityMode::Memory => {
                    // Memory mode: bloom "maybe" → treat as duplicate (acceptable false positive)
                    return Ok(DedupResult::Duplicate);
                }
                DurabilityMode::Balanced => {
                    // Balanced: bloom "maybe" → treat as duplicate (no sled confirmation)
                    return Ok(DedupResult::Duplicate);
                }
                DurabilityMode::Durable => {
                    // Durable: bloom "maybe" → confirm with sled
                    // (falls through to sled check below)
                }
            }
        }

        // Sled check (durable mode, or no bloom filter)
        if self.state_db.dedup_contains(msg_id)? {
            Ok(DedupResult::Duplicate)
        } else {
            Ok(DedupResult::New)
        }
    }

    /// Record a message as seen. Call after WAL write succeeds.
    pub fn insert(&self, msg_id: &MessageId, topic: &str) -> Result<(), BrokerError> {
        // Always update bloom filter (fast, ~50ns)
        if let Some(bloom) = &self.bloom {
            bloom.write().insert(msg_id);
        }

        // Sled insert only in durable mode
        if self.mode == DurabilityMode::Durable {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            self.state_db.dedup_insert(
                msg_id,
                &DedupEntry {
                    stored_at: now,
                    topic: topic.to_owned(),
                },
            )?;
        }

        Ok(())
    }

    /// Current durability mode.
    pub fn mode(&self) -> DurabilityMode {
        self.mode
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (tempfile::TempDir, DedupEngine) {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(StateDb::open(dir.path().join("state")).unwrap());
        let engine = DedupEngine::new(db);
        (dir, engine)
    }

    fn setup_tiered(mode: DurabilityMode) -> (tempfile::TempDir, DedupEngine) {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(StateDb::open(dir.path().join("state")).unwrap());
        let engine = DedupEngine::tiered(db, mode);
        (dir, engine)
    }

    #[test]
    fn new_message_is_not_duplicate() {
        let (_dir, engine) = setup();
        let id = MessageId::new();
        assert_eq!(engine.check(&id).unwrap(), DedupResult::New);
    }

    #[test]
    fn inserted_message_is_duplicate() {
        let (_dir, engine) = setup();
        let id = MessageId::new();

        engine.insert(&id, "test.topic").unwrap();
        assert_eq!(engine.check(&id).unwrap(), DedupResult::Duplicate);
    }

    #[test]
    fn different_ids_are_independent() {
        let (_dir, engine) = setup();
        let a = MessageId::new();
        let b = MessageId::new();

        engine.insert(&a, "t").unwrap();
        assert_eq!(engine.check(&a).unwrap(), DedupResult::Duplicate);
        assert_eq!(engine.check(&b).unwrap(), DedupResult::New);
    }

    #[test]
    fn idempotent_insert() {
        let (_dir, engine) = setup();
        let id = MessageId::new();

        engine.insert(&id, "t").unwrap();
        engine.insert(&id, "t").unwrap(); // no error
        assert_eq!(engine.check(&id).unwrap(), DedupResult::Duplicate);
    }

    #[test]
    fn check_then_insert_flow() {
        let (_dir, engine) = setup();
        let id = MessageId::new();

        // Simulate the real pattern
        assert_eq!(engine.check(&id).unwrap(), DedupResult::New);
        // "WAL write happens here"
        engine.insert(&id, "order.created").unwrap();
        assert_eq!(engine.check(&id).unwrap(), DedupResult::Duplicate);
    }

    #[test]
    fn many_messages_dedup() {
        let (_dir, engine) = setup();
        let ids: Vec<MessageId> = (0..1000).map(|_| MessageId::new()).collect();

        for id in &ids {
            engine.insert(id, "t").unwrap();
        }

        for id in &ids {
            assert_eq!(engine.check(id).unwrap(), DedupResult::Duplicate);
        }

        // New ID is not a duplicate
        assert_eq!(engine.check(&MessageId::new()).unwrap(), DedupResult::New);
    }

    // ─── Tiered mode tests ───

    #[test]
    fn balanced_mode_uses_bloom_only() {
        let (_dir, engine) = setup_tiered(DurabilityMode::Balanced);
        let id = MessageId::new();

        assert_eq!(engine.check(&id).unwrap(), DedupResult::New);
        engine.insert(&id, "t").unwrap();
        assert_eq!(engine.check(&id).unwrap(), DedupResult::Duplicate);
    }

    #[test]
    fn memory_mode_uses_bloom_only() {
        let (_dir, engine) = setup_tiered(DurabilityMode::Memory);
        let id = MessageId::new();

        assert_eq!(engine.check(&id).unwrap(), DedupResult::New);
        engine.insert(&id, "t").unwrap();
        assert_eq!(engine.check(&id).unwrap(), DedupResult::Duplicate);
    }

    #[test]
    fn durable_mode_uses_bloom_plus_sled() {
        let (_dir, engine) = setup_tiered(DurabilityMode::Durable);
        let id = MessageId::new();

        assert_eq!(engine.check(&id).unwrap(), DedupResult::New);
        engine.insert(&id, "t").unwrap();
        assert_eq!(engine.check(&id).unwrap(), DedupResult::Duplicate);
    }

    #[test]
    fn balanced_mode_no_sled_writes() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(StateDb::open(dir.path().join("state")).unwrap());
        let engine = DedupEngine::tiered(db.clone(), DurabilityMode::Balanced);

        let id = MessageId::new();
        engine.insert(&id, "t").unwrap();

        // Sled should NOT contain it (balanced mode skips sled)
        assert!(!db.dedup_contains(&id).unwrap());
        // But bloom should catch it
        assert_eq!(engine.check(&id).unwrap(), DedupResult::Duplicate);
    }
}
