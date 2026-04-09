use std::path::PathBuf;

use pulse_protocol::MessageId;
use serde::{Deserialize, Serialize};

use crate::error::BrokerError;

const CURRENT_SCHEMA_VERSION: u32 = 1;

/// Thin wrapper around sled for typed storage access.
#[derive(Debug)]
pub struct StateDb {
    #[allow(dead_code)]
    db: sled::Db,
    dedup_tree: sled::Tree,
    meta_tree: sled::Tree,
}

/// Entry stored in the dedup tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupEntry {
    pub stored_at: u64,
    pub topic: String,
}

impl StateDb {
    /// Access the underlying sled database.
    pub fn db(&self) -> &sled::Db {
        &self.db
    }

    /// Open or create the state database, running migrations as needed.
    pub fn open(state_dir: PathBuf) -> Result<Self, BrokerError> {
        let db = sled::Config::new().path(state_dir).open()?;
        let meta_tree = db.open_tree("meta")?;
        let dedup_tree = db.open_tree("dedup")?;

        let state_db = Self {
            db,
            dedup_tree,
            meta_tree,
        };
        state_db.migrate()?;

        Ok(state_db)
    }

    /// Check if a message ID exists in the dedup index.
    pub fn dedup_contains(&self, msg_id: &MessageId) -> Result<bool, BrokerError> {
        Ok(self.dedup_tree.contains_key(msg_id.as_bytes())?)
    }

    /// Insert a dedup entry for a message.
    pub fn dedup_insert(&self, msg_id: &MessageId, entry: &DedupEntry) -> Result<(), BrokerError> {
        let value =
            rmp_serde::to_vec_named(entry).map_err(|e| BrokerError::Serialize(e.to_string()))?;
        self.dedup_tree.insert(msg_id.as_bytes(), value)?;
        Ok(())
    }

    /// Flush all pending writes to disk.
    pub fn flush(&self) -> Result<(), BrokerError> {
        self.dedup_tree.flush()?;
        self.meta_tree.flush()?;
        Ok(())
    }

    /// Bulk-insert message IDs into the dedup index (for WAL recovery).
    pub fn dedup_bulk_insert(
        &self,
        ids: impl IntoIterator<Item = MessageId>,
    ) -> Result<u64, BrokerError> {
        let mut count = 0u64;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        for id in ids {
            if !self.dedup_contains(&id)? {
                let entry = DedupEntry {
                    stored_at: now,
                    topic: String::new(), // topic not available during replay
                };
                self.dedup_insert(&id, &entry)?;
                count += 1;
            }
        }
        Ok(count)
    }

    fn migrate(&self) -> Result<(), BrokerError> {
        let version = self
            .meta_tree
            .get("schema_version")?
            .map(|v| {
                let bytes: [u8; 4] = v.as_ref().try_into().unwrap_or([0; 4]);
                u32::from_be_bytes(bytes)
            })
            .unwrap_or(0);

        match version {
            0 => {
                // Initial setup — trees are already created by open_tree calls above.
                // Just set the version.
                self.meta_tree
                    .insert("schema_version", &CURRENT_SCHEMA_VERSION.to_be_bytes())?;
                self.meta_tree.flush()?;
                tracing::info!("State DB migrated to schema version {CURRENT_SCHEMA_VERSION}");
            }
            v if v == CURRENT_SCHEMA_VERSION => {
                // Current version — nothing to do.
            }
            v => {
                return Err(BrokerError::Config(format!(
                    "unknown state DB schema version: {v} (expected <= {CURRENT_SCHEMA_VERSION})"
                )));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_test_db() -> (tempfile::TempDir, StateDb) {
        let dir = tempfile::tempdir().unwrap();
        let db = StateDb::open(dir.path().join("state")).unwrap();
        (dir, db)
    }

    #[test]
    fn open_creates_fresh_db() {
        let (_dir, db) = open_test_db();
        let version = db
            .meta_tree
            .get("schema_version")
            .unwrap()
            .map(|v| u32::from_be_bytes(v.as_ref().try_into().unwrap()))
            .unwrap();
        assert_eq!(version, CURRENT_SCHEMA_VERSION);
    }

    #[test]
    fn reopen_existing_db() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state");

        // First open
        {
            let db = StateDb::open(path.clone()).unwrap();
            db.dedup_insert(
                &MessageId::new(),
                &DedupEntry {
                    stored_at: 100,
                    topic: "test".into(),
                },
            )
            .unwrap();
            db.flush().unwrap();
        }

        // Second open — should not error
        let db = StateDb::open(path).unwrap();
        let version = db
            .meta_tree
            .get("schema_version")
            .unwrap()
            .map(|v| u32::from_be_bytes(v.as_ref().try_into().unwrap()))
            .unwrap();
        assert_eq!(version, CURRENT_SCHEMA_VERSION);
    }

    #[test]
    fn dedup_insert_and_contains() {
        let (_dir, db) = open_test_db();
        let id = MessageId::new();

        assert!(!db.dedup_contains(&id).unwrap());

        db.dedup_insert(
            &id,
            &DedupEntry {
                stored_at: 1700000000,
                topic: "order.created".into(),
            },
        )
        .unwrap();

        assert!(db.dedup_contains(&id).unwrap());
    }

    #[test]
    fn dedup_missing_returns_false() {
        let (_dir, db) = open_test_db();
        assert!(!db.dedup_contains(&MessageId::new()).unwrap());
    }

    #[test]
    fn dedup_multiple_entries() {
        let (_dir, db) = open_test_db();
        let ids: Vec<MessageId> = (0..100).map(|_| MessageId::new()).collect();

        for id in &ids {
            db.dedup_insert(
                id,
                &DedupEntry {
                    stored_at: 0,
                    topic: "t".into(),
                },
            )
            .unwrap();
        }

        for id in &ids {
            assert!(db.dedup_contains(id).unwrap());
        }
        // Random new ID should not be present
        assert!(!db.dedup_contains(&MessageId::new()).unwrap());
    }

    #[test]
    fn dedup_entry_round_trip() {
        let (_dir, db) = open_test_db();
        let id = MessageId::new();
        let entry = DedupEntry {
            stored_at: 1700000000,
            topic: "payment.completed".into(),
        };

        db.dedup_insert(&id, &entry).unwrap();

        // Read raw bytes and deserialize
        let raw = db.dedup_tree.get(id.as_bytes()).unwrap().unwrap();
        let decoded: DedupEntry = rmp_serde::from_slice(&raw).unwrap();
        assert_eq!(decoded.stored_at, 1700000000);
        assert_eq!(decoded.topic, "payment.completed");
    }

    #[test]
    fn unknown_version_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state");

        // Create DB and set a future version
        {
            let db = sled::Config::new().path(&path).open().unwrap();
            let meta = db.open_tree("meta").unwrap();
            meta.insert("schema_version", &99u32.to_be_bytes()).unwrap();
            meta.flush().unwrap();
        }

        let result = StateDb::open(path);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unknown state DB schema version: 99"));
    }

    #[test]
    fn flush_persists_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state");
        let id = MessageId::new();

        {
            let db = StateDb::open(path.clone()).unwrap();
            db.dedup_insert(
                &id,
                &DedupEntry {
                    stored_at: 42,
                    topic: "test".into(),
                },
            )
            .unwrap();
            db.flush().unwrap();
        }

        // Reopen and check
        let db = StateDb::open(path).unwrap();
        assert!(db.dedup_contains(&id).unwrap());
    }

    #[test]
    fn bulk_insert_from_replay() {
        let (_dir, db) = open_test_db();
        let ids: Vec<MessageId> = (0..50).map(|_| MessageId::new()).collect();

        let count = db.dedup_bulk_insert(ids.iter().copied()).unwrap();
        assert_eq!(count, 50);

        for id in &ids {
            assert!(db.dedup_contains(id).unwrap());
        }

        // Re-inserting should not double-count
        let count = db.dedup_bulk_insert(ids.iter().copied()).unwrap();
        assert_eq!(count, 0);
    }
}
