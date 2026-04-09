use lru::LruCache;
use std::num::NonZeroUsize;

use pulse_protocol::MessageId;

/// Consumer-side deduplication using an LRU cache.
///
/// Tracks recently processed message IDs to prevent duplicate processing
/// when the broker re-delivers events (e.g., after ACK loss).
pub struct ConsumerDedup {
    cache: LruCache<MessageId, ()>,
}

impl ConsumerDedup {
    /// Create a new dedup cache with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: LruCache::new(NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::MIN)),
        }
    }

    /// Check if a message was already processed. If not, mark it as processed.
    /// Returns `true` if this is a new (not yet seen) message.
    pub fn check_and_insert(&mut self, msg_id: &MessageId) -> bool {
        if self.cache.contains(msg_id) {
            false
        } else {
            self.cache.put(*msg_id, ());
            true
        }
    }

    /// Check without inserting.
    pub fn contains(&self, msg_id: &MessageId) -> bool {
        self.cache.contains(msg_id)
    }

    /// Number of tracked message IDs.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_message_is_accepted() {
        let mut dedup = ConsumerDedup::new(100);
        let id = MessageId::new();
        assert!(dedup.check_and_insert(&id));
    }

    #[test]
    fn duplicate_is_rejected() {
        let mut dedup = ConsumerDedup::new(100);
        let id = MessageId::new();
        assert!(dedup.check_and_insert(&id));
        assert!(!dedup.check_and_insert(&id));
    }

    #[test]
    fn eviction_on_capacity() {
        let mut dedup = ConsumerDedup::new(3);
        let ids: Vec<MessageId> = (0..5).map(|_| MessageId::new()).collect();

        for id in &ids {
            dedup.check_and_insert(id);
        }

        // First two should be evicted
        assert!(!dedup.contains(&ids[0]));
        assert!(!dedup.contains(&ids[1]));
        // Last three should be present
        assert!(dedup.contains(&ids[2]));
        assert!(dedup.contains(&ids[3]));
        assert!(dedup.contains(&ids[4]));
    }

    #[test]
    fn len_tracks_count() {
        let mut dedup = ConsumerDedup::new(100);
        assert!(dedup.is_empty());
        dedup.check_and_insert(&MessageId::new());
        dedup.check_and_insert(&MessageId::new());
        assert_eq!(dedup.len(), 2);
    }
}
