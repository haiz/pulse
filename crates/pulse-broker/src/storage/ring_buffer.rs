use bytes::Bytes;
use crossbeam::queue::ArrayQueue;
use dashmap::DashMap;
use std::sync::Arc;

use pulse_protocol::MessageId;

use crate::storage::wal::WalPosition;

/// An entry in the ring buffer.
#[derive(Debug, Clone)]
pub struct BufferEntry {
    pub msg_id: MessageId,
    pub topic: String,
    pub payload: Bytes,
    pub wal_position: Option<WalPosition>,
    pub inserted_at: std::time::Instant,
}

/// Lock-free ring buffer for in-memory event storage.
///
/// Primary storage in memory mode, fast delivery cache in balanced/durable modes.
/// Uses crossbeam `ArrayQueue` for MPMC lock-free operations and `DashMap` for
/// O(1) message ID lookups.
pub struct RingBuffer {
    queue: ArrayQueue<MessageId>,
    index: DashMap<MessageId, BufferEntry>,
    capacity: usize,
}

impl RingBuffer {
    pub fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            queue: ArrayQueue::new(capacity),
            index: DashMap::with_capacity(capacity),
            capacity,
        })
    }

    /// Insert an entry. If the buffer is full, evict the oldest entry.
    pub fn push(&self, entry: BufferEntry) {
        let msg_id = entry.msg_id;
        self.index.insert(msg_id, entry);

        if self.queue.push(msg_id).is_err() {
            // Queue full — evict oldest
            if let Some(old_id) = self.queue.pop() {
                self.index.remove(&old_id);
            }
            // Retry push (should succeed now)
            let _ = self.queue.push(msg_id);
        }
    }

    /// Look up an entry by message ID.
    pub fn get(&self, msg_id: &MessageId) -> Option<BufferEntry> {
        self.index.get(msg_id).map(|r| r.value().clone())
    }

    /// Current number of entries in the buffer.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Buffer capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(topic: &str) -> BufferEntry {
        BufferEntry {
            msg_id: MessageId::new(),
            topic: topic.into(),
            payload: Bytes::from_static(b"test"),
            wal_position: None,
            inserted_at: std::time::Instant::now(),
        }
    }

    #[test]
    fn push_and_get() {
        let buf = RingBuffer::new(100);
        let entry = make_entry("test.topic");
        let id = entry.msg_id;

        buf.push(entry);
        assert_eq!(buf.len(), 1);

        let found = buf.get(&id).unwrap();
        assert_eq!(found.topic, "test.topic");
    }

    #[test]
    fn eviction_on_full() {
        let buf = RingBuffer::new(3);
        let mut ids = Vec::new();

        for i in 0..5 {
            let entry = make_entry(&format!("topic.{i}"));
            ids.push(entry.msg_id);
            buf.push(entry);
        }

        // First 2 should be evicted
        assert!(buf.get(&ids[0]).is_none());
        assert!(buf.get(&ids[1]).is_none());

        // Last 3 should be present
        assert!(buf.get(&ids[2]).is_some());
        assert!(buf.get(&ids[3]).is_some());
        assert!(buf.get(&ids[4]).is_some());
        assert_eq!(buf.len(), 3);
    }

    #[test]
    fn empty_buffer() {
        let buf = RingBuffer::new(10);
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
        assert!(buf.get(&MessageId::new()).is_none());
    }

    #[test]
    fn capacity_returns_correct_value() {
        let buf = RingBuffer::new(42);
        assert_eq!(buf.capacity(), 42);
    }
}
