use std::time::{Duration, Instant};

use dashmap::DashMap;

use pulse_protocol::MessageId;

/// Tracking entry for an in-flight delivery.
#[derive(Debug, Clone)]
pub struct InflightEntry {
    pub msg_id: MessageId,
    pub consumer_id: String,
    pub topic: String,
    pub delivered_at: Instant,
    pub attempt: u32,
}

/// Tracks in-flight deliveries and detects timeouts.
pub struct AckTracker {
    /// Key: (msg_id, consumer_id)
    inflight: DashMap<(MessageId, String), InflightEntry>,
    timeout: Duration,
}

impl AckTracker {
    pub fn new(timeout_secs: u64) -> Self {
        Self {
            inflight: DashMap::new(),
            timeout: Duration::from_secs(timeout_secs),
        }
    }

    /// Track a delivery as in-flight.
    pub fn track(&self, entry: InflightEntry) {
        let key = (entry.msg_id, entry.consumer_id.clone());
        self.inflight.insert(key, entry);
    }

    /// Mark a delivery as ACKed (remove from tracking).
    /// Returns the entry if it was being tracked.
    pub fn ack(&self, msg_id: &MessageId, consumer_id: &str) -> Option<InflightEntry> {
        self.inflight
            .remove(&(msg_id.to_owned(), consumer_id.to_owned()))
            .map(|(_, v)| v)
    }

    /// Find all timed-out entries and remove them.
    pub fn collect_timeouts(&self) -> Vec<InflightEntry> {
        let now = Instant::now();
        let mut timed_out = Vec::new();
        let mut keys_to_remove = Vec::new();

        for entry in self.inflight.iter() {
            if now.duration_since(entry.value().delivered_at) > self.timeout {
                keys_to_remove.push(entry.key().clone());
                timed_out.push(entry.value().clone());
            }
        }

        for key in keys_to_remove {
            self.inflight.remove(&key);
        }

        timed_out
    }

    /// Number of currently in-flight deliveries.
    pub fn inflight_count(&self) -> usize {
        self.inflight.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(topic: &str) -> InflightEntry {
        InflightEntry {
            msg_id: MessageId::new(),
            consumer_id: "test-consumer".into(),
            topic: topic.into(),
            delivered_at: Instant::now(),
            attempt: 1,
        }
    }

    #[test]
    fn track_and_ack() {
        let tracker = AckTracker::new(30);
        let entry = make_entry("test.topic");
        let msg_id = entry.msg_id;

        tracker.track(entry);
        assert_eq!(tracker.inflight_count(), 1);

        let acked = tracker.ack(&msg_id, "test-consumer");
        assert!(acked.is_some());
        assert_eq!(tracker.inflight_count(), 0);
    }

    #[test]
    fn ack_unknown_returns_none() {
        let tracker = AckTracker::new(30);
        assert!(tracker.ack(&MessageId::new(), "unknown").is_none());
    }

    #[test]
    fn collect_timeouts() {
        let tracker = AckTracker::new(0); // 0 second timeout = immediate

        let entry = InflightEntry {
            msg_id: MessageId::new(),
            consumer_id: "consumer".into(),
            topic: "test".into(),
            delivered_at: Instant::now() - Duration::from_secs(1),
            attempt: 1,
        };
        tracker.track(entry);

        let timeouts = tracker.collect_timeouts();
        assert_eq!(timeouts.len(), 1);
        assert_eq!(tracker.inflight_count(), 0);
    }
}
