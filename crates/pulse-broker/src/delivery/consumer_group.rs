use std::sync::atomic::{AtomicU64, Ordering};

use crate::routing::engine::SubscriptionTarget;

/// A consumer group that load-balances events across members.
pub struct ConsumerGroup {
    pub name: String,
    pub members: Vec<SubscriptionTarget>,
    next_index: AtomicU64,
    pub partition_key: Option<String>,
}

impl ConsumerGroup {
    pub fn new(name: String, partition_key: Option<String>) -> Self {
        Self {
            name,
            members: Vec::new(),
            next_index: AtomicU64::new(0),
            partition_key,
        }
    }

    /// Add a member to the group.
    pub fn add_member(&mut self, target: SubscriptionTarget) {
        self.members.push(target);
    }

    /// Remove a member by sub_id.
    pub fn remove_member(&mut self, sub_id: &str) {
        self.members.retain(|m| m.sub_id != sub_id);
    }

    /// Select a member for delivery using round-robin or partition key.
    ///
    /// If partition_key is configured and can be resolved from the payload,
    /// uses hash-based assignment. Otherwise, falls back to round-robin.
    pub fn select(&self, payload: Option<&rmpv::Value>) -> Option<&SubscriptionTarget> {
        if self.members.is_empty() {
            return None;
        }

        // Try partition key first
        if let (Some(key_path), Some(payload)) = (&self.partition_key, payload) {
            if let Some(key_val) = resolve_partition_key(payload, key_path) {
                let hash = hash_value(&key_val);
                let idx = hash as usize % self.members.len();
                return Some(&self.members[idx]);
            }
        }

        // Fallback: round-robin
        let idx = self.next_index.fetch_add(1, Ordering::Relaxed);
        Some(&self.members[idx as usize % self.members.len()])
    }

    pub fn member_count(&self) -> usize {
        self.members.len()
    }
}

fn resolve_partition_key(payload: &rmpv::Value, key_path: &str) -> Option<String> {
    let segments: Vec<&str> = key_path.split('.').collect();
    let mut current = payload;

    for segment in &segments {
        match current {
            rmpv::Value::Map(entries) => {
                let found = entries.iter().find(|(k, _)| match k {
                    rmpv::Value::String(s) => s.as_str() == Some(*segment),
                    _ => false,
                });
                match found {
                    Some((_, v)) => current = v,
                    None => return None,
                }
            }
            _ => return None,
        }
    }

    // Convert the value to a string for hashing
    match current {
        rmpv::Value::String(s) => s.as_str().map(|s| s.to_string()),
        rmpv::Value::Integer(i) => Some(i.to_string()),
        _ => None,
    }
}

fn hash_value(s: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::hash::DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    fn make_target(id: &str, sub_id: &str) -> SubscriptionTarget {
        let (tx, _rx) = mpsc::channel(1);
        SubscriptionTarget {
            consumer_id: id.into(),
            sub_id: sub_id.into(),
            group: None,
            filter: None,
            deliver_tx: tx,
            partition_key: None,
        }
    }

    #[test]
    fn round_robin_selection() {
        let mut group = ConsumerGroup::new("test".into(), None);
        group.add_member(make_target("a", "s1"));
        group.add_member(make_target("b", "s2"));
        group.add_member(make_target("c", "s3"));

        let ids: Vec<String> = (0..6)
            .map(|_| group.select(None).unwrap().consumer_id.clone())
            .collect();

        // Should cycle through a, b, c
        assert_eq!(ids[0], "a");
        assert_eq!(ids[1], "b");
        assert_eq!(ids[2], "c");
        assert_eq!(ids[3], "a");
    }

    #[test]
    fn partition_key_consistent() {
        let mut group = ConsumerGroup::new("test".into(), Some("user_id".into()));
        group.add_member(make_target("a", "s1"));
        group.add_member(make_target("b", "s2"));
        group.add_member(make_target("c", "s3"));

        let payload = rmpv::Value::Map(vec![(
            rmpv::Value::String("user_id".into()),
            rmpv::Value::String("u_42".into()),
        )]);

        // Same key always goes to same member
        let first = group.select(Some(&payload)).unwrap().consumer_id.clone();
        for _ in 0..10 {
            assert_eq!(group.select(Some(&payload)).unwrap().consumer_id, first);
        }
    }

    #[test]
    fn empty_group_returns_none() {
        let group = ConsumerGroup::new("test".into(), None);
        assert!(group.select(None).is_none());
    }

    #[test]
    fn remove_member() {
        let mut group = ConsumerGroup::new("test".into(), None);
        group.add_member(make_target("a", "s1"));
        group.add_member(make_target("b", "s2"));
        assert_eq!(group.member_count(), 2);

        group.remove_member("s1");
        assert_eq!(group.member_count(), 1);
    }
}
