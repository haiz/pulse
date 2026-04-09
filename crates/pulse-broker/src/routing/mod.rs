pub mod config;
pub mod engine;
pub mod filter;
pub mod transform;

use std::sync::RwLock;

use crate::routing::engine::{SubscriptionTarget, TopicTrie};

/// Unified router combining subscription-based (TopicTrie) routing.
pub struct Router {
    trie: RwLock<TopicTrie>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            trie: RwLock::new(TopicTrie::new()),
        }
    }

    /// Register a subscription.
    pub fn subscribe(&self, pattern: &str, target: SubscriptionTarget) {
        let mut trie = self.trie.write().unwrap();
        trie.insert(pattern, target);
    }

    /// Remove a subscription.
    pub fn unsubscribe(&self, pattern: &str, sub_id: &str) {
        let mut trie = self.trie.write().unwrap();
        trie.remove(pattern, sub_id);
    }

    /// Resolve a topic to all matching subscription targets.
    pub fn resolve(&self, topic: &str) -> Vec<SubscriptionTarget> {
        let trie = self.trie.read().unwrap();
        trie.resolve(topic).into_iter().cloned().collect()
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}
