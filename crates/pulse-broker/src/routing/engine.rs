use std::collections::HashMap;

use tokio::sync::mpsc;

use pulse_protocol::Frame;

/// A subscription target — where to deliver a matched event.
#[derive(Debug, Clone)]
pub struct SubscriptionTarget {
    pub consumer_id: String,
    pub sub_id: String,
    pub group: Option<String>,
    pub filter: Option<super::filter::CompiledFilter>,
    pub deliver_tx: mpsc::Sender<Frame>,
    pub partition_key: Option<String>,
}

/// Trie-based topic matching engine.
///
/// Supports exact match, single wildcard (`*`), and multi-wildcard (`>`).
/// O(number of topic segments) resolution.
pub struct TopicTrie {
    children: HashMap<String, TopicTrie>,
    /// Exact subscriptions at this node (leaf)
    subscribers: Vec<SubscriptionTarget>,
    /// `*` wildcard subscribers at this level
    single_wildcard: Vec<SubscriptionTarget>,
    /// `>` wildcard subscribers at this level (match all remaining)
    multi_wildcard: Vec<SubscriptionTarget>,
}

impl TopicTrie {
    pub fn new() -> Self {
        Self {
            children: HashMap::new(),
            subscribers: Vec::new(),
            single_wildcard: Vec::new(),
            multi_wildcard: Vec::new(),
        }
    }

    /// Insert a subscription for a topic pattern.
    ///
    /// Patterns:
    /// - `order.created` — exact match
    /// - `order.*` — single wildcard (one segment)
    /// - `order.>` — multi-wildcard (one or more segments)
    /// - `*` — global wildcard (all topics)
    pub fn insert(&mut self, pattern: &str, target: SubscriptionTarget) {
        let segments: Vec<&str> = pattern.split('.').collect();
        self.insert_recursive(&segments, 0, target);
    }

    fn insert_recursive(&mut self, segments: &[&str], depth: usize, target: SubscriptionTarget) {
        if depth >= segments.len() {
            self.subscribers.push(target);
            return;
        }

        let segment = segments[depth];
        let is_last = depth == segments.len() - 1;

        if segment == ">" {
            // Multi-wildcard: matches everything from here
            self.multi_wildcard.push(target);
        } else if segment == "*" && is_last {
            // Single wildcard at the last position
            self.single_wildcard.push(target);
        } else {
            // Descend into child (or create it)
            let child = self.children.entry(segment.to_string()).or_default();
            child.insert_recursive(segments, depth + 1, target);
        }
    }

    /// Remove a subscription by sub_id from a topic pattern.
    pub fn remove(&mut self, pattern: &str, sub_id: &str) {
        let segments: Vec<&str> = pattern.split('.').collect();
        self.remove_recursive(&segments, 0, sub_id);
    }

    fn remove_recursive(&mut self, segments: &[&str], depth: usize, sub_id: &str) {
        if depth >= segments.len() {
            self.subscribers.retain(|t| t.sub_id != sub_id);
            return;
        }

        let segment = segments[depth];
        let is_last = depth == segments.len() - 1;

        if segment == ">" {
            self.multi_wildcard.retain(|t| t.sub_id != sub_id);
        } else if segment == "*" && is_last {
            self.single_wildcard.retain(|t| t.sub_id != sub_id);
        } else if let Some(child) = self.children.get_mut(segment) {
            child.remove_recursive(segments, depth + 1, sub_id);
        }
    }

    /// Resolve a concrete topic to all matching subscription targets.
    pub fn resolve(&self, topic: &str) -> Vec<&SubscriptionTarget> {
        let segments: Vec<&str> = topic.split('.').collect();
        let mut results = Vec::new();
        self.resolve_recursive(&segments, 0, &mut results);
        results
    }

    fn resolve_recursive<'a>(
        &'a self,
        segments: &[&str],
        depth: usize,
        results: &mut Vec<&'a SubscriptionTarget>,
    ) {
        // Multi-wildcard ">" matches if there are remaining segments
        // (i.e., ">" requires at least one more segment after this point)
        if depth < segments.len() {
            results.extend(self.multi_wildcard.iter());
        }

        if depth >= segments.len() {
            // Reached end of topic — collect exact matches at this node
            results.extend(self.subscribers.iter());
            return;
        }

        let segment = segments[depth];
        let is_last = depth == segments.len() - 1;

        // Exact segment match: descend into child
        if let Some(child) = self.children.get(segment) {
            child.resolve_recursive(segments, depth + 1, results);
        }

        // `*` child: matches any single segment at this position
        if let Some(wildcard_child) = self.children.get("*") {
            wildcard_child.resolve_recursive(segments, depth + 1, results);
        }

        // Single wildcard subscribers at this level match if this is the last segment
        if is_last {
            results.extend(self.single_wildcard.iter());
        }
    }
}

impl Default for TopicTrie {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn exact_match() {
        let mut trie = TopicTrie::new();
        trie.insert("order.created", make_target("svc-a", "s1"));

        let results = trie.resolve("order.created");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].consumer_id, "svc-a");

        assert!(trie.resolve("order.updated").is_empty());
        assert!(trie.resolve("order").is_empty());
    }

    #[test]
    fn single_wildcard() {
        let mut trie = TopicTrie::new();
        trie.insert("order.*", make_target("svc-a", "s1"));

        // Matches one segment after "order."
        assert_eq!(trie.resolve("order.created").len(), 1);
        assert_eq!(trie.resolve("order.updated").len(), 1);

        // Does NOT match two segments
        assert!(trie.resolve("order.us.created").is_empty());

        // Does NOT match bare "order"
        assert!(trie.resolve("order").is_empty());
    }

    #[test]
    fn multi_wildcard() {
        let mut trie = TopicTrie::new();
        trie.insert("order.>", make_target("svc-a", "s1"));

        // Matches one or more segments
        assert_eq!(trie.resolve("order.created").len(), 1);
        assert_eq!(trie.resolve("order.us.created").len(), 1);
        assert_eq!(trie.resolve("order.us.vn.created").len(), 1);

        // Does NOT match bare "order" (> needs at least one segment)
        assert!(trie.resolve("order").is_empty());

        // Does NOT match unrelated topics
        assert!(trie.resolve("payment.completed").is_empty());
    }

    #[test]
    fn global_wildcard() {
        let mut trie = TopicTrie::new();
        trie.insert("*", make_target("analytics", "s1"));

        // Single segment topics match single wildcard
        assert_eq!(trie.resolve("order").len(), 1);
        assert_eq!(trie.resolve("payment").len(), 1);

        // Multi-segment topics do NOT match single wildcard
        assert!(trie.resolve("order.created").is_empty());
    }

    #[test]
    fn global_multi_wildcard() {
        let mut trie = TopicTrie::new();
        trie.insert(">", make_target("analytics", "s1"));

        // Matches everything
        assert_eq!(trie.resolve("order").len(), 1);
        assert_eq!(trie.resolve("order.created").len(), 1);
        assert_eq!(trie.resolve("a.b.c.d").len(), 1);
    }

    #[test]
    fn multiple_subscribers_same_pattern() {
        let mut trie = TopicTrie::new();
        trie.insert("order.created", make_target("svc-a", "s1"));
        trie.insert("order.created", make_target("svc-b", "s2"));

        let results = trie.resolve("order.created");
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn mixed_patterns() {
        let mut trie = TopicTrie::new();
        trie.insert("order.created", make_target("exact", "s1"));
        trie.insert("order.*", make_target("single-wc", "s2"));
        trie.insert("order.>", make_target("multi-wc", "s3"));

        let results = trie.resolve("order.created");
        assert_eq!(results.len(), 3);

        // Multi-segment only matches ">"
        let results2 = trie.resolve("order.us.created");
        assert_eq!(results2.len(), 1);
        assert_eq!(results2[0].consumer_id, "multi-wc");
    }

    #[test]
    fn remove_subscription() {
        let mut trie = TopicTrie::new();
        trie.insert("order.created", make_target("svc-a", "s1"));
        trie.insert("order.created", make_target("svc-b", "s2"));

        assert_eq!(trie.resolve("order.created").len(), 2);

        trie.remove("order.created", "s1");
        let results = trie.resolve("order.created");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].sub_id, "s2");
    }

    #[test]
    fn remove_wildcard_subscription() {
        let mut trie = TopicTrie::new();
        trie.insert("order.*", make_target("svc-a", "s1"));
        trie.insert("order.>", make_target("svc-b", "s2"));

        trie.remove("order.*", "s1");
        assert!(trie.resolve("order.created").len() == 1);

        trie.remove("order.>", "s2");
        assert!(trie.resolve("order.created").is_empty());
    }

    #[test]
    fn deeply_nested_topics() {
        let mut trie = TopicTrie::new();
        trie.insert("a.b.c.d.e", make_target("deep", "s1"));

        assert_eq!(trie.resolve("a.b.c.d.e").len(), 1);
        assert!(trie.resolve("a.b.c.d").is_empty());
        assert!(trie.resolve("a.b.c.d.e.f").is_empty());
    }
}
