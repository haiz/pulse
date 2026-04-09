use std::collections::{BTreeMap, HashSet};
use std::hash::{Hash, Hasher};

/// Unique identifier for a node in the cluster.
pub type NodeId = String;

/// Consistent hash ring for topic-to-node ownership.
///
/// Each physical node contributes `virtual_nodes_per_node` virtual nodes
/// to the ring for balanced distribution. Uses a BTreeMap for efficient
/// clockwise lookups.
pub struct HashRing {
    ring: BTreeMap<u64, NodeId>,
    nodes: HashSet<NodeId>,
    virtual_nodes_per_node: usize,
    version: u64,
}

impl HashRing {
    /// Create a new empty hash ring.
    pub fn new(virtual_nodes_per_node: usize) -> Self {
        Self {
            ring: BTreeMap::new(),
            nodes: HashSet::new(),
            virtual_nodes_per_node,
            version: 0,
        }
    }

    /// Add a node to the ring. Returns `true` if the node was newly added.
    pub fn add_node(&mut self, node_id: NodeId) -> bool {
        if !self.nodes.insert(node_id.clone()) {
            return false;
        }

        for i in 0..self.virtual_nodes_per_node {
            let hash = hash_virtual_node(&node_id, i);
            self.ring.insert(hash, node_id.clone());
        }

        self.version += 1;
        true
    }

    /// Remove a node from the ring. Returns `true` if the node existed.
    pub fn remove_node(&mut self, node_id: &str) -> bool {
        if !self.nodes.remove(node_id) {
            return false;
        }

        for i in 0..self.virtual_nodes_per_node {
            let hash = hash_virtual_node(node_id, i);
            self.ring.remove(&hash);
        }

        self.version += 1;
        true
    }

    /// Get the owner node for a given topic.
    /// Returns the first node clockwise from the topic's hash position.
    pub fn get_owner(&self, topic: &str) -> Option<&NodeId> {
        if self.ring.is_empty() {
            return None;
        }

        let hash = hash_topic(topic);

        // Find the first node clockwise from the hash
        self.ring
            .range(hash..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, node)| node)
    }

    /// Get the owner node and its replicas (next N distinct nodes clockwise).
    pub fn get_owner_and_replicas(&self, topic: &str, replication_factor: usize) -> Vec<&NodeId> {
        if self.ring.is_empty() {
            return Vec::new();
        }

        let hash = hash_topic(topic);
        let mut result = Vec::new();
        let mut seen = HashSet::new();

        // Walk clockwise from hash, collecting distinct nodes
        let iter = self
            .ring
            .range(hash..)
            .chain(self.ring.iter())
            .map(|(_, node)| node);

        for node in iter {
            if seen.insert(node) {
                result.push(node);
                if result.len() > replication_factor {
                    break;
                }
            }
        }

        result
    }

    /// Current ring version (incremented on any topology change).
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Number of physical nodes in the ring.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// All physical node IDs.
    pub fn nodes(&self) -> &HashSet<NodeId> {
        &self.nodes
    }
}

fn hash_topic(topic: &str) -> u64 {
    let mut hasher = std::hash::DefaultHasher::new();
    topic.hash(&mut hasher);
    hasher.finish()
}

fn hash_virtual_node(node_id: &str, vnode_idx: usize) -> u64 {
    let mut hasher = std::hash::DefaultHasher::new();
    node_id.hash(&mut hasher);
    vnode_idx.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_get_owner() {
        let mut ring = HashRing::new(128);
        ring.add_node("node-1".into());

        // With only one node, all topics should map to it
        assert_eq!(ring.get_owner("order.created").unwrap(), "node-1");
        assert_eq!(ring.get_owner("payment.completed").unwrap(), "node-1");
    }

    #[test]
    fn multiple_nodes_distribute() {
        let mut ring = HashRing::new(128);
        ring.add_node("node-1".into());
        ring.add_node("node-2".into());
        ring.add_node("node-3".into());

        let mut distribution = std::collections::HashMap::new();
        for i in 0..1000 {
            let topic = format!("topic.{i}");
            let owner = ring.get_owner(&topic).unwrap().clone();
            *distribution.entry(owner).or_insert(0u32) += 1;
        }

        // All 3 nodes should have some topics
        assert_eq!(distribution.len(), 3);
        for (_, count) in &distribution {
            // Each node should have at least 10% of topics (rough balance)
            assert!(*count > 100, "unbalanced: {distribution:?}");
        }
    }

    #[test]
    fn remove_node() {
        let mut ring = HashRing::new(128);
        ring.add_node("node-1".into());
        ring.add_node("node-2".into());

        assert_eq!(ring.node_count(), 2);

        ring.remove_node("node-1");
        assert_eq!(ring.node_count(), 1);

        // All topics should now map to node-2
        assert_eq!(ring.get_owner("any.topic").unwrap(), "node-2");
    }

    #[test]
    fn empty_ring_returns_none() {
        let ring = HashRing::new(128);
        assert!(ring.get_owner("test").is_none());
    }

    #[test]
    fn version_increments() {
        let mut ring = HashRing::new(128);
        assert_eq!(ring.version(), 0);

        ring.add_node("node-1".into());
        assert_eq!(ring.version(), 1);

        ring.add_node("node-2".into());
        assert_eq!(ring.version(), 2);

        ring.remove_node("node-1");
        assert_eq!(ring.version(), 3);
    }

    #[test]
    fn duplicate_add_is_noop() {
        let mut ring = HashRing::new(128);
        assert!(ring.add_node("node-1".into()));
        assert!(!ring.add_node("node-1".into()));
        assert_eq!(ring.version(), 1); // didn't increment
    }

    #[test]
    fn replicas() {
        let mut ring = HashRing::new(128);
        ring.add_node("node-1".into());
        ring.add_node("node-2".into());
        ring.add_node("node-3".into());

        let nodes = ring.get_owner_and_replicas("order.created", 1);
        assert_eq!(nodes.len(), 2); // owner + 1 replica
        assert_ne!(nodes[0], nodes[1]); // different nodes
    }

    #[test]
    fn minimal_disruption_on_add() {
        let mut ring = HashRing::new(128);
        ring.add_node("node-1".into());
        ring.add_node("node-2".into());

        // Record ownership before adding node-3
        let topics: Vec<String> = (0..100).map(|i| format!("topic.{i}")).collect();
        let before: Vec<String> = topics
            .iter()
            .map(|t| ring.get_owner(t).unwrap().clone())
            .collect();

        ring.add_node("node-3".into());

        let after: Vec<String> = topics
            .iter()
            .map(|t| ring.get_owner(t).unwrap().clone())
            .collect();

        // Most topics should keep their original owner
        let unchanged = before
            .iter()
            .zip(after.iter())
            .filter(|(b, a)| b == a)
            .count();
        assert!(
            unchanged > 50,
            "too much disruption: only {unchanged}/100 unchanged"
        );
    }
}
