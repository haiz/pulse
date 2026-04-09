use std::sync::Arc;

use tokio::sync::RwLock;

use crate::consistent_hash::{HashRing, NodeId};
use crate::gossip::MembershipEvent;

/// Manages the cluster topology: reacts to gossip events,
/// updates the consistent hash ring, and tracks topic ownership.
pub struct TopologyManager {
    ring: Arc<RwLock<HashRing>>,
    local_node_id: NodeId,
}

impl TopologyManager {
    pub fn new(local_node_id: NodeId, virtual_nodes: usize) -> Self {
        let mut ring = HashRing::new(virtual_nodes);
        ring.add_node(local_node_id.clone());

        Self {
            ring: Arc::new(RwLock::new(ring)),
            local_node_id,
        }
    }

    /// Get the shared hash ring.
    pub fn ring(&self) -> Arc<RwLock<HashRing>> {
        self.ring.clone()
    }

    /// Process membership events from the gossip protocol and update the ring.
    pub async fn handle_events(&self, events: Vec<MembershipEvent>) {
        if events.is_empty() {
            return;
        }

        let mut ring = self.ring.write().await;
        for event in &events {
            match event {
                MembershipEvent::Join(node_id) => {
                    if ring.add_node(node_id.clone()) {
                        tracing::info!(node = %node_id, version = ring.version(), "node joined ring");
                    }
                }
                MembershipEvent::Dead(node_id) => {
                    if ring.remove_node(node_id) {
                        tracing::warn!(node = %node_id, version = ring.version(), "node removed from ring");
                    }
                }
                MembershipEvent::Recovered(node_id) => {
                    if ring.add_node(node_id.clone()) {
                        tracing::info!(node = %node_id, version = ring.version(), "node recovered into ring");
                    }
                }
                MembershipEvent::Suspect(_) => {
                    // Don't remove from ring on suspect — wait for dead
                }
            }
        }
    }

    /// Check if the local node owns a given topic.
    pub async fn is_local_owner(&self, topic: &str) -> bool {
        let ring = self.ring.read().await;
        ring.get_owner(topic)
            .map(|owner| owner == &self.local_node_id)
            .unwrap_or(true) // if ring is empty, local node is owner by default
    }

    /// Get the owner of a topic.
    pub async fn get_owner(&self, topic: &str) -> Option<NodeId> {
        let ring = self.ring.read().await;
        ring.get_owner(topic).cloned()
    }

    /// Local node ID.
    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_node_owns_everything_when_alone() {
        let topo = TopologyManager::new("node-1".into(), 128);
        assert!(topo.is_local_owner("any.topic").await);
        assert!(topo.is_local_owner("other.topic").await);
    }

    #[tokio::test]
    async fn join_event_adds_to_ring() {
        let topo = TopologyManager::new("node-1".into(), 128);

        topo.handle_events(vec![MembershipEvent::Join("node-2".into())])
            .await;

        let ring_ref = topo.ring();
        let ring = ring_ref.read().await;
        assert_eq!(ring.node_count(), 2);
    }

    #[tokio::test]
    async fn dead_event_removes_from_ring() {
        let topo = TopologyManager::new("node-1".into(), 128);

        topo.handle_events(vec![
            MembershipEvent::Join("node-2".into()),
            MembershipEvent::Dead("node-2".into()),
        ])
        .await;

        let ring_ref = topo.ring();
        let ring = ring_ref.read().await;
        assert_eq!(ring.node_count(), 1);
    }

    #[tokio::test]
    async fn ownership_changes_with_topology() {
        let topo = TopologyManager::new("node-1".into(), 128);

        // Initially, node-1 owns everything
        assert!(topo.is_local_owner("test.topic").await);

        // Add node-2: some topics may move
        topo.handle_events(vec![MembershipEvent::Join("node-2".into())])
            .await;

        // node-1 still owns some topics
        let mut local_count = 0;
        for i in 0..100 {
            if topo.is_local_owner(&format!("topic.{i}")).await {
                local_count += 1;
            }
        }
        // Should own roughly half with 2 nodes
        assert!(local_count > 20 && local_count < 80);
    }
}
