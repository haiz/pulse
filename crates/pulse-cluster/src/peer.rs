use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::consistent_hash::NodeId;

/// A handle to a peer connection for inter-node communication.
#[derive(Debug)]
pub struct PeerHandle {
    pub node_id: NodeId,
    pub addr: SocketAddr,
    pub connected: bool,
}

/// Manages TCP connections to peer nodes in the cluster.
pub struct PeerManager {
    local_node_id: NodeId,
    peers: Arc<RwLock<HashMap<NodeId, PeerHandle>>>,
}

impl PeerManager {
    pub fn new(local_node_id: NodeId) -> Self {
        Self {
            local_node_id,
            peers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add a known peer (doesn't establish connection yet).
    pub async fn add_peer(&self, node_id: NodeId, addr: SocketAddr) {
        let mut peers = self.peers.write().await;
        peers.entry(node_id.clone()).or_insert_with(|| PeerHandle {
            node_id,
            addr,
            connected: false,
        });
    }

    /// Remove a peer.
    pub async fn remove_peer(&self, node_id: &str) {
        let mut peers = self.peers.write().await;
        peers.remove(node_id);
    }

    /// Get all known peer node IDs.
    pub async fn peer_ids(&self) -> Vec<NodeId> {
        let peers = self.peers.read().await;
        peers.keys().cloned().collect()
    }

    /// Number of known peers.
    pub async fn peer_count(&self) -> usize {
        self.peers.read().await.len()
    }

    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    #[tokio::test]
    async fn add_and_remove_peer() {
        let mgr = PeerManager::new("node-1".into());
        mgr.add_peer("node-2".into(), addr(4223)).await;
        assert_eq!(mgr.peer_count().await, 1);

        mgr.remove_peer("node-2").await;
        assert_eq!(mgr.peer_count().await, 0);
    }

    #[tokio::test]
    async fn duplicate_add_is_noop() {
        let mgr = PeerManager::new("node-1".into());
        mgr.add_peer("node-2".into(), addr(4223)).await;
        mgr.add_peer("node-2".into(), addr(4223)).await;
        assert_eq!(mgr.peer_count().await, 1);
    }
}
