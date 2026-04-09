use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

use crate::consistent_hash::NodeId;

/// SWIM protocol member state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberState {
    Alive,
    Suspect,
    Dead,
}

/// Information about a cluster member.
#[derive(Debug, Clone)]
pub struct Member {
    pub node_id: NodeId,
    pub addr: SocketAddr,
    pub state: MemberState,
    pub last_state_change: Instant,
    pub incarnation: u64,
}

/// SWIM gossip protocol configuration.
#[derive(Debug, Clone)]
pub struct SwimConfig {
    /// How often to probe a random peer (default: 200ms).
    pub probe_interval: Duration,
    /// Number of indirect probes on direct probe failure (default: 3).
    pub indirect_probes: usize,
    /// Time before suspect -> dead transition (default: 5s).
    pub suspect_timeout: Duration,
    /// Time before dead nodes are removed from membership (default: 30s).
    pub dead_cleanup_timeout: Duration,
}

impl Default for SwimConfig {
    fn default() -> Self {
        Self {
            probe_interval: Duration::from_millis(200),
            indirect_probes: 3,
            suspect_timeout: Duration::from_secs(5),
            dead_cleanup_timeout: Duration::from_secs(30),
        }
    }
}

/// Event emitted when membership changes.
#[derive(Debug, Clone)]
pub enum MembershipEvent {
    Join(NodeId),
    Suspect(NodeId),
    Dead(NodeId),
    Recovered(NodeId),
}

/// SWIM protocol implementation for cluster membership and failure detection.
pub struct SwimProtocol {
    node_id: NodeId,
    #[allow(dead_code)]
    addr: SocketAddr,
    members: Arc<RwLock<MemberList>>,
    config: SwimConfig,
}

/// The shared membership list.
pub struct MemberList {
    members: HashMap<NodeId, Member>,
    event_log: Vec<MembershipEvent>,
}

impl MemberList {
    pub fn new() -> Self {
        Self {
            members: HashMap::new(),
            event_log: Vec::new(),
        }
    }

    /// Get all alive or suspect members.
    pub fn active_members(&self) -> Vec<&Member> {
        self.members
            .values()
            .filter(|m| m.state != MemberState::Dead)
            .collect()
    }

    /// Get all members.
    pub fn all_members(&self) -> &HashMap<NodeId, Member> {
        &self.members
    }

    /// Get a specific member.
    pub fn get(&self, node_id: &str) -> Option<&Member> {
        self.members.get(node_id)
    }

    /// Mark a member as alive (join or recovery).
    pub fn mark_alive(&mut self, node_id: NodeId, addr: SocketAddr, incarnation: u64) {
        let is_new = !self.members.contains_key(&node_id);
        let member = self
            .members
            .entry(node_id.clone())
            .or_insert_with(|| Member {
                node_id: node_id.clone(),
                addr,
                state: MemberState::Alive,
                last_state_change: Instant::now(),
                incarnation,
            });

        if member.incarnation <= incarnation {
            let was_dead = member.state == MemberState::Dead;
            member.state = MemberState::Alive;
            member.last_state_change = Instant::now();
            member.incarnation = incarnation;
            member.addr = addr;

            if is_new {
                self.event_log.push(MembershipEvent::Join(node_id));
            } else if was_dead {
                self.event_log.push(MembershipEvent::Recovered(node_id));
            }
        }
    }

    /// Mark a member as suspect.
    pub fn mark_suspect(&mut self, node_id: &str) {
        if let Some(member) = self.members.get_mut(node_id) {
            if member.state == MemberState::Alive {
                member.state = MemberState::Suspect;
                member.last_state_change = Instant::now();
                self.event_log
                    .push(MembershipEvent::Suspect(node_id.to_string()));
            }
        }
    }

    /// Mark a member as dead.
    pub fn mark_dead(&mut self, node_id: &str) {
        if let Some(member) = self.members.get_mut(node_id) {
            if member.state != MemberState::Dead {
                member.state = MemberState::Dead;
                member.last_state_change = Instant::now();
                self.event_log
                    .push(MembershipEvent::Dead(node_id.to_string()));
            }
        }
    }

    /// Transition suspect members to dead after timeout.
    pub fn expire_suspects(&mut self, suspect_timeout: Duration) {
        let now = Instant::now();
        let to_kill: Vec<NodeId> = self
            .members
            .values()
            .filter(|m| {
                m.state == MemberState::Suspect
                    && now.duration_since(m.last_state_change) > suspect_timeout
            })
            .map(|m| m.node_id.clone())
            .collect();

        for id in to_kill {
            self.mark_dead(&id);
        }
    }

    /// Remove dead members after cleanup timeout.
    pub fn cleanup_dead(&mut self, cleanup_timeout: Duration) {
        let now = Instant::now();
        self.members.retain(|_, m| {
            !(m.state == MemberState::Dead
                && now.duration_since(m.last_state_change) > cleanup_timeout)
        });
    }

    /// Drain all pending membership events.
    pub fn drain_events(&mut self) -> Vec<MembershipEvent> {
        std::mem::take(&mut self.event_log)
    }

    /// Number of known members (all states).
    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }
}

impl Default for MemberList {
    fn default() -> Self {
        Self::new()
    }
}

impl SwimProtocol {
    /// Create a new SWIM protocol instance.
    pub fn new(node_id: NodeId, addr: SocketAddr, config: SwimConfig) -> Self {
        Self {
            node_id,
            addr,
            members: Arc::new(RwLock::new(MemberList::new())),
            config,
        }
    }

    /// Get a reference to the shared membership list.
    pub fn members(&self) -> Arc<RwLock<MemberList>> {
        self.members.clone()
    }

    /// The local node ID.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Join the cluster by contacting seed nodes.
    pub async fn join(&self, seeds: &[SocketAddr]) {
        let mut members = self.members.write().await;
        for seed in seeds {
            // Add seed as alive member with a placeholder node_id
            // In a full implementation, we'd send a JOIN message and receive
            // the actual node_id in the response
            let seed_id = format!("seed-{seed}");
            members.mark_alive(seed_id, *seed, 0);
        }
        tracing::info!(
            node_id = %self.node_id,
            seeds = seeds.len(),
            "joining cluster"
        );
    }

    /// Run the SWIM protocol loop.
    /// This periodically probes peers, detects failures, and disseminates
    /// membership changes.
    pub async fn run(&self, mut shutdown: tokio::sync::broadcast::Receiver<()>) {
        let mut probe_timer = tokio::time::interval(self.config.probe_interval);

        loop {
            tokio::select! {
                _ = probe_timer.tick() => {
                    self.probe_cycle().await;
                }
                _ = shutdown.recv() => {
                    tracing::info!(node_id = %self.node_id, "SWIM protocol shutdown");
                    break;
                }
            }
        }
    }

    async fn probe_cycle(&self) {
        let mut members = self.members.write().await;

        // Expire suspects -> dead
        members.expire_suspects(self.config.suspect_timeout);

        // Cleanup long-dead members
        members.cleanup_dead(self.config.dead_cleanup_timeout);

        // In a full implementation, we'd:
        // 1. Select a random alive/suspect member
        // 2. Send PING via UDP
        // 3. If no PONG within probe timeout, send indirect probes
        // 4. If still no response, mark as suspect
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    #[test]
    fn member_list_join() {
        let mut list = MemberList::new();
        list.mark_alive("node-1".into(), addr(4222), 1);
        list.mark_alive("node-2".into(), addr(4223), 1);

        assert_eq!(list.len(), 2);
        assert_eq!(list.active_members().len(), 2);
    }

    #[test]
    fn member_state_transitions() {
        let mut list = MemberList::new();
        list.mark_alive("node-1".into(), addr(4222), 1);
        assert_eq!(list.get("node-1").unwrap().state, MemberState::Alive);

        list.mark_suspect("node-1");
        assert_eq!(list.get("node-1").unwrap().state, MemberState::Suspect);

        list.mark_dead("node-1");
        assert_eq!(list.get("node-1").unwrap().state, MemberState::Dead);
    }

    #[test]
    fn suspect_expiry() {
        let mut list = MemberList::new();
        list.mark_alive("node-1".into(), addr(4222), 1);
        list.mark_suspect("node-1");

        // Not expired yet with 5s timeout
        list.expire_suspects(Duration::from_secs(5));
        assert_eq!(list.get("node-1").unwrap().state, MemberState::Suspect);

        // Expired with 0s timeout
        list.expire_suspects(Duration::from_secs(0));
        assert_eq!(list.get("node-1").unwrap().state, MemberState::Dead);
    }

    #[test]
    fn dead_cleanup() {
        let mut list = MemberList::new();
        list.mark_alive("node-1".into(), addr(4222), 1);
        list.mark_dead("node-1");

        // Not cleaned up yet
        list.cleanup_dead(Duration::from_secs(30));
        assert_eq!(list.len(), 1);

        // Cleaned up with 0s timeout
        list.cleanup_dead(Duration::from_secs(0));
        assert_eq!(list.len(), 0);
    }

    #[test]
    fn membership_events() {
        let mut list = MemberList::new();
        list.mark_alive("node-1".into(), addr(4222), 1);
        list.mark_suspect("node-1");
        list.mark_dead("node-1");

        let events = list.drain_events();
        assert_eq!(events.len(), 3);
        assert!(matches!(events[0], MembershipEvent::Join(_)));
        assert!(matches!(events[1], MembershipEvent::Suspect(_)));
        assert!(matches!(events[2], MembershipEvent::Dead(_)));
    }

    #[test]
    fn incarnation_prevents_stale_updates() {
        let mut list = MemberList::new();
        list.mark_alive("node-1".into(), addr(4222), 5);
        list.mark_suspect("node-1");

        // Stale incarnation shouldn't override suspect
        list.mark_alive("node-1".into(), addr(4222), 3);
        assert_eq!(list.get("node-1").unwrap().state, MemberState::Suspect);

        // Higher incarnation should recover
        list.mark_alive("node-1".into(), addr(4222), 6);
        assert_eq!(list.get("node-1").unwrap().state, MemberState::Alive);
    }

    #[test]
    fn active_members_excludes_dead() {
        let mut list = MemberList::new();
        list.mark_alive("node-1".into(), addr(4222), 1);
        list.mark_alive("node-2".into(), addr(4223), 1);
        list.mark_dead("node-1");

        assert_eq!(list.active_members().len(), 1);
        assert_eq!(list.active_members()[0].node_id, "node-2");
    }
}
