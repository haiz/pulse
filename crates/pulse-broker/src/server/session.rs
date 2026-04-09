use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;

use pulse_protocol::Frame;

/// Monotonically increasing session ID counter.
static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

pub type SessionId = u64;

/// Authenticated session state per connection.
pub struct Session {
    pub id: SessionId,
    pub service_id: String,
    pub namespace: String,
    pub connected_at: tokio::time::Instant,
    pub permissions: Permissions,
    pub subscriptions: Vec<String>,
    pub max_inflight: u32,
    pub deliver_tx: mpsc::Sender<Frame>,
}

/// Publish/subscribe ACL patterns.
#[derive(Debug, Clone, Default)]
pub struct Permissions {
    pub publish_topics: Vec<String>,
    pub subscribe_topics: Vec<String>,
}

impl Session {
    /// Create a new session from a CONNECT frame.
    /// Auth is a stub in Phase 2 — accepts all connections.
    pub fn new(
        service_id: String,
        namespace: String,
        max_inflight: Option<u32>,
        deliver_tx: mpsc::Sender<Frame>,
    ) -> Self {
        Self {
            id: NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed),
            service_id,
            namespace,
            connected_at: tokio::time::Instant::now(),
            permissions: Permissions {
                publish_topics: vec!["*".into()],
                subscribe_topics: vec!["*".into()],
            },
            subscriptions: Vec::new(),
            max_inflight: max_inflight.unwrap_or(1024),
            deliver_tx,
        }
    }

    /// Check if session can publish to a topic (stub: always true in Phase 2).
    pub fn can_publish(&self, _topic: &str) -> bool {
        true
    }

    /// Check if session can subscribe to a topic (stub: always true in Phase 2).
    pub fn can_subscribe(&self, _topic: &str) -> bool {
        true
    }
}
