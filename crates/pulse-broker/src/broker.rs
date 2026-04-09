use std::sync::Arc;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use tokio::sync::mpsc;

use pulse_protocol::Frame;

use crate::config::BrokerConfig;
use crate::delivery::manager::DeliveryManager;
use crate::pipeline::dispatcher::IngestMessage;
use crate::routing::Router;
use crate::server::session::{Session, SessionId};
use crate::storage::state_db::StateDb;

/// Handle to a running session's write side.
pub struct SessionHandle {
    pub session: Arc<Session>,
    pub deliver_tx: mpsc::Sender<Frame>,
}

/// Central shared state for the broker.
///
/// Held behind `Arc` and shared across all tasks (listener, connections,
/// dispatcher, delivery).
pub struct BrokerHandle {
    pub config: ArcSwap<BrokerConfig>,
    pub dispatch_tx: mpsc::Sender<IngestMessage>,
    pub sessions: DashMap<SessionId, SessionHandle>,
    pub state_db: Arc<StateDb>,
    pub router: Arc<Router>,
    pub delivery: DeliveryManager,
}

impl BrokerHandle {
    pub fn new(
        config: BrokerConfig,
        dispatch_tx: mpsc::Sender<IngestMessage>,
        state_db: Arc<StateDb>,
        delivery: DeliveryManager,
        router: Arc<Router>,
    ) -> Arc<Self> {
        Arc::new(Self {
            config: ArcSwap::new(Arc::new(config)),
            dispatch_tx,
            sessions: DashMap::new(),
            state_db,
            router,
            delivery,
        })
    }

    /// Register a new session.
    pub fn register_session(&self, session: Arc<Session>) {
        let deliver_tx = session.deliver_tx.clone();
        self.sessions.insert(
            session.id,
            SessionHandle {
                session,
                deliver_tx,
            },
        );
    }

    /// Unregister a session on disconnect.
    pub fn unregister_session(&self, session_id: SessionId) {
        self.sessions.remove(&session_id);
    }
}
