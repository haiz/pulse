use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use pulse_protocol::MessageId;

use crate::PulseError;

/// An event received from the broker.
#[derive(Debug, Clone)]
pub struct Event {
    pub msg_id: MessageId,
    pub topic: String,
    pub data: rmpv::Value,
    pub headers: HashMap<String, String>,
    pub attempt: u32,
}

/// Handler function for processing received events.
pub type EventHandler = Box<
    dyn Fn(Event) -> Pin<Box<dyn Future<Output = Result<(), PulseError>> + Send>> + Send + Sync,
>;

/// Options for subscribing.
#[derive(Debug, Clone, Default)]
pub struct SubscribeOpts {
    /// Consumer group name. Events load-balanced within group.
    pub group: Option<String>,
    /// Content filter expression.
    pub filter: Option<String>,
    /// Start position: "latest" (default) or "earliest".
    pub position: Option<String>,
}

/// Options for publishing.
#[derive(Debug, Clone, Default)]
pub struct PublishOpts {
    /// Custom headers to attach.
    pub headers: HashMap<String, String>,
    /// Override message ID (for retry with same ID).
    pub msg_id: Option<MessageId>,
}
