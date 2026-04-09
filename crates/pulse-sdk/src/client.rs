use crate::connection::Connection;
use crate::error::PulseError;
use crate::publish;
use crate::subscribe::{self, Subscription};
use crate::types::{Event, PublishOpts, SubscribeOpts};

use pulse_protocol::MessageId;

/// Main entry point for the Pulse SDK.
///
/// # Example
/// ```no_run
/// # async fn example() -> Result<(), pulse_sdk::PulseError> {
/// let mut client = pulse_sdk::PulseBuilder::new("my-service", "default")
///     .connect()
///     .await?;
///
/// // Publish an event
/// client.publish("order.created", rmpv::Value::Nil, None).await?;
///
/// // Subscribe to events
/// client.subscribe("order.*", None).await?;
/// # Ok(())
/// # }
/// ```
pub struct Pulse {
    conn: Connection,
    dedup_capacity: usize,
}

impl Pulse {
    pub(crate) fn new(conn: Connection, dedup_capacity: usize) -> Self {
        Self {
            conn,
            dedup_capacity,
        }
    }

    /// Publish an event to a topic.
    pub async fn publish(
        &mut self,
        topic: &str,
        data: rmpv::Value,
        opts: Option<PublishOpts>,
    ) -> Result<MessageId, PulseError> {
        publish::publish(&mut self.conn, topic, data, opts).await
    }

    /// Publish with automatic retry (preserves msg_id across retries).
    pub async fn publish_with_retry(
        &mut self,
        topic: &str,
        data: rmpv::Value,
        opts: Option<PublishOpts>,
        max_retries: u32,
    ) -> Result<MessageId, PulseError> {
        publish::publish_with_retry(&mut self.conn, topic, data, opts, max_retries).await
    }

    /// Subscribe to a topic pattern.
    pub async fn subscribe(
        &mut self,
        topic: &str,
        opts: Option<SubscribeOpts>,
    ) -> Result<Subscription, PulseError> {
        subscribe::subscribe(&mut self.conn, topic, opts).await
    }

    /// Unsubscribe from a subscription.
    pub async fn unsubscribe(&mut self, subscription: &Subscription) -> Result<(), PulseError> {
        subscribe::unsubscribe(&mut self.conn, subscription).await
    }

    /// Run a consumer loop that dispatches events to the handler.
    ///
    /// This blocks until the connection is closed or an error occurs.
    /// The handler is called for each received event. Return `Ok(())`
    /// to ACK, or `Err(...)` to NACK (trigger retry).
    pub async fn consume<F, Fut>(&mut self, handler: F) -> Result<(), PulseError>
    where
        F: Fn(Event) -> Fut,
        Fut: std::future::Future<Output = Result<(), PulseError>>,
    {
        subscribe::consume_loop(&mut self.conn, handler, self.dedup_capacity).await
    }

    /// Get the broker ID from the CONNACK.
    pub fn broker_id(&self) -> &str {
        &self.conn.broker_id
    }

    /// Get the max payload size from the CONNACK.
    pub fn max_payload(&self) -> u32 {
        self.conn.max_payload
    }
}
