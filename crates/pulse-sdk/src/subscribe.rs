use futures::{SinkExt, StreamExt};

use pulse_protocol::*;

use crate::connection::Connection;
use crate::dedup::ConsumerDedup;
use crate::error::PulseError;
use crate::types::{Event, SubscribeOpts};

/// Active subscription handle.
pub struct Subscription {
    pub sub_id: String,
    pub topic: String,
}

/// Subscribe to a topic pattern.
pub async fn subscribe(
    conn: &mut Connection,
    topic: &str,
    opts: Option<SubscribeOpts>,
) -> Result<Subscription, PulseError> {
    let opts = opts.unwrap_or_default();
    let sub_id = format!("sub-{}", MessageId::new());

    let sub_frame = Frame::sub(
        MessageId::new(),
        SubPayload {
            topic: topic.to_string(),
            group: opts.group,
            filter: opts.filter,
            position: opts.position,
            sub_id: sub_id.clone(),
        },
    );

    conn.framed
        .send(sub_frame)
        .await
        .map_err(|e| PulseError::SubscribeFailed(e.to_string()))?;

    Ok(Subscription {
        sub_id,
        topic: topic.to_string(),
    })
}

/// Unsubscribe from a subscription.
pub async fn unsubscribe(
    conn: &mut Connection,
    subscription: &Subscription,
) -> Result<(), PulseError> {
    let unsub_frame = Frame::unsub(
        MessageId::new(),
        UnsubPayload {
            sub_id: subscription.sub_id.clone(),
        },
    );

    conn.framed
        .send(unsub_frame)
        .await
        .map_err(|e| PulseError::SubscribeFailed(e.to_string()))?;

    Ok(())
}

/// Run a consumer loop that dispatches events to the handler.
///
/// Handles deduplication, ACK/NACK, and PING/PONG keepalive.
pub async fn consume_loop<F, Fut>(
    conn: &mut Connection,
    handler: F,
    dedup_capacity: usize,
) -> Result<(), PulseError>
where
    F: Fn(Event) -> Fut,
    Fut: std::future::Future<Output = Result<(), PulseError>>,
{
    let mut dedup = ConsumerDedup::new(dedup_capacity);

    loop {
        let frame = conn
            .framed
            .next()
            .await
            .ok_or(PulseError::ChannelClosed)?
            .map_err(|e| PulseError::Protocol(e.to_string()))?;

        match frame.msg_type {
            MessageType::Pub => {
                let pub_payload = match &frame.payload {
                    Payload::Pub(p) => p,
                    _ => continue,
                };

                // Consumer-side dedup
                if !dedup.check_and_insert(&frame.msg_id) {
                    // Already processed — send ACK immediately
                    let ack = Frame::ack(
                        frame.msg_id,
                        AckPayload {
                            status: AckStatus::Done,
                            msg_id: frame.msg_id.as_bytes().to_vec(),
                            reason: None,
                        },
                    );
                    let _ = conn.framed.send(ack).await;
                    continue;
                }

                let event = Event {
                    msg_id: frame.msg_id,
                    topic: pub_payload.topic.clone(),
                    data: pub_payload.data.clone(),
                    headers: pub_payload.headers.clone(),
                    attempt: pub_payload
                        .delivery
                        .as_ref()
                        .map(|d| d.attempt)
                        .unwrap_or(1),
                };

                // Call handler
                let ack_status = match handler(event).await {
                    Ok(()) => AckStatus::Done,
                    Err(_) => AckStatus::Rejected,
                };

                let ack = Frame::ack(
                    frame.msg_id,
                    AckPayload {
                        status: ack_status,
                        msg_id: frame.msg_id.as_bytes().to_vec(),
                        reason: None,
                    },
                );
                conn.framed
                    .send(ack)
                    .await
                    .map_err(|e| PulseError::Connection(e.to_string()))?;
            }

            MessageType::Ping => {
                let pong = Frame::pong(frame.msg_id);
                conn.framed
                    .send(pong)
                    .await
                    .map_err(|e| PulseError::Connection(e.to_string()))?;
            }

            MessageType::Err => {
                if let Payload::Err(e) = &frame.payload {
                    return Err(PulseError::BrokerError {
                        code: e.code,
                        message: e.message.clone(),
                    });
                }
            }

            _ => {} // Ignore other frame types
        }
    }
}
