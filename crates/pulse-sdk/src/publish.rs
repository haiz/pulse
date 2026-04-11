use std::time::Duration;

use futures::{SinkExt, StreamExt};

use pulse_protocol::*;

use crate::connection::Connection;
use crate::error::PulseError;
use crate::types::PublishOpts;

/// Publish an event to the broker.
///
/// Retries with the same message ID if the publish fails, ensuring
/// at-most-once delivery even on retries (broker deduplicates by msg_id).
pub async fn publish(
    conn: &mut Connection,
    topic: &str,
    data: rmpv::Value,
    opts: Option<PublishOpts>,
) -> Result<MessageId, PulseError> {
    let opts = opts.unwrap_or_default();
    let msg_id = opts.msg_id.unwrap_or_default();

    let pub_frame = Frame::publish(
        msg_id,
        PubPayload {
            topic: topic.to_string(),
            data,
            headers: opts.headers,
            produced_at: Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            ),
            delivery: None,
            raw_payload: None,
        },
    );

    conn.framed
        .send(pub_frame)
        .await
        .map_err(|e| PulseError::PublishFailed(e.to_string()))?;

    // Wait for ACK
    let response = tokio::time::timeout(Duration::from_secs(30), conn.framed.next())
        .await
        .map_err(|_| PulseError::Timeout)?
        .ok_or(PulseError::ChannelClosed)?
        .map_err(|e| PulseError::Protocol(e.to_string()))?;

    match &response.payload {
        Payload::Ack(ack) => match ack.status {
            AckStatus::Stored | AckStatus::Duplicate => Ok(msg_id),
            AckStatus::Rejected => Err(PulseError::PublishFailed("rejected".into())),
            _ => Ok(msg_id),
        },
        Payload::Err(e) => Err(PulseError::BrokerError {
            code: e.code,
            message: e.message.clone(),
        }),
        _ => Err(PulseError::Protocol("expected ACK".into())),
    }
}

/// Publish with automatic retry on failure (same msg_id preserved).
pub async fn publish_with_retry(
    conn: &mut Connection,
    topic: &str,
    data: rmpv::Value,
    opts: Option<PublishOpts>,
    max_retries: u32,
) -> Result<MessageId, PulseError> {
    let opts = opts.unwrap_or_default();
    let msg_id = opts.msg_id.unwrap_or_default();
    let retry_opts = PublishOpts {
        msg_id: Some(msg_id),
        ..opts
    };

    let mut last_err = None;
    for attempt in 0..=max_retries {
        match publish(conn, topic, data.clone(), Some(retry_opts.clone())).await {
            Ok(id) => return Ok(id),
            Err(e) => {
                if attempt < max_retries {
                    let delay = Duration::from_millis(100 * 2u64.pow(attempt));
                    tracing::debug!(attempt, error = %e, "publish retry");
                    tokio::time::sleep(delay).await;
                }
                last_err = Some(e);
            }
        }
    }

    Err(last_err.unwrap_or(PulseError::PublishFailed("unknown".into())))
}
