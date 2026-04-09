use std::sync::Arc;
use std::time::Instant;

use pulse_protocol::{AckStatus, DeliveryInfo, Frame, MessageId, PubPayload};

use crate::config::DeliveryConfig;
use crate::delivery::ack_tracker::{AckTracker, InflightEntry};
use crate::delivery::dlq::{DeadLetterQueue, DlqEntry};
use crate::delivery::retry::RetryScheduler;
use crate::routing::engine::SubscriptionTarget;

/// An event ready for delivery to a consumer.
#[derive(Debug, Clone)]
pub struct DeliveryEvent {
    pub msg_id: MessageId,
    pub topic: String,
    pub payload: PubPayload,
    pub attempt: u32,
}

/// Manages per-consumer delivery: routing events to the right consumers,
/// tracking ACKs, retrying on failure, and moving to DLQ.
pub struct DeliveryManager {
    ack_tracker: Arc<AckTracker>,
    retry_scheduler: RetryScheduler,
    dlq: Option<DeadLetterQueue>,
    max_redeliveries: u32,
}

impl DeliveryManager {
    pub fn new(config: &DeliveryConfig, dlq: Option<DeadLetterQueue>) -> Self {
        Self {
            ack_tracker: Arc::new(AckTracker::new(config.ack_timeout_secs)),
            retry_scheduler: RetryScheduler::new(config.backoff.clone()),
            dlq,
            max_redeliveries: config.max_redeliveries,
        }
    }

    /// Deliver an event to matched subscription targets.
    pub async fn deliver(&self, event: &DeliveryEvent, targets: &[&SubscriptionTarget]) {
        for target in targets {
            let delivery_frame = self.build_delivery_frame(event);

            // Track in-flight
            self.ack_tracker.track(InflightEntry {
                msg_id: event.msg_id,
                consumer_id: target.consumer_id.clone(),
                topic: event.topic.clone(),
                delivered_at: Instant::now(),
                attempt: event.attempt,
            });

            // Send to consumer's delivery channel
            if target.deliver_tx.send(delivery_frame).await.is_err() {
                // Consumer disconnected — will be cleaned up
                self.ack_tracker.ack(&event.msg_id, &target.consumer_id);
                tracing::debug!(
                    consumer = %target.consumer_id,
                    msg_id = %event.msg_id,
                    "delivery channel closed"
                );
            }
        }
    }

    /// Handle an ACK from a consumer.
    pub fn handle_ack(&self, msg_id: &MessageId, consumer_id: &str, status: &AckStatus) {
        match status {
            AckStatus::Done | AckStatus::Ok => {
                self.ack_tracker.ack(msg_id, consumer_id);
            }
            AckStatus::Rejected => {
                if let Some(entry) = self.ack_tracker.ack(msg_id, consumer_id) {
                    self.handle_nack(entry, Some("rejected by consumer"));
                }
            }
            _ => {
                self.ack_tracker.ack(msg_id, consumer_id);
            }
        }
    }

    /// Collect timed-out deliveries and handle them (retry or DLQ).
    pub fn process_timeouts(&self) {
        let timeouts = self.ack_tracker.collect_timeouts();
        for entry in timeouts {
            self.handle_nack(entry, Some("ack timeout"));
        }
    }

    /// Number of currently in-flight deliveries.
    pub fn inflight_count(&self) -> usize {
        self.ack_tracker.inflight_count()
    }

    fn handle_nack(&self, entry: InflightEntry, reason: Option<&str>) {
        let next_attempt = entry.attempt + 1;

        if self
            .retry_scheduler
            .should_dlq(next_attempt, self.max_redeliveries)
        {
            // Move to DLQ
            if let Some(dlq) = &self.dlq {
                let _ = dlq.enqueue(&DlqEntry {
                    msg_id: entry.msg_id,
                    original_topic: entry.topic.clone(),
                    consumer_id: entry.consumer_id.clone(),
                    payload_data: Vec::new(), // TODO: store actual payload
                    attempts: next_attempt,
                    first_error_at: 0, // TODO: track first error time
                    last_error: reason.map(|s| s.to_string()),
                });
            }
            tracing::warn!(
                msg_id = %entry.msg_id,
                consumer = %entry.consumer_id,
                topic = %entry.topic,
                attempts = next_attempt,
                "event moved to DLQ"
            );
        } else {
            let delay = self.retry_scheduler.next_delay(next_attempt);
            tracing::debug!(
                msg_id = %entry.msg_id,
                consumer = %entry.consumer_id,
                attempt = next_attempt,
                delay_ms = delay.as_millis(),
                "scheduling retry"
            );
            // TODO: schedule actual retry delivery after delay
        }
    }

    fn build_delivery_frame(&self, event: &DeliveryEvent) -> Frame {
        let mut payload = event.payload.clone();
        payload.delivery = Some(DeliveryInfo {
            attempt: event.attempt,
            first_sent: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            msg_id: event.msg_id.as_bytes().to_vec(),
        });
        Frame::publish(event.msg_id, payload)
    }
}
