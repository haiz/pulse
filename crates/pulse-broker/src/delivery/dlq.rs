use pulse_protocol::MessageId;

/// A dead letter queue entry.
#[derive(Debug, Clone)]
pub struct DlqEntry {
    pub msg_id: MessageId,
    pub original_topic: String,
    pub consumer_id: String,
    pub payload_data: Vec<u8>,
    pub attempts: u32,
    pub first_error_at: u64,
    pub last_error: Option<String>,
}

/// Dead Letter Queue — stores events that exceeded max retries.
///
/// DLQ events are stored in sled and can be inspected, replayed, or purged
/// via the admin API.
pub struct DeadLetterQueue {
    db: sled::Tree,
}

impl DeadLetterQueue {
    pub fn new(db: &sled::Db) -> Result<Self, sled::Error> {
        let tree = db.open_tree("dlq")?;
        Ok(Self { db: tree })
    }

    /// Add an event to the DLQ.
    pub fn enqueue(&self, entry: &DlqEntry) -> Result<(), sled::Error> {
        let key = dlq_key(&entry.msg_id, &entry.consumer_id);
        let value = rmp_serde::to_vec_named(&DlqRecord {
            original_topic: &entry.original_topic,
            attempts: entry.attempts,
            first_error_at: entry.first_error_at,
            last_error: entry.last_error.as_deref(),
            payload_data: &entry.payload_data,
        })
        .unwrap_or_default();
        self.db.insert(key, value)?;
        Ok(())
    }

    /// Count of entries in the DLQ.
    pub fn count(&self) -> usize {
        self.db.len()
    }
}

fn dlq_key(msg_id: &MessageId, consumer_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(16 + consumer_id.len());
    key.extend_from_slice(msg_id.as_bytes());
    key.extend_from_slice(consumer_id.as_bytes());
    key
}

#[derive(serde::Serialize)]
struct DlqRecord<'a> {
    original_topic: &'a str,
    attempts: u32,
    first_error_at: u64,
    last_error: Option<&'a str>,
    #[serde(with = "serde_bytes")]
    payload_data: &'a [u8],
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enqueue_and_count() {
        let db = sled::Config::new().temporary(true).open().unwrap();
        let dlq = DeadLetterQueue::new(&db).unwrap();

        assert_eq!(dlq.count(), 0);

        dlq.enqueue(&DlqEntry {
            msg_id: MessageId::new(),
            original_topic: "order.created".into(),
            consumer_id: "payment-svc".into(),
            payload_data: vec![1, 2, 3],
            attempts: 5,
            first_error_at: 1700000000000,
            last_error: Some("timeout".into()),
        })
        .unwrap();

        assert_eq!(dlq.count(), 1);
    }
}
