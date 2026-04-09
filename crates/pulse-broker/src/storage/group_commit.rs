use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use pulse_protocol::MessageId;

use crate::error::BrokerError;
use crate::storage::wal::{WalPosition, WalWriter};

/// A request to write an event to the WAL via group commit.
pub struct GroupCommitRequest {
    pub msg_id: MessageId,
    pub data: Vec<u8>,
    pub reply: oneshot::Sender<Result<WalPosition, BrokerError>>,
}

/// Handle for submitting writes to the group commit writer.
#[derive(Clone)]
pub struct GroupCommitHandle {
    tx: mpsc::Sender<GroupCommitRequest>,
}

impl GroupCommitHandle {
    /// Submit a write request and wait for the result.
    pub async fn append(
        &self,
        msg_id: MessageId,
        data: Vec<u8>,
    ) -> Result<WalPosition, BrokerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(GroupCommitRequest {
                msg_id,
                data,
                reply: reply_tx,
            })
            .await
            .map_err(|_| BrokerError::Wal("group commit writer closed".into()))?;

        reply_rx
            .await
            .map_err(|_| BrokerError::Wal("group commit reply dropped".into()))?
    }
}

/// Batches WAL writes and issues a single fsync per batch.
///
/// Collects writes for up to `flush_interval` (default 5ms) or until
/// `max_batch` events accumulate, whichever comes first. Then writes
/// all records sequentially and fsyncs once.
pub struct GroupCommitWriter {
    wal: WalWriter,
    rx: mpsc::Receiver<GroupCommitRequest>,
    flush_interval: Duration,
    max_batch: usize,
}

impl GroupCommitWriter {
    /// Spawn the group commit writer as a background task.
    /// Returns a handle for submitting write requests.
    pub fn spawn(wal: WalWriter, flush_interval_ms: u64, max_batch: usize) -> GroupCommitHandle {
        let (tx, rx) = mpsc::channel(max_batch * 4);

        let writer = Self {
            wal,
            rx,
            flush_interval: Duration::from_millis(flush_interval_ms),
            max_batch,
        };

        tokio::spawn(writer.run());

        GroupCommitHandle { tx }
    }

    async fn run(mut self) {
        let mut pending: Vec<GroupCommitRequest> = Vec::with_capacity(self.max_batch);

        loop {
            // Wait for first request or shutdown
            match self.rx.recv().await {
                Some(req) => pending.push(req),
                None => break, // Channel closed, shutdown
            }

            // Collect more requests until flush interval expires or batch is full
            let deadline = tokio::time::sleep(self.flush_interval);
            tokio::pin!(deadline);

            loop {
                if pending.len() >= self.max_batch {
                    break;
                }

                tokio::select! {
                    biased;
                    req = self.rx.recv() => {
                        match req {
                            Some(r) => pending.push(r),
                            None => break,
                        }
                    }
                    _ = &mut deadline => break,
                }
            }

            // Flush the batch
            self.flush_batch(&mut pending).await;
        }

        // Flush any remaining
        if !pending.is_empty() {
            self.flush_batch(&mut pending).await;
        }

        tracing::debug!("group commit writer shutdown");
    }

    async fn flush_batch(&mut self, pending: &mut Vec<GroupCommitRequest>) {
        let batch_size = pending.len();
        let mut results: Vec<Result<WalPosition, BrokerError>> = Vec::with_capacity(batch_size);

        // Write all records without fsync
        for req in pending.iter() {
            match self.wal.append_event_no_sync(req.msg_id, &req.data).await {
                Ok(pos) => results.push(Ok(pos)),
                Err(e) => {
                    // If any write fails, fail remaining too
                    results.push(Err(e));
                    for _ in results.len()..batch_size {
                        results.push(Err(BrokerError::Wal("batch aborted".into())));
                    }
                    break;
                }
            }
        }

        // Single fsync for the entire batch
        if results.iter().any(|r| r.is_ok()) {
            if let Err(e) = self.wal.sync().await {
                // fsync failed — all writes in this batch are unreliable
                tracing::error!(error = %e, "group commit fsync failed");
                results = pending
                    .iter()
                    .map(|_| Err(BrokerError::Wal(format!("fsync failed: {e}"))))
                    .collect();
            }
        }

        // Notify all waiters
        for (req, result) in pending.drain(..).zip(results) {
            let _ = req.reply.send(result);
        }

        if batch_size > 1 {
            tracing::trace!(batch_size, "group commit flush");
        }
    }
}
