use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crossbeam::channel::{self, Receiver, Sender};
use tokio::sync::oneshot;

use crate::config::WalConfig;
use crate::error::BrokerError;
use crate::storage::wal::{encode_segment_header, segment_path, SyncMode, WalPosition, SEGMENT_HEADER_SIZE};
use crate::storage::wal_record::{encode_record, RecordType, RECORD_OVERHEAD};
use pulse_protocol::MessageId;

// ─── Messages ───

pub struct WriteRequest {
    pub record_type: RecordType,
    pub msg_id: MessageId,
    pub data: Vec<u8>,
    pub reply: oneshot::Sender<Result<WalPosition, BrokerError>>,
}

pub struct SyncRequest {
    pub reply: oneshot::Sender<Result<(), BrokerError>>,
}

enum ThreadMessage {
    Write(WriteRequest),
    Sync(SyncRequest),
    Shutdown,
}

// ─── Handle ───

/// A cloneable handle that sends write requests to a dedicated OS writer thread.
#[derive(Clone)]
pub struct WalThreadHandle {
    tx: Sender<ThreadMessage>,
}

impl WalThreadHandle {
    /// Spawn a dedicated OS thread that owns the WAL file.
    ///
    /// All file I/O happens on that thread via `std::io::BufWriter`, avoiding
    /// the ~23µs overhead of `tokio::fs` (which uses `spawn_blocking` internally).
    pub fn spawn(
        wal_dir: PathBuf,
        config: &WalConfig,
        flush_interval: Duration,
        max_batch: usize,
    ) -> Result<Self, BrokerError> {
        let sync_mode = SyncMode::parse(&config.sync_mode)?;
        let segment_max_size = config.segment_size_bytes;

        // Synchronous I/O on the caller thread: create dir, find segments, open file.
        fs::create_dir_all(&wal_dir)?;

        let mut segments = list_segment_numbers_sync(&wal_dir)?;
        segments.sort();

        let (file, segment_number, segment_offset) = if let Some(&last_seg) = segments.last() {
            let path = segment_path(&wal_dir, last_seg);
            let file = fs::OpenOptions::new()
                .append(true)
                .read(true)
                .open(&path)?;
            let offset = file.metadata()?.len();
            (file, last_seg, offset)
        } else {
            let seg_num = 1;
            let path = segment_path(&wal_dir, seg_num);
            let mut file = fs::File::create(&path)?;
            let header = encode_segment_header(seg_num);
            file.write_all(&header)?;
            file.flush()?;
            (file, seg_num, SEGMENT_HEADER_SIZE)
        };

        let writer = std::io::BufWriter::with_capacity(256 * 1024, file);
        let (tx, rx) = channel::unbounded();

        let mut state = WriterState {
            writer,
            wal_dir,
            segment_number,
            segment_offset,
            segment_max_size,
            sync_mode,
            write_buf: Vec::with_capacity(8192),
        };

        std::thread::Builder::new()
            .name(format!("wal-writer-{}", state.wal_dir.display()))
            .spawn(move || {
                writer_thread_main(rx, &mut state, flush_interval, max_batch);
            })?;

        Ok(Self { tx })
    }

    /// Append an event record. Returns after the writer thread has flushed + synced the batch.
    pub async fn append_event(
        &self,
        msg_id: MessageId,
        data: Vec<u8>,
    ) -> Result<WalPosition, BrokerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ThreadMessage::Write(WriteRequest {
                record_type: RecordType::EventWrite,
                msg_id,
                data,
                reply: reply_tx,
            }))
            .map_err(|_| BrokerError::Wal("writer thread gone".into()))?;
        reply_rx
            .await
            .map_err(|_| BrokerError::Wal("writer thread dropped reply".into()))?
    }

    /// Append a completion record.
    pub async fn append_completion(
        &self,
        msg_id: MessageId,
        consumer_id: String,
    ) -> Result<WalPosition, BrokerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ThreadMessage::Write(WriteRequest {
                record_type: RecordType::Completion,
                msg_id,
                data: consumer_id.into_bytes(),
                reply: reply_tx,
            }))
            .map_err(|_| BrokerError::Wal("writer thread gone".into()))?;
        reply_rx
            .await
            .map_err(|_| BrokerError::Wal("writer thread dropped reply".into()))?
    }

    /// Explicitly sync the WAL to disk.
    pub async fn sync(&self) -> Result<(), BrokerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ThreadMessage::Sync(SyncRequest { reply: reply_tx }))
            .map_err(|_| BrokerError::Wal("writer thread gone".into()))?;
        reply_rx
            .await
            .map_err(|_| BrokerError::Wal("writer thread dropped reply".into()))?
    }

    /// Request a graceful shutdown of the writer thread.
    pub fn shutdown(&self) {
        let _ = self.tx.send(ThreadMessage::Shutdown);
    }
}

// ─── Writer Thread ───

struct PendingWrite {
    reply: oneshot::Sender<Result<WalPosition, BrokerError>>,
    position: WalPosition,
}

/// Mutable state owned by the writer thread.
struct WriterState {
    writer: std::io::BufWriter<fs::File>,
    wal_dir: PathBuf,
    segment_number: u32,
    segment_offset: u64,
    segment_max_size: u64,
    sync_mode: SyncMode,
    write_buf: Vec<u8>,
}

impl WriterState {
    /// Write a single record, handling segment rotation if needed.
    fn write_one(&mut self, req: &WriteRequest) -> Result<WalPosition, BrokerError> {
        let record_len = RECORD_OVERHEAD + req.data.len();

        // Check if we need to rotate.
        if self.segment_offset + record_len as u64 > self.segment_max_size {
            self.flush_and_sync()?;

            self.segment_number += 1;
            let path = segment_path(&self.wal_dir, self.segment_number);
            let mut file = fs::File::create(&path)?;
            let header = encode_segment_header(self.segment_number);
            file.write_all(&header)?;
            file.flush()?;

            self.writer = std::io::BufWriter::with_capacity(256 * 1024, file);
            self.segment_offset = SEGMENT_HEADER_SIZE;
        }

        let position = WalPosition {
            segment: self.segment_number,
            offset: self.segment_offset,
        };

        encode_record(&mut self.write_buf, req.record_type, req.msg_id, &req.data);
        self.writer.write_all(&self.write_buf)?;

        self.segment_offset += record_len as u64;
        Ok(position)
    }

    /// Flush the BufWriter and optionally sync to disk.
    fn flush_and_sync(&mut self) -> Result<(), BrokerError> {
        self.writer.flush()?;
        match self.sync_mode {
            SyncMode::Fsync => self.writer.get_ref().sync_all()?,
            SyncMode::Fdatasync => self.writer.get_ref().sync_data()?,
            SyncMode::None => {}
        }
        Ok(())
    }
}

fn writer_thread_main(
    rx: Receiver<ThreadMessage>,
    state: &mut WriterState,
    flush_interval: Duration,
    max_batch: usize,
) {
    let mut pending: Vec<PendingWrite> = Vec::with_capacity(max_batch);

    loop {
        // 1. Block waiting for the first message.
        let first = match rx.recv() {
            Ok(msg) => msg,
            Err(_) => break, // channel closed
        };

        let batch_start = Instant::now();

        match first {
            ThreadMessage::Shutdown => {
                let _ = state.flush_and_sync();
                break;
            }
            ThreadMessage::Sync(req) => {
                let result = state.flush_and_sync();
                let _ = req.reply.send(result);
                continue;
            }
            ThreadMessage::Write(req) => {
                match state.write_one(&req) {
                    Ok(pos) => {
                        pending.push(PendingWrite {
                            reply: req.reply,
                            position: pos,
                        });
                    }
                    Err(e) => {
                        let _ = req.reply.send(Err(e));
                    }
                }
            }
        }

        // 2. Drain all immediately-available messages (non-blocking).
        //    Under load this collects a full batch without any wait.
        //    Under light load we proceed to flush immediately.
        while pending.len() < max_batch {
            match rx.try_recv() {
                Ok(ThreadMessage::Write(req)) => {
                    match state.write_one(&req) {
                        Ok(pos) => {
                            pending.push(PendingWrite {
                                reply: req.reply,
                                position: pos,
                            });
                        }
                        Err(e) => {
                            let _ = req.reply.send(Err(e));
                        }
                    }
                }
                Ok(ThreadMessage::Sync(req)) => {
                    let flush_result = state.flush_and_sync();
                    for pw in pending.drain(..) {
                        let _ = pw.reply.send(Ok(pw.position));
                    }
                    let _ = req.reply.send(flush_result);
                }
                Ok(ThreadMessage::Shutdown) => {
                    let _ = state.flush_and_sync();
                    for pw in pending.drain(..) {
                        let _ = pw.reply.send(Ok(pw.position));
                    }
                    return;
                }
                Err(_) => break, // empty or disconnected — flush what we have
            }
        }

        // 3. Flush + sync the batch.
        let flush_result = state.flush_and_sync();

        // 4. Notify all pending callers.
        for pw in pending.drain(..) {
            match &flush_result {
                Ok(()) => {
                    let _ = pw.reply.send(Ok(pw.position));
                }
                Err(e) => {
                    let _ = pw
                        .reply
                        .send(Err(BrokerError::Wal(format!("flush error: {e}"))));
                }
            }
        }
    }
}

/// Synchronous directory scan for segment files (avoids async runtime dependency).
fn list_segment_numbers_sync(wal_dir: &Path) -> Result<Vec<u32>, BrokerError> {
    let mut result = Vec::new();

    if !wal_dir.exists() {
        return Ok(result);
    }

    for entry in fs::read_dir(wal_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(num_str) = name
            .strip_prefix("segment-")
            .and_then(|s| s.strip_suffix(".wal"))
        {
            if let Ok(num) = num_str.parse::<u32>() {
                result.push(num);
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::wal::{replay_wal, segment_path, WalReader, SEGMENT_HEADER_SIZE};

    fn test_wal_config(segment_size: u64) -> WalConfig {
        WalConfig {
            segment_size_bytes: segment_size,
            sync_mode: "none".into(),
            shards: 1,
        }
    }

    #[tokio::test]
    async fn writer_thread_write_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config(64 * 1024 * 1024);

        let msg_id = MessageId::new();
        let data = b"hello world".to_vec();

        let handle = WalThreadHandle::spawn(
            wal_dir.clone(),
            &config,
            Duration::from_millis(5),
            100,
        )
        .unwrap();

        let pos = handle.append_event(msg_id, data.clone()).await.unwrap();
        assert_eq!(pos.segment, 1);
        assert_eq!(pos.offset, SEGMENT_HEADER_SIZE);

        handle.sync().await.unwrap();
        handle.shutdown();

        // Small delay to let the thread exit cleanly.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Read back.
        let path = segment_path(&wal_dir, 1);
        let mut reader = WalReader::open(path).await.unwrap();
        let record = reader.next_record().unwrap().unwrap();
        match record {
            crate::storage::wal::ReadRecord::EventWrite {
                msg_id: read_id,
                data: read_data,
            } => {
                assert_eq!(read_id, msg_id);
                assert_eq!(read_data, data);
            }
            _ => panic!("expected EventWrite"),
        }
        assert!(reader.next_record().unwrap().is_none());
    }

    #[tokio::test]
    async fn writer_thread_batch_writes() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config(64 * 1024 * 1024);

        let handle = WalThreadHandle::spawn(
            wal_dir.clone(),
            &config,
            Duration::from_millis(5),
            100,
        )
        .unwrap();

        let ids: Vec<MessageId> = (0..50).map(|_| MessageId::new()).collect();

        for (i, &id) in ids.iter().enumerate() {
            let data = format!("event-{i}");
            handle.append_event(id, data.into_bytes()).await.unwrap();
        }

        handle.sync().await.unwrap();
        handle.shutdown();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let result = replay_wal(&wal_dir).await.unwrap();
        assert_eq!(result.record_count, 50);
        for &id in &ids {
            assert!(result.event_ids.contains(&id), "missing event {id}");
        }
    }

    #[tokio::test]
    async fn writer_thread_concurrent_writers() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config(64 * 1024 * 1024);

        let handle = WalThreadHandle::spawn(
            wal_dir.clone(),
            &config,
            Duration::from_millis(5),
            100,
        )
        .unwrap();

        let mut tasks = Vec::new();
        let mut all_ids = Vec::new();

        for _ in 0..20 {
            let h = handle.clone();
            let id = MessageId::new();
            all_ids.push(id);
            tasks.push(tokio::spawn(async move {
                h.append_event(id, b"concurrent-data".to_vec())
                    .await
                    .unwrap();
            }));
        }

        futures::future::join_all(tasks).await;

        handle.sync().await.unwrap();
        handle.shutdown();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let result = replay_wal(&wal_dir).await.unwrap();
        assert_eq!(result.record_count, 20);
        for &id in &all_ids {
            assert!(result.event_ids.contains(&id), "missing event {id}");
        }
    }

    #[tokio::test]
    async fn writer_thread_segment_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = WalConfig {
            segment_size_bytes: 200,
            sync_mode: "none".into(),
            shards: 1,
        };

        let handle = WalThreadHandle::spawn(
            wal_dir.clone(),
            &config,
            Duration::from_millis(5),
            100,
        )
        .unwrap();

        let ids: Vec<MessageId> = (0..10).map(|_| MessageId::new()).collect();

        for &id in &ids {
            handle
                .append_event(id, b"some data that fills up the segment".to_vec())
                .await
                .unwrap();
        }

        handle.sync().await.unwrap();
        handle.shutdown();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Verify multiple segments were created.
        let segments = list_segment_numbers_sync(&wal_dir).unwrap();
        assert!(
            segments.len() > 1,
            "expected multiple segments, got {}",
            segments.len()
        );

        // Verify all data is readable.
        let result = replay_wal(&wal_dir).await.unwrap();
        assert_eq!(result.event_ids.len(), 10);
        for &id in &ids {
            assert!(result.event_ids.contains(&id), "missing event {id}");
        }
    }
}
