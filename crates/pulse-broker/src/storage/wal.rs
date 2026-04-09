use std::collections::HashSet;
use std::path::{Path, PathBuf};

use pulse_protocol::MessageId;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use crate::config::WalConfig;
use crate::error::BrokerError;

// ─── Constants ───

/// Segment header magic: "PLWL" (Pulse WAL).
pub const WAL_MAGIC: [u8; 4] = [0x50, 0x4C, 0x57, 0x4C];
pub const WAL_VERSION: u8 = 0x01;
pub const SEGMENT_HEADER_SIZE: u64 = 32;
/// Minimum record size: length(4) + type(1) + msg_id(16) + crc(4) = 25 (no data).
const RECORD_OVERHEAD: usize = 25;

// ─── Types ───

/// Position of a record within the WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalPosition {
    pub segment: u32,
    pub offset: u64,
}

/// Sync strategy after WAL writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    Fsync,
    Fdatasync,
    None,
}

impl SyncMode {
    pub fn parse(s: &str) -> Result<Self, BrokerError> {
        match s {
            "fsync" => Ok(Self::Fsync),
            "fdatasync" => Ok(Self::Fdatasync),
            "none" => Ok(Self::None),
            other => Err(BrokerError::Config(format!("unknown sync_mode: {other}"))),
        }
    }
}

/// Record types stored in WAL segments.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    EventWrite = 0x01,
    Completion = 0x02,
}

impl RecordType {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::EventWrite),
            0x02 => Some(Self::Completion),
            _ => None,
        }
    }
}

/// A record read back from the WAL.
#[derive(Debug, Clone)]
pub enum ReadRecord {
    EventWrite {
        msg_id: MessageId,
        data: Vec<u8>,
    },
    Completion {
        msg_id: MessageId,
        consumer_id: String,
    },
}

/// Result of replaying all WAL segments.
#[derive(Debug)]
pub struct WalReplayResult {
    pub event_ids: HashSet<MessageId>,
    pub last_segment: u32,
    pub record_count: u64,
}

// ─── Segment Header ───

fn encode_segment_header(segment_number: u32) -> [u8; 32] {
    let mut buf = [0u8; 32];
    buf[0..4].copy_from_slice(&WAL_MAGIC);
    buf[4] = WAL_VERSION;
    buf[5..9].copy_from_slice(&segment_number.to_be_bytes());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    buf[9..17].copy_from_slice(&now.to_be_bytes());
    // bytes 17..32 reserved (already zero)
    buf
}

fn validate_segment_header(buf: &[u8; 32]) -> Result<u32, BrokerError> {
    if buf[0..4] != WAL_MAGIC {
        return Err(BrokerError::Wal("invalid segment magic".into()));
    }
    if buf[4] != WAL_VERSION {
        return Err(BrokerError::Wal(format!(
            "unsupported WAL version: {}",
            buf[4]
        )));
    }
    let segment_number = u32::from_be_bytes([buf[5], buf[6], buf[7], buf[8]]);
    Ok(segment_number)
}

// ─── WalWriter ───

/// Appends records to the active WAL segment with configurable fsync.
pub struct WalWriter {
    wal_dir: PathBuf,
    active_file: tokio::fs::File,
    segment_number: u32,
    segment_offset: u64,
    segment_max_size: u64,
    sync_mode: SyncMode,
}

impl WalWriter {
    /// Open or create the WAL directory and position at the end of the latest segment.
    pub async fn open(wal_dir: PathBuf, config: &WalConfig) -> Result<Self, BrokerError> {
        let sync_mode = SyncMode::parse(&config.sync_mode)?;
        tokio::fs::create_dir_all(&wal_dir).await?;

        // Find existing segments
        let mut segments = list_segment_numbers(&wal_dir).await?;
        segments.sort();

        if let Some(&last_seg) = segments.last() {
            // Open existing segment and seek to end
            let path = segment_path(&wal_dir, last_seg);
            let mut file = tokio::fs::OpenOptions::new()
                .append(true)
                .read(true)
                .open(&path)
                .await?;
            let offset = file.seek(std::io::SeekFrom::End(0)).await?;

            Ok(Self {
                wal_dir,
                active_file: file,
                segment_number: last_seg,
                segment_offset: offset,
                segment_max_size: config.segment_size_bytes,
                sync_mode,
            })
        } else {
            // No segments — create the first one
            let seg_num = 1;
            let path = segment_path(&wal_dir, seg_num);
            let mut file = tokio::fs::File::create(&path).await?;
            let header = encode_segment_header(seg_num);
            file.write_all(&header).await?;
            file.flush().await?;

            Ok(Self {
                wal_dir,
                active_file: file,
                segment_number: seg_num,
                segment_offset: SEGMENT_HEADER_SIZE,
                segment_max_size: config.segment_size_bytes,
                sync_mode,
            })
        }
    }

    /// Append an EVENT_WRITE record. Returns the position after fsync.
    pub async fn append_event(
        &mut self,
        msg_id: MessageId,
        data: &[u8],
    ) -> Result<WalPosition, BrokerError> {
        self.write_record(RecordType::EventWrite, msg_id, data)
            .await
    }

    /// Append a COMPLETION record.
    pub async fn append_completion(
        &mut self,
        msg_id: MessageId,
        consumer_id: &str,
    ) -> Result<WalPosition, BrokerError> {
        self.write_record(RecordType::Completion, msg_id, consumer_id.as_bytes())
            .await
    }

    /// Return the current segment number.
    pub fn segment_number(&self) -> u32 {
        self.segment_number
    }

    /// Append an EVENT_WRITE record without fsync (for group commit).
    pub async fn append_event_no_sync(
        &mut self,
        msg_id: MessageId,
        data: &[u8],
    ) -> Result<WalPosition, BrokerError> {
        self.write_record_no_sync(RecordType::EventWrite, msg_id, data)
            .await
    }

    /// Explicitly sync the WAL file to disk. Used by group commit after batch writes.
    pub async fn sync(&mut self) -> Result<(), BrokerError> {
        self.sync_internal().await
    }

    async fn write_record_no_sync(
        &mut self,
        record_type: RecordType,
        msg_id: MessageId,
        data: &[u8],
    ) -> Result<WalPosition, BrokerError> {
        let record_len = RECORD_OVERHEAD + data.len();

        if self.segment_offset + record_len as u64 > self.segment_max_size {
            self.rotate_segment().await?;
        }

        let position = WalPosition {
            segment: self.segment_number,
            offset: self.segment_offset,
        };

        let mut buf = Vec::with_capacity(record_len);
        buf.extend_from_slice(&(record_len as u32).to_be_bytes());
        buf.push(record_type as u8);
        buf.extend_from_slice(msg_id.as_bytes());
        buf.extend_from_slice(data);

        let crc = pulse_protocol::crc::compute(&buf);
        buf.extend_from_slice(&crc.to_be_bytes());

        self.active_file.write_all(&buf).await?;
        self.segment_offset += record_len as u64;
        Ok(position)
    }

    async fn write_record(
        &mut self,
        record_type: RecordType,
        msg_id: MessageId,
        data: &[u8],
    ) -> Result<WalPosition, BrokerError> {
        let record_len = RECORD_OVERHEAD + data.len();

        // Check rotation
        if self.segment_offset + record_len as u64 > self.segment_max_size {
            self.rotate_segment().await?;
        }

        let position = WalPosition {
            segment: self.segment_number,
            offset: self.segment_offset,
        };

        // Build record bytes
        let mut buf = Vec::with_capacity(record_len);
        buf.extend_from_slice(&(record_len as u32).to_be_bytes()); // length
        buf.push(record_type as u8); // type
        buf.extend_from_slice(msg_id.as_bytes()); // msg_id
        buf.extend_from_slice(data); // data

        // CRC over everything so far (before appending CRC itself)
        let crc = pulse_protocol::crc::compute(&buf);
        buf.extend_from_slice(&crc.to_be_bytes());

        // Write + sync
        self.active_file.write_all(&buf).await?;
        self.sync_internal().await?;

        self.segment_offset += record_len as u64;
        Ok(position)
    }

    async fn rotate_segment(&mut self) -> Result<(), BrokerError> {
        // Sync current segment
        self.active_file.sync_all().await?;

        // Create new segment
        self.segment_number += 1;
        let path = segment_path(&self.wal_dir, self.segment_number);
        let mut file = tokio::fs::File::create(&path).await?;

        let header = encode_segment_header(self.segment_number);
        file.write_all(&header).await?;
        file.flush().await?;

        self.active_file = file;
        self.segment_offset = SEGMENT_HEADER_SIZE;

        Ok(())
    }

    async fn sync_internal(&mut self) -> Result<(), BrokerError> {
        match self.sync_mode {
            SyncMode::Fsync => self.active_file.sync_all().await?,
            SyncMode::Fdatasync => self.active_file.sync_data().await?,
            SyncMode::None => self.active_file.flush().await?,
        }
        Ok(())
    }
}

// ─── WalReader ───

/// Reads records sequentially from a single WAL segment.
pub struct WalReader {
    segment_number: u32,
    data: Vec<u8>,
    offset: usize,
}

impl WalReader {
    /// Open a segment file for reading, validate the header.
    pub async fn open(path: PathBuf) -> Result<Self, BrokerError> {
        let data = tokio::fs::read(&path).await?;

        if data.len() < SEGMENT_HEADER_SIZE as usize {
            return Err(BrokerError::Wal("segment file too small for header".into()));
        }

        let mut header = [0u8; 32];
        header.copy_from_slice(&data[..32]);
        let segment_number = validate_segment_header(&header)?;

        Ok(Self {
            segment_number,
            data,
            offset: SEGMENT_HEADER_SIZE as usize,
        })
    }

    /// Return the segment number.
    pub fn segment_number(&self) -> u32 {
        self.segment_number
    }

    /// Read the next record. Returns `None` at EOF.
    /// Returns `Err(WalCorrupt)` if CRC check fails.
    pub fn next_record(&mut self) -> Result<Option<ReadRecord>, BrokerError> {
        let remaining = self.data.len() - self.offset;

        // Need at least 4 bytes for record length
        if remaining < 4 {
            return Ok(None);
        }

        let record_len = u32::from_be_bytes([
            self.data[self.offset],
            self.data[self.offset + 1],
            self.data[self.offset + 2],
            self.data[self.offset + 3],
        ]) as usize;

        // Sanity check
        if record_len < RECORD_OVERHEAD {
            return Err(BrokerError::WalCorrupt {
                segment: self.segment_number,
                offset: self.offset as u64,
            });
        }

        // Check if we have the full record
        if remaining < record_len {
            // Truncated record at end of file (crash during write)
            return Ok(None);
        }

        let record_start = self.offset;
        let record_bytes = &self.data[record_start..record_start + record_len];

        // Verify CRC (last 4 bytes of record)
        let crc_offset = record_len - 4;
        let stored_crc = u32::from_be_bytes([
            record_bytes[crc_offset],
            record_bytes[crc_offset + 1],
            record_bytes[crc_offset + 2],
            record_bytes[crc_offset + 3],
        ]);
        let computed_crc = pulse_protocol::crc::compute(&record_bytes[..crc_offset]);

        if stored_crc != computed_crc {
            return Err(BrokerError::WalCorrupt {
                segment: self.segment_number,
                offset: self.offset as u64,
            });
        }

        // Parse record fields
        let type_byte = record_bytes[4];
        let record_type = RecordType::from_u8(type_byte).ok_or(BrokerError::WalCorrupt {
            segment: self.segment_number,
            offset: self.offset as u64,
        })?;

        let mut id_bytes = [0u8; 16];
        id_bytes.copy_from_slice(&record_bytes[5..21]);
        let msg_id = MessageId::from_bytes(id_bytes);

        let data = &record_bytes[21..crc_offset];

        self.offset += record_len;

        match record_type {
            RecordType::EventWrite => Ok(Some(ReadRecord::EventWrite {
                msg_id,
                data: data.to_vec(),
            })),
            RecordType::Completion => {
                let consumer_id = String::from_utf8(data.to_vec())
                    .map_err(|e| BrokerError::Wal(format!("invalid consumer_id UTF-8: {e}")))?;
                Ok(Some(ReadRecord::Completion {
                    msg_id,
                    consumer_id,
                }))
            }
        }
    }
}

// ─── Recovery ───

/// Replay all WAL segments in order, collecting event message IDs.
pub async fn replay_wal(wal_dir: &Path) -> Result<WalReplayResult, BrokerError> {
    let mut segments = list_segment_numbers(wal_dir).await?;
    segments.sort();

    let mut event_ids = HashSet::new();
    let mut last_segment = 0u32;
    let mut record_count = 0u64;

    for seg_num in &segments {
        let path = segment_path(wal_dir, *seg_num);
        let mut reader = WalReader::open(path).await?;
        last_segment = *seg_num;

        loop {
            match reader.next_record() {
                Ok(Some(ReadRecord::EventWrite { msg_id, .. })) => {
                    event_ids.insert(msg_id);
                    record_count += 1;
                }
                Ok(Some(ReadRecord::Completion { .. })) => {
                    record_count += 1;
                }
                Ok(None) => break,
                Err(BrokerError::WalCorrupt { segment, offset }) => {
                    tracing::warn!(
                        segment,
                        offset,
                        "corrupt record — stopping replay of this segment"
                    );
                    break;
                }
                Err(e) => return Err(e),
            }
        }
    }

    Ok(WalReplayResult {
        event_ids,
        last_segment,
        record_count,
    })
}

// ─── Helpers ───

fn segment_path(wal_dir: &Path, segment_number: u32) -> PathBuf {
    wal_dir.join(format!("segment-{segment_number:06}.wal"))
}

async fn list_segment_numbers(wal_dir: &Path) -> Result<Vec<u32>, BrokerError> {
    let mut result = Vec::new();

    if !wal_dir.exists() {
        return Ok(result);
    }

    let mut entries = tokio::fs::read_dir(wal_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        // Parse "segment-NNNNNN.wal"
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
    use crate::config::WalConfig;

    fn test_wal_config(segment_size: u64) -> WalConfig {
        WalConfig {
            segment_size_bytes: segment_size,
            sync_mode: "none".into(),
        }
    }

    #[tokio::test]
    async fn write_and_read_single_event() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config(64 * 1024 * 1024);

        let msg_id = MessageId::new();
        let data = b"hello world";

        // Write
        {
            let mut writer = WalWriter::open(wal_dir.clone(), &config).await.unwrap();
            let pos = writer.append_event(msg_id, data).await.unwrap();
            assert_eq!(pos.segment, 1);
            assert_eq!(pos.offset, SEGMENT_HEADER_SIZE);
        }

        // Read
        let path = segment_path(&wal_dir, 1);
        let mut reader = WalReader::open(path).await.unwrap();
        let record = reader.next_record().unwrap().unwrap();
        match record {
            ReadRecord::EventWrite {
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
    async fn write_and_read_multiple_events() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config(64 * 1024 * 1024);

        let ids: Vec<MessageId> = (0..100).map(|_| MessageId::new()).collect();

        {
            let mut writer = WalWriter::open(wal_dir.clone(), &config).await.unwrap();
            for (i, &id) in ids.iter().enumerate() {
                let data = format!("event-{i}");
                writer.append_event(id, data.as_bytes()).await.unwrap();
            }
        }

        let path = segment_path(&wal_dir, 1);
        let mut reader = WalReader::open(path).await.unwrap();
        for (i, &expected_id) in ids.iter().enumerate() {
            let record = reader.next_record().unwrap().unwrap();
            match record {
                ReadRecord::EventWrite { msg_id, data } => {
                    assert_eq!(msg_id, expected_id);
                    assert_eq!(data, format!("event-{i}").as_bytes());
                }
                _ => panic!("expected EventWrite at index {i}"),
            }
        }
        assert!(reader.next_record().unwrap().is_none());
    }

    #[tokio::test]
    async fn write_completion_record() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config(64 * 1024 * 1024);

        let msg_id = MessageId::new();

        {
            let mut writer = WalWriter::open(wal_dir.clone(), &config).await.unwrap();
            writer
                .append_completion(msg_id, "payment-service")
                .await
                .unwrap();
        }

        let path = segment_path(&wal_dir, 1);
        let mut reader = WalReader::open(path).await.unwrap();
        match reader.next_record().unwrap().unwrap() {
            ReadRecord::Completion {
                msg_id: read_id,
                consumer_id,
            } => {
                assert_eq!(read_id, msg_id);
                assert_eq!(consumer_id, "payment-service");
            }
            _ => panic!("expected Completion"),
        }
    }

    #[tokio::test]
    async fn mixed_event_and_completion() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config(64 * 1024 * 1024);

        let id1 = MessageId::new();
        let id2 = MessageId::new();

        {
            let mut writer = WalWriter::open(wal_dir.clone(), &config).await.unwrap();
            writer.append_event(id1, b"event-data").await.unwrap();
            writer.append_completion(id1, "consumer-a").await.unwrap();
            writer.append_event(id2, b"event-data-2").await.unwrap();
        }

        let path = segment_path(&wal_dir, 1);
        let mut reader = WalReader::open(path).await.unwrap();

        assert!(matches!(
            reader.next_record().unwrap().unwrap(),
            ReadRecord::EventWrite { .. }
        ));
        assert!(matches!(
            reader.next_record().unwrap().unwrap(),
            ReadRecord::Completion { .. }
        ));
        assert!(matches!(
            reader.next_record().unwrap().unwrap(),
            ReadRecord::EventWrite { .. }
        ));
        assert!(reader.next_record().unwrap().is_none());
    }

    #[tokio::test]
    async fn segment_header_format() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config(64 * 1024 * 1024);

        {
            let _writer = WalWriter::open(wal_dir.clone(), &config).await.unwrap();
        }

        let path = segment_path(&wal_dir, 1);
        let data = tokio::fs::read(&path).await.unwrap();
        assert!(data.len() >= 32);
        assert_eq!(&data[0..4], &WAL_MAGIC);
        assert_eq!(data[4], WAL_VERSION);
        let seg_num = u32::from_be_bytes([data[5], data[6], data[7], data[8]]);
        assert_eq!(seg_num, 1);
    }

    #[tokio::test]
    async fn segment_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        // Tiny segment: header(32) + a few records should trigger rotation
        let config = test_wal_config(200);

        {
            let mut writer = WalWriter::open(wal_dir.clone(), &config).await.unwrap();
            // Write enough events to force rotation
            for _ in 0..10 {
                writer
                    .append_event(MessageId::new(), b"some data that fills up the segment")
                    .await
                    .unwrap();
            }
            assert!(writer.segment_number() > 1, "expected segment rotation");
        }

        let segments = list_segment_numbers(&wal_dir).await.unwrap();
        assert!(segments.len() > 1, "expected multiple segment files");
    }

    #[tokio::test]
    async fn rotation_preserves_data() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config(200);

        let ids: Vec<MessageId> = (0..10).map(|_| MessageId::new()).collect();

        {
            let mut writer = WalWriter::open(wal_dir.clone(), &config).await.unwrap();
            for &id in &ids {
                writer.append_event(id, b"data").await.unwrap();
            }
        }

        let result = replay_wal(&wal_dir).await.unwrap();
        for &id in &ids {
            assert!(result.event_ids.contains(&id), "missing event {id}");
        }
        assert_eq!(result.event_ids.len(), 10);
    }

    #[tokio::test]
    async fn corrupt_record_detected() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config(64 * 1024 * 1024);

        {
            let mut writer = WalWriter::open(wal_dir.clone(), &config).await.unwrap();
            writer
                .append_event(MessageId::new(), b"good data")
                .await
                .unwrap();
        }

        // Corrupt a byte in the record (after the segment header)
        let path = segment_path(&wal_dir, 1);
        let mut data = tokio::fs::read(&path).await.unwrap();
        // Flip the last byte before CRC — guaranteed to be within the record
        // regardless of exact serialization size
        let corrupt_idx = data.len() - 5; // last data byte before 4-byte CRC
        data[corrupt_idx] ^= 0xFF;
        tokio::fs::write(&path, &data).await.unwrap();

        let mut reader = WalReader::open(path).await.unwrap();
        let result = reader.next_record();
        assert!(matches!(result, Err(BrokerError::WalCorrupt { .. })));
    }

    #[tokio::test]
    async fn truncated_record_at_eof() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config(64 * 1024 * 1024);

        {
            let mut writer = WalWriter::open(wal_dir.clone(), &config).await.unwrap();
            writer
                .append_event(MessageId::new(), b"good data")
                .await
                .unwrap();
        }

        // Append garbage bytes (simulating crash mid-write of next record)
        let path = segment_path(&wal_dir, 1);
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .unwrap();
        // Write a length field claiming 200 bytes, but only write 10 more
        file.write_all(&200u32.to_be_bytes()).await.unwrap();
        file.write_all(&[0xAA; 10]).await.unwrap();
        file.flush().await.unwrap();

        let mut reader = WalReader::open(path).await.unwrap();
        // First record should be fine
        assert!(reader.next_record().unwrap().is_some());
        // Second "record" is truncated — should return None
        assert!(reader.next_record().unwrap().is_none());
    }

    #[tokio::test]
    async fn replay_empty_wal() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        tokio::fs::create_dir_all(&wal_dir).await.unwrap();

        let result = replay_wal(&wal_dir).await.unwrap();
        assert!(result.event_ids.is_empty());
        assert_eq!(result.record_count, 0);
    }

    #[tokio::test]
    async fn replay_rebuilds_dedup_set() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config(64 * 1024 * 1024);

        let ids: Vec<MessageId> = (0..50).map(|_| MessageId::new()).collect();

        {
            let mut writer = WalWriter::open(wal_dir.clone(), &config).await.unwrap();
            for &id in &ids {
                writer.append_event(id, b"payload").await.unwrap();
            }
        }

        let result = replay_wal(&wal_dir).await.unwrap();
        assert_eq!(result.event_ids.len(), 50);
        assert_eq!(result.record_count, 50);
        for &id in &ids {
            assert!(result.event_ids.contains(&id));
        }
    }

    #[tokio::test]
    async fn record_crc_uses_protocol_crc() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config(64 * 1024 * 1024);

        {
            let mut writer = WalWriter::open(wal_dir.clone(), &config).await.unwrap();
            writer
                .append_event(MessageId::new(), b"test")
                .await
                .unwrap();
        }

        // Read raw bytes and verify CRC manually
        let path = segment_path(&wal_dir, 1);
        let data = tokio::fs::read(&path).await.unwrap();
        let record_start = SEGMENT_HEADER_SIZE as usize;
        let record_len = u32::from_be_bytes([
            data[record_start],
            data[record_start + 1],
            data[record_start + 2],
            data[record_start + 3],
        ]) as usize;

        let record = &data[record_start..record_start + record_len];
        let crc_offset = record_len - 4;
        let stored_crc = u32::from_be_bytes([
            record[crc_offset],
            record[crc_offset + 1],
            record[crc_offset + 2],
            record[crc_offset + 3],
        ]);
        let computed = pulse_protocol::crc::compute(&record[..crc_offset]);
        assert_eq!(stored_crc, computed);
    }

    #[tokio::test]
    async fn sync_mode_from_config() {
        assert_eq!(SyncMode::parse("fsync").unwrap(), SyncMode::Fsync);
        assert_eq!(SyncMode::parse("fdatasync").unwrap(), SyncMode::Fdatasync);
        assert_eq!(SyncMode::parse("none").unwrap(), SyncMode::None);
        assert!(SyncMode::parse("invalid").is_err());
    }

    #[tokio::test]
    async fn crash_mid_write_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config(64 * 1024 * 1024);

        let ids: Vec<MessageId> = (0..10).map(|_| MessageId::new()).collect();

        {
            let mut writer = WalWriter::open(wal_dir.clone(), &config).await.unwrap();
            for &id in &ids {
                writer.append_event(id, b"data").await.unwrap();
            }
        }

        // Simulate crash: append partial garbage
        let path = segment_path(&wal_dir, 1);
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .unwrap();
        file.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05])
            .await
            .unwrap();

        // Replay should recover all 10 clean events
        let result = replay_wal(&wal_dir).await.unwrap();
        assert_eq!(result.event_ids.len(), 10);
        for &id in &ids {
            assert!(result.event_ids.contains(&id));
        }
    }

    #[tokio::test]
    async fn wal_positions_monotonically_increase() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config(64 * 1024 * 1024);

        let mut writer = WalWriter::open(wal_dir, &config).await.unwrap();
        let mut prev_offset = 0u64;

        for _ in 0..20 {
            let pos = writer
                .append_event(MessageId::new(), b"test data")
                .await
                .unwrap();
            assert!(
                pos.offset > prev_offset || pos.segment > 1,
                "positions must increase"
            );
            prev_offset = pos.offset;
        }
    }

    #[tokio::test]
    async fn reopen_existing_wal_appends_correctly() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = test_wal_config(64 * 1024 * 1024);

        let id1 = MessageId::new();
        let id2 = MessageId::new();

        // First session
        {
            let mut writer = WalWriter::open(wal_dir.clone(), &config).await.unwrap();
            writer.append_event(id1, b"first").await.unwrap();
        }

        // Second session (reopen)
        {
            let mut writer = WalWriter::open(wal_dir.clone(), &config).await.unwrap();
            writer.append_event(id2, b"second").await.unwrap();
        }

        // Both events should be there
        let result = replay_wal(&wal_dir).await.unwrap();
        assert_eq!(result.event_ids.len(), 2);
        assert!(result.event_ids.contains(&id1));
        assert!(result.event_ids.contains(&id2));
    }
}
