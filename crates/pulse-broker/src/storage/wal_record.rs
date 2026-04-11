use pulse_protocol::MessageId;

/// Minimum record size: length(4) + type(1) + msg_id(16) + crc(4) = 25 (no data).
pub const RECORD_OVERHEAD: usize = 25;

/// Record types stored in WAL segments.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    EventWrite = 0x01,
    Completion = 0x02,
}

impl RecordType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::EventWrite),
            0x02 => Some(Self::Completion),
            _ => None,
        }
    }
}

/// Encode a WAL record into `buf`. Clears and reuses the buffer.
/// Layout: [length(4 BE)][type(1)][msg_id(16)][data(N)][crc32c(4 BE)]
/// Returns the total record length.
pub fn encode_record(
    buf: &mut Vec<u8>,
    record_type: RecordType,
    msg_id: MessageId,
    data: &[u8],
) -> usize {
    let record_len = RECORD_OVERHEAD + data.len();
    buf.clear();
    buf.reserve(record_len);
    buf.extend_from_slice(&(record_len as u32).to_be_bytes());
    buf.push(record_type as u8);
    buf.extend_from_slice(msg_id.as_bytes());
    buf.extend_from_slice(data);
    let crc = pulse_protocol::crc::compute(buf);
    buf.extend_from_slice(&crc.to_be_bytes());
    record_len
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_record_layout() {
        let msg_id = MessageId::new();
        let data = b"hello";
        let mut buf = Vec::new();

        let len = encode_record(&mut buf, RecordType::EventWrite, msg_id, data);

        // Total length = RECORD_OVERHEAD (25) + data (5) = 30
        assert_eq!(len, 30);
        // buf includes the trailing CRC (4 bytes), so total buf len = 30 + 4... no:
        // record_len is 30, buf contains length(4)+type(1)+id(16)+data(5)+crc(4) = 30+4? No.
        // Wait: record_len = RECORD_OVERHEAD + data.len() = 25 + 5 = 30.
        // RECORD_OVERHEAD already includes the 4-byte CRC, so buf = 30 bytes total.
        assert_eq!(buf.len(), 30);

        // Verify length field (first 4 bytes, big-endian)
        let stored_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        assert_eq!(stored_len, 30);

        // Verify type byte
        assert_eq!(buf[4], RecordType::EventWrite as u8);

        // Verify msg_id position (bytes 5..21)
        assert_eq!(&buf[5..21], msg_id.as_bytes());

        // Verify data position (bytes 21..26)
        assert_eq!(&buf[21..26], data);

        // Verify CRC validity (last 4 bytes)
        let crc_offset = buf.len() - 4;
        let stored_crc = u32::from_be_bytes([
            buf[crc_offset],
            buf[crc_offset + 1],
            buf[crc_offset + 2],
            buf[crc_offset + 3],
        ]);
        let computed_crc = pulse_protocol::crc::compute(&buf[..crc_offset]);
        assert_eq!(stored_crc, computed_crc);
    }

    #[test]
    fn encode_record_reuses_buffer() {
        let msg_id = MessageId::new();
        let data = b"some payload data that is fairly long to ensure good capacity";
        let mut buf = Vec::new();

        // First encode to establish capacity
        encode_record(&mut buf, RecordType::EventWrite, msg_id, data);
        let capacity_after_first = buf.capacity();

        // Second encode with same-or-smaller data should not grow capacity
        let msg_id2 = MessageId::new();
        encode_record(&mut buf, RecordType::Completion, msg_id2, b"short");
        assert_eq!(buf.capacity(), capacity_after_first);
    }
}
