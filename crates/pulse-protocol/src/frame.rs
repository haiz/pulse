use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::crc;
use crate::message_id::MessageId;
use crate::types::*;

/// A decoded protocol frame.
#[derive(Debug, Clone)]
pub struct Frame {
    pub msg_type: MessageType,
    pub flags: Flags,
    pub msg_id: MessageId,
    pub payload: Payload,
}

/// Errors that can occur during frame encoding/decoding.
#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("invalid magic bytes")]
    InvalidMagic,
    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u8),
    #[error("unknown message type: 0x{0:02X}")]
    UnknownMessageType(u8),
    #[error("payload too large: {size} bytes (max: {max})")]
    PayloadTooLarge { size: u32, max: u32 },
    #[error("CRC mismatch: expected 0x{expected:08X}, got 0x{actual:08X}")]
    CrcMismatch { expected: u32, actual: u32 },
    #[error("incomplete frame: need {needed} more bytes")]
    Incomplete { needed: usize },
    #[error("serialization error: {0}")]
    Serialize(String),
    #[error("deserialization error: {0}")]
    Deserialize(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl Frame {
    // ─── Constructors ───

    pub fn connect(msg_id: MessageId, payload: ConnectPayload) -> Self {
        Self {
            msg_type: MessageType::Connect,
            flags: Flags::default(),
            msg_id,
            payload: Payload::Connect(payload),
        }
    }

    pub fn connack(msg_id: MessageId, payload: ConnAckPayload) -> Self {
        Self {
            msg_type: MessageType::ConnAck,
            flags: Flags::default(),
            msg_id,
            payload: Payload::ConnAck(payload),
        }
    }

    pub fn publish(msg_id: MessageId, payload: PubPayload) -> Self {
        Self {
            msg_type: MessageType::Pub,
            flags: Flags::default(),
            msg_id,
            payload: Payload::Pub(payload),
        }
    }

    pub fn ack(msg_id: MessageId, payload: AckPayload) -> Self {
        Self {
            msg_type: MessageType::Ack,
            flags: Flags::default(),
            msg_id,
            payload: Payload::Ack(payload),
        }
    }

    pub fn sub(msg_id: MessageId, payload: SubPayload) -> Self {
        Self {
            msg_type: MessageType::Sub,
            flags: Flags::default(),
            msg_id,
            payload: Payload::Sub(payload),
        }
    }

    pub fn unsub(msg_id: MessageId, payload: UnsubPayload) -> Self {
        Self {
            msg_type: MessageType::Unsub,
            flags: Flags::default(),
            msg_id,
            payload: Payload::Unsub(payload),
        }
    }

    pub fn ping(msg_id: MessageId) -> Self {
        Self {
            msg_type: MessageType::Ping,
            flags: Flags::default(),
            msg_id,
            payload: Payload::Ping,
        }
    }

    pub fn pong(msg_id: MessageId) -> Self {
        Self {
            msg_type: MessageType::Pong,
            flags: Flags::default(),
            msg_id,
            payload: Payload::Pong,
        }
    }

    pub fn flow(msg_id: MessageId, payload: FlowPayload) -> Self {
        Self {
            msg_type: MessageType::Flow,
            flags: Flags::default(),
            msg_id,
            payload: Payload::Flow(payload),
        }
    }

    pub fn err(msg_id: MessageId, payload: ErrPayload) -> Self {
        Self {
            msg_type: MessageType::Err,
            flags: Flags::default(),
            msg_id,
            payload: Payload::Err(payload),
        }
    }

    /// Set frame flags.
    pub fn with_flags(mut self, flags: Flags) -> Self {
        self.flags = flags;
        self
    }

    // ─── Encode ───

    /// Encode this frame into bytes.
    ///
    /// Layout: [magic 2][version 1][type 1][flags 1][msg_id 16][payload_len 4][payload N][crc 4]
    pub fn encode(&self) -> Result<Bytes, FrameError> {
        let payload_bytes = self.serialize_payload()?;
        let payload_len = payload_bytes.len() as u32;

        let total_size = HEADER_SIZE + payload_bytes.len() + CRC_SIZE;
        let mut buf = BytesMut::with_capacity(total_size);

        // Header
        buf.put_slice(&MAGIC);
        buf.put_u8(PROTOCOL_VERSION);
        buf.put_u8(self.msg_type as u8);
        buf.put_u8(self.flags.bits());
        buf.put_slice(self.msg_id.as_bytes());
        buf.put_u32(payload_len);

        // Payload
        buf.put_slice(&payload_bytes);

        // CRC over header + payload
        let checksum = crc::compute(&buf);
        buf.put_u32(checksum);

        Ok(buf.freeze())
    }

    /// Encode this frame directly into a caller-provided buffer.
    /// Avoids the intermediate BytesMut allocation of `encode()`.
    pub fn encode_into(&self, dst: &mut BytesMut) -> Result<(), FrameError> {
        let payload_bytes = self.serialize_payload()?;
        let payload_len = payload_bytes.len() as u32;
        let total_size = HEADER_SIZE + payload_bytes.len() + CRC_SIZE;

        dst.reserve(total_size);
        let crc_start = dst.len();

        // Header
        dst.put_slice(&MAGIC);
        dst.put_u8(PROTOCOL_VERSION);
        dst.put_u8(self.msg_type as u8);
        dst.put_u8(self.flags.bits());
        dst.put_slice(self.msg_id.as_bytes());
        dst.put_u32(payload_len);

        // Payload
        dst.put_slice(&payload_bytes);

        // CRC over header + payload just written
        let checksum = crc::compute(&dst[crc_start..]);
        dst.put_u32(checksum);

        Ok(())
    }

    // ─── Decode ───

    /// Decode a frame from a byte buffer.
    ///
    /// `max_payload` is the maximum allowed payload size (for validation).
    pub fn decode(data: &[u8], max_payload: u32) -> Result<Self, FrameError> {
        if data.len() < MIN_FRAME_SIZE {
            return Err(FrameError::Incomplete {
                needed: MIN_FRAME_SIZE - data.len(),
            });
        }

        let mut cursor: &[u8] = data;

        // Magic
        let magic = [cursor[0], cursor[1]];
        cursor.advance(2);
        if magic != MAGIC {
            return Err(FrameError::InvalidMagic);
        }

        // Version
        let version = cursor[0];
        cursor.advance(1);
        if version != PROTOCOL_VERSION {
            return Err(FrameError::UnsupportedVersion(version));
        }

        // Type
        let type_byte = cursor[0];
        cursor.advance(1);
        let msg_type =
            MessageType::from_u8(type_byte).ok_or(FrameError::UnknownMessageType(type_byte))?;

        // Flags
        let flags = Flags::new(cursor[0]);
        cursor.advance(1);

        // Message ID (16 bytes)
        let mut id_bytes = [0u8; 16];
        id_bytes.copy_from_slice(&cursor[..16]);
        let msg_id = MessageId::from_bytes(id_bytes);
        cursor.advance(16);

        // Payload length
        let payload_len = u32::from_be_bytes([cursor[0], cursor[1], cursor[2], cursor[3]]);
        cursor.advance(4);

        if payload_len > max_payload {
            return Err(FrameError::PayloadTooLarge {
                size: payload_len,
                max: max_payload,
            });
        }

        let total_expected = HEADER_SIZE + payload_len as usize + CRC_SIZE;
        if data.len() < total_expected {
            return Err(FrameError::Incomplete {
                needed: total_expected - data.len(),
            });
        }

        // CRC verification: over header + payload (everything before CRC)
        let crc_offset = HEADER_SIZE + payload_len as usize;
        let expected_crc = u32::from_be_bytes([
            data[crc_offset],
            data[crc_offset + 1],
            data[crc_offset + 2],
            data[crc_offset + 3],
        ]);
        let actual_crc = crc::compute(&data[..crc_offset]);
        if expected_crc != actual_crc {
            return Err(FrameError::CrcMismatch {
                expected: expected_crc,
                actual: actual_crc,
            });
        }

        // Payload bytes
        let payload_bytes = &cursor[..payload_len as usize];

        // Deserialize payload based on message type
        let payload = Self::deserialize_payload(msg_type, payload_bytes)?;

        Ok(Frame {
            msg_type,
            flags,
            msg_id,
            payload,
        })
    }

    /// Returns the total encoded size of a frame given the payload length.
    pub fn frame_size(payload_len: usize) -> usize {
        HEADER_SIZE + payload_len + CRC_SIZE
    }

    // ─── Internal ───

    fn serialize_payload(&self) -> Result<Vec<u8>, FrameError> {
        match &self.payload {
            Payload::Connect(p) => {
                rmp_serde::to_vec_named(p).map_err(|e| FrameError::Serialize(e.to_string()))
            }
            Payload::ConnAck(p) => {
                rmp_serde::to_vec_named(p).map_err(|e| FrameError::Serialize(e.to_string()))
            }
            Payload::Pub(p) => {
                rmp_serde::to_vec_named(p).map_err(|e| FrameError::Serialize(e.to_string()))
            }
            Payload::Ack(p) => {
                rmp_serde::to_vec_named(p).map_err(|e| FrameError::Serialize(e.to_string()))
            }
            Payload::Sub(p) => {
                rmp_serde::to_vec_named(p).map_err(|e| FrameError::Serialize(e.to_string()))
            }
            Payload::Unsub(p) => {
                rmp_serde::to_vec_named(p).map_err(|e| FrameError::Serialize(e.to_string()))
            }
            Payload::Ping | Payload::Pong => Ok(Vec::new()),
            Payload::Flow(p) => {
                rmp_serde::to_vec_named(p).map_err(|e| FrameError::Serialize(e.to_string()))
            }
            Payload::Err(p) => {
                rmp_serde::to_vec_named(p).map_err(|e| FrameError::Serialize(e.to_string()))
            }
        }
    }

    fn deserialize_payload(msg_type: MessageType, data: &[u8]) -> Result<Payload, FrameError> {
        match msg_type {
            MessageType::Connect => {
                let p: ConnectPayload = rmp_serde::from_slice(data)
                    .map_err(|e| FrameError::Deserialize(e.to_string()))?;
                Ok(Payload::Connect(p))
            }
            MessageType::ConnAck => {
                let p: ConnAckPayload = rmp_serde::from_slice(data)
                    .map_err(|e| FrameError::Deserialize(e.to_string()))?;
                Ok(Payload::ConnAck(p))
            }
            MessageType::Pub => {
                let p: PubPayload = rmp_serde::from_slice(data)
                    .map_err(|e| FrameError::Deserialize(e.to_string()))?;
                Ok(Payload::Pub(p))
            }
            MessageType::Ack => {
                let p: AckPayload = rmp_serde::from_slice(data)
                    .map_err(|e| FrameError::Deserialize(e.to_string()))?;
                Ok(Payload::Ack(p))
            }
            MessageType::Sub => {
                let p: SubPayload = rmp_serde::from_slice(data)
                    .map_err(|e| FrameError::Deserialize(e.to_string()))?;
                Ok(Payload::Sub(p))
            }
            MessageType::Unsub => {
                let p: UnsubPayload = rmp_serde::from_slice(data)
                    .map_err(|e| FrameError::Deserialize(e.to_string()))?;
                Ok(Payload::Unsub(p))
            }
            MessageType::Ping => Ok(Payload::Ping),
            MessageType::Pong => Ok(Payload::Pong),
            MessageType::Flow => {
                let p: FlowPayload = rmp_serde::from_slice(data)
                    .map_err(|e| FrameError::Deserialize(e.to_string()))?;
                Ok(Payload::Flow(p))
            }
            MessageType::Err => {
                let p: ErrPayload = rmp_serde::from_slice(data)
                    .map_err(|e| FrameError::Deserialize(e.to_string()))?;
                Ok(Payload::Err(p))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn round_trip(frame: Frame) -> Frame {
        let encoded = frame.encode().unwrap();
        Frame::decode(&encoded, MAX_PAYLOAD_SIZE).unwrap()
    }

    #[test]
    fn round_trip_connect() {
        let msg_id = MessageId::new();
        let frame = Frame::connect(
            msg_id,
            ConnectPayload {
                service_id: "order-service".into(),
                namespace: "ecommerce".into(),
                timestamp: 1700000000,
                hmac: vec![0xAA; 32],
                client_ver: Some("pulse-sdk-rust/0.1.0".into()),
                max_inflight: Some(10),
                codec: None,
            },
        );

        let decoded = round_trip(frame);
        assert_eq!(decoded.msg_type, MessageType::Connect);
        assert_eq!(decoded.msg_id, msg_id);
        if let Payload::Connect(p) = &decoded.payload {
            assert_eq!(p.service_id, "order-service");
            assert_eq!(p.namespace, "ecommerce");
            assert_eq!(p.timestamp, 1700000000);
            assert_eq!(p.hmac.len(), 32);
        } else {
            panic!("expected Connect payload");
        }
    }

    #[test]
    fn round_trip_connack() {
        let msg_id = MessageId::new();
        let frame = Frame::connack(
            msg_id,
            ConnAckPayload {
                status: "ok".into(),
                broker_id: "pulse-broker-01".into(),
                server_time: 1700000001,
                max_payload: 1048576,
                features: vec!["batch".into(), "compress".into()],
            },
        );

        let decoded = round_trip(frame);
        assert_eq!(decoded.msg_type, MessageType::ConnAck);
        if let Payload::ConnAck(p) = &decoded.payload {
            assert_eq!(p.status, "ok");
            assert_eq!(p.features.len(), 2);
        } else {
            panic!("expected ConnAck payload");
        }
    }

    #[test]
    fn round_trip_pub() {
        let msg_id = MessageId::new();
        let mut headers = HashMap::new();
        headers.insert("trace_id".into(), "abc123".into());

        let frame = Frame::publish(
            msg_id,
            PubPayload {
                topic: "order.created".into(),
                data: rmpv::Value::Map(vec![(
                    rmpv::Value::String("id".into()),
                    rmpv::Value::String("ord_123".into()),
                )]),
                headers,
                produced_at: Some(1700000000000),
                delivery: None,
            },
        );

        let decoded = round_trip(frame);
        assert_eq!(decoded.msg_type, MessageType::Pub);
        if let Payload::Pub(p) = &decoded.payload {
            assert_eq!(p.topic, "order.created");
            assert_eq!(p.headers.get("trace_id").unwrap(), "abc123");
        } else {
            panic!("expected Pub payload");
        }
    }

    #[test]
    fn round_trip_ack() {
        let msg_id = MessageId::new();
        let frame = Frame::ack(
            msg_id,
            AckPayload {
                status: AckStatus::Stored,
                msg_id: msg_id.as_bytes().to_vec(),
                reason: None,
            },
        );

        let decoded = round_trip(frame);
        assert_eq!(decoded.msg_type, MessageType::Ack);
        if let Payload::Ack(p) = &decoded.payload {
            assert_eq!(p.status, AckStatus::Stored);
        } else {
            panic!("expected Ack payload");
        }
    }

    #[test]
    fn round_trip_sub() {
        let msg_id = MessageId::new();
        let frame = Frame::sub(
            msg_id,
            SubPayload {
                topic: "order.*".into(),
                group: Some("payment-processors".into()),
                filter: Some("payload.amount > 1000".into()),
                position: Some("latest".into()),
                sub_id: "sub_001".into(),
            },
        );

        let decoded = round_trip(frame);
        assert_eq!(decoded.msg_type, MessageType::Sub);
        if let Payload::Sub(p) = &decoded.payload {
            assert_eq!(p.topic, "order.*");
            assert_eq!(p.sub_id, "sub_001");
            assert_eq!(p.group.as_deref(), Some("payment-processors"));
        } else {
            panic!("expected Sub payload");
        }
    }

    #[test]
    fn round_trip_unsub() {
        let msg_id = MessageId::new();
        let frame = Frame::unsub(
            msg_id,
            UnsubPayload {
                sub_id: "sub_001".into(),
            },
        );

        let decoded = round_trip(frame);
        assert_eq!(decoded.msg_type, MessageType::Unsub);
        if let Payload::Unsub(p) = &decoded.payload {
            assert_eq!(p.sub_id, "sub_001");
        } else {
            panic!("expected Unsub payload");
        }
    }

    #[test]
    fn round_trip_ping_pong() {
        let msg_id = MessageId::new();

        let ping = round_trip(Frame::ping(msg_id));
        assert_eq!(ping.msg_type, MessageType::Ping);
        assert_eq!(ping.msg_id, msg_id);

        let pong = round_trip(Frame::pong(msg_id));
        assert_eq!(pong.msg_type, MessageType::Pong);
        assert_eq!(pong.msg_id, msg_id);
    }

    #[test]
    fn round_trip_flow() {
        let msg_id = MessageId::new();
        let frame = Frame::flow(
            msg_id,
            FlowPayload {
                max_inflight: 5,
                sub_id: Some("sub_001".into()),
            },
        );

        let decoded = round_trip(frame);
        assert_eq!(decoded.msg_type, MessageType::Flow);
        if let Payload::Flow(p) = &decoded.payload {
            assert_eq!(p.max_inflight, 5);
        } else {
            panic!("expected Flow payload");
        }
    }

    #[test]
    fn round_trip_err() {
        let msg_id = MessageId::new();
        let frame = Frame::err(
            msg_id,
            ErrPayload {
                code: 4010,
                message: "authentication failed".into(),
            },
        );

        let decoded = round_trip(frame);
        assert_eq!(decoded.msg_type, MessageType::Err);
        if let Payload::Err(p) = &decoded.payload {
            assert_eq!(p.code, 4010);
            assert_eq!(p.message, "authentication failed");
        } else {
            panic!("expected Err payload");
        }
    }

    #[test]
    fn decode_invalid_magic() {
        let mut data = vec![0xFF, 0xFF]; // bad magic
        data.extend_from_slice(&[0; MIN_FRAME_SIZE - 2]);
        let result = Frame::decode(&data, MAX_PAYLOAD_SIZE);
        assert!(matches!(result, Err(FrameError::InvalidMagic)));
    }

    #[test]
    fn decode_crc_mismatch() {
        let frame = Frame::ping(MessageId::new());
        let mut encoded = frame.encode().unwrap().to_vec();
        // Corrupt a byte in the message ID area (offset 5..21) — CRC check still runs
        encoded[10] ^= 0xFF;
        let result = Frame::decode(&encoded, MAX_PAYLOAD_SIZE);
        assert!(matches!(result, Err(FrameError::CrcMismatch { .. })));
    }

    #[test]
    fn decode_payload_too_large() {
        let frame = Frame::ping(MessageId::new());
        let mut encoded = frame.encode().unwrap().to_vec();
        // Overwrite payload_len to a huge number (but keep CRC wrong — that's fine, payload size check first)
        // Actually payload_len is at offset 21..25
        encoded[21] = 0x01;
        encoded[22] = 0x00;
        encoded[23] = 0x00;
        encoded[24] = 0x01; // 16 MB + 1
        let result = Frame::decode(&encoded, DEFAULT_MAX_PAYLOAD_SIZE);
        assert!(matches!(result, Err(FrameError::PayloadTooLarge { .. })));
    }

    #[test]
    fn flags_preserved_in_round_trip() {
        let frame = Frame::ping(MessageId::new())
            .with_flags(Flags::new(Flags::COMPRESSED | Flags::PRIORITY));
        let decoded = round_trip(frame);
        assert!(decoded.flags.is_compressed());
        assert!(decoded.flags.is_priority());
        assert!(!decoded.flags.is_batch());
    }

    // ─── Error path tests ───

    #[test]
    fn decode_unsupported_version() {
        let frame = Frame::ping(MessageId::new());
        let mut encoded = frame.encode().unwrap().to_vec();
        // Version is at offset 2 — set to 0x02
        encoded[2] = 0x02;
        let result = Frame::decode(&encoded, MAX_PAYLOAD_SIZE);
        assert!(matches!(result, Err(FrameError::UnsupportedVersion(0x02))));
    }

    #[test]
    fn decode_unknown_message_type() {
        let frame = Frame::ping(MessageId::new());
        let mut encoded = frame.encode().unwrap().to_vec();
        // Type is at offset 3 — set to invalid
        encoded[3] = 0x0B;
        let result = Frame::decode(&encoded, MAX_PAYLOAD_SIZE);
        assert!(matches!(result, Err(FrameError::UnknownMessageType(0x0B))));
    }

    #[test]
    fn decode_unknown_message_type_zero() {
        let frame = Frame::ping(MessageId::new());
        let mut encoded = frame.encode().unwrap().to_vec();
        encoded[3] = 0x00;
        let result = Frame::decode(&encoded, MAX_PAYLOAD_SIZE);
        assert!(matches!(result, Err(FrameError::UnknownMessageType(0x00))));
    }

    #[test]
    fn decode_malformed_payload() {
        // Build a valid frame header for a PUB type, but put garbage in the payload.
        let msg_id = MessageId::new();
        let garbage_payload = b"this is not valid msgpack!@#$";
        let payload_len = garbage_payload.len() as u32;

        let mut buf = Vec::new();
        buf.extend_from_slice(&MAGIC);
        buf.push(PROTOCOL_VERSION);
        buf.push(MessageType::Pub as u8);
        buf.push(0); // flags
        buf.extend_from_slice(msg_id.as_bytes());
        buf.extend_from_slice(&payload_len.to_be_bytes());
        buf.extend_from_slice(garbage_payload);
        let checksum = crc::compute(&buf);
        buf.extend_from_slice(&checksum.to_be_bytes());

        let result = Frame::decode(&buf, MAX_PAYLOAD_SIZE);
        assert!(matches!(result, Err(FrameError::Deserialize(_))));
    }

    // ─── Incomplete frame edge cases ───

    #[test]
    fn decode_zero_bytes() {
        let result = Frame::decode(&[], MAX_PAYLOAD_SIZE);
        assert!(matches!(result, Err(FrameError::Incomplete { needed: 29 })));
    }

    #[test]
    fn decode_one_byte() {
        let result = Frame::decode(&[0x50], MAX_PAYLOAD_SIZE);
        assert!(matches!(result, Err(FrameError::Incomplete { needed: 28 })));
    }

    #[test]
    fn decode_header_minus_one() {
        let data = vec![0; HEADER_SIZE - 1]; // 24 bytes
        let result = Frame::decode(&data, MAX_PAYLOAD_SIZE);
        assert!(matches!(result, Err(FrameError::Incomplete { needed: 5 })));
        // MIN_FRAME_SIZE (29) - 24 = 5
    }

    #[test]
    fn decode_header_only_no_crc() {
        // 25 bytes: full header but no CRC (need 4 more for min frame)
        let mut data = vec![0; HEADER_SIZE];
        data[0] = MAGIC[0];
        data[1] = MAGIC[1];
        data[2] = PROTOCOL_VERSION;
        data[3] = MessageType::Ping as u8;
        // payload_len = 0, so total expected = 25 + 0 + 4 = 29
        let result = Frame::decode(&data, MAX_PAYLOAD_SIZE);
        assert!(matches!(result, Err(FrameError::Incomplete { needed: 4 })));
    }

    #[test]
    fn decode_incomplete_payload() {
        // Build a header claiming 100 bytes of payload, but only provide 10
        let msg_id = MessageId::new();
        let mut buf = Vec::new();
        buf.extend_from_slice(&MAGIC);
        buf.push(PROTOCOL_VERSION);
        buf.push(MessageType::Ping as u8);
        buf.push(0);
        buf.extend_from_slice(msg_id.as_bytes());
        buf.extend_from_slice(&100u32.to_be_bytes()); // claim 100 bytes payload
        buf.extend_from_slice(&[0u8; 10]); // only 10 bytes of "payload"

        let result = Frame::decode(&buf, MAX_PAYLOAD_SIZE);
        // total expected = 25 + 100 + 4 = 129, we have 25 + 10 = 35
        assert!(matches!(result, Err(FrameError::Incomplete { needed: 94 })));
    }

    // ─── Frame size and boundary tests ───

    #[test]
    fn frame_size_calculation() {
        assert_eq!(Frame::frame_size(0), MIN_FRAME_SIZE); // 25 + 0 + 4 = 29
        assert_eq!(Frame::frame_size(100), HEADER_SIZE + 100 + CRC_SIZE);
        assert_eq!(Frame::frame_size(1024), HEADER_SIZE + 1024 + CRC_SIZE);
    }

    #[test]
    fn ping_frame_is_minimum_size() {
        let frame = Frame::ping(MessageId::new());
        let encoded = frame.encode().unwrap();
        assert_eq!(encoded.len(), MIN_FRAME_SIZE); // 29 bytes
    }

    #[test]
    fn pong_frame_is_minimum_size() {
        let frame = Frame::pong(MessageId::new());
        let encoded = frame.encode().unwrap();
        assert_eq!(encoded.len(), MIN_FRAME_SIZE);
    }

    #[test]
    fn encoded_frame_starts_with_magic() {
        let frame = Frame::ping(MessageId::new());
        let encoded = frame.encode().unwrap();
        assert_eq!(encoded[0], 0x50);
        assert_eq!(encoded[1], 0x4C);
    }

    #[test]
    fn encoded_frame_has_correct_version() {
        let frame = Frame::ping(MessageId::new());
        let encoded = frame.encode().unwrap();
        assert_eq!(encoded[2], PROTOCOL_VERSION);
    }

    #[test]
    fn encoded_frame_has_correct_type_byte() {
        for (msg_type, expected) in [
            (MessageType::Connect, 0x01u8),
            (MessageType::Ping, 0x07),
            (MessageType::Err, 0x0A),
        ] {
            let frame = match msg_type {
                MessageType::Ping => Frame::ping(MessageId::new()),
                MessageType::Err => Frame::err(
                    MessageId::new(),
                    ErrPayload {
                        code: 0,
                        message: String::new(),
                    },
                ),
                MessageType::Connect => Frame::connect(
                    MessageId::new(),
                    ConnectPayload {
                        service_id: "s".into(),
                        namespace: "n".into(),
                        timestamp: 0,
                        hmac: vec![],
                        client_ver: None,
                        max_inflight: None,
                        codec: None,
                    },
                ),
                _ => unreachable!(),
            };
            let encoded = frame.encode().unwrap();
            assert_eq!(encoded[3], expected, "wrong type byte for {msg_type:?}");
        }
    }

    #[test]
    fn msg_id_preserved_in_encoding() {
        let msg_id = MessageId::new();
        let frame = Frame::ping(msg_id);
        let encoded = frame.encode().unwrap();
        // msg_id is at offset 5..21
        assert_eq!(&encoded[5..21], msg_id.as_bytes());
    }

    // ─── CRC corruption variants ───

    #[test]
    fn crc_detects_payload_corruption() {
        let frame = Frame::err(
            MessageId::new(),
            ErrPayload {
                code: 5000,
                message: "important".into(),
            },
        );
        let mut encoded = frame.encode().unwrap().to_vec();
        // Corrupt a byte in the payload area (after header)
        let payload_start = HEADER_SIZE;
        encoded[payload_start] ^= 0xFF;
        let result = Frame::decode(&encoded, MAX_PAYLOAD_SIZE);
        assert!(matches!(result, Err(FrameError::CrcMismatch { .. })));
    }

    #[test]
    fn crc_detects_crc_field_corruption() {
        let frame = Frame::ping(MessageId::new());
        let mut encoded = frame.encode().unwrap().to_vec();
        let crc_offset = encoded.len() - CRC_SIZE;
        // Flip one bit in the CRC itself
        encoded[crc_offset] ^= 0x01;
        let result = Frame::decode(&encoded, MAX_PAYLOAD_SIZE);
        assert!(matches!(result, Err(FrameError::CrcMismatch { .. })));
    }

    #[test]
    fn crc_detects_flags_corruption() {
        let frame = Frame::ping(MessageId::new());
        let mut encoded = frame.encode().unwrap().to_vec();
        // Flags at offset 4
        encoded[4] ^= 0x01;
        let result = Frame::decode(&encoded, MAX_PAYLOAD_SIZE);
        assert!(matches!(result, Err(FrameError::CrcMismatch { .. })));
    }

    // ─── Round-trip with different ACK statuses ───

    #[test]
    fn round_trip_ack_all_statuses() {
        for status in [
            AckStatus::Stored,
            AckStatus::Done,
            AckStatus::Rejected,
            AckStatus::Duplicate,
            AckStatus::Ok,
        ] {
            let msg_id = MessageId::new();
            let frame = Frame::ack(
                msg_id,
                AckPayload {
                    status: status.clone(),
                    msg_id: msg_id.as_bytes().to_vec(),
                    reason: if status == AckStatus::Rejected {
                        Some("test rejection".into())
                    } else {
                        None
                    },
                },
            );
            let decoded = round_trip(frame);
            if let Payload::Ack(p) = &decoded.payload {
                assert_eq!(p.status, status);
            } else {
                panic!("expected Ack payload for status {status:?}");
            }
        }
    }

    // ─── Round-trip with varied data shapes ───

    #[test]
    fn round_trip_pub_complex_data() {
        let data = rmpv::Value::Map(vec![
            (
                rmpv::Value::String("items".into()),
                rmpv::Value::Array(vec![
                    rmpv::Value::Integer(1.into()),
                    rmpv::Value::Integer(2.into()),
                    rmpv::Value::Integer(3.into()),
                ]),
            ),
            (
                rmpv::Value::String("nested".into()),
                rmpv::Value::Map(vec![(
                    rmpv::Value::String("key".into()),
                    rmpv::Value::Boolean(true),
                )]),
            ),
        ]);

        let frame = Frame::publish(
            MessageId::new(),
            PubPayload {
                topic: "test.complex".into(),
                data,
                headers: HashMap::new(),
                produced_at: None,
                delivery: None,
            },
        );

        let decoded = round_trip(frame);
        if let Payload::Pub(p) = &decoded.payload {
            assert_eq!(p.topic, "test.complex");
            // Verify nested structure survived
            if let rmpv::Value::Map(map) = &p.data {
                assert_eq!(map.len(), 2);
            } else {
                panic!("expected Map data");
            }
        } else {
            panic!("expected Pub payload");
        }
    }

    #[test]
    fn round_trip_pub_with_delivery_info() {
        let original_id = MessageId::new();
        let frame = Frame::publish(
            MessageId::new(),
            PubPayload {
                topic: "order.created".into(),
                data: rmpv::Value::Nil,
                headers: HashMap::new(),
                produced_at: None,
                delivery: Some(DeliveryInfo {
                    attempt: 3,
                    first_sent: 1700000000000,
                    msg_id: original_id.as_bytes().to_vec(),
                }),
            },
        );

        let decoded = round_trip(frame);
        if let Payload::Pub(p) = &decoded.payload {
            let d = p.delivery.as_ref().unwrap();
            assert_eq!(d.attempt, 3);
            assert_eq!(d.first_sent, 1700000000000);
            assert_eq!(d.msg_id, original_id.as_bytes().to_vec());
        } else {
            panic!("expected Pub payload");
        }
    }

    #[test]
    fn round_trip_connect_minimal() {
        let frame = Frame::connect(
            MessageId::new(),
            ConnectPayload {
                service_id: "s".into(),
                namespace: "n".into(),
                timestamp: 0,
                hmac: vec![],
                client_ver: None,
                max_inflight: None,
                codec: None,
            },
        );
        let decoded = round_trip(frame);
        if let Payload::Connect(p) = &decoded.payload {
            assert_eq!(p.service_id, "s");
            assert!(p.client_ver.is_none());
            assert!(p.max_inflight.is_none());
            assert!(p.codec.is_none());
        } else {
            panic!("expected Connect payload");
        }
    }

    // ─── FrameError Display ───

    #[test]
    fn frame_error_display() {
        let e = FrameError::InvalidMagic;
        assert_eq!(e.to_string(), "invalid magic bytes");

        let e = FrameError::UnsupportedVersion(99);
        assert!(e.to_string().contains("99"));

        let e = FrameError::CrcMismatch {
            expected: 0xAABBCCDD,
            actual: 0x11223344,
        };
        let s = e.to_string();
        assert!(s.contains("AABBCCDD"));
        assert!(s.contains("11223344"));

        let e = FrameError::Incomplete { needed: 42 };
        assert!(e.to_string().contains("42"));

        let e = FrameError::PayloadTooLarge {
            size: 2_000_000,
            max: 1_000_000,
        };
        let s = e.to_string();
        assert!(s.contains("2000000"));
        assert!(s.contains("1000000"));
    }

    #[test]
    fn encode_into_matches_encode() {
        use bytes::BytesMut;
        let frames = vec![
            Frame::ping(MessageId::new()),
            Frame::pong(MessageId::new()),
            Frame::publish(
                MessageId::new(),
                PubPayload {
                    topic: "test.topic".into(),
                    data: rmpv::Value::String("hello".into()),
                    headers: std::collections::HashMap::new(),
                    produced_at: None,
                    delivery: None,
                },
            ),
        ];

        for frame in frames {
            let encoded = frame.encode().unwrap();
            let mut buf = BytesMut::new();
            frame.encode_into(&mut buf).unwrap();
            assert_eq!(
                &encoded[..],
                &buf[..],
                "encode_into mismatch for {:?}",
                frame.msg_type
            );
        }
    }
}
