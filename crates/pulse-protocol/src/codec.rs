use bytes::{Buf, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::frame::{Frame, FrameError};
use crate::types::{CRC_SIZE, DEFAULT_MAX_PAYLOAD_SIZE, HEADER_SIZE};

/// A tokio codec for framing Pulse protocol messages over TCP.
///
/// Handles frame boundaries so that upstream code works with complete `Frame` values.
pub struct PulseCodec {
    max_payload: u32,
}

impl PulseCodec {
    pub fn new() -> Self {
        Self {
            max_payload: DEFAULT_MAX_PAYLOAD_SIZE,
        }
    }

    pub fn with_max_payload(max_payload: u32) -> Self {
        Self { max_payload }
    }
}

impl Default for PulseCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for PulseCodec {
    type Item = Frame;
    type Error = FrameError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, FrameError> {
        // Need at least the fixed header to read payload_len
        if src.len() < HEADER_SIZE {
            return Ok(None);
        }

        // Read payload length from header (offset 21..25, big-endian u32)
        let payload_len = u32::from_be_bytes([src[21], src[22], src[23], src[24]]) as usize;

        let total_frame_size = HEADER_SIZE + payload_len + CRC_SIZE;

        // Wait for the complete frame
        if src.len() < total_frame_size {
            // Reserve capacity hint for the remaining bytes
            src.reserve(total_frame_size - src.len());
            return Ok(None);
        }

        // We have a complete frame — decode it
        let frame_bytes = &src[..total_frame_size];
        let frame = Frame::decode(frame_bytes, self.max_payload)?;

        // Advance the buffer past this frame
        src.advance(total_frame_size);

        Ok(Some(frame))
    }
}

impl Encoder<Frame> for PulseCodec {
    type Error = FrameError;

    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> Result<(), FrameError> {
        frame.encode_into(dst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_id::MessageId;
    use crate::types::*;

    #[test]
    fn codec_round_trip_single_frame() {
        let mut codec = PulseCodec::new();
        let frame = Frame::ping(MessageId::new());

        // Encode
        let mut buf = BytesMut::new();
        codec.encode(frame.clone(), &mut buf).unwrap();

        // Decode
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.msg_type, MessageType::Ping);
        assert!(buf.is_empty());
    }

    #[test]
    fn codec_handles_partial_data() {
        let mut codec = PulseCodec::new();
        let frame = Frame::ping(MessageId::new());

        let mut buf = BytesMut::new();
        codec.encode(frame, &mut buf).unwrap();

        // Split the buffer — give only partial data
        let full = buf.split();
        let mut partial = BytesMut::from(&full[..10]);

        // Should return None (incomplete)
        assert!(codec.decode(&mut partial).unwrap().is_none());

        // Now provide the rest
        partial.extend_from_slice(&full[10..]);
        let decoded = codec.decode(&mut partial).unwrap().unwrap();
        assert_eq!(decoded.msg_type, MessageType::Ping);
    }

    #[test]
    fn codec_multiple_frames() {
        let mut codec = PulseCodec::new();
        let mut buf = BytesMut::new();

        // Encode 3 frames
        codec
            .encode(Frame::ping(MessageId::new()), &mut buf)
            .unwrap();
        codec
            .encode(Frame::pong(MessageId::new()), &mut buf)
            .unwrap();
        codec
            .encode(
                Frame::err(
                    MessageId::new(),
                    ErrPayload {
                        code: 5000,
                        message: "test".into(),
                    },
                ),
                &mut buf,
            )
            .unwrap();

        // Decode all 3
        let f1 = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(f1.msg_type, MessageType::Ping);

        let f2 = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(f2.msg_type, MessageType::Pong);

        let f3 = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(f3.msg_type, MessageType::Err);

        // Buffer should be empty
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    // ─── Edge cases ───

    #[test]
    fn codec_empty_buffer_returns_none() {
        let mut codec = PulseCodec::new();
        let mut buf = BytesMut::new();
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn codec_single_byte_returns_none() {
        let mut codec = PulseCodec::new();
        let mut buf = BytesMut::from(&[0x50u8][..]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
        // Buffer untouched — still 1 byte
        assert_eq!(buf.len(), 1);
    }

    #[test]
    fn codec_header_minus_one_returns_none() {
        let mut codec = PulseCodec::new();
        let mut buf = BytesMut::from(&vec![0u8; HEADER_SIZE - 1][..]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn codec_byte_by_byte_feed() {
        let mut codec = PulseCodec::new();
        let frame = Frame::err(
            MessageId::new(),
            ErrPayload {
                code: 4010,
                message: "auth failed".into(),
            },
        );

        let full = frame.encode().unwrap();
        let mut buf = BytesMut::new();

        // Feed one byte at a time — all but the last should return None
        for (i, &byte) in full.iter().enumerate() {
            buf.extend_from_slice(&[byte]);
            let result = codec.decode(&mut buf).unwrap();
            if i < full.len() - 1 {
                assert!(result.is_none(), "decoded too early at byte {i}");
            } else {
                let decoded = result.unwrap();
                assert_eq!(decoded.msg_type, MessageType::Err);
            }
        }
    }

    #[test]
    fn codec_split_at_payload_len_boundary() {
        let mut codec = PulseCodec::new();
        let frame = Frame::sub(
            MessageId::new(),
            SubPayload {
                topic: "order.*".into(),
                group: None,
                filter: None,
                position: None,
                sub_id: "sub_1".into(),
            },
        );
        let full = frame.encode().unwrap();

        // Split exactly at offset 21 (middle of payload_len field)
        let mut buf = BytesMut::from(&full[..21]);
        assert!(codec.decode(&mut buf).unwrap().is_none());

        // Complete the rest
        buf.extend_from_slice(&full[21..]);
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.msg_type, MessageType::Sub);
    }

    #[test]
    fn codec_split_between_header_and_payload() {
        let mut codec = PulseCodec::new();
        let frame = Frame::err(
            MessageId::new(),
            ErrPayload {
                code: 5000,
                message: "test".into(),
            },
        );
        let full = frame.encode().unwrap();

        // Give exact header, no payload
        let mut buf = BytesMut::from(&full[..HEADER_SIZE]);
        assert!(codec.decode(&mut buf).unwrap().is_none());

        // Give one more byte (first payload byte)
        buf.extend_from_slice(&full[HEADER_SIZE..HEADER_SIZE + 1]);
        assert!(codec.decode(&mut buf).unwrap().is_none());

        // Give the rest
        buf.extend_from_slice(&full[HEADER_SIZE + 1..]);
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.msg_type, MessageType::Err);
    }

    #[test]
    fn codec_crc_error_propagated() {
        let mut codec = PulseCodec::new();
        let frame = Frame::ping(MessageId::new());
        let mut encoded = frame.encode().unwrap().to_vec();

        // Corrupt a message ID byte (CRC will mismatch)
        encoded[10] ^= 0xFF;

        let mut buf = BytesMut::from(&encoded[..]);
        let result = codec.decode(&mut buf);
        assert!(matches!(result, Err(FrameError::CrcMismatch { .. })));
    }

    #[test]
    fn codec_with_custom_max_payload() {
        let mut codec = PulseCodec::with_max_payload(64);

        // Create a frame with a payload that will exceed 64 bytes
        let frame = Frame::connect(
            MessageId::new(),
            ConnectPayload {
                service_id: "a]very-long-service-name-that-makes-payload-large".into(),
                namespace: "a-namespace-that-is-also-quite-long-for-testing".into(),
                timestamp: 1700000000,
                hmac: vec![0xAA; 32],
                client_ver: Some("pulse-sdk/0.1.0".into()),
                max_inflight: Some(100),
                codec: Some("msgpack".into()),
            },
        );

        let encoded = frame.encode().unwrap();
        let mut buf = BytesMut::from(&encoded[..]);
        let result = codec.decode(&mut buf);
        assert!(matches!(result, Err(FrameError::PayloadTooLarge { .. })));
    }

    #[test]
    fn codec_large_payload_frame() {
        let mut codec = PulseCodec::with_max_payload(MAX_PAYLOAD_SIZE);

        // A PUB frame with a ~10KB payload
        let large_data = "x".repeat(10_000);
        let frame = Frame::publish(
            MessageId::new(),
            PubPayload {
                topic: "test.large".into(),
                data: rmpv::Value::String(large_data.clone().into()),
                headers: std::collections::HashMap::new(),
                produced_at: None,
                delivery: None,
            },
        );

        let mut buf = BytesMut::new();
        codec.encode(frame, &mut buf).unwrap();

        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.msg_type, MessageType::Pub);
        if let Payload::Pub(p) = &decoded.payload {
            assert_eq!(p.topic, "test.large");
        } else {
            panic!("expected Pub payload");
        }
    }

    #[test]
    fn codec_trailing_data_preserved() {
        let mut codec = PulseCodec::new();
        let frame = Frame::ping(MessageId::new());

        let mut buf = BytesMut::new();
        codec.encode(frame, &mut buf).unwrap();

        // Append some trailing garbage
        buf.extend_from_slice(b"trailing");

        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.msg_type, MessageType::Ping);

        // Trailing data should remain in the buffer
        assert_eq!(&buf[..], b"trailing");
    }

    #[test]
    fn codec_two_frames_interleaved_with_partial() {
        let mut codec = PulseCodec::new();

        let frame1 = Frame::ping(MessageId::new());
        let frame2 = Frame::pong(MessageId::new());
        let enc1 = frame1.encode().unwrap();
        let enc2 = frame2.encode().unwrap();

        let mut buf = BytesMut::new();

        // Feed first frame + half of second
        buf.extend_from_slice(&enc1);
        buf.extend_from_slice(&enc2[..enc2.len() / 2]);

        // Decode first
        let d1 = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(d1.msg_type, MessageType::Ping);

        // Second is incomplete
        assert!(codec.decode(&mut buf).unwrap().is_none());

        // Feed remaining
        buf.extend_from_slice(&enc2[enc2.len() / 2..]);
        let d2 = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(d2.msg_type, MessageType::Pong);
    }

    #[test]
    fn codec_default_max_payload() {
        let codec = PulseCodec::new();
        assert_eq!(codec.max_payload, DEFAULT_MAX_PAYLOAD_SIZE);

        let codec2 = PulseCodec::default();
        assert_eq!(codec2.max_payload, DEFAULT_MAX_PAYLOAD_SIZE);
    }
}
