use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Protocol magic bytes: "PL" (Pulse).
pub const MAGIC: [u8; 2] = [0x50, 0x4C];

/// Current protocol version.
pub const PROTOCOL_VERSION: u8 = 0x01;

/// Fixed header size in bytes (magic 2 + version 1 + type 1 + flags 1 + msg_id 16 + payload_len 4).
pub const HEADER_SIZE: usize = 25;

/// CRC32 trailer size in bytes.
pub const CRC_SIZE: usize = 4;

/// Minimum frame size: header + CRC (no payload).
pub const MIN_FRAME_SIZE: usize = HEADER_SIZE + CRC_SIZE;

/// Maximum payload size: 16 MB.
pub const MAX_PAYLOAD_SIZE: u32 = 16 * 1024 * 1024;

/// Default maximum payload size: 1 MB.
pub const DEFAULT_MAX_PAYLOAD_SIZE: u32 = 1024 * 1024;

// ─── Message Types ───

/// Wire message type identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MessageType {
    /// Client → Broker: initial authentication handshake.
    Connect = 0x01,
    /// Broker → Client: connection acknowledgement.
    ConnAck = 0x02,
    /// Client → Broker: publish an event. Also used Broker → Client for delivery.
    Pub = 0x03,
    /// Bidirectional: acknowledge a message (stored, done, rejected, duplicate).
    Ack = 0x04,
    /// Client → Broker: subscribe to a topic pattern.
    Sub = 0x05,
    /// Client → Broker: unsubscribe.
    Unsub = 0x06,
    /// Bidirectional: keepalive ping.
    Ping = 0x07,
    /// Bidirectional: keepalive pong.
    Pong = 0x08,
    /// Client → Broker: flow control signal.
    Flow = 0x09,
    /// Broker → Client: error response.
    Err = 0x0A,
}

impl MessageType {
    /// Parse a u8 into a MessageType.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x01 => Some(Self::Connect),
            0x02 => Some(Self::ConnAck),
            0x03 => Some(Self::Pub),
            0x04 => Some(Self::Ack),
            0x05 => Some(Self::Sub),
            0x06 => Some(Self::Unsub),
            0x07 => Some(Self::Ping),
            0x08 => Some(Self::Pong),
            0x09 => Some(Self::Flow),
            0x0A => Some(Self::Err),
            _ => None,
        }
    }
}

// ─── Flags ───

/// Frame flags bitfield (bits 0-3 defined, 4-7 reserved).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Flags(u8);

impl Flags {
    pub const COMPRESSED: u8 = 0b0000_0001;
    pub const BATCH: u8 = 0b0000_0010;
    pub const REPLY_TO: u8 = 0b0000_0100;
    pub const PRIORITY: u8 = 0b0000_1000;

    pub fn new(bits: u8) -> Self {
        Self(bits & 0x0F) // mask reserved bits
    }

    pub fn bits(self) -> u8 {
        self.0
    }

    pub fn is_compressed(self) -> bool {
        self.0 & Self::COMPRESSED != 0
    }

    pub fn is_batch(self) -> bool {
        self.0 & Self::BATCH != 0
    }

    pub fn has_reply_to(self) -> bool {
        self.0 & Self::REPLY_TO != 0
    }

    pub fn is_priority(self) -> bool {
        self.0 & Self::PRIORITY != 0
    }

    pub fn set_compressed(&mut self, val: bool) {
        if val {
            self.0 |= Self::COMPRESSED;
        } else {
            self.0 &= !Self::COMPRESSED;
        }
    }

    pub fn set_batch(&mut self, val: bool) {
        if val {
            self.0 |= Self::BATCH;
        } else {
            self.0 &= !Self::BATCH;
        }
    }
}

// ─── Payload Types ───

/// CONNECT payload (Client → Broker).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectPayload {
    pub service_id: String,
    pub namespace: String,
    pub timestamp: u64,
    #[serde(with = "serde_bytes")]
    pub hmac: Vec<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_ver: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_inflight: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
}

/// CONNACK payload (Broker → Client).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnAckPayload {
    pub status: String,
    pub broker_id: String,
    pub server_time: u64,
    pub max_payload: u32,
    #[serde(default)]
    pub features: Vec<String>,
}

/// PUB payload (Client → Broker, also Broker → Consumer for delivery).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubPayload {
    pub topic: String,
    pub data: rmpv::Value,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub headers: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub produced_at: Option<u64>,
    /// Added by broker on delivery to consumers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery: Option<DeliveryInfo>,
}

/// Delivery metadata appended by broker when delivering to consumers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryInfo {
    pub attempt: u32,
    pub first_sent: u64,
    #[serde(with = "serde_bytes")]
    pub msg_id: Vec<u8>,
}

/// ACK payload (bidirectional).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckPayload {
    pub status: AckStatus,
    #[serde(with = "serde_bytes")]
    pub msg_id: Vec<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// ACK status values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AckStatus {
    Stored,
    Done,
    Rejected,
    Duplicate,
    Ok,
}

/// SUB payload (Client → Broker).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubPayload {
    pub topic: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<String>,
    pub sub_id: String,
}

/// UNSUB payload (Client → Broker).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnsubPayload {
    pub sub_id: String,
}

/// FLOW payload (Client → Broker).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowPayload {
    pub max_inflight: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_id: Option<String>,
}

/// ERR payload (Broker → Client).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrPayload {
    pub code: u32,
    pub message: String,
}

// ─── Error Codes ───

/// Well-known broker error codes.
pub mod error_code {
    pub const BAD_REQUEST: u32 = 4000;
    pub const INVALID_CRC: u32 = 4001;
    pub const AUTH_FAILED: u32 = 4010;
    pub const FORBIDDEN: u32 = 4030;
    pub const NAMESPACE_NOT_FOUND: u32 = 4040;
    pub const PAYLOAD_TOO_LARGE: u32 = 4090;
    pub const RATE_LIMITED: u32 = 4290;
    pub const INTERNAL_ERROR: u32 = 5000;
    pub const SHUTTING_DOWN: u32 = 5030;
}

/// The typed payload of a protocol frame.
#[derive(Debug, Clone)]
pub enum Payload {
    Connect(ConnectPayload),
    ConnAck(ConnAckPayload),
    Pub(PubPayload),
    Ack(AckPayload),
    Sub(SubPayload),
    Unsub(UnsubPayload),
    Ping,
    Pong,
    Flow(FlowPayload),
    Err(ErrPayload),
}

impl Payload {
    /// Returns the corresponding MessageType for this payload variant.
    pub fn message_type(&self) -> MessageType {
        match self {
            Payload::Connect(_) => MessageType::Connect,
            Payload::ConnAck(_) => MessageType::ConnAck,
            Payload::Pub(_) => MessageType::Pub,
            Payload::Ack(_) => MessageType::Ack,
            Payload::Sub(_) => MessageType::Sub,
            Payload::Unsub(_) => MessageType::Unsub,
            Payload::Ping => MessageType::Ping,
            Payload::Pong => MessageType::Pong,
            Payload::Flow(_) => MessageType::Flow,
            Payload::Err(_) => MessageType::Err,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_type_round_trip() {
        for val in 0x01..=0x0Au8 {
            let mt = MessageType::from_u8(val).unwrap();
            assert_eq!(mt as u8, val);
        }
    }

    #[test]
    fn message_type_invalid() {
        assert!(MessageType::from_u8(0x00).is_none());
        assert!(MessageType::from_u8(0x0B).is_none());
        assert!(MessageType::from_u8(0xFF).is_none());
    }

    #[test]
    fn flags_operations() {
        let mut f = Flags::new(0);
        assert!(!f.is_compressed());

        f.set_compressed(true);
        assert!(f.is_compressed());
        assert!(!f.is_batch());

        f.set_batch(true);
        assert!(f.is_batch());
        assert_eq!(f.bits(), 0b0000_0011);
    }

    #[test]
    fn flags_masks_reserved_bits() {
        let f = Flags::new(0xFF);
        assert_eq!(f.bits(), 0x0F);
    }

    #[test]
    fn payload_message_type() {
        assert_eq!(Payload::Ping.message_type(), MessageType::Ping);
        assert_eq!(Payload::Pong.message_type(), MessageType::Pong);
    }

    #[test]
    fn payload_message_type_all_variants() {
        let connect = Payload::Connect(ConnectPayload {
            service_id: "svc".into(),
            namespace: "ns".into(),
            timestamp: 0,
            hmac: vec![],
            client_ver: None,
            max_inflight: None,
            codec: None,
        });
        assert_eq!(connect.message_type(), MessageType::Connect);

        let connack = Payload::ConnAck(ConnAckPayload {
            status: "ok".into(),
            broker_id: "b".into(),
            server_time: 0,
            max_payload: 0,
            features: vec![],
        });
        assert_eq!(connack.message_type(), MessageType::ConnAck);

        let pub_payload = Payload::Pub(PubPayload {
            topic: "t".into(),
            data: rmpv::Value::Nil,
            headers: HashMap::new(),
            produced_at: None,
            delivery: None,
        });
        assert_eq!(pub_payload.message_type(), MessageType::Pub);

        let ack = Payload::Ack(AckPayload {
            status: AckStatus::Ok,
            msg_id: vec![],
            reason: None,
        });
        assert_eq!(ack.message_type(), MessageType::Ack);

        let sub = Payload::Sub(SubPayload {
            topic: "t".into(),
            group: None,
            filter: None,
            position: None,
            sub_id: "s".into(),
        });
        assert_eq!(sub.message_type(), MessageType::Sub);

        let unsub = Payload::Unsub(UnsubPayload { sub_id: "s".into() });
        assert_eq!(unsub.message_type(), MessageType::Unsub);

        let flow = Payload::Flow(FlowPayload {
            max_inflight: 1,
            sub_id: None,
        });
        assert_eq!(flow.message_type(), MessageType::Flow);

        let err = Payload::Err(ErrPayload {
            code: 5000,
            message: "e".into(),
        });
        assert_eq!(err.message_type(), MessageType::Err);
    }

    // ─── AckStatus serde round-trip ───

    fn ack_status_round_trip(status: AckStatus) {
        let bytes = rmp_serde::to_vec_named(&status).unwrap();
        let decoded: AckStatus = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded, status);
    }

    #[test]
    fn ack_status_stored_serde() {
        ack_status_round_trip(AckStatus::Stored);
    }

    #[test]
    fn ack_status_done_serde() {
        ack_status_round_trip(AckStatus::Done);
    }

    #[test]
    fn ack_status_rejected_serde() {
        ack_status_round_trip(AckStatus::Rejected);
    }

    #[test]
    fn ack_status_duplicate_serde() {
        ack_status_round_trip(AckStatus::Duplicate);
    }

    #[test]
    fn ack_status_ok_serde() {
        ack_status_round_trip(AckStatus::Ok);
    }

    // ─── Payload serde round-trips ───

    #[test]
    fn connect_payload_minimal_options() {
        let payload = ConnectPayload {
            service_id: "svc".into(),
            namespace: "ns".into(),
            timestamp: 1700000000,
            hmac: vec![0xAA; 32],
            client_ver: None,
            max_inflight: None,
            codec: None,
        };
        let bytes = rmp_serde::to_vec_named(&payload).unwrap();
        let decoded: ConnectPayload = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.service_id, "svc");
        assert!(decoded.client_ver.is_none());
        assert!(decoded.max_inflight.is_none());
        assert!(decoded.codec.is_none());
    }

    #[test]
    fn connect_payload_all_options() {
        let payload = ConnectPayload {
            service_id: "svc".into(),
            namespace: "ns".into(),
            timestamp: 1700000000,
            hmac: vec![0xBB; 32],
            client_ver: Some("test/1.0".into()),
            max_inflight: Some(50),
            codec: Some("json".into()),
        };
        let bytes = rmp_serde::to_vec_named(&payload).unwrap();
        let decoded: ConnectPayload = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.client_ver.as_deref(), Some("test/1.0"));
        assert_eq!(decoded.max_inflight, Some(50));
        assert_eq!(decoded.codec.as_deref(), Some("json"));
    }

    #[test]
    fn pub_payload_empty_headers() {
        let payload = PubPayload {
            topic: "order.created".into(),
            data: rmpv::Value::String("test".into()),
            headers: HashMap::new(),
            produced_at: None,
            delivery: None,
        };
        let bytes = rmp_serde::to_vec_named(&payload).unwrap();
        let decoded: PubPayload = rmp_serde::from_slice(&bytes).unwrap();
        assert!(decoded.headers.is_empty());
        assert!(decoded.produced_at.is_none());
        assert!(decoded.delivery.is_none());
    }

    #[test]
    fn pub_payload_with_delivery_info() {
        let payload = PubPayload {
            topic: "order.created".into(),
            data: rmpv::Value::Integer(42.into()),
            headers: HashMap::from([("trace".into(), "abc".into())]),
            produced_at: Some(1700000000000),
            delivery: Some(DeliveryInfo {
                attempt: 3,
                first_sent: 1700000000000,
                msg_id: vec![0x01; 16],
            }),
        };
        let bytes = rmp_serde::to_vec_named(&payload).unwrap();
        let decoded: PubPayload = rmp_serde::from_slice(&bytes).unwrap();
        let delivery = decoded.delivery.unwrap();
        assert_eq!(delivery.attempt, 3);
        assert_eq!(delivery.msg_id.len(), 16);
    }

    #[test]
    fn sub_payload_minimal() {
        let payload = SubPayload {
            topic: "order.*".into(),
            group: None,
            filter: None,
            position: None,
            sub_id: "sub_001".into(),
        };
        let bytes = rmp_serde::to_vec_named(&payload).unwrap();
        let decoded: SubPayload = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.topic, "order.*");
        assert!(decoded.group.is_none());
        assert!(decoded.filter.is_none());
        assert!(decoded.position.is_none());
    }

    #[test]
    fn sub_payload_all_options() {
        let payload = SubPayload {
            topic: "order.>".into(),
            group: Some("processors".into()),
            filter: Some("payload.amount > 100".into()),
            position: Some("earliest".into()),
            sub_id: "sub_002".into(),
        };
        let bytes = rmp_serde::to_vec_named(&payload).unwrap();
        let decoded: SubPayload = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.group.as_deref(), Some("processors"));
        assert_eq!(decoded.filter.as_deref(), Some("payload.amount > 100"));
        assert_eq!(decoded.position.as_deref(), Some("earliest"));
    }

    #[test]
    fn flow_payload_without_sub_id() {
        let payload = FlowPayload {
            max_inflight: 10,
            sub_id: None,
        };
        let bytes = rmp_serde::to_vec_named(&payload).unwrap();
        let decoded: FlowPayload = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.max_inflight, 10);
        assert!(decoded.sub_id.is_none());
    }

    #[test]
    fn ack_payload_with_reason() {
        let payload = AckPayload {
            status: AckStatus::Rejected,
            msg_id: vec![0xFF; 16],
            reason: Some("payload_too_large".into()),
        };
        let bytes = rmp_serde::to_vec_named(&payload).unwrap();
        let decoded: AckPayload = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.status, AckStatus::Rejected);
        assert_eq!(decoded.reason.as_deref(), Some("payload_too_large"));
    }

    #[test]
    fn ack_payload_without_reason() {
        let payload = AckPayload {
            status: AckStatus::Done,
            msg_id: vec![0x01; 16],
            reason: None,
        };
        let bytes = rmp_serde::to_vec_named(&payload).unwrap();
        let decoded: AckPayload = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.status, AckStatus::Done);
        assert!(decoded.reason.is_none());
    }

    #[test]
    fn err_payload_all_error_codes() {
        let codes = [
            error_code::BAD_REQUEST,
            error_code::INVALID_CRC,
            error_code::AUTH_FAILED,
            error_code::FORBIDDEN,
            error_code::NAMESPACE_NOT_FOUND,
            error_code::PAYLOAD_TOO_LARGE,
            error_code::RATE_LIMITED,
            error_code::INTERNAL_ERROR,
            error_code::SHUTTING_DOWN,
        ];
        for code in codes {
            let payload = ErrPayload {
                code,
                message: format!("error {code}"),
            };
            let bytes = rmp_serde::to_vec_named(&payload).unwrap();
            let decoded: ErrPayload = rmp_serde::from_slice(&bytes).unwrap();
            assert_eq!(decoded.code, code);
        }
    }

    // ─── Flags edge cases ───

    #[test]
    fn flags_all_defined_bits() {
        let f = Flags::new(Flags::COMPRESSED | Flags::BATCH | Flags::REPLY_TO | Flags::PRIORITY);
        assert!(f.is_compressed());
        assert!(f.is_batch());
        assert!(f.has_reply_to());
        assert!(f.is_priority());
        assert_eq!(f.bits(), 0x0F);
    }

    #[test]
    fn flags_clear_individual() {
        let mut f = Flags::new(Flags::COMPRESSED | Flags::BATCH);
        assert!(f.is_compressed());
        assert!(f.is_batch());

        f.set_compressed(false);
        assert!(!f.is_compressed());
        assert!(f.is_batch()); // batch unaffected
    }

    #[test]
    fn flags_default_is_zero() {
        let f = Flags::default();
        assert_eq!(f.bits(), 0);
        assert!(!f.is_compressed());
        assert!(!f.is_batch());
        assert!(!f.has_reply_to());
        assert!(!f.is_priority());
    }

    // ─── MessageType exhaustive invalid range ───

    #[test]
    fn message_type_invalid_full_range() {
        // All invalid values in 0..=255
        for val in 0..=0xFFu8 {
            let result = MessageType::from_u8(val);
            if (0x01..=0x0A).contains(&val) {
                assert!(result.is_some(), "expected valid for 0x{val:02X}");
            } else {
                assert!(result.is_none(), "expected None for 0x{val:02X}");
            }
        }
    }

    // ─── Constants ───

    #[test]
    fn protocol_constants() {
        assert_eq!(MAGIC, [0x50, 0x4C]);
        assert_eq!(PROTOCOL_VERSION, 0x01);
        assert_eq!(HEADER_SIZE, 25);
        assert_eq!(CRC_SIZE, 4);
        assert_eq!(MIN_FRAME_SIZE, 29);
        assert_eq!(MAX_PAYLOAD_SIZE, 16 * 1024 * 1024);
        assert_eq!(DEFAULT_MAX_PAYLOAD_SIZE, 1024 * 1024);
    }
}
