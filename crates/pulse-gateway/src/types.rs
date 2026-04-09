use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// ─── REST Request/Response Types ───

#[derive(Debug, Deserialize)]
pub struct PublishRequest {
    pub topic: String,
    pub data: serde_json::Value,
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
pub struct PublishResponse {
    pub msg_id: String,
    pub status: String,
}

#[derive(Debug, Deserialize)]
pub struct BatchPublishRequest {
    pub events: Vec<PublishRequest>,
}

#[derive(Debug, Serialize)]
pub struct BatchPublishResponse {
    pub results: Vec<PublishResponse>,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: u32,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct InfoResponse {
    pub version: String,
    pub broker_id: String,
    pub gateway_mode: String,
}

#[derive(Debug, Serialize)]
pub struct TopicInfo {
    pub name: String,
}

#[derive(Debug, Serialize)]
pub struct TopicsResponse {
    pub topics: Vec<TopicInfo>,
}

// ─── WebSocket Message Types ───

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum WsClientMessage {
    Sub {
        topic: String,
        sub_id: String,
        #[serde(default)]
        group: Option<String>,
        #[serde(default)]
        filter: Option<String>,
    },
    Unsub {
        sub_id: String,
    },
    Ack {
        msg_id: String,
    },
    Ping,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum WsServerMessage {
    Event {
        msg_id: String,
        topic: String,
        data: serde_json::Value,
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        headers: HashMap<String, String>,
        attempt: u32,
    },
    Subscribed {
        sub_id: String,
        topic: String,
    },
    Error {
        code: u32,
        message: String,
    },
    Pong,
}

// ─── JSON ↔ rmpv conversion ───

pub fn json_to_rmpv(val: &serde_json::Value) -> rmpv::Value {
    match val {
        serde_json::Value::Null => rmpv::Value::Nil,
        serde_json::Value::Bool(b) => rmpv::Value::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                rmpv::Value::Integer(i.into())
            } else if let Some(f) = n.as_f64() {
                rmpv::Value::F64(f)
            } else {
                rmpv::Value::Nil
            }
        }
        serde_json::Value::String(s) => rmpv::Value::String(s.clone().into()),
        serde_json::Value::Array(arr) => rmpv::Value::Array(arr.iter().map(json_to_rmpv).collect()),
        serde_json::Value::Object(obj) => rmpv::Value::Map(
            obj.iter()
                .map(|(k, v)| (rmpv::Value::String(k.clone().into()), json_to_rmpv(v)))
                .collect(),
        ),
    }
}

pub fn rmpv_to_json(val: &rmpv::Value) -> serde_json::Value {
    match val {
        rmpv::Value::Nil => serde_json::Value::Null,
        rmpv::Value::Boolean(b) => serde_json::Value::Bool(*b),
        rmpv::Value::Integer(i) => {
            if let Some(n) = i.as_i64() {
                serde_json::Value::Number(n.into())
            } else if let Some(n) = i.as_u64() {
                serde_json::Value::Number(n.into())
            } else {
                serde_json::Value::Null
            }
        }
        rmpv::Value::F32(f) => serde_json::Number::from_f64(*f as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        rmpv::Value::F64(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        rmpv::Value::String(s) => serde_json::Value::String(s.as_str().unwrap_or("").to_string()),
        rmpv::Value::Binary(b) => serde_json::Value::String(base64_encode(b)),
        rmpv::Value::Array(arr) => serde_json::Value::Array(arr.iter().map(rmpv_to_json).collect()),
        rmpv::Value::Map(entries) => {
            let obj: serde_json::Map<String, serde_json::Value> = entries
                .iter()
                .filter_map(|(k, v)| {
                    let key = match k {
                        rmpv::Value::String(s) => s.as_str().map(|s| s.to_string()),
                        _ => Some(format!("{k}")),
                    };
                    key.map(|k| (k, rmpv_to_json(v)))
                })
                .collect();
            serde_json::Value::Object(obj)
        }
        rmpv::Value::Ext(_, _) => serde_json::Value::Null,
    }
}

fn base64_encode(data: &[u8]) -> String {
    // Simple hex encoding (no base64 dep needed)
    data.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_to_rmpv_roundtrip() {
        let json = serde_json::json!({
            "name": "test",
            "count": 42,
            "active": true,
            "tags": ["a", "b"],
            "nested": {"x": 1}
        });

        let rmpv = json_to_rmpv(&json);
        let back = rmpv_to_json(&rmpv);

        assert_eq!(json, back);
    }

    #[test]
    fn json_null_roundtrip() {
        let json = serde_json::Value::Null;
        let rmpv = json_to_rmpv(&json);
        let back = rmpv_to_json(&rmpv);
        assert_eq!(json, back);
    }

    #[test]
    fn ws_client_message_deserialize() {
        let sub: WsClientMessage =
            serde_json::from_str(r#"{"type":"sub","topic":"order.*","sub_id":"s1"}"#).unwrap();
        assert!(matches!(sub, WsClientMessage::Sub { .. }));

        let ack: WsClientMessage =
            serde_json::from_str(r#"{"type":"ack","msg_id":"abc"}"#).unwrap();
        assert!(matches!(ack, WsClientMessage::Ack { .. }));

        let ping: WsClientMessage = serde_json::from_str(r#"{"type":"ping"}"#).unwrap();
        assert!(matches!(ping, WsClientMessage::Ping));
    }

    #[test]
    fn ws_server_message_serialize() {
        let event = WsServerMessage::Event {
            msg_id: "abc".into(),
            topic: "order.created".into(),
            data: serde_json::json!({"id": 1}),
            headers: HashMap::new(),
            attempt: 1,
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"type\":\"event\""));
        assert!(json.contains("\"topic\":\"order.created\""));
    }
}
