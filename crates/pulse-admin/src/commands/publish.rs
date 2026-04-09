use std::net::SocketAddr;

use pulse_sdk::PulseBuilder;

pub async fn run(
    addr: SocketAddr,
    service_id: &str,
    namespace: &str,
    api_key: &str,
    topic: &str,
    payload_json: &str,
) -> anyhow::Result<()> {
    let mut client = PulseBuilder::new(service_id, namespace)
        .addr(addr)
        .api_key(api_key)
        .auto_reconnect(false)
        .connect()
        .await?;

    let data: serde_json::Value = serde_json::from_str(payload_json)
        .unwrap_or_else(|_| serde_json::Value::String(payload_json.to_string()));

    let rmpv_data = json_to_rmpv(&data);
    let msg_id = client.publish(topic, rmpv_data, None).await?;

    println!("Published to {topic}");
    println!("  Message ID: {msg_id}");

    Ok(())
}

fn json_to_rmpv(val: &serde_json::Value) -> rmpv::Value {
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
