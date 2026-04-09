use std::net::SocketAddr;

use pulse_sdk::PulseBuilder;

pub async fn run(
    addr: SocketAddr,
    service_id: &str,
    namespace: &str,
    api_key: &str,
    topic: &str,
    _max_count: u64,
) -> anyhow::Result<()> {
    let mut client = PulseBuilder::new(service_id, namespace)
        .addr(addr)
        .api_key(api_key)
        .auto_reconnect(false)
        .connect()
        .await?;

    client.subscribe(topic, None).await?;
    println!("Subscribed to {topic}");
    println!("Waiting for events (Ctrl+C to stop)...");
    println!();

    client
        .consume(|event| async move {
            println!("--- Event ---");
            println!("  Topic:   {}", event.topic);
            println!("  Msg ID:  {}", event.msg_id);
            println!("  Attempt: {}", event.attempt);
            println!("  Data:    {:?}", event.data);
            if !event.headers.is_empty() {
                println!("  Headers: {:?}", event.headers);
            }
            println!();
            Ok(())
        })
        .await
        .ok();

    Ok(())
}
