use std::net::SocketAddr;
use std::time::Instant;

use pulse_sdk::PulseBuilder;

pub async fn run(
    addr: SocketAddr,
    service_id: &str,
    namespace: &str,
    api_key: &str,
) -> anyhow::Result<()> {
    let start = Instant::now();

    let client = PulseBuilder::new(service_id, namespace)
        .addr(addr)
        .api_key(api_key)
        .auto_reconnect(false)
        .connect()
        .await?;

    let elapsed = start.elapsed();

    println!("Connected to broker");
    println!("  Broker ID:    {}", client.broker_id());
    println!("  Max payload:  {} bytes", client.max_payload());
    println!("  Connect time: {:.1}ms", elapsed.as_secs_f64() * 1000.0);
    println!("  Status:       OK");

    Ok(())
}
