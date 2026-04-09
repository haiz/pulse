use std::net::SocketAddr;
use std::time::Instant;

use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use pulse_protocol::*;

pub async fn run(
    addr: SocketAddr,
    service_id: &str,
    namespace: &str,
    api_key: &str,
) -> anyhow::Result<()> {
    // Connect
    let stream = TcpStream::connect(addr).await?;
    let mut framed = Framed::new(stream, PulseCodec::new());

    // CONNECT handshake
    let connect = Frame::connect(
        MessageId::new(),
        ConnectPayload {
            service_id: service_id.into(),
            namespace: namespace.into(),
            timestamp: 0,
            hmac: api_key.as_bytes().to_vec(),
            client_ver: None,
            max_inflight: None,
            codec: None,
        },
    );
    framed.send(connect).await?;
    let _ = framed.next().await; // CONNACK

    // PING
    let ping_id = MessageId::new();
    let start = Instant::now();
    framed.send(Frame::ping(ping_id)).await?;

    let response = framed
        .next()
        .await
        .ok_or(anyhow::anyhow!("no response"))??;
    let elapsed = start.elapsed();

    if response.msg_type == MessageType::Pong {
        println!(
            "PONG from {} in {:.3}ms",
            addr,
            elapsed.as_secs_f64() * 1000.0
        );
    } else {
        println!("Unexpected response: {:?}", response.msg_type);
    }

    Ok(())
}
