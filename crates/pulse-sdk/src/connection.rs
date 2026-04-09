use std::net::SocketAddr;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use pulse_protocol::*;

use crate::error::PulseError;

/// Manages the TCP connection to a Pulse broker with auto-reconnect.
pub struct ConnectionManager {
    addr: SocketAddr,
    service_id: String,
    namespace: String,
    api_key: String,
    max_reconnect_attempts: u32,
    reconnect_delay: Duration,
}

/// An established connection to the broker.
pub struct Connection {
    pub framed: Framed<TcpStream, PulseCodec>,
    pub broker_id: String,
    pub max_payload: u32,
}

impl ConnectionManager {
    pub fn new(addr: SocketAddr, service_id: String, namespace: String, api_key: String) -> Self {
        Self {
            addr,
            service_id,
            namespace,
            api_key,
            max_reconnect_attempts: 10,
            reconnect_delay: Duration::from_secs(1),
        }
    }

    /// Connect to the broker and perform the CONNECT handshake.
    pub async fn connect(&self) -> Result<Connection, PulseError> {
        let stream = TcpStream::connect(self.addr)
            .await
            .map_err(|e| PulseError::Connection(format!("TCP connect to {}: {e}", self.addr)))?;

        let mut framed = Framed::new(stream, PulseCodec::new());

        // Send CONNECT
        let connect_frame = Frame::connect(
            MessageId::new(),
            ConnectPayload {
                service_id: self.service_id.clone(),
                namespace: self.namespace.clone(),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                hmac: self.api_key.as_bytes().to_vec(),
                client_ver: Some(env!("CARGO_PKG_VERSION").into()),
                max_inflight: None,
                codec: None,
            },
        );

        framed
            .send(connect_frame)
            .await
            .map_err(|e| PulseError::Connection(format!("send CONNECT: {e}")))?;

        // Await CONNACK
        let response = framed
            .next()
            .await
            .ok_or_else(|| PulseError::Connection("connection closed before CONNACK".into()))?
            .map_err(|e| PulseError::Protocol(e.to_string()))?;

        match &response.payload {
            Payload::ConnAck(ca) => {
                if ca.status != "ok" {
                    return Err(PulseError::Connection(format!(
                        "CONNACK rejected: {}",
                        ca.status
                    )));
                }
                Ok(Connection {
                    framed,
                    broker_id: ca.broker_id.clone(),
                    max_payload: ca.max_payload,
                })
            }
            Payload::Err(e) => Err(PulseError::BrokerError {
                code: e.code,
                message: e.message.clone(),
            }),
            _ => Err(PulseError::Protocol("expected CONNACK".into())),
        }
    }

    /// Connect with auto-reconnect and exponential backoff.
    pub async fn connect_with_retry(&self) -> Result<Connection, PulseError> {
        let mut attempts = 0;
        let mut delay = self.reconnect_delay;

        loop {
            match self.connect().await {
                Ok(conn) => return Ok(conn),
                Err(e) => {
                    attempts += 1;
                    if attempts >= self.max_reconnect_attempts {
                        return Err(PulseError::Connection(format!(
                            "failed after {attempts} attempts: {e}"
                        )));
                    }
                    tracing::warn!(
                        attempt = attempts,
                        max = self.max_reconnect_attempts,
                        delay_ms = delay.as_millis(),
                        error = %e,
                        "reconnecting"
                    );
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(30));
                }
            }
        }
    }
}
