use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::SinkExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tokio_util::codec::Framed;

use pulse_protocol::{
    AckPayload, AckStatus, ConnAckPayload, ErrPayload, Frame, MessageId, MessageType, Payload,
    PulseCodec,
};

use crate::broker::BrokerHandle;
use crate::error::BrokerError;
use crate::pipeline::dispatcher::IngestMessage;
use crate::pipeline::ingest::IngestResult;
use crate::server::session::Session;

use futures::StreamExt;

/// Per-connection handler implementing the bidirectional frame loop.
pub struct ConnectionHandler;

impl ConnectionHandler {
    /// Run a plain TCP connection.
    pub async fn run(
        stream: TcpStream,
        broker: Arc<BrokerHandle>,
        peer_addr: SocketAddr,
    ) -> Result<(), BrokerError> {
        Self::run_inner(stream, broker, peer_addr).await
    }

    /// Run a TLS connection.
    pub async fn run_tls(
        stream: tokio_rustls::server::TlsStream<TcpStream>,
        broker: Arc<BrokerHandle>,
        peer_addr: SocketAddr,
    ) -> Result<(), BrokerError> {
        Self::run_inner(stream, broker, peer_addr).await
    }

    async fn run_inner<S>(
        stream: S,
        broker: Arc<BrokerHandle>,
        peer_addr: SocketAddr,
    ) -> Result<(), BrokerError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let config = broker.config.load();
        let connect_timeout = Duration::from_secs(config.connect_timeout_secs);
        let keepalive_interval = Duration::from_secs(config.keepalive_interval_secs);

        let mut framed = Framed::new(stream, PulseCodec::new());

        // Step 1: Await CONNECT frame
        let connect_frame = timeout(connect_timeout, framed.next())
            .await
            .map_err(|_| BrokerError::Connection("connect timeout".into()))?
            .ok_or_else(|| BrokerError::Connection("connection closed before CONNECT".into()))?
            .map_err(|e| BrokerError::Protocol(e.to_string()))?;

        let (service_id, namespace, max_inflight) = match &connect_frame.payload {
            Payload::Connect(c) => (c.service_id.clone(), c.namespace.clone(), c.max_inflight),
            _ => {
                let err_frame = Frame::err(
                    MessageId::new(),
                    ErrPayload {
                        code: 4000,
                        message: "expected CONNECT frame".into(),
                    },
                );
                let _ = framed.send(err_frame).await;
                return Err(BrokerError::Protocol("expected CONNECT frame".into()));
            }
        };

        tracing::info!(
            %peer_addr,
            service = %service_id,
            namespace = %namespace,
            "client connected"
        );

        // Step 2: Create session and delivery channel
        let (deliver_tx, mut deliver_rx) = mpsc::channel::<Frame>(4096);
        let session = Arc::new(Session::new(
            service_id.clone(),
            namespace.clone(),
            max_inflight,
            deliver_tx,
        ));
        let session_id = session.id;
        broker.register_session(session.clone());

        // Step 3: Send CONNACK
        let connack = Frame::connack(
            connect_frame.msg_id,
            ConnAckPayload {
                status: "ok".into(),
                broker_id: "pulse-1".into(),
                server_time: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
                max_payload: config.max_payload_bytes,
                features: vec![],
            },
        );
        framed
            .send(connack)
            .await
            .map_err(|e| BrokerError::Connection(e.to_string()))?;

        // Step 4: Bidirectional loop
        let result = Self::run_loop(
            &mut framed,
            &mut deliver_rx,
            &broker,
            &session,
            keepalive_interval,
        )
        .await;

        // Cleanup on disconnect
        broker.unregister_session(session_id);
        tracing::info!(
            %peer_addr,
            service = %service_id,
            "client disconnected"
        );

        result
    }

    async fn run_loop<S>(
        framed: &mut Framed<S, PulseCodec>,
        deliver_rx: &mut mpsc::Receiver<Frame>,
        broker: &Arc<BrokerHandle>,
        session: &Arc<Session>,
        keepalive_interval: Duration,
    ) -> Result<(), BrokerError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut keepalive_timer = tokio::time::interval(keepalive_interval);
        keepalive_timer.tick().await; // skip first immediate tick

        loop {
            tokio::select! {
                // Inbound: frames from client
                frame = framed.next() => {
                    let frame = match frame {
                        Some(Ok(f)) => f,
                        Some(Err(e)) => {
                            return Err(BrokerError::Protocol(e.to_string()));
                        }
                        None => {
                            // Client disconnected
                            return Ok(());
                        }
                    };

                    Self::handle_inbound(framed, broker, session, frame).await?;
                }

                // Outbound: events to deliver to this client
                event = deliver_rx.recv() => {
                    match event {
                        Some(frame) => {
                            framed.send(frame).await
                                .map_err(|e| BrokerError::Connection(e.to_string()))?;
                        }
                        None => {
                            // Delivery channel closed
                            return Ok(());
                        }
                    }
                }

                // Keepalive
                _ = keepalive_timer.tick() => {
                    let ping = Frame::ping(MessageId::new());
                    framed.send(ping).await
                        .map_err(|e| BrokerError::Connection(e.to_string()))?;
                }
            }
        }
    }

    async fn handle_inbound<S>(
        framed: &mut Framed<S, PulseCodec>,
        broker: &Arc<BrokerHandle>,
        session: &Arc<Session>,
        frame: Frame,
    ) -> Result<(), BrokerError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        match frame.msg_type {
            MessageType::Pub => {
                let pub_payload = match frame.payload {
                    Payload::Pub(p) => p,
                    _ => unreachable!(),
                };

                // Send to dispatcher and wait for result
                let (reply_tx, reply_rx) = oneshot::channel();
                let msg = IngestMessage {
                    msg_id: frame.msg_id,
                    pub_payload: pub_payload.clone(),
                    namespace: session.namespace.clone(),
                    reply_tx,
                };

                if broker.dispatch_tx.send(msg).await.is_err() {
                    let err = Frame::err(
                        frame.msg_id,
                        ErrPayload {
                            code: 5000,
                            message: "broker pipeline unavailable".into(),
                        },
                    );
                    framed
                        .send(err)
                        .await
                        .map_err(|e| BrokerError::Connection(e.to_string()))?;
                    return Ok(());
                }

                // Wait for pipeline result
                let result = reply_rx.await.unwrap_or(IngestResult::Failed {
                    error: BrokerError::Connection("pipeline dropped".into()),
                });

                let ack_frame = match result {
                    IngestResult::Stored { .. } => Frame::ack(
                        frame.msg_id,
                        AckPayload {
                            status: AckStatus::Stored,
                            msg_id: frame.msg_id.as_bytes().to_vec(),
                            reason: None,
                        },
                    ),
                    IngestResult::Duplicate => Frame::ack(
                        frame.msg_id,
                        AckPayload {
                            status: AckStatus::Duplicate,
                            msg_id: frame.msg_id.as_bytes().to_vec(),
                            reason: None,
                        },
                    ),
                    IngestResult::Failed { error } => Frame::err(
                        frame.msg_id,
                        ErrPayload {
                            code: 5000,
                            message: error.to_string(),
                        },
                    ),
                };

                framed
                    .send(ack_frame)
                    .await
                    .map_err(|e| BrokerError::Connection(e.to_string()))?;
            }

            MessageType::Ping => {
                let pong = Frame::pong(frame.msg_id);
                framed
                    .send(pong)
                    .await
                    .map_err(|e| BrokerError::Connection(e.to_string()))?;
            }

            MessageType::Pong => {
                // Keepalive response — no action needed
            }

            MessageType::Ack => {
                if let Payload::Ack(ack) = &frame.payload {
                    broker
                        .delivery
                        .handle_ack(&frame.msg_id, &session.service_id, &ack.status);
                }
            }

            MessageType::Sub => {
                if let Payload::Sub(sub) = &frame.payload {
                    let target = crate::routing::engine::SubscriptionTarget {
                        consumer_id: session.service_id.clone(),
                        sub_id: sub.sub_id.clone(),
                        group: sub.group.clone(),
                        filter: sub
                            .filter
                            .as_ref()
                            .and_then(|f| crate::routing::filter::CompiledFilter::compile(f).ok()),
                        deliver_tx: session.deliver_tx.clone(),
                        partition_key: None,
                    };

                    broker.router.subscribe(&sub.topic, target);
                    tracing::debug!(
                        sub_id = %sub.sub_id,
                        topic = %sub.topic,
                        "subscription registered"
                    );
                }
            }

            MessageType::Unsub => {
                if let Payload::Unsub(unsub) = &frame.payload {
                    // We don't know the topic pattern here, so we'd need
                    // to track sub_id -> pattern. For now, log it.
                    tracing::debug!(sub_id = %unsub.sub_id, "UNSUB received");
                }
            }

            MessageType::Flow => {
                // Flow control — will be implemented in Phase 8
                tracing::debug!(msg_id = %frame.msg_id, "FLOW received (not yet implemented)");
            }

            _ => {
                let err = Frame::err(
                    frame.msg_id,
                    ErrPayload {
                        code: 4000,
                        message: format!("unexpected frame type: {:?}", frame.msg_type),
                    },
                );
                framed
                    .send(err)
                    .await
                    .map_err(|e| BrokerError::Connection(e.to_string()))?;
            }
        }

        Ok(())
    }
}
