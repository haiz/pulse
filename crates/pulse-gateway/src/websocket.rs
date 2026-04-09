use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Query, State, WebSocketUpgrade};
use axum::response::IntoResponse;
use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc;

use crate::types::*;
use crate::GatewayState;

#[derive(serde::Deserialize)]
pub struct WsQuery {
    #[serde(default)]
    pub token: Option<String>,
}

/// WS /v1/subscribe — upgrade handler
pub async fn subscribe_ws(
    State(state): State<Arc<GatewayState>>,
    Query(query): Query<WsQuery>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, state, query.token))
}

async fn handle_ws(socket: WebSocket, _state: Arc<GatewayState>, _token: Option<String>) {
    let (mut ws_tx, mut ws_rx) = socket.split();

    // Channel for sending events to the WS client
    let (event_tx, mut event_rx) = mpsc::channel::<WsServerMessage>(256);

    // Task: forward events from channel to WS
    let send_task = tokio::spawn(async move {
        while let Some(msg) = event_rx.recv().await {
            let json = match serde_json::to_string(&msg) {
                Ok(j) => j,
                Err(_) => continue,
            };
            if ws_tx.send(Message::Text(json.into())).await.is_err() {
                break;
            }
        }
    });

    // Main loop: read client messages
    while let Some(msg) = ws_rx.next().await {
        let msg = match msg {
            Ok(Message::Text(t)) => t,
            Ok(Message::Close(_)) => break,
            Ok(Message::Ping(_)) => {
                let _ = event_tx.send(WsServerMessage::Pong).await;
                continue;
            }
            Ok(_) => continue,
            Err(_) => break,
        };

        let client_msg: WsClientMessage = match serde_json::from_str(&msg) {
            Ok(m) => m,
            Err(e) => {
                let _ = event_tx
                    .send(WsServerMessage::Error {
                        code: 4000,
                        message: format!("invalid message: {e}"),
                    })
                    .await;
                continue;
            }
        };

        match client_msg {
            WsClientMessage::Sub {
                topic,
                sub_id,
                group: _,
                filter: _,
            } => {
                // In sidecar mode, subscribe via SDK
                // For now, acknowledge the subscription
                let _ = event_tx
                    .send(WsServerMessage::Subscribed {
                        sub_id: sub_id.clone(),
                        topic: topic.clone(),
                    })
                    .await;

                tracing::debug!(sub_id, topic, "WS subscription registered");
            }

            WsClientMessage::Unsub { sub_id } => {
                tracing::debug!(sub_id, "WS subscription removed");
            }

            WsClientMessage::Ack { msg_id } => {
                tracing::trace!(msg_id, "WS ACK received");
            }

            WsClientMessage::Ping => {
                let _ = event_tx.send(WsServerMessage::Pong).await;
            }
        }
    }

    send_task.abort();
    tracing::debug!("WS connection closed");
}
