pub mod auth;
pub mod rest;
pub mod types;
pub mod websocket;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;
use tokio::sync::Mutex;
use tower_http::cors::{Any, CorsLayer};

use pulse_sdk::Pulse;

/// Shared state for the gateway.
pub struct GatewayState {
    pub client: Mutex<Pulse>,
}

/// Build the axum router with all gateway routes.
pub fn build_router(state: Arc<GatewayState>) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        .route("/v1/publish", post(rest::publish))
        .route("/v1/publish/batch", post(rest::publish_batch))
        .route("/v1/topics", get(rest::topics))
        .route("/v1/health", get(rest::health))
        .route("/v1/info", get(rest::info))
        .route("/v1/subscribe", get(websocket::subscribe_ws))
        .layer(cors)
        .with_state(state)
}

/// Start the gateway HTTP server.
pub async fn serve(addr: SocketAddr, state: Arc<GatewayState>) -> Result<(), std::io::Error> {
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "gateway HTTP server listening");
    axum::serve(listener, app).await
}
