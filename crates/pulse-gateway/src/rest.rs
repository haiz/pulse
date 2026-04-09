use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Json;

use crate::auth::extract_token;
use crate::types::*;
use crate::GatewayState;

/// POST /v1/publish
pub async fn publish(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Json(req): Json<PublishRequest>,
) -> Result<Json<PublishResponse>, (StatusCode, Json<ErrorResponse>)> {
    let _token = extract_token(&headers, None).map_err(|s| {
        (
            s,
            Json(ErrorResponse {
                error: "unauthorized".into(),
                code: 4010,
            }),
        )
    })?;

    let data = json_to_rmpv(&req.data);
    let mut client = state.client.lock().await;

    match client.publish(&req.topic, data, None).await {
        Ok(msg_id) => Ok(Json(PublishResponse {
            msg_id: msg_id.to_string(),
            status: "stored".into(),
        })),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
                code: 5000,
            }),
        )),
    }
}

/// POST /v1/publish/batch
pub async fn publish_batch(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Json(req): Json<BatchPublishRequest>,
) -> Result<Json<BatchPublishResponse>, (StatusCode, Json<ErrorResponse>)> {
    let _token = extract_token(&headers, None).map_err(|s| {
        (
            s,
            Json(ErrorResponse {
                error: "unauthorized".into(),
                code: 4010,
            }),
        )
    })?;

    let mut results = Vec::with_capacity(req.events.len());
    let mut client = state.client.lock().await;

    for event in &req.events {
        let data = json_to_rmpv(&event.data);
        match client.publish(&event.topic, data, None).await {
            Ok(msg_id) => results.push(PublishResponse {
                msg_id: msg_id.to_string(),
                status: "stored".into(),
            }),
            Err(e) => results.push(PublishResponse {
                msg_id: String::new(),
                status: format!("error: {e}"),
            }),
        }
    }

    Ok(Json(BatchPublishResponse { results }))
}

/// GET /v1/health
pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".into(),
    })
}

/// GET /v1/info
pub async fn info(State(state): State<Arc<GatewayState>>) -> Json<InfoResponse> {
    let client = state.client.lock().await;
    Json(InfoResponse {
        version: env!("CARGO_PKG_VERSION").into(),
        broker_id: client.broker_id().to_string(),
        gateway_mode: "sidecar".into(),
    })
}

/// GET /v1/topics
pub async fn topics() -> Json<TopicsResponse> {
    // In a full implementation, this would query the broker's topic registry
    Json(TopicsResponse { topics: vec![] })
}
