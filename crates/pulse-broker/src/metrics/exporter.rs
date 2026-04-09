use std::net::SocketAddr;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

/// Metrics server serving Prometheus `/metrics` and `/health` endpoints.
pub struct MetricsServer {
    addr: SocketAddr,
    handle: PrometheusHandle,
}

impl MetricsServer {
    /// Install the Prometheus recorder and create the metrics server.
    pub fn new(addr: SocketAddr) -> Result<Self, Box<dyn std::error::Error>> {
        let builder = PrometheusBuilder::new();
        let handle = builder.install_recorder()?;

        Ok(Self { addr, handle })
    }

    /// Run the HTTP metrics server.
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = tokio::net::TcpListener::bind(self.addr).await?;
        tracing::info!(addr = %self.addr, "metrics server listening");

        let handle = self.handle;
        loop {
            let (stream, _) = listener.accept().await?;
            let handle = handle.clone();

            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |req: Request<Incoming>| {
                    let handle = handle.clone();
                    async move { handle_request(req, &handle) }
                });

                let io = TokioIo::new(stream);
                if let Err(e) = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service)
                    .await
                {
                    tracing::debug!(error = %e, "metrics connection error");
                }
            });
        }
    }
}

fn handle_request(
    req: Request<Incoming>,
    handle: &PrometheusHandle,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    match req.uri().path() {
        "/metrics" => {
            let body = handle.render();
            Ok(Response::builder()
                .header("content-type", "text/plain; charset=utf-8")
                .body(Full::new(Bytes::from(body)))
                .unwrap())
        }
        "/health" => Ok(Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(r#"{"status":"ok"}"#)))
            .unwrap()),
        "/ready" => Ok(Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(r#"{"ready":true}"#)))
            .unwrap()),
        _ => Ok(Response::builder()
            .status(404)
            .body(Full::new(Bytes::from("not found")))
            .unwrap()),
    }
}
