use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

use pulse_broker::broker::BrokerHandle;
use pulse_broker::config::BrokerConfig;
use pulse_broker::delivery::manager::DeliveryManager;
use pulse_broker::pipeline::admission::AdmissionController;
use pulse_broker::pipeline::dedup::DedupEngine;
use pulse_broker::pipeline::dispatcher::Dispatcher;
use pulse_broker::routing::Router;
use pulse_broker::server::listener::Listener;
use pulse_broker::storage::state_db::StateDb;
use pulse_broker::storage::sharded_wal::ShardedWalWriter;
use pulse_broker::storage::wal;

use pulse_gateway::GatewayState;
use pulse_sdk::PulseBuilder;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,pulse_broker=debug")),
        )
        .init();

    println!();
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║        Pulse E-Commerce Demo — Microservices Flow        ║");
    println!("╠═══════════════════════════════════════════════════════════╣");
    println!("║                                                           ║");
    println!("║  Services:                                                ║");
    println!("║    [Go]     Order Service      → HTTP POST publish        ║");
    println!("║    [Python] Payment Service    → HTTP POST publish        ║");
    println!("║    [Node]   Notification Svc   → WebSocket subscribe      ║");
    println!("║    [Rust]   Analytics Service  → Native TCP subscribe     ║");
    println!("║    [curl]   Admin              → HTTP ad-hoc              ║");
    println!("║                                                           ║");
    println!("║  Flow: order.created → payment.completed → notification   ║");
    println!("║                                                           ║");
    println!("╚═══════════════════════════════════════════════════════════╝");
    println!();

    // ─── Step 1: Start Broker ───
    println!("▸ Starting Pulse broker on :4222...");
    let dir = tempfile::tempdir()?;
    let config = BrokerConfig::for_testing(dir.path().to_path_buf());

    let state_db = Arc::new(StateDb::open(config.data_dir.join("state"))?);
    let wal_dir = config.data_dir.join("wal");
    let _ = wal::replay_wal_sharded(&wal_dir, config.wal.shards).await?;
    let wal = ShardedWalWriter::open(
        wal_dir,
        &config.wal,
        config.wal.shards,
        Duration::from_millis(5),
        100,
    )?;

    let router = Arc::new(Router::new());
    let delivery = DeliveryManager::new(&config.delivery, None);
    let dedup = DedupEngine::new(state_db.clone());

    let (dispatch_tx, dispatch_rx) = mpsc::channel(4096);
    let admission = Arc::new(AdmissionController::new(50_000));
    Dispatcher::spawn(dedup, wal, dispatch_rx, Some(router.clone()), Some(admission.clone()));

    let broker_addr: std::net::SocketAddr = "127.0.0.1:4222".parse()?;
    let broker = BrokerHandle::new(config, dispatch_tx, state_db, delivery, router, admission);
    let listener = Listener::bind(broker_addr, broker).await?;

    tokio::spawn(async move {
        let _ = listener.run().await;
    });

    println!("  ✓ Broker running on 127.0.0.1:4222");

    // ─── Step 2: Start HTTP/WS Gateway ───
    println!("▸ Starting HTTP/WS gateway on :8080...");
    let gateway_client = PulseBuilder::new("pulse-gateway", "default")
        .addr(broker_addr)
        .auto_reconnect(false)
        .connect()
        .await?;

    let gateway_state = Arc::new(GatewayState {
        client: tokio::sync::Mutex::new(gateway_client),
    });

    let gateway_addr: std::net::SocketAddr = "127.0.0.1:8080".parse()?;
    tokio::spawn(async move {
        let _ = pulse_gateway::serve(gateway_addr, gateway_state).await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    println!("  ✓ Gateway running on 127.0.0.1:8080");

    // ─── Step 3: Start Rust Analytics Service (native TCP) ───
    println!("▸ Starting [Rust] Analytics Service (subscribe to '>')...");
    let mut analytics = PulseBuilder::new("analytics-svc", "default")
        .addr(broker_addr)
        .auto_reconnect(false)
        .connect()
        .await?;

    analytics.subscribe(">", None).await?;
    println!("  ✓ Analytics subscribed to all events (>)");

    // ─── Step 4: Start Rust Payment Listener (native TCP) ───
    println!("▸ Starting [Rust] Payment Listener (subscribe to 'order.*')...");
    let mut payment_listener = PulseBuilder::new("payment-svc", "default")
        .addr(broker_addr)
        .auto_reconnect(false)
        .connect()
        .await?;

    payment_listener.subscribe("order.*", None).await?;
    println!("  ✓ Payment listener subscribed to order.*");

    tokio::time::sleep(Duration::from_millis(100)).await;

    println!();
    println!("═══════════════════════════════════════════════════════════");
    println!("  All services running. Ready for demo flow.");
    println!("═══════════════════════════════════════════════════════════");
    println!();

    // ─── Demo Flow ───

    // 1. [Go] Order Service publishes via HTTP gateway
    println!("━━━ STEP 1: [Go/HTTP] Order Service creates order ━━━");
    let http_client = reqwest::Client::new();
    let resp = http_client
        .post("http://127.0.0.1:8080/v1/publish")
        .json(&serde_json::json!({
            "topic": "order.created",
            "data": {
                "order_id": "ORD-2024-001",
                "customer": "Hai Cao",
                "items": [
                    {"name": "Rust Programming", "price": 49.99},
                    {"name": "Systems Design", "price": 39.99}
                ],
                "total": 89.98,
                "region": "VN"
            }
        }))
        .send()
        .await?;

    let pub_result: serde_json::Value = resp.json().await?;
    println!("  → Published order.created via HTTP");
    println!("    msg_id: {}", pub_result["msg_id"]);
    println!("    status: {}", pub_result["status"]);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // 2. [Python] Payment Service publishes via HTTP gateway
    println!();
    println!("━━━ STEP 2: [Python/HTTP] Payment Service processes payment ━━━");
    let resp2 = http_client
        .post("http://127.0.0.1:8080/v1/publish")
        .json(&serde_json::json!({
            "topic": "payment.completed",
            "data": {
                "order_id": "ORD-2024-001",
                "amount": 89.98,
                "currency": "USD",
                "method": "credit_card",
                "transaction_id": "TXN-98765"
            }
        }))
        .send()
        .await?;

    let pub_result2: serde_json::Value = resp2.json().await?;
    println!("  → Published payment.completed via HTTP");
    println!("    msg_id: {}", pub_result2["msg_id"]);
    println!("    status: {}", pub_result2["status"]);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // 3. Batch publish (simulating Go batch endpoint)
    println!();
    println!("━━━ STEP 3: [Go/HTTP] Batch publish inventory updates ━━━");
    let resp3 = http_client
        .post("http://127.0.0.1:8080/v1/publish/batch")
        .json(&serde_json::json!({
            "events": [
                {"topic": "inventory.updated", "data": {"sku": "RUST-BOOK", "qty": -1}},
                {"topic": "inventory.updated", "data": {"sku": "SYS-DESIGN", "qty": -1}},
                {"topic": "shipping.requested", "data": {"order_id": "ORD-2024-001", "address": "HCM, Vietnam"}}
            ]
        }))
        .send()
        .await?;

    let batch_result: serde_json::Value = resp3.json().await?;
    println!("  → Batch published 3 events via HTTP");
    for (i, r) in batch_result["results"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .enumerate()
    {
        println!("    [{}] status: {}", i + 1, r["status"]);
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // 4. Health check
    println!();
    println!("━━━ STEP 4: Health & Info checks ━━━");
    let health: serde_json::Value = http_client
        .get("http://127.0.0.1:8080/v1/health")
        .send()
        .await?
        .json()
        .await?;
    println!("  Health: {}", health["status"]);

    let info: serde_json::Value = http_client
        .get("http://127.0.0.1:8080/v1/info")
        .send()
        .await?
        .json()
        .await?;
    println!("  Gateway version: {}", info["version"]);
    println!("  Broker ID: {}", info["broker_id"]);

    // Summary
    println!();
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║                    Demo Complete ✓                        ║");
    println!("╠═══════════════════════════════════════════════════════════╣");
    println!("║                                                           ║");
    println!("║  Events published:                                        ║");
    println!("║    ✓ order.created      (Go → HTTP gateway)               ║");
    println!("║    ✓ payment.completed  (Python → HTTP gateway)           ║");
    println!("║    ✓ inventory.updated  (Go → HTTP batch)                 ║");
    println!("║    ✓ shipping.requested (Go → HTTP batch)                 ║");
    println!("║                                                           ║");
    println!("║  Subscribers active:                                      ║");
    println!("║    ✓ Analytics (Rust/TCP)  → all events (>)               ║");
    println!("║    ✓ Payment (Rust/TCP)    → order.* events               ║");
    println!("║                                                           ║");
    println!("║  Integration paths tested:                                ║");
    println!("║    ✓ HTTP REST publish (single + batch)                   ║");
    println!("║    ✓ HTTP health + info endpoints                         ║");
    println!("║    ✓ Native Rust TCP SDK (subscribe)                      ║");
    println!("║    ✓ Broker ↔ Gateway ↔ SDK pipeline                     ║");
    println!("║                                                           ║");
    println!("╚═══════════════════════════════════════════════════════════╝");
    println!();

    // Keep running for manual testing
    println!("Broker + Gateway still running for manual testing:");
    println!("  Publish:   curl -X POST http://127.0.0.1:8080/v1/publish -H 'Content-Type: application/json' -d '{{\"topic\":\"test\",\"data\":{{\"hello\":\"world\"}}}}'");
    println!("  Health:    curl http://127.0.0.1:8080/v1/health");
    println!("  WebSocket: wscat -c ws://127.0.0.1:8080/v1/subscribe");
    println!();
    println!("Press Ctrl+C to stop.");

    tokio::signal::ctrl_c().await?;
    println!("Shutting down...");

    Ok(())
}
