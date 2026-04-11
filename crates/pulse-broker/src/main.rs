use std::sync::Arc;

use clap::Parser;
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

use pulse_broker::broker::BrokerHandle;
use pulse_broker::config::BrokerConfig;
use pulse_broker::delivery::dlq::DeadLetterQueue;
use pulse_broker::delivery::manager::DeliveryManager;
use pulse_broker::pipeline::dedup::DedupEngine;
use pulse_broker::pipeline::dispatcher::Dispatcher;
use pulse_broker::server::listener::Listener;
use pulse_broker::storage::state_db::StateDb;
use pulse_broker::storage::sharded_wal::ShardedWalWriter;
use pulse_broker::storage::wal;

#[derive(Parser)]
#[command(name = "pulse-broker", about = "Pulse event broker")]
struct Cli {
    /// Path to broker.yaml config file (optional — zero-config mode if omitted)
    #[arg(short, long)]
    config: Option<String>,

    /// Override listen address
    #[arg(long)]
    listen: Option<String>,

    /// Durability mode: memory, balanced, durable
    #[arg(long)]
    durability: Option<String>,

    /// Log level: trace, debug, info, warn, error
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .json()
        .init();

    let cli = Cli::parse();
    tracing::info!("Pulse broker starting");

    // Load config (zero-config mode if no file specified)
    let mut config = match &cli.config {
        Some(path) => {
            tracing::info!(config = %path, "loading config");
            BrokerConfig::load(path)?
        }
        None => {
            tracing::info!("no config file — using zero-config defaults");
            BrokerConfig::default()
        }
    };

    // CLI overrides
    if let Some(addr) = &cli.listen {
        config.listen_addr = addr
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid listen address: {e}"))?;
    }
    if let Some(mode) = &cli.durability {
        config.durability.mode = match mode.as_str() {
            "memory" => pulse_broker::config::DurabilityMode::Memory,
            "balanced" => pulse_broker::config::DurabilityMode::Balanced,
            "durable" => pulse_broker::config::DurabilityMode::Durable,
            other => anyhow::bail!("invalid durability mode: {other}"),
        };
    }

    tracing::info!(
        listen_addr = %config.listen_addr,
        data_dir = %config.data_dir.display(),
        durability = ?config.durability.mode,
        "config loaded"
    );

    // Open storage
    let state_db = Arc::new(StateDb::open(config.data_dir.join("state"))?);
    let wal_dir = config.data_dir.join("wal");

    // WAL recovery: replay and rebuild dedup index
    let replay = if config.wal.shards > 1 {
        wal::replay_wal_sharded(&wal_dir, config.wal.shards).await?
    } else {
        let shard_dir = wal_dir.join("shard-00");
        if shard_dir.exists() {
            wal::replay_wal_sharded(&wal_dir, 1).await?
        } else {
            wal::replay_wal(&wal_dir).await?
        }
    };
    if replay.record_count > 0 {
        tracing::info!(
            events = replay.event_ids.len(),
            records = replay.record_count,
            last_segment = replay.last_segment,
            "WAL recovery complete"
        );
        let inserted = state_db.dedup_bulk_insert(replay.event_ids.into_iter())?;
        if inserted > 0 {
            tracing::info!(inserted, "rebuilt dedup index from WAL");
        }
    }

    // Open WAL writer
    let wal = ShardedWalWriter::open(wal_dir, &config.wal, config.wal.shards).await?;

    // Build shared router and delivery
    let router = Arc::new(pulse_broker::routing::Router::new());
    let dlq = DeadLetterQueue::new(state_db.db()).ok();
    let delivery = DeliveryManager::new(&config.delivery, dlq);

    // Build broker handle (shared state) — router is shared with dispatcher
    let (dispatch_tx, dispatch_rx) = mpsc::channel(1024);
    let broker = BrokerHandle::new(
        config.clone(),
        dispatch_tx,
        state_db.clone(),
        delivery,
        router.clone(),
    );

    // Build pipeline — same router instance used by connections and dispatcher
    let dedup = DedupEngine::new(state_db);
    let _dispatcher_handle = Dispatcher::spawn(dedup, wal, dispatch_rx, Some(router));

    // Start metrics server
    if config.metrics.enabled {
        let metrics_addr = config.metrics.listen_addr;
        match pulse_broker::metrics::exporter::MetricsServer::new(metrics_addr) {
            Ok(metrics) => {
                tokio::spawn(async move {
                    if let Err(e) = metrics.run().await {
                        tracing::error!(error = %e, "metrics server error");
                    }
                });
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to start metrics server");
            }
        }
    }

    // Start TCP/TLS listener
    let listener = if let Some(tls_config) = &config.tls {
        Listener::bind_tls(config.listen_addr, broker, tls_config)
            .await
            .map_err(|e| anyhow::anyhow!("TLS bind error: {e}"))?
    } else {
        Listener::bind(config.listen_addr, broker).await?
    };

    tracing::info!("Pulse broker ready");

    // Run listener until shutdown
    tokio::select! {
        result = listener.run() => {
            if let Err(e) = result {
                tracing::error!(error = %e, "listener error");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("received SIGINT, shutting down");
        }
    }

    tracing::info!("Pulse broker stopped");

    Ok(())
}
