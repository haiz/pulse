use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use tokio::sync::Mutex;
use tracing_subscriber::EnvFilter;

use pulse_gateway::GatewayState;
use pulse_sdk::PulseBuilder;

#[derive(Parser)]
#[command(
    name = "pulse-gateway",
    about = "Pulse HTTP/WebSocket gateway (sidecar mode)"
)]
struct Cli {
    /// Broker address to connect to
    #[arg(short, long, default_value = "127.0.0.1:4222")]
    broker: String,

    /// HTTP listen address
    #[arg(short, long, default_value = "0.0.0.0:8080")]
    listen: String,

    /// Service ID for broker authentication
    #[arg(long, default_value = "pulse-gateway")]
    service_id: String,

    /// Namespace
    #[arg(long, default_value = "default")]
    namespace: String,

    /// API key
    #[arg(long, default_value = "")]
    api_key: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let broker_addr: SocketAddr = cli.broker.parse()?;
    let listen_addr: SocketAddr = cli.listen.parse()?;

    tracing::info!(broker = %broker_addr, "connecting to Pulse broker");

    let client = PulseBuilder::new(&cli.service_id, &cli.namespace)
        .addr(broker_addr)
        .api_key(&cli.api_key)
        .connect()
        .await?;

    tracing::info!(broker_id = client.broker_id(), "connected to broker");

    let state = Arc::new(GatewayState {
        client: Mutex::new(client),
    });

    pulse_gateway::serve(listen_addr, state).await?;

    Ok(())
}
