use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

mod commands;

#[derive(Parser)]
#[command(name = "pulse-admin", about = "Pulse broker administration tool")]
struct Cli {
    /// Broker address
    #[arg(short, long, default_value = "127.0.0.1:4222")]
    broker: String,

    /// Service ID for authentication
    #[arg(long, default_value = "pulse-admin")]
    service_id: String,

    /// Namespace
    #[arg(long, default_value = "default")]
    namespace: String,

    /// API key
    #[arg(long, default_value = "")]
    api_key: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Show broker status
    Status,
    /// Publish a test event
    Pub {
        /// Topic to publish to
        topic: String,
        /// JSON payload
        #[arg(default_value = "{}")]
        payload: String,
    },
    /// Subscribe and print events
    Sub {
        /// Topic pattern to subscribe to
        topic: String,
        /// Maximum number of events to receive (0 = unlimited)
        #[arg(short, long, default_value = "0")]
        count: u64,
    },
    /// Send a ping to the broker
    Ping,
    /// Validate a config file
    ConfigCheck {
        /// Path to config file
        path: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .init();

    let cli = Cli::parse();
    let addr: std::net::SocketAddr = cli.broker.parse()?;

    match cli.command {
        Commands::Status => {
            commands::status::run(addr, &cli.service_id, &cli.namespace, &cli.api_key).await
        }
        Commands::Pub { topic, payload } => {
            commands::publish::run(
                addr,
                &cli.service_id,
                &cli.namespace,
                &cli.api_key,
                &topic,
                &payload,
            )
            .await
        }
        Commands::Sub { topic, count } => {
            commands::subscribe::run(
                addr,
                &cli.service_id,
                &cli.namespace,
                &cli.api_key,
                &topic,
                count,
            )
            .await
        }
        Commands::Ping => {
            commands::ping::run(addr, &cli.service_id, &cli.namespace, &cli.api_key).await
        }
        Commands::ConfigCheck { path } => commands::config_check::run(&path),
    }
}
