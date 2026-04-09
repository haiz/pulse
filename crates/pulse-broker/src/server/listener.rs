use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::Arc;

use rustls::ServerConfig;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

use crate::broker::BrokerHandle;
use crate::config::TlsConfig;
use crate::server::connection::ConnectionHandler;

/// TCP/TLS accept loop that spawns a connection handler per client.
pub struct Listener {
    tcp_listener: TcpListener,
    broker: Arc<BrokerHandle>,
    tls_acceptor: Option<TlsAcceptor>,
}

impl Listener {
    /// Bind to the given address (plain TCP).
    pub async fn bind(addr: SocketAddr, broker: Arc<BrokerHandle>) -> std::io::Result<Self> {
        let tcp_listener = TcpListener::bind(addr).await?;
        tracing::info!(%addr, tls = false, "listening for connections");
        Ok(Self {
            tcp_listener,
            broker,
            tls_acceptor: None,
        })
    }

    /// Bind with TLS enabled.
    pub async fn bind_tls(
        addr: SocketAddr,
        broker: Arc<BrokerHandle>,
        tls_config: &TlsConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let tcp_listener = TcpListener::bind(addr).await?;
        let tls_acceptor = build_tls_acceptor(&tls_config.cert_path, &tls_config.key_path)?;
        tracing::info!(%addr, tls = true, "listening for TLS connections");
        Ok(Self {
            tcp_listener,
            broker,
            tls_acceptor: Some(tls_acceptor),
        })
    }

    /// Get the local address this listener is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.tcp_listener.local_addr().unwrap()
    }

    /// Whether TLS is enabled.
    pub fn is_tls(&self) -> bool {
        self.tls_acceptor.is_some()
    }

    /// Run the accept loop. Spawns a connection handler task per client.
    pub async fn run(self) -> std::io::Result<()> {
        loop {
            let (stream, peer_addr) = self.tcp_listener.accept().await?;
            tracing::debug!(%peer_addr, "accepted connection");

            let broker = self.broker.clone();
            let tls_acceptor = self.tls_acceptor.clone();

            tokio::spawn(async move {
                let result = if let Some(acceptor) = tls_acceptor {
                    match acceptor.accept(stream).await {
                        Ok(tls_stream) => {
                            ConnectionHandler::run_tls(tls_stream, broker, peer_addr).await
                        }
                        Err(e) => {
                            tracing::debug!(%peer_addr, error = %e, "TLS handshake failed");
                            return;
                        }
                    }
                } else {
                    ConnectionHandler::run(stream, broker, peer_addr).await
                };

                if let Err(e) = result {
                    tracing::debug!(%peer_addr, error = %e, "connection closed");
                }
            });
        }
    }
}

fn build_tls_acceptor(
    cert_path: &str,
    key_path: &str,
) -> Result<TlsAcceptor, Box<dyn std::error::Error>> {
    let cert_file = File::open(cert_path)?;
    let key_file = File::open(key_path)?;

    let certs: Vec<_> =
        rustls_pemfile::certs(&mut BufReader::new(cert_file)).collect::<Result<Vec<_>, _>>()?;

    let key = rustls_pemfile::private_key(&mut BufReader::new(key_file))?
        .ok_or("no private key found")?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}
