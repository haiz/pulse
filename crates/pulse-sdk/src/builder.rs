use std::net::SocketAddr;

use crate::client::Pulse;
use crate::connection::ConnectionManager;
use crate::error::PulseError;

/// Builder for configuring and creating a Pulse client.
///
/// # Example
/// ```no_run
/// # async fn example() -> Result<(), pulse_sdk::PulseError> {
/// let client = pulse_sdk::PulseBuilder::new("order-service", "ecommerce")
///     .addr("127.0.0.1:4222".parse().unwrap())
///     .api_key("psk_live_abc123")
///     .connect()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct PulseBuilder {
    service_id: String,
    namespace: String,
    addr: SocketAddr,
    api_key: String,
    dedup_capacity: usize,
    auto_reconnect: bool,
}

impl PulseBuilder {
    /// Create a new builder with service ID and namespace.
    pub fn new(service_id: &str, namespace: &str) -> Self {
        Self {
            service_id: service_id.to_string(),
            namespace: namespace.to_string(),
            addr: "127.0.0.1:4222".parse().unwrap(),
            api_key: String::new(),
            dedup_capacity: 10_000,
            auto_reconnect: true,
        }
    }

    /// Set the broker address.
    pub fn addr(mut self, addr: SocketAddr) -> Self {
        self.addr = addr;
        self
    }

    /// Set the API key for authentication.
    pub fn api_key(mut self, key: &str) -> Self {
        self.api_key = key.to_string();
        self
    }

    /// Set consumer-side dedup cache capacity (default: 10,000).
    pub fn dedup_capacity(mut self, capacity: usize) -> Self {
        self.dedup_capacity = capacity;
        self
    }

    /// Enable or disable auto-reconnect (default: true).
    pub fn auto_reconnect(mut self, enabled: bool) -> Self {
        self.auto_reconnect = enabled;
        self
    }

    /// Connect to the broker and return a Pulse client.
    pub async fn connect(self) -> Result<Pulse, PulseError> {
        let conn_mgr =
            ConnectionManager::new(self.addr, self.service_id, self.namespace, self.api_key);

        let conn = if self.auto_reconnect {
            conn_mgr.connect_with_retry().await?
        } else {
            conn_mgr.connect().await?
        };

        Ok(Pulse::new(conn, self.dedup_capacity))
    }
}
