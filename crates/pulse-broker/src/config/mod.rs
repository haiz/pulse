use serde::Deserialize;
use std::net::SocketAddr;
use std::path::PathBuf;

use crate::error::BrokerError;

fn default_data_dir() -> PathBuf {
    PathBuf::from("./pulse-data")
}

fn default_listen_addr() -> SocketAddr {
    "0.0.0.0:4222".parse().unwrap()
}

fn default_segment_size() -> u64 {
    67_108_864 // 64 MB
}

fn default_sync_mode() -> String {
    "fsync".into()
}

fn default_wal_shards() -> usize {
    1
}

fn default_max_connections() -> usize {
    5000
}

fn default_max_payload_bytes() -> u32 {
    1_048_576 // 1 MB
}

fn default_keepalive_interval_secs() -> u64 {
    10
}

fn default_keepalive_timeout_secs() -> u64 {
    30
}

fn default_connect_timeout_secs() -> u64 {
    5
}

fn default_durability_mode() -> DurabilityMode {
    DurabilityMode::Balanced
}

fn default_group_commit_interval_ms() -> u64 {
    5
}

fn default_group_commit_max_batch() -> usize {
    100
}

fn default_ack_timeout_secs() -> u64 {
    30
}

fn default_max_redeliveries() -> u32 {
    5
}

fn default_backoff_initial_secs() -> u64 {
    1
}

fn default_backoff_max_secs() -> u64 {
    60
}

fn default_backoff_multiplier() -> f64 {
    2.0
}

/// Durability mode for event persistence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DurabilityMode {
    /// No WAL. In-memory only. ~800K msg/sec.
    Memory,
    /// Async WAL with group commit (fsync every 5ms). ~100K msg/sec.
    #[default]
    Balanced,
    /// Per-event fsync. Exactly-once. ~10K msg/sec.
    Durable,
}

/// TLS configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct TlsConfig {
    pub cert_path: String,
    pub key_path: String,
}

/// Metrics configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    #[serde(default = "default_metrics_enabled")]
    pub enabled: bool,
    #[serde(default = "default_metrics_addr")]
    pub listen_addr: SocketAddr,
}

fn default_metrics_enabled() -> bool {
    true
}

fn default_metrics_addr() -> SocketAddr {
    "0.0.0.0:9090".parse().unwrap()
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: default_metrics_enabled(),
            listen_addr: default_metrics_addr(),
        }
    }
}

/// Top-level broker configuration (loaded from broker.yaml).
#[derive(Debug, Clone, Deserialize)]
pub struct BrokerConfig {
    #[serde(default = "default_listen_addr")]
    pub listen_addr: SocketAddr,

    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,

    #[serde(default)]
    pub tls: Option<TlsConfig>,

    #[serde(default)]
    pub wal: WalConfig,

    #[serde(default)]
    pub durability: DurabilityConfig,

    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    #[serde(default = "default_max_payload_bytes")]
    pub max_payload_bytes: u32,

    #[serde(default = "default_keepalive_interval_secs")]
    pub keepalive_interval_secs: u64,

    #[serde(default = "default_keepalive_timeout_secs")]
    pub keepalive_timeout_secs: u64,

    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,

    #[serde(default)]
    pub delivery: DeliveryConfig,

    #[serde(default)]
    pub metrics: MetricsConfig,
}

/// WAL-specific configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct WalConfig {
    #[serde(default = "default_segment_size")]
    pub segment_size_bytes: u64,
    #[serde(default = "default_sync_mode")]
    pub sync_mode: String,
    #[serde(default = "default_wal_shards")]
    pub shards: usize,
}

/// Durability mode configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct DurabilityConfig {
    #[serde(default = "default_durability_mode")]
    pub mode: DurabilityMode,
    #[serde(default = "default_group_commit_interval_ms")]
    pub group_commit_interval_ms: u64,
    #[serde(default = "default_group_commit_max_batch")]
    pub group_commit_max_batch: usize,
}

/// Delivery and retry configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct DeliveryConfig {
    #[serde(default = "default_ack_timeout_secs")]
    pub ack_timeout_secs: u64,
    #[serde(default = "default_max_redeliveries")]
    pub max_redeliveries: u32,
    #[serde(default)]
    pub backoff: BackoffConfig,
}

/// Exponential backoff configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct BackoffConfig {
    #[serde(default = "default_backoff_initial_secs")]
    pub initial_secs: u64,
    #[serde(default = "default_backoff_max_secs")]
    pub max_secs: u64,
    #[serde(default = "default_backoff_multiplier")]
    pub multiplier: f64,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            segment_size_bytes: default_segment_size(),
            sync_mode: default_sync_mode(),
            shards: default_wal_shards(),
        }
    }
}

impl Default for DurabilityConfig {
    fn default() -> Self {
        Self {
            mode: default_durability_mode(),
            group_commit_interval_ms: default_group_commit_interval_ms(),
            group_commit_max_batch: default_group_commit_max_batch(),
        }
    }
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            ack_timeout_secs: default_ack_timeout_secs(),
            max_redeliveries: default_max_redeliveries(),
            backoff: BackoffConfig::default(),
        }
    }
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial_secs: default_backoff_initial_secs(),
            max_secs: default_backoff_max_secs(),
            multiplier: default_backoff_multiplier(),
        }
    }
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            listen_addr: default_listen_addr(),
            data_dir: default_data_dir(),
            tls: None,
            wal: WalConfig::default(),
            durability: DurabilityConfig::default(),
            max_connections: default_max_connections(),
            max_payload_bytes: default_max_payload_bytes(),
            keepalive_interval_secs: default_keepalive_interval_secs(),
            keepalive_timeout_secs: default_keepalive_timeout_secs(),
            connect_timeout_secs: default_connect_timeout_secs(),
            delivery: DeliveryConfig::default(),
            metrics: MetricsConfig::default(),
        }
    }
}

impl BrokerConfig {
    /// Load config from a YAML file.
    pub fn load(path: &str) -> Result<Self, BrokerError> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| BrokerError::Config(format!("failed to read {path}: {e}")))?;
        let config: Self = serde_yaml::from_str(&contents)
            .map_err(|e| BrokerError::Config(format!("failed to parse {path}: {e}")))?;
        Ok(config)
    }

    /// Create a config suitable for testing (sync_mode: "none", temp data dir).
    pub fn for_testing(tmp_dir: PathBuf) -> Self {
        Self {
            data_dir: tmp_dir,
            wal: WalConfig {
                segment_size_bytes: default_segment_size(),
                sync_mode: "none".into(),
                shards: default_wal_shards(),
            },
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn defaults_are_sane() {
        let config = BrokerConfig::default();
        assert_eq!(config.data_dir, PathBuf::from("./pulse-data"));
        assert_eq!(config.wal.segment_size_bytes, 67_108_864);
        assert_eq!(config.wal.sync_mode, "fsync");
        assert_eq!(config.listen_addr.port(), 4222);
        assert_eq!(config.max_connections, 5000);
        assert_eq!(config.max_payload_bytes, 1_048_576);
        assert_eq!(config.durability.mode, DurabilityMode::Balanced);
    }

    #[test]
    fn parse_minimal_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("broker.yaml");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "data_dir: /tmp/pulse-test").unwrap();

        let config = BrokerConfig::load(path.to_str().unwrap()).unwrap();
        assert_eq!(config.data_dir, PathBuf::from("/tmp/pulse-test"));
        // Defaults kick in for wal
        assert_eq!(config.wal.segment_size_bytes, 67_108_864);
        assert_eq!(config.wal.sync_mode, "fsync");
    }

    #[test]
    fn parse_full_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("broker.yaml");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(
            f,
            r#"data_dir: /data/pulse
listen_addr: "127.0.0.1:5222"
max_connections: 1000
durability:
  mode: "durable"
wal:
  segment_size_bytes: 1048576
  sync_mode: "none""#
        )
        .unwrap();

        let config = BrokerConfig::load(path.to_str().unwrap()).unwrap();
        assert_eq!(config.data_dir, PathBuf::from("/data/pulse"));
        assert_eq!(config.wal.segment_size_bytes, 1_048_576);
        assert_eq!(config.wal.sync_mode, "none");
        assert_eq!(config.listen_addr.port(), 5222);
        assert_eq!(config.max_connections, 1000);
        assert_eq!(config.durability.mode, DurabilityMode::Durable);
    }

    #[test]
    fn invalid_yaml_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.yaml");
        std::fs::write(&path, "{{{{not valid yaml").unwrap();

        let result = BrokerConfig::load(path.to_str().unwrap());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("failed to parse"));
    }

    #[test]
    fn missing_file_returns_error() {
        let result = BrokerConfig::load("/nonexistent/broker.yaml");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("failed to read"));
    }

    #[test]
    fn for_testing_uses_none_sync() {
        let dir = tempfile::tempdir().unwrap();
        let config = BrokerConfig::for_testing(dir.path().to_path_buf());
        assert_eq!(config.wal.sync_mode, "none");
    }

    #[test]
    fn durability_mode_deserialize() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("broker.yaml");

        for mode in ["memory", "balanced", "durable"] {
            let mut f = std::fs::File::create(&path).unwrap();
            writeln!(f, "durability:\n  mode: \"{mode}\"").unwrap();
            let config = BrokerConfig::load(path.to_str().unwrap()).unwrap();
            match mode {
                "memory" => assert_eq!(config.durability.mode, DurabilityMode::Memory),
                "balanced" => assert_eq!(config.durability.mode, DurabilityMode::Balanced),
                "durable" => assert_eq!(config.durability.mode, DurabilityMode::Durable),
                _ => unreachable!(),
            }
        }
    }
}
