/// Errors that can occur within the broker.
#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    #[error("config error: {0}")]
    Config(String),

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("WAL corrupt record at segment {segment}, offset {offset}")]
    WalCorrupt { segment: u32, offset: u64 },

    #[error("storage error: {0}")]
    Storage(#[from] sled::Error),

    #[error("serialization error: {0}")]
    Serialize(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("connection error: {0}")]
    Connection(String),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("server overloaded: {0}")]
    Overloaded(String),
}
