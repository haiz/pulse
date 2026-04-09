/// Errors that can occur in the Pulse SDK.
#[derive(Debug, thiserror::Error)]
pub enum PulseError {
    #[error("connection error: {0}")]
    Connection(String),

    #[error("not connected")]
    NotConnected,

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("publish failed: {0}")]
    PublishFailed(String),

    #[error("subscribe failed: {0}")]
    SubscribeFailed(String),

    #[error("timeout")]
    Timeout,

    #[error("broker error ({code}): {message}")]
    BrokerError { code: u32, message: String },

    #[error("serialization error: {0}")]
    Serialize(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("channel closed")]
    ChannelClosed,
}
