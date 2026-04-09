pub mod codec;
pub mod crc;
pub mod frame;
pub mod message_id;
pub mod types;

// Re-exports for convenience
pub use codec::PulseCodec;
pub use frame::{Frame, FrameError};
pub use message_id::MessageId;
pub use types::*;
