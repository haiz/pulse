use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// A unique message identifier using UUIDv7 (time-sortable).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId(Uuid);

impl MessageId {
    /// Generate a new time-sortable MessageId (UUIDv7).
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Create a MessageId from raw 16 bytes.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    /// Return the raw 16-byte representation.
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }

    /// Return the underlying UUID.
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MessageId({})", self.0)
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_generates_unique_ids() {
        let a = MessageId::new();
        let b = MessageId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn round_trip_bytes() {
        let id = MessageId::new();
        let bytes = *id.as_bytes();
        let restored = MessageId::from_bytes(bytes);
        assert_eq!(id, restored);
    }

    #[test]
    fn display_format() {
        let id = MessageId::new();
        let s = id.to_string();
        // UUIDv7 format: xxxxxxxx-xxxx-7xxx-xxxx-xxxxxxxxxxxx
        assert_eq!(s.len(), 36);
        assert!(s.contains('-'));
    }

    #[test]
    fn uuidv7_time_ordering() {
        // UUIDv7 embeds timestamp — sequential generation should be monotonically ordered.
        let mut prev = MessageId::new();
        for _ in 0..100 {
            let next = MessageId::new();
            assert!(
                next.as_bytes() >= prev.as_bytes(),
                "UUIDv7 ordering violated: {prev} >= {next}"
            );
            prev = next;
        }
    }

    #[test]
    fn from_zero_bytes() {
        let id = MessageId::from_bytes([0u8; 16]);
        assert_eq!(*id.as_bytes(), [0u8; 16]);
        assert_eq!(id.to_string(), "00000000-0000-0000-0000-000000000000");
    }

    #[test]
    fn from_max_bytes() {
        let id = MessageId::from_bytes([0xFF; 16]);
        assert_eq!(*id.as_bytes(), [0xFF; 16]);
    }

    #[test]
    fn hash_consistent_with_eq() {
        use std::collections::HashMap;

        let id = MessageId::new();
        let id_clone = MessageId::from_bytes(*id.as_bytes());

        let mut map = HashMap::new();
        map.insert(id, "first");
        // Same id (via from_bytes) should find the existing entry
        assert_eq!(map.get(&id_clone), Some(&"first"));
    }

    #[test]
    fn eq_reflexive_and_symmetric() {
        let a = MessageId::new();
        let b = MessageId::from_bytes(*a.as_bytes());
        assert_eq!(a, a); // reflexive
        assert_eq!(a, b); // symmetric
        assert_eq!(b, a);
    }

    #[test]
    fn different_ids_are_not_equal() {
        let ids: Vec<MessageId> = (0..50).map(|_| MessageId::new()).collect();
        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                assert_ne!(ids[i], ids[j]);
            }
        }
    }

    #[test]
    fn debug_format_contains_uuid() {
        let id = MessageId::new();
        let debug = format!("{id:?}");
        assert!(debug.starts_with("MessageId("));
        assert!(debug.ends_with(')'));
    }
}
