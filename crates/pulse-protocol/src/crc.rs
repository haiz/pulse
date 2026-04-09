/// Compute CRC32C (Castagnoli) checksum over the given data.
pub fn compute(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// Verify that the CRC32C checksum matches the expected value.
pub fn verify(data: &[u8], expected: u32) -> bool {
    compute(data) == expected
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_deterministic() {
        let data = b"hello pulse";
        assert_eq!(compute(data), compute(data));
    }

    #[test]
    fn verify_correct() {
        let data = b"test data";
        let checksum = compute(data);
        assert!(verify(data, checksum));
    }

    #[test]
    fn verify_detects_corruption() {
        let data = b"original";
        let checksum = compute(data);
        assert!(!verify(b"corrupted", checksum));
    }

    #[test]
    fn empty_data() {
        let checksum = compute(b"");
        assert_eq!(checksum, 0); // CRC32C of empty data is 0
        assert!(verify(b"", checksum));
    }

    #[test]
    fn single_byte() {
        let a = compute(b"a");
        let b = compute(b"b");
        assert_ne!(a, b);
        assert!(verify(b"a", a));
    }

    #[test]
    fn known_test_vector() {
        // Standard CRC32C test vector: "123456789" → 0xE3069283
        let checksum = compute(b"123456789");
        assert_eq!(checksum, 0xE3069283);
    }

    #[test]
    fn large_data() {
        let data = vec![0xABu8; 1024 * 1024]; // 1 MB
        let checksum = compute(&data);
        assert!(verify(&data, checksum));
        // Flip one bit — should fail
        let mut corrupted = data;
        corrupted[500_000] ^= 0x01;
        assert!(!verify(&corrupted, checksum));
    }

    #[test]
    fn single_bit_flip_detected() {
        let data = b"important message payload";
        let checksum = compute(data);
        for i in 0..data.len() {
            for bit in 0..8u8 {
                let mut corrupted = data.to_vec();
                corrupted[i] ^= 1 << bit;
                assert!(
                    !verify(&corrupted, checksum),
                    "bit flip at byte {i} bit {bit} not detected"
                );
            }
        }
    }
}
