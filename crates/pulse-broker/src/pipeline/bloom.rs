use std::hash::{Hash, Hasher};

/// A simple Bloom filter for deduplication.
///
/// Uses double hashing (two hash functions derived from a single u128 hash)
/// to produce k hash positions.
pub struct BloomFilter {
    bits: Vec<u64>,
    num_bits: usize,
    num_hashes: u32,
    count: usize,
}

impl BloomFilter {
    /// Create a new bloom filter with the given capacity and false positive rate.
    ///
    /// `capacity`: expected number of elements
    /// `fp_rate`: desired false positive rate (e.g., 0.001 for 0.1%)
    pub fn new(capacity: usize, fp_rate: f64) -> Self {
        let num_bits = optimal_num_bits(capacity, fp_rate);
        let num_hashes = optimal_num_hashes(num_bits, capacity);
        let num_words = num_bits.div_ceil(64);

        Self {
            bits: vec![0u64; num_words],
            num_bits,
            num_hashes,
            count: 0,
        }
    }

    /// Insert an item into the filter.
    pub fn insert<T: Hash>(&mut self, item: &T) {
        let (h1, h2) = double_hash(item);
        for i in 0..self.num_hashes {
            let pos = combined_hash(h1, h2, i) % self.num_bits as u64;
            let word = pos as usize / 64;
            let bit = pos as usize % 64;
            self.bits[word] |= 1u64 << bit;
        }
        self.count += 1;
    }

    /// Check if an item may be in the filter.
    /// Returns `false` if definitely not present, `true` if possibly present.
    pub fn may_contain<T: Hash>(&self, item: &T) -> bool {
        let (h1, h2) = double_hash(item);
        for i in 0..self.num_hashes {
            let pos = combined_hash(h1, h2, i) % self.num_bits as u64;
            let word = pos as usize / 64;
            let bit = pos as usize % 64;
            if self.bits[word] & (1u64 << bit) == 0 {
                return false;
            }
        }
        true
    }

    /// Number of items inserted.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Reset the filter, clearing all bits.
    pub fn clear(&mut self) {
        self.bits.fill(0);
        self.count = 0;
    }
}

fn double_hash<T: Hash>(item: &T) -> (u64, u64) {
    let mut hasher = std::hash::DefaultHasher::new();
    item.hash(&mut hasher);
    let h1 = hasher.finish();

    // Second hash: feed h1 back through another hasher with a different seed
    let mut hasher2 = std::hash::DefaultHasher::new();
    h1.hash(&mut hasher2);
    0xDEADBEEFu64.hash(&mut hasher2);
    let h2 = hasher2.finish();

    (h1, h2)
}

fn combined_hash(h1: u64, h2: u64, i: u32) -> u64 {
    h1.wrapping_add((i as u64).wrapping_mul(h2))
}

fn optimal_num_bits(n: usize, fp: f64) -> usize {
    let ln2_sq = std::f64::consts::LN_2 * std::f64::consts::LN_2;
    let m = -(n as f64 * fp.ln()) / ln2_sq;
    m.ceil() as usize
}

fn optimal_num_hashes(m: usize, n: usize) -> u32 {
    let k = (m as f64 / n as f64) * std::f64::consts::LN_2;
    std::cmp::max(1, k.round() as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_check() {
        let mut bf = BloomFilter::new(1000, 0.001);
        bf.insert(&"hello");
        assert!(bf.may_contain(&"hello"));
        assert!(!bf.may_contain(&"world")); // very likely false
    }

    #[test]
    fn empty_filter_returns_false() {
        let bf = BloomFilter::new(1000, 0.01);
        assert!(!bf.may_contain(&42u64));
    }

    #[test]
    fn false_positive_rate_is_reasonable() {
        let n = 10_000;
        let mut bf = BloomFilter::new(n, 0.01);

        // Insert n items
        for i in 0..n {
            bf.insert(&i);
        }

        // Check all inserted items are found
        for i in 0..n {
            assert!(bf.may_contain(&i));
        }

        // Check false positive rate on non-inserted items
        let test_count = 10_000;
        let mut false_positives = 0;
        for i in n..(n + test_count) {
            if bf.may_contain(&i) {
                false_positives += 1;
            }
        }

        let fp_rate = false_positives as f64 / test_count as f64;
        // Allow up to 3% (generous margin over target 1%)
        assert!(fp_rate < 0.03, "false positive rate too high: {fp_rate:.4}");
    }

    #[test]
    fn clear_resets_filter() {
        let mut bf = BloomFilter::new(100, 0.01);
        bf.insert(&"test");
        assert!(bf.may_contain(&"test"));

        bf.clear();
        assert!(!bf.may_contain(&"test"));
        assert_eq!(bf.count(), 0);
    }

    #[test]
    fn count_tracks_inserts() {
        let mut bf = BloomFilter::new(100, 0.01);
        assert_eq!(bf.count(), 0);
        bf.insert(&1u64);
        bf.insert(&2u64);
        assert_eq!(bf.count(), 2);
    }
}
