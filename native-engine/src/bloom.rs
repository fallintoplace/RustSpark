//! In-memory Bloom filter for local hash joins and future shuffle pruning.
//! Not a persisted wire format: Rust's std hash implementation may change.
use std::hash::{Hash, Hasher};
use crate::{scalar::Value, Error, Result};

#[derive(Clone, Debug)]
pub struct BloomFilter {
    words: Vec<u64>,
    bits: usize,
    hashes: u32,
}

impl BloomFilter {
    pub fn new(bits: usize, hashes: u32) -> Result<Self> {
        if bits == 0 || bits > 1 << 29 || !(1..=16).contains(&hashes) {
            return Err(Error::Execution("invalid Bloom filter sizing".into()));
        }
        Ok(Self { words: vec![0; bits.div_ceil(64)], bits, hashes })
    }

    fn hash_pair(key: &Value) -> (u64, u64) {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let first = hasher.finish();
        // Make the second step odd to avoid degenerate probe sequences.
        let second = (first.rotate_left(29) ^ 0x9e37_79b9_7f4a_7c15).wrapping_mul(0xbf58_476d_1ce4_e5b9) | 1;
        (first, second)
    }

    fn bit(&self, first: u64, second: u64, probe: u32) -> usize {
        (first.wrapping_add(second.wrapping_mul(u64::from(probe))) % self.bits as u64) as usize
    }

    pub fn insert(&mut self, key: &Value) {
        let (first, second) = Self::hash_pair(key);
        for probe in 0..self.hashes {
            let bit = self.bit(first, second, probe);
            self.words[bit / 64] |= 1_u64 << (bit % 64);
        }
    }

    pub fn might_contain(&self, key: &Value) -> bool {
        let (first, second) = Self::hash_pair(key);
        (0..self.hashes).all(|probe| {
            let bit = self.bit(first, second, probe);
            (self.words[bit / 64] & (1_u64 << (bit % 64))) != 0
        })
    }

    pub fn byte_size(&self) -> usize { self.words.len() * 8 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inserted_values_cannot_be_missing() {
        let mut filter = BloomFilter::new(32_768, 5).unwrap();
        for n in 0..1000 { filter.insert(&Value::Integer(n)); }
        for n in 0..1000 { assert!(filter.might_contain(&Value::Integer(n))); }
        filter.insert(&Value::Float(-0.0));
        assert!(filter.might_contain(&Value::Float(0.0)));
        filter.insert(&Value::Null);
        assert!(filter.might_contain(&Value::Null));
    }

    #[test]
    fn invalid_sizes_are_rejected() {
        assert!(BloomFilter::new(0, 2).is_err());
        assert!(BloomFilter::new(64, 0).is_err());
    }
}

 
impl BloomFilter {
    /// Union compatible filters without losing any previously inserted keys.
    pub fn union_with(&mut self, other: &Self) -> Result<()> {
        if self.bits != other.bits || self.hashes != other.hashes {
            return Err(Error::Execution("cannot merge incompatible Bloom filters".into()));
        }
        for (destination, source) in self.words.iter_mut().zip(&other.words) {
            *destination |= *source;
        }
        Ok(())
    }
}

#[cfg(test)]
mod bloom_union_feature_tests {
    use super::*;

    #[test]
    fn union_preserves_keys_and_rejects_incompatible_layouts() {
        let mut a = BloomFilter::new(2048, 4).unwrap();
        let mut b = BloomFilter::new(2048, 4).unwrap();
        a.insert(&Value::Integer(11));
        b.insert(&Value::Integer(99));
        a.union_with(&b).unwrap();
        assert!(a.might_contain(&Value::Integer(11)));
        assert!(a.might_contain(&Value::Integer(99)));
        assert!(a.union_with(&BloomFilter::new(1024, 4).unwrap()).is_err());
        assert!(a.union_with(&BloomFilter::new(2048, 5).unwrap()).is_err());
    }
}


impl BloomFilter {
    /// Intersect filters with identical layouts. Keys present in both remain
    /// possible matches, with no false negatives for the intersection.
    pub fn intersect_with(&mut self, other: &Self) -> Result<()> {
        if self.bits != other.bits || self.hashes != other.hashes {
            return Err(Error::Execution("cannot intersect incompatible Bloom filters".into()));
        }
        for (destination, source) in self.words.iter_mut().zip(&other.words) {
            *destination &= *source;
        }
        Ok(())
    }
}

#[cfg(test)]
mod bloom_intersection_tests {
    use super::*;

    #[test]
    fn intersection_keeps_common_keys_and_rejects_layout_mismatch() {
        let mut a = BloomFilter::new(8192, 5).unwrap();
        let mut b = BloomFilter::new(8192, 5).unwrap();
        a.insert(&Value::Integer(1));
        a.insert(&Value::Integer(5));
        b.insert(&Value::Integer(5));
        b.insert(&Value::Integer(9));
        a.intersect_with(&b).unwrap();
        assert!(a.might_contain(&Value::Integer(5)));
        assert!(a.intersect_with(&BloomFilter::new(1024, 5).unwrap()).is_err());
    }
}

#[cfg(test)]
mod double_hash_probe_tests {
    use super::*;
    #[test]
    fn all_inserted_keys_survive_different_probe_counts() {
        for hashes in [1, 2, 5, 12, 16] {
            let mut filter = BloomFilter::new(65_537, hashes).unwrap();
            for n in 0..5_000 { filter.insert(&Value::Integer(n)); }
            for n in 0..5_000 { assert!(filter.might_contain(&Value::Integer(n))); }
            filter.insert(&Value::Float(-0.0));
            assert!(filter.might_contain(&Value::Float(0.0)));
        }
    }
}
