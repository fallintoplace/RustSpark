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

    fn hash_pair(&self, key: &Value) -> (u64, u64) {
        let mut first = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut first);
        let step = if self.hashes > 1 {
            let mut second = std::collections::hash_map::DefaultHasher::new();
            1_u8.hash(&mut second);
            key.hash(&mut second);
            second.finish() | 1
        } else {
            0
        };
        (first.finish(), step)
    }

    fn probe(&self, first: u64, step: u64, seed: u32) -> usize {
        let hash = first.wrapping_add(u64::from(seed).wrapping_mul(step));
        if self.bits.is_power_of_two() {
            (hash as usize) & (self.bits - 1)
        } else {
            (hash % self.bits as u64) as usize
        }
    }

    pub fn insert(&mut self, key: &Value) {
        let (first, step) = self.hash_pair(key);
        for seed in 0..self.hashes {
            let bit = self.probe(first, step, seed);
            self.words[bit / 64] |= 1_u64 << (bit % 64);
        }
    }

    pub fn might_contain(&self, key: &Value) -> bool {
        let (first, step) = self.hash_pair(key);
        (0..self.hashes).all(|seed| {
            let bit = self.probe(first, step, seed);
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
mod bloom_power_of_two_probe_tests {
    use super::*;
    #[test]
    fn masked_and_modulo_sizes_retain_inserted_keys() {
        for bits in [1, 63, 64, 100, 4096] {
            let mut bloom = BloomFilter::new(bits, 7).unwrap();
            for n in 0..100 { bloom.insert(&Value::Integer(n)); }
            for n in 0..100 { assert!(bloom.might_contain(&Value::Integer(n))); }
        }
    }
}
