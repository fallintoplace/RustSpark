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

    fn bit(&self, key: &Value, seed: u32) -> usize {
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        seed.hash(&mut hash);
        key.hash(&mut hash);
        (hash.finish() % self.bits as u64) as usize
    }

    pub fn insert(&mut self, key: &Value) {
        for seed in 0..self.hashes {
            let bit = self.bit(key, seed);
            self.words[bit / 64] |= 1_u64 << (bit % 64);
        }
    }

    pub fn might_contain(&self, key: &Value) -> bool {
        (0..self.hashes).all(|seed| {
            let bit = self.bit(key, seed);
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
