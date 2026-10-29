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

    fn seeded_hash(key: &Value, seed: u32) -> u64 {
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        seed.hash(&mut hash);
        key.hash(&mut hash);
        hash.finish()
    }

    fn probes(&self, key: &Value) -> (u64, u64) {
        let first = Self::seeded_hash(key, 0);
        let step = if self.hashes == 1 { 0 } else {
            Self::seeded_hash(key, 1).wrapping_sub(first) | 1
        };
        (first, step)
    }

    fn bit(&self, first: u64, step: u64, probe: u32) -> usize {
        (first.wrapping_add(step.wrapping_mul(u64::from(probe))) % self.bits as u64) as usize
    }

    pub fn insert(&mut self, key: &Value) {
        let (first, step) = self.probes(key);
        for probe in 0..self.hashes {
            let bit = self.bit(first, step, probe);
            self.words[bit / 64] |= 1_u64 << (bit % 64);
        }
    }

    pub fn might_contain(&self, key: &Value) -> bool {
        let (first, step) = self.probes(key);
        (0..self.hashes).all(|probe| {
            let bit = self.bit(first, step, probe);
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
mod bloom_double_hash_perf_tests {
    use super::*;
    #[test]
    fn maximum_probe_count_has_no_false_negatives() {
        let mut filter = BloomFilter::new(4096, 16).unwrap();
        for value in 0..1024 { filter.insert(&Value::Integer(value)); }
        for value in 0..1024 { assert!(filter.might_contain(&Value::Integer(value))); }
        let single = BloomFilter::new(4096, 1).unwrap();
        let (first, step) = single.probes(&Value::Text("key".into()));
        assert_eq!(step, 0);
        assert!(single.bit(first, step, 0) < 4096);
    }
}
