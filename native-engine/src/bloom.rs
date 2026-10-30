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
    /// Estimate a Bloom layout for a build-side cardinality and target false-positive rate.
    /// The result is bounded by the same memory and hash-count limits as `new`.
    pub fn for_expected_items(items: usize, false_positive_rate: f64) -> Result<Self> {
        if items == 0 || !false_positive_rate.is_finite()
            || !(0.0..1.0).contains(&false_positive_rate)
        {
            return Err(Error::Execution("invalid Bloom cardinality or false-positive rate".into()));
        }
        let ln2 = std::f64::consts::LN_2;
        let bits = (-(items as f64) * false_positive_rate.ln() / (ln2 * ln2)).ceil();
        if !bits.is_finite() || bits < 1.0 || bits > (1_u64 << 29) as f64 {
            return Err(Error::Execution("Bloom sizing exceeds native memory budget".into()));
        }
        let bits = bits as usize;
        let hashes = ((bits as f64 / items as f64) * ln2).round().clamp(1.0, 16.0) as u32;
        Self::new(bits, hashes)
    }

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
        (first.wrapping_add(u64::from(seed).wrapping_mul(step)) % self.bits as u64) as usize
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

    /// Number of active bits for monitoring filter saturation.
    pub fn bits_set(&self) -> usize {
        self.words.iter().map(|word| word.count_ones() as usize).sum()
    }

    /// Approximate false-positive chance from observed occupancy.
    /// This is an estimate, not a guarantee about a particular key distribution.
    pub fn estimated_false_positive_rate(&self) -> f64 {
        (self.bits_set() as f64 / self.bits as f64).powi(self.hashes as i32)
    }
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
mod bloom_occupancy_metrics_tests {
    use super::*;

    #[test]
    fn estimates_saturation_without_mutating_filter_membership() {
        let mut bloom = BloomFilter::new(4096, 5).unwrap();
        assert_eq!(bloom.bits_set(), 0);
        assert_eq!(bloom.estimated_false_positive_rate(), 0.0);
        for n in 0..100 {
            bloom.insert(&Value::Integer(n));
        }
        let bits = bloom.bits_set();
        assert!(bits > 0 && bits <= 4096);
        let fpr = bloom.estimated_false_positive_rate();
        assert!(fpr > 0.0 && fpr <= 1.0);
        for n in 0..100 {
            assert!(bloom.might_contain(&Value::Integer(n)));
        }
        let copy = bloom.clone();
        bloom.union_with(&copy).unwrap();
        assert_eq!(bloom.bits_set(), bits);
        assert_eq!(bloom.estimated_false_positive_rate(), fpr);
    }
}

#[cfg(test)]
mod cardinality_sizing_tests {
    use super::*;

    #[test]
    fn estimated_layout_retains_all_inserted_keys() {
        let mut bloom = BloomFilter::for_expected_items(1_000, 0.01).unwrap();
        assert!(bloom.byte_size() < 2_000);
        for value in 0..1_000 { bloom.insert(&Value::Integer(value)); }
        for value in 0..1_000 { assert!(bloom.might_contain(&Value::Integer(value))); }
        assert!(bloom.estimated_false_positive_rate() <= 1.0);
    }

    #[test]
    fn invalid_and_unbounded_requests_are_rejected() {
        for fpr in [-0.1, 0.0, 1.0, 2.0, f64::NAN, f64::INFINITY] {
            assert!(BloomFilter::for_expected_items(100, fpr).is_err());
        }
        assert!(BloomFilter::for_expected_items(0, 0.01).is_err());
        assert!(BloomFilter::for_expected_items(usize::MAX, 0.01).is_err());
    }
}
