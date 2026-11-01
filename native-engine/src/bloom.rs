//! In-memory Bloom filter for local hash joins and future shuffle pruning.
//! Not a persisted wire format: Rust's std hash implementation may change.
use std::hash::{Hash, Hasher};
use crate::{scalar::Value, Error, Result};

#[derive(Clone, Debug)]
pub struct BloomFilter {
    words: Vec<u64>,
    bits: usize,
    hashes: u32,
    populated: bool,
    occupied_bits: usize,
}

impl BloomFilter {
    fn allocate_words(count: usize) -> Result<Vec<u64>> {
        // A valid filter layout can still exceed the host's available RAM.
        // Fail with a native execution error instead of a capacity panic or
        // allocator abort on the exchange/join admission path.
        let mut words = Vec::new();
        words.try_reserve_exact(count)
            .map_err(|_| Error::Execution("Bloom filter bitset allocation failed".into()))?;
        words.resize(count, 0);
        Ok(words)
    }

    fn bit_position(bit: usize) -> (usize, u64) {
        (bit / 64, 1_u64 << (bit % 64))
    }

    /// Size a join filter from its expected key count and target false-positive
    /// probability. Reject unachievable layouts rather than silently clipping
    /// the hashing or bitset limits, which would promise a false FPR.
    pub fn for_expected_items(expected_items: usize, target_fpr: f64) -> Result<Self> {
        if expected_items == 0 || !target_fpr.is_finite() ||
            !(0.0 < target_fpr && target_fpr < 1.0)
        {
            return Err(Error::Execution("invalid Bloom filter capacity or target false-positive rate".into()));
        }
        let ln2 = std::f64::consts::LN_2;
        let bits = -(expected_items as f64) * target_fpr.ln() / (ln2 * ln2);
        if !bits.is_finite() || bits < 1.0 || bits > (1usize << 29) as f64 {
            return Err(Error::Execution("Bloom filter target exceeds bitset budget".into()));
        }
        let bits = bits.ceil() as usize;
        let optimal_hashes = (bits as f64 / expected_items as f64) * ln2;
        if !optimal_hashes.is_finite() || optimal_hashes > 16.5 {
            return Err(Error::Execution("Bloom filter target exceeds hash probe budget".into()));
        }
        let hashes = optimal_hashes.round().clamp(1.0, 16.0) as u32;
        Self::new(bits, hashes)
    }

    pub fn new(bits: usize, hashes: u32) -> Result<Self> {
        if bits == 0 || bits > 1 << 29 || !(1..=16).contains(&hashes) {
            return Err(Error::Execution("invalid Bloom filter sizing".into()));
        }
        Ok(Self { words: Self::allocate_words(bits.div_ceil(64))?,
            bits, hashes, populated: false, occupied_bits: 0 })
    }

    // Double hashing needs a stride coprime to the bit count. An odd stride
    // is sufficient for powers of two but can repeat probes on other layouts.
    fn coprime_stride(&self, mut step: u64) -> u64 {
        if self.bits.is_power_of_two() { return step; }
        let bits = self.bits as u64;
        loop {
            let (mut left, mut right) = (step % bits, bits);
            while right != 0 {
                let remainder = left % right;
                left = right;
                right = remainder;
            }
            if left == 1 { return step; }
            step = step.wrapping_add(2);
        }
    }

    fn hash_pair(&self, key: &Value) -> (u64, u64) {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let first = hasher.finish();
        // The Bloom layout is only local, never serialized. Derive the
        // second probe stride with an avalanche mix rather than hashing the
        // entire key twice, especially expensive for large text join keys.
        // The odd stride visits independent positions in power-of-two tables.
        let step = if self.hashes > 1 {
            let mut mixed = first.wrapping_add(0x9e37_79b9_7f4a_7c15);
            mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            self.coprime_stride((mixed ^ (mixed >> 31)) | 1)
        } else {
            0
        };
        (first, step)
    }

    fn probe(&self, first: u64, step: u64, seed: u32) -> usize {
        let hash = first.wrapping_add(u64::from(seed).wrapping_mul(step));
        // Power-of-two bitsets are common for join filters. Masking avoids
        // a variable integer division for every Bloom probe.
        let bit = if self.bits.is_power_of_two() {
            hash & (self.bits as u64 - 1)
        } else {
            hash % self.bits as u64
        };
        bit as usize
    }

    pub fn insert(&mut self, key: &Value) {
        self.populated = true;
        if self.occupied_bits == self.bits { return; }
        let (first, step) = self.hash_pair(key);
        for seed in 0..self.hashes {
            let bit = self.probe(first, step, seed);
            let (word, mask) = Self::bit_position(bit);
            if self.words[word] & mask == 0 {
                self.words[word] |= mask;
                self.occupied_bits += 1;
            }
        }
    }

    pub fn might_contain(&self, key: &Value) -> bool {
        if !self.populated { return false; }
        if self.occupied_bits == self.bits { return true; }
        let (first, step) = self.hash_pair(key);
        (0..self.hashes).all(|seed| {
            let bit = self.probe(first, step, seed);
            let (word, mask) = Self::bit_position(bit);
            (self.words[word] & mask) != 0
        })
    }

    /// Admit a native column's keys without materializing row tuples.
    pub fn insert_batch(&mut self, keys: &[Value]) {
        // Inserting into a saturated filter cannot change membership.
        if self.occupied_bits == self.bits && self.populated { return; }
        // Sorted join partitions often contain long runs of the same key.
        // Equality uses the same canonical NaN / signed-zero semantics as Hash.
        let mut previous: Option<&Value> = None;
        for key in keys {
            if previous != Some(key) {
                self.insert(key);
                if self.occupied_bits == self.bits { break; }
            }
            previous = Some(key);
        }
    }

    /// Build from a selection vector without gathering or cloning the
    /// original key column. Invalid indices fail before any mutation.
    pub fn insert_selected(&mut self, keys: &[Value], selected: &[usize]) -> Result<()> {
        if selected.iter().any(|&index| index >= keys.len()) {
            return Err(Error::Execution("selected Bloom row out of bounds".into()));
        }
        if self.occupied_bits == self.bits && self.populated { return Ok(()); }
        let mut previous: Option<&Value> = None;
        for &index in selected {
            let key = &keys[index];
            if previous != Some(key) {
                self.insert(key);
                if self.occupied_bits == self.bits { break; }
            }
            previous = Some(key);
        }
        Ok(())
    }

    /// Return one membership bit per input key in the original row order.
    /// A true bit means a possible match, never a confirmed exact match.
    pub fn probe_batch(&self, keys: &[Value]) -> Vec<bool> {
        if !self.populated { return vec![false; keys.len()]; }
        if self.occupied_bits == self.bits { return vec![true; keys.len()]; }
        // Reuse the exact result across adjacent equal values. Do not infer
        // membership from equality alone: a Bloom hit can be a false positive.
        let mut output = Vec::with_capacity(keys.len());
        let mut previous: Option<&Value> = None;
        let mut previous_result = false;
        for key in keys {
            if previous != Some(key) {
                previous_result = self.might_contain(key);
            }
            output.push(previous_result);
            previous = Some(key);
        }
        output
    }

    pub fn byte_size(&self) -> usize { self.words.len() * 8 }

    /// Number of active bits for monitoring filter saturation.
    pub fn bits_set(&self) -> usize { self.occupied_bits }

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
        // An unpopulated filter has no set bits. Avoid visiting large
        // bitsets for the common case of an empty build-side partition.
        if !other.populated || self.occupied_bits == self.bits { return Ok(()); }
        if !self.populated {
            self.words.copy_from_slice(&other.words);
            self.occupied_bits = other.occupied_bits;
            self.populated = true;
            return Ok(());
        }
        for (destination, source) in self.words.iter_mut().zip(&other.words) {
            let before = *destination;
            *destination |= *source;
            self.occupied_bits += (*destination ^ before).count_ones() as usize;
        }
        // An empty destination must become queryable after unioning a
        // populated filter. Otherwise might_contain returns false for all keys.
        self.populated |= other.populated;
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
        if !self.populated || other.occupied_bits == other.bits { return Ok(()); }
        if !other.populated {
            self.words.fill(0);
            self.occupied_bits = 0;
            self.populated = false;
            return Ok(());
        }
        // AND with a saturated bitset is identity. If only the target is
        // saturated, copying the source avoids one popcount per word.
        if other.occupied_bits == other.bits { return Ok(()); }
        if self.occupied_bits == self.bits {
            self.words.copy_from_slice(&other.words);
            self.occupied_bits = other.occupied_bits;
            return Ok(());
        }
        for (destination, source) in self.words.iter_mut().zip(&other.words) {
            let before = *destination;
            *destination &= *source;
            self.occupied_bits -= (before ^ *destination).count_ones() as usize;
        }
        self.populated &= other.populated;
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
mod power_of_two_bloom_probe_tests {
    use super::*;

    #[test]
    fn masked_and_modulo_probes_agree_for_all_hash_seeds() {
        for bits in [1, 2, 63, 64, 65, 128, 1024, 2048] {
            let mut filter = BloomFilter::new(bits, 7).unwrap();
            for n in 0..128 { filter.insert(&Value::Integer(n)); }
            for n in 0..128 { assert!(filter.might_contain(&Value::Integer(n))); }
            let first = u64::MAX - 2;
            let step = u64::MAX - 4;
            for seed in 0..7 {
                let expected = (first.wrapping_add(u64::from(seed).wrapping_mul(step))
                    % bits as u64) as usize;
                assert_eq!(filter.probe(first, step, seed), expected);
            }
        }
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
mod native_bloom_batch_operator_tests {
    use super::*;

    #[test]
    fn bulk_membership_preserves_input_order_and_sql_key_hashes() {
        let keys = vec![
            Value::Integer(1), Value::Integer(2), Value::Null,
            Value::Float(-0.0), Value::Integer(2),
        ];
        let mut bloom = BloomFilter::new(65_536, 5).unwrap();
        bloom.insert_batch(&keys);
        assert_eq!(bloom.probe_batch(&keys), vec![true; keys.len()]);
        assert!(bloom.might_contain(&Value::Float(0.0)));
        assert_eq!(bloom.probe_batch(&[]), Vec::<bool>::new());
        let empty = BloomFilter::new(65_536, 5).unwrap();
        assert_eq!(empty.probe_batch(&keys), vec![false; keys.len()]);
    }
}

#[cfg(test)]
mod empty_bloom_probe_fast_path_tests {
    use super::*;
    #[test]
    fn empty_filters_reject_keys_without_hashing_until_inserted() {
        let mut filter = BloomFilter::new(1 << 16, 5).unwrap();
        let key = Value::Text("é🙂".repeat(50_000));
        assert!(!filter.might_contain(&key));
        assert_eq!(filter.probe_batch(&[key.clone(), Value::Null]), vec![false, false]);
        filter.insert_batch(&[]);
        assert!(!filter.might_contain(&key));
        filter.insert(&key);
        assert!(filter.might_contain(&key));
        assert!(filter.bits_set() > 0);
        assert!(filter.clone().might_contain(&key));
    }
}

#[cfg(test)]
mod bloom_empty_merge_regressions {
    use super::*;

    #[test]
    fn union_into_fresh_filter_preserves_all_inserted_keys() {
        let mut empty = BloomFilter::new(8192, 5).unwrap();
        let mut populated = BloomFilter::new(8192, 5).unwrap();
        let values = [Value::Integer(7), Value::Float(-0.0), Value::Null];
        populated.insert_batch(&values);
        empty.union_with(&populated).unwrap();
        assert!(values.iter().all(|value| empty.might_contain(value)));
        assert!(empty.might_contain(&Value::Float(0.0)));
        let before = empty.bits_set();
        empty.union_with(&BloomFilter::new(8192, 5).unwrap()).unwrap();
        assert_eq!(empty.bits_set(), before);
    }

    #[test]
    fn intersecting_with_empty_filter_clears_membership() {
        let mut populated = BloomFilter::new(8192, 5).unwrap();
        populated.insert(&Value::Integer(7));
        populated.intersect_with(&BloomFilter::new(8192, 5).unwrap()).unwrap();
        assert_eq!(populated.bits_set(), 0);
        assert!(!populated.might_contain(&Value::Integer(7)));
        populated.insert(&Value::Integer(9));
        assert!(populated.might_contain(&Value::Integer(9)));
    }
}

#[cfg(test)]
mod cached_bloom_occupancy_regressions {
    use super::*;

    fn verify(filter: &BloomFilter) {
        assert_eq!(filter.bits_set(), filter.words.iter()
            .map(|word| word.count_ones() as usize).sum::<usize>());
    }

    #[test]
    fn occupancy_survives_duplicate_inserts_and_set_operations() {
        let mut first = BloomFilter::new(513, 7).unwrap();
        let mut second = BloomFilter::new(513, 7).unwrap();
        for n in 0..300 {
            first.insert(&Value::Integer(n));
            first.insert(&Value::Integer(n));
            second.insert(&Value::Integer(n + 100));
        }
        verify(&first);
        verify(&second);
        first.union_with(&second).unwrap();
        verify(&first);
        let merged = first.clone();
        first.intersect_with(&second).unwrap();
        verify(&first);
        assert!(first.bits_set() <= merged.bits_set());
        first.intersect_with(&BloomFilter::new(513, 7).unwrap()).unwrap();
        assert_eq!(first.bits_set(), 0);
        first.insert(&Value::Integer(5));
        verify(&first);
    }
}

#[cfg(test)]
mod single_hash_double_probing_tests {
    use super::*;

    #[test]
    fn derived_stride_is_odd_and_null_numeric_and_unicode_membership_is_exact() {
        for bits in [1, 63, 64, 65, 2048, 8191, 65_536] {
            for hashes in [1, 2, 5, 16] {
                let mut filter = BloomFilter::new(bits, hashes).unwrap();
                let values = vec![
                    Value::Null,
                    Value::Integer(i64::MIN),
                    Value::Integer(i64::MAX),
                    Value::Float(-0.0),
                    Value::Float(f64::NAN),
                    Value::Text("é🙂東京".repeat(4096)),
                ];
                for value in &values {
                    let (_, step) = filter.hash_pair(value);
                    if hashes == 1 { assert_eq!(step, 0); }
                    else { assert_eq!(step & 1, 1); }
                    filter.insert(value);
                }
                assert!(values.iter().all(|value| filter.might_contain(value)));
                assert!(filter.might_contain(&Value::Float(0.0)));
                assert!(filter.might_contain(&Value::Float(-f64::NAN)));
                let mut other = BloomFilter::new(bits, hashes).unwrap();
                other.insert(&Value::Text("another".into()));
                filter.union_with(&other).unwrap();
                assert!(filter.might_contain(&Value::Text("another".into())));
                assert!(values.iter().all(|value| filter.might_contain(value)));
            }
        }
    }

    #[test]
    fn large_text_keys_hash_once_for_multiple_bloom_probes() {
        let filter = BloomFilter::new(65_536, 7).unwrap();
        let text = Value::Text("wide-join-payload".repeat(8192));
        let (first, step) = filter.hash_pair(&text);
        let mut direct = std::collections::hash_map::DefaultHasher::new();
        text.hash(&mut direct);
        assert_eq!(first, direct.finish());
        assert_eq!(step & 1, 1);
        for seed in 0..7 {
            assert!(filter.probe(first, step, seed) < 65_536);
        }
    }
}

#[cfg(test)]
mod empty_bloom_set_operation_shortcuts {
    use super::*;

    #[test]
    fn empty_merges_match_reference_across_large_and_tail_bitsets() {
        for bits in [65usize, 65_536, 1 << 20] {
            let mut full = BloomFilter::new(bits, 7).unwrap();
            let empty = BloomFilter::new(bits, 7).unwrap();
            for n in 0..100 { full.insert(&Value::Integer(n)); }
            let expected = full.clone();
            full.union_with(&empty).unwrap();
            assert_eq!(full.words, expected.words);
            assert_eq!(full.bits_set(), expected.bits_set());
            let mut combined = empty.clone();
            combined.union_with(&full).unwrap();
            assert_eq!(combined.words, full.words);
            assert_eq!(combined.bits_set(), full.bits_set());
            assert!(combined.might_contain(&Value::Integer(50)));
            combined.intersect_with(&empty).unwrap();
            assert_eq!(combined.bits_set(), 0);
            assert!(!combined.might_contain(&Value::Integer(50)));
            combined.intersect_with(&full).unwrap();
            assert_eq!(combined.bits_set(), 0);
            combined.insert(&Value::Integer(51));
            assert!(combined.might_contain(&Value::Integer(51)));
        }
    }
}

#[cfg(test)]
mod saturated_filter_fast_path_tests {
    use super::*;

    #[test]
    fn fully_occupied_filters_accept_all_probes_and_preserve_accounting() {
        let mut filter = BloomFilter::new(1, 8).unwrap();
        assert!(!filter.might_contain(&Value::Null));
        filter.insert(&Value::Integer(1));
        assert_eq!(filter.bits_set(), 1);
        for key in [
            Value::Integer(-1234),
            Value::Text("large-key".repeat(1024)),
            Value::Null,
            Value::Float(123.5),
        ] {
            assert!(filter.might_contain(&key));
            filter.insert(&key);
            assert_eq!(filter.bits_set(), 1);
        }
        let empty = BloomFilter::new(1, 8).unwrap();
        filter.intersect_with(&empty).unwrap();
        assert_eq!(filter.bits_set(), 0);
        assert!(!filter.might_contain(&Value::Integer(1)));
    }
}

#[cfg(test)]
mod saturated_bloom_shortcut_regressions {
    use super::*;

    #[test]
    fn saturated_filters_admit_every_probe_without_changing_occupancy() {
        for bits in [1, 2, 63, 64, 65] {
            let mut filter = BloomFilter::new(bits, 7).unwrap();
            for n in 0..10_000 {
                filter.insert(&Value::Integer(n));
                if filter.bits_set() == bits { break; }
            }
            if filter.bits_set() != bits { continue; }
            let unseen = Value::Text("new probe".into());
            assert!(filter.might_contain(&unseen));
            assert_eq!(filter.probe_batch(&[unseen.clone(), Value::Null]), vec![true, true]);
            filter.insert(&unseen);
            assert_eq!(filter.bits_set(), bits);
            let mut other = BloomFilter::new(bits, 7).unwrap();
            other.insert(&Value::Integer(123));
            filter.intersect_with(&other).unwrap();
            assert!(filter.might_contain(&Value::Integer(123)));
            assert_eq!(filter.bits_set(), other.bits_set());
        }
    }
}

#[cfg(test)]
mod saturated_bloom_bulk_operations_tests {
    use super::*;

    fn saturated(bits: usize, hashes: u32) -> BloomFilter {
        let mut filter = BloomFilter::new(bits, hashes).unwrap();
        for word in &mut filter.words { *word = u64::MAX; }
        filter.populated = true;
        filter.occupied_bits = bits;
        filter
    }

    #[test]
    fn union_and_inserts_leave_saturated_filter_unchanged() {
        let mut all = saturated(8192, 5);
        let mut other = BloomFilter::new(8192, 5).unwrap();
        other.insert(&Value::Text("data".repeat(5000)));
        let original = all.words.clone();
        all.insert_batch(&(0..250).map(Value::Integer).collect::<Vec<_>>());
        all.union_with(&other).unwrap();
        assert_eq!(all.words, original);
        assert_eq!(all.bits_set(), 8192);
        assert!(all.might_contain(&Value::Integer(99)));
    }

    #[test]
    fn intersection_with_full_filter_is_identity_and_from_full_copies_source() {
        let full = saturated(8192, 5);
        let mut partial = BloomFilter::new(8192, 5).unwrap();
        partial.insert(&Value::Text("🙂".into()));
        partial.insert(&Value::Integer(777));
        let before = partial.words.clone();
        partial.intersect_with(&full).unwrap();
        assert_eq!(partial.words, before);
        let mut destination = full;
        destination.intersect_with(&partial).unwrap();
        assert_eq!(destination.words, before);
        assert_eq!(destination.bits_set(), partial.bits_set());
        for value in [Value::Integer(777), Value::Text("🙂".into())] {
            assert!(destination.might_contain(&value));
        }
    }
}

#[cfg(test)]
mod saturated_batch_short_circuit_tests {
    use super::*;

    #[test]
    fn fully_occupied_bitset_keeps_all_memberships_and_metrics() {
        let mut filter = BloomFilter::new(1, 3).unwrap();
        filter.insert_batch(&[Value::Integer(123)]);
        assert_eq!(filter.bits_set(), 1);
        let existing = filter.clone();
        let large = (0..20_000).map(|n| Value::Text(n.to_string())).collect::<Vec<_>>();
        filter.insert_batch(&large);
        assert_eq!(filter.bits_set(), existing.bits_set());
        assert!(large.iter().all(|value| filter.might_contain(value)));
        assert_eq!(filter.probe_batch(&large), vec![true; large.len()]);

        let mut partial = BloomFilter::new(1024, 3).unwrap();
        let keys = (0..100).map(Value::Integer).collect::<Vec<_>>();
        partial.insert_batch(&keys);
        assert!(keys.iter().all(|key| partial.might_contain(key)));
    }
}

#[cfg(test)]
mod saturated_bloom_intersection_regressions {
    use super::*;

    #[test]
    fn intersecting_with_a_saturated_source_preserves_membership_and_counts() {
        let mut partial = BloomFilter::new(64, 5).unwrap();
        partial.insert(&Value::Integer(23));
        let before = partial.clone();
        let mut full = BloomFilter::new(64, 5).unwrap();
        for n in 0..10_000 {
            if full.bits_set() == 64 { break; }
            full.insert(&Value::Integer(n));
        }
        assert_eq!(full.bits_set(), 64);
        partial.intersect_with(&full).unwrap();
        assert_eq!(partial.words, before.words);
        assert_eq!(partial.bits_set(), before.bits_set());
        assert!(partial.might_contain(&Value::Integer(23)));
        assert!(partial.intersect_with(&BloomFilter::new(128, 5).unwrap()).is_err());
    }
}

#[cfg(test)]
mod saturated_bloom_union_regressions {
    use super::*;

    #[test]
    fn saturated_union_is_identity_and_still_validates_layout() {
        let mut full = BloomFilter::new(64, 5).unwrap();
        for n in 0..10_000 {
            if full.bits_set() == 64 { break; }
            full.insert(&Value::Integer(n));
        }
        assert_eq!(full.bits_set(), 64);
        let before = full.clone();
        let mut other = BloomFilter::new(64, 5).unwrap();
        other.insert(&Value::Text("different".into()));
        full.union_with(&other).unwrap();
        assert_eq!(full.words, before.words);
        assert_eq!(full.bits_set(), 64);
        assert!(full.might_contain(&Value::Integer(10_000_000)));
        assert!(full.union_with(&BloomFilter::new(128, 5).unwrap()).is_err());
    }
}

#[cfg(test)]
mod bloom_consecutive_key_run_tests {
    use super::*;

    #[test]
    fn duplicate_runs_match_elementwise_insert_probe_and_occupancy() {
        let wide = "é🙂".repeat(4096);
        let mut keys = Vec::new();
        for key in [
            Value::Text(wide), Value::Null, Value::Float(-0.0),
            Value::Float(0.0), Value::Float(f64::NAN),
            Value::Float(-f64::NAN), Value::Integer(17),
        ] {
            keys.extend(std::iter::repeat_n(key, 71));
        }
        let mut scalar = BloomFilter::new(32_769, 5).unwrap();
        let mut batched = BloomFilter::new(32_769, 5).unwrap();
        for key in &keys { scalar.insert(key); }
        batched.insert_batch(&keys);
        assert_eq!(batched.words, scalar.words);
        assert_eq!(batched.bits_set(), scalar.bits_set());
        assert_eq!(batched.probe_batch(&keys),
            keys.iter().map(|key| scalar.might_contain(key)).collect::<Vec<_>>());

        let probes = [
            Value::Integer(-1), Value::Integer(-1), Value::Integer(17),
            Value::Float(f64::NAN), Value::Float(-f64::NAN),
            Value::Null, Value::Text("missing".into()),
        ];
        assert_eq!(batched.probe_batch(&probes),
            probes.iter().map(|value| batched.might_contain(value))
                .collect::<Vec<_>>());
    }

    #[test]
    fn empty_saturated_and_power_of_two_filters_keep_bulk_contract() {
        for bits in [1, 64, 1024] {
            let mut batch = BloomFilter::new(bits, 5).unwrap();
            assert_eq!(batch.probe_batch(&[Value::Null, Value::Integer(1)]),
                vec![false, false]);
            batch.insert_batch(&[]);
            assert!(!batch.might_contain(&Value::Null));
            let keys = vec![Value::Text("hot".into()); 400];
            batch.insert_batch(&keys);
            assert!(batch.probe_batch(&keys).into_iter().all(|hit| hit));
            let mut scalar = BloomFilter::new(bits, 5).unwrap();
            for key in &keys { scalar.insert(key); }
            assert_eq!(batch.bits_set(), scalar.bits_set());
        }
    }
}

#[cfg(test)]
mod bloom_optimal_sizing_tests {
    use super::*;

    #[test]
    fn target_fpr_selects_bounded_layout_and_keeps_inserted_keys() {
        let mut filter = BloomFilter::for_expected_items(10_000, 0.01).unwrap();
        assert!(filter.bits >= 95_800 && filter.bits <= 96_000);
        assert_eq!(filter.hashes, 7);
        for i in 0..10_000 {
            filter.insert(&Value::Integer(i));
        }
        assert!(filter.estimated_false_positive_rate() < 0.025);
        for i in 0..10_000 {
            assert!(filter.might_contain(&Value::Integer(i)));
        }
        let observed = (20_000..30_000)
            .filter(|&i| filter.might_contain(&Value::Integer(i))).count();
        assert!(observed < 350, "unexpected Bloom collision rate: {observed}");
    }

    #[test]
    fn rejects_unachievable_targets_without_allocating_large_bitsets() {
        for bad_rate in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 0.0, -0.5, 1.0, 2.0] {
            assert!(BloomFilter::for_expected_items(100, bad_rate).is_err());
        }
        assert!(BloomFilter::for_expected_items(0, 0.01).is_err());
        assert!(BloomFilter::for_expected_items(usize::MAX, 0.01).is_err());
        assert!(BloomFilter::for_expected_items(100, f64::MIN_POSITIVE).is_err());
    }

    #[test]
    fn stricter_target_allocates_more_bits_for_fixed_cardinality() {
        let loose = BloomFilter::for_expected_items(1000, 0.1).unwrap();
        let medium = BloomFilter::for_expected_items(1000, 0.01).unwrap();
        let tight = BloomFilter::for_expected_items(1000, 0.001).unwrap();
        assert!(loose.bits < medium.bits && medium.bits < tight.bits);
        assert!(loose.hashes <= medium.hashes && medium.hashes <= tight.hashes);
    }
}

#[cfg(test)]
mod irregular_bloom_stride_regressions {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn every_probe_visits_a_distinct_position_when_capacity_allows() {
        for bits in [17, 21, 30, 33, 35, 63, 65, 127, 255] {
            let mut filter = BloomFilter::new(bits, 16).unwrap();
            for value in 0..120 {
                let key = Value::Integer(value);
                let (first, step) = filter.hash_pair(&key);
                let positions = (0..16)
                    .map(|index| filter.probe(first, step, index))
                    .collect::<HashSet<_>>();
                assert_eq!(positions.len(), 16, "duplicate probe for {bits} bits");
                filter.insert(&key);
                assert!(filter.might_contain(&key));
            }
        }
    }

    #[test]
    fn power_of_two_layout_keeps_the_original_odd_stride() {
        let filter = BloomFilter::new(64, 8).unwrap();
        for value in 0..100 {
            let (_, stride) = filter.hash_pair(&Value::Integer(value));
            assert_eq!(stride & 1, 1);
        }
    }
}

#[cfg(test)]
mod fallible_bloom_admission_regressions {
    use super::*;

    #[test]
    fn capacity_overflow_is_a_recoverable_error() {
        let err = BloomFilter::allocate_words(usize::MAX).unwrap_err();
        assert!(err.to_string().contains("allocation failed"));
    }

    #[test]
    fn exact_tail_word_allocation_preserves_membership_and_size() {
        for bits in [1, 63, 64, 65, 129, 513] {
            let mut filter = BloomFilter::new(bits, 7).unwrap();
            assert_eq!(filter.byte_size(), bits.div_ceil(64) * 8);
            for value in 0..100 {
                filter.insert(&Value::Integer(value));
            }
            for value in 0..100 {
                assert!(filter.might_contain(&Value::Integer(value)));
            }
            assert!(filter.bits_set() <= bits);
        }
    }
}

#[cfg(test)]
mod selected_bloom_build_tests {
    use super::*;

    #[test]
    fn selection_build_matches_elementwise_hashing_on_irregular_and_power_of_two_bits() {
        let values = vec![
            Value::Null, Value::Integer(17), Value::Text("é🙂".into()),
            Value::Float(-0.0), Value::Float(f64::NAN),
            Value::Integer(17), Value::Float(0.0), Value::Text("Berlin".into()),
        ];
        let selection = [1, 1, 2, 4, 0, 3, 6, 6, 5, 7, 2];
        for bits in [513, 1024, 8192] {
            let mut actual = BloomFilter::new(bits, 5).unwrap();
            let mut reference = BloomFilter::new(bits, 5).unwrap();
            actual.insert_selected(&values, &selection).unwrap();
            for &row in &selection { reference.insert(&values[row]); }
            assert_eq!(actual.words, reference.words);
            assert_eq!(actual.bits_set(), reference.bits_set());
            assert_eq!(actual.probe_batch(&values), reference.probe_batch(&values));
        }
    }

    #[test]
    fn malformed_selection_does_not_mutate_an_existing_filter() {
        let mut filter = BloomFilter::new(513, 7).unwrap();
        filter.insert(&Value::Integer(42));
        let before = filter.clone();
        assert!(filter.insert_selected(&[Value::Integer(4)], &[0, 1]).is_err());
        assert_eq!(filter.words, before.words);
        assert_eq!(filter.bits_set(), before.bits_set());
        assert_eq!(filter.insert_selected(&[], &[]), Ok(()));
    }
}
