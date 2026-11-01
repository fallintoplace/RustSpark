//! Mergeable HyperLogLog sketch with fixed 4 KiB register memory.
use std::{collections::hash_map::DefaultHasher, hash::{Hash, Hasher}};
use crate::{scalar::Value, Error, Result};

const PRECISION: u32 = 12;
const REGISTERS: usize = 1 << PRECISION;
const WIRE_HEADER: &[u8; 4] = b"RSH1";
const SPARSE_WIRE_HEADER: &[u8; 4] = b"RSH2";
const SPARSE_HEADER_BYTES: usize = SPARSE_WIRE_HEADER.len() + 2;
const WIRE_BYTES: usize = WIRE_HEADER.len() + REGISTERS;
// Use exact fixed-point harmonic weights. Every reachable HLL rank has
// a power-of-two reciprocal, so register updates are lossless in u128.
const HARMONIC_BITS: u32 = 65 - PRECISION;
const HARMONIC_SCALE: u128 = 1u128 << HARMONIC_BITS;
fn harmonic_weight(rank: u8) -> u128 {
    1u128 << (HARMONIC_BITS - u32::from(rank))
}

// Retained as the reference table for differential regression tests.
#[cfg(test)]
const RECIPROCAL_POWERS: [f64; 54] = {
    let mut values = [0.0; 54];
    let mut index = 0;
    let mut value = 1.0;
    while index < values.len() {
        values[index] = value;
        value *= 0.5;
        index += 1;
    }
    values
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HyperLogLog {
    registers: [u8; REGISTERS],
    nonempty: usize,
    harmonic_scaled: u128,
}

impl HyperLogLog {
    fn update_register(&mut self, index: usize, rank: u8) {
        let old = self.registers[index];
        if rank <= old { return; }
        self.registers[index] = rank;
        self.harmonic_scaled = self.harmonic_scaled - harmonic_weight(old)
            + harmonic_weight(rank);
        if old == 0 { self.nonempty += 1; }
    }

    pub fn new() -> Self {
        Self { registers: [0; REGISTERS], nonempty: 0,
            harmonic_scaled: REGISTERS as u128 * HARMONIC_SCALE }
    }

    /// Compact low-cardinality sketches on the worker exchange path. RSH1
    /// remains the dense format and can still be read from older workers.
    /// Neither format is persistent across native hash implementation changes.
    pub fn to_wire_bytes(&self) -> Vec<u8> {
        let sparse_bytes = SPARSE_HEADER_BYTES + self.nonempty * 3;
        if sparse_bytes < WIRE_BYTES {
            let mut encoded = Vec::with_capacity(sparse_bytes);
            encoded.extend_from_slice(SPARSE_WIRE_HEADER);
            encoded.extend_from_slice(&(self.nonempty as u16).to_le_bytes());
            for (index, &rank) in self.registers.iter().enumerate() {
                if rank != 0 {
                    encoded.extend_from_slice(&(index as u16).to_le_bytes());
                    encoded.push(rank);
                }
            }
            return encoded;
        }
        let mut encoded = Vec::with_capacity(WIRE_BYTES);
        encoded.extend_from_slice(WIRE_HEADER);
        encoded.extend_from_slice(&self.registers);
        encoded
    }

    /// Reject duplicate, out-of-order, zero, or unreachable sparse entries.
    /// Decode the same wire format used by distributed worker sketches.
    pub fn from_wire_bytes(bytes: &[u8]) -> Result<Self> {
        let mut sketch = Self::new();
        sketch.merge_wire_bytes(bytes)?;
        Ok(sketch)
    }

    /// Merge a worker sketch without constructing another dense 4 KiB sketch.
    /// Validate the *entire* payload before modifying any register, so
    /// corrupt input cannot leave a partially updated aggregate.
    pub fn merge_wire_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.starts_with(SPARSE_WIRE_HEADER) {
            if bytes.len() < SPARSE_HEADER_BYTES {
                return Err(Error::Parse("truncated sparse HLL header".into()));
            }
            let count = u16::from_le_bytes([bytes[4], bytes[5]]) as usize;
            if count > REGISTERS || bytes.len() != SPARSE_HEADER_BYTES + count * 3 {
                return Err(Error::Parse("invalid sparse HLL entry count".into()));
            }
            let mut previous = None;
            for entry in bytes[SPARSE_HEADER_BYTES..].chunks_exact(3) {
                let index = u16::from_le_bytes([entry[0], entry[1]]) as usize;
                let rank = entry[2];
                if index >= REGISTERS || previous.is_some_and(|old| index <= old) {
                    return Err(Error::Parse("duplicate or unordered HLL register".into()));
                }
                if rank == 0 || u32::from(rank) > 65 - PRECISION {
                    return Err(Error::Parse("invalid HLL register rank".into()));
                }
                previous = Some(index);
            }
            for entry in bytes[SPARSE_HEADER_BYTES..].chunks_exact(3) {
                let index = u16::from_le_bytes([entry[0], entry[1]]) as usize;
                self.update_register(index, entry[2]);
            }
            return Ok(());
        }
        if bytes.len() != WIRE_BYTES || !bytes.starts_with(WIRE_HEADER) {
            return Err(Error::Parse("unsupported HLL sketch wire layout".into()));
        }
        if bytes[WIRE_HEADER.len()..].iter()
            .any(|&rank| u32::from(rank) > 65 - PRECISION) {
            return Err(Error::Parse("invalid HLL register rank".into()));
        }
        for (index, &rank) in bytes[WIRE_HEADER.len()..].iter().enumerate() {
            if rank != 0 { self.update_register(index, rank); }
        }
        Ok(())
    }

    /// NULL is ignored, matching COUNT(DISTINCT expression) semantics.
    pub fn add(&mut self, value: &Value) {
        if value.is_null() { return; }
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        let hash = hasher.finish();
        let index = (hash >> (64 - PRECISION)) as usize;
        let rank = ((hash << PRECISION).leading_zeros() + 1)
            .min(65 - PRECISION) as u8;
        self.update_register(index, rank);
    }

    /// Add an arbitrary row selection without materializing an intermediate
    /// column or hashing adjacent repeated values. A bad index never
    /// partially updates a live worker sketch.
    pub fn add_selected(&mut self, values: &[Value], selected: &[usize]) -> Result<()> {
        if selected.iter().any(|&index| index >= values.len()) {
            return Err(Error::Execution("selected HLL row out of bounds".into()));
        }
        let mut previous: Option<&Value> = None;
        for &index in selected {
            let value = &values[index];
            if previous != Some(value) { self.add(value); }
            previous = Some(value);
        }
        Ok(())
    }

    pub fn merge(&mut self, other: &Self) {
        if other.nonempty == 0 { return; }
        if self.nonempty == 0 {
            // Copy a populated sketch into a fresh target in one operation.
            self.registers.copy_from_slice(&other.registers);
            self.nonempty = other.nonempty;
            self.harmonic_scaled = other.harmonic_scaled;
            return;
        }
        // Merge the 4 KiB register arrays with one harmonic adjustment.
        // Updating the fixed-point accumulator per changed register creates
        // an unnecessary dependency chain on dense distributed sketches.
        let mut replaced_weight = 0u128;
        let mut merged_weight = 0u128;
        let mut new_nonempty = 0usize;
        for (current, &incoming) in self.registers.iter_mut().zip(other.registers.iter()) {
            if incoming > *current {
                replaced_weight += harmonic_weight(*current);
                merged_weight += harmonic_weight(incoming);
                new_nonempty += usize::from(*current == 0);
                *current = incoming;
            }
        }
        self.harmonic_scaled = self.harmonic_scaled - replaced_weight + merged_weight;
        self.nonempty += new_nonempty;
    }

    pub fn estimate(&self) -> f64 {
        if self.nonempty == 0 { return 0.0; }
        let m = REGISTERS as f64;
        let zeros = REGISTERS - self.nonempty;
        let harmonic = self.harmonic_scaled as f64 / HARMONIC_SCALE as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let raw = alpha * m * m / harmonic;
        if raw <= 2.5 * m && zeros > 0 {
            m * (m / zeros as f64).ln()
        } else {
            raw
        }
    }

    pub fn nonempty_registers(&self) -> usize {
        self.nonempty
    }
}

impl Default for HyperLogLog {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sketch_merges_across_partitions_and_ignores_nulls() {
        let mut a = HyperLogLog::new();
        let mut b = HyperLogLog::new();
        for n in 0..50_000 {
            if n % 2 == 0 { a.add(&Value::Integer(n)); }
            else { b.add(&Value::Integer(n)); }
        }
        a.add(&Value::Null);
        a.merge(&b);
        let estimate = a.estimate();
        assert!((estimate - 50_000.0).abs() / 50_000.0 < 0.10);
        assert!(a.nonempty_registers() > 0);
        let before = a.estimate();
        a.add(&Value::Integer(2));
        assert_eq!(a.estimate(), before);
    }

    #[test]
    fn empty_estimate_is_zero() {
        assert_eq!(HyperLogLog::new().estimate(), 0.0);
    }
}

 
// Sorted and run-length encoded columns often contain long runs of equal
// values. The registers depend on each key's hash, not its multiplicity.
// Compare the previously seen borrowed value to avoid repeated hashing.
// SQL equality is not used here: the Value key equality also canonicalizes
// signed zero and NaNs, just like hashing.
fn add_value_run<'a>(sketch: &mut HyperLogLog, values: impl Iterator<Item = &'a Value>) {
    let mut previous: Option<&Value> = None;
    for value in values {
        if previous != Some(value) {
            sketch.add(value);
        }
        previous = Some(value);
    }
}

/// Estimate non-NULL distinct values in a native column, without materializing a set.
pub fn approximate_distinct(batch: &crate::batch::Batch, column: &str) -> Result<f64> {
    let index = batch.schema().column(column)?;
    let mut sketch = HyperLogLog::new();
    add_value_run(&mut sketch, batch.column(index)?.iter());
    Ok(sketch.estimate())
}

#[cfg(test)]
mod approximate_column_feature_tests {
    use super::*;
    use crate::{batch::Batch, schema::{DataType, Field, Schema}};

    #[test]
    fn counts_high_cardinality_input_with_bounded_registers() {
        let schema = Schema::new(vec![Field::new("user_id", DataType::Integer, true)]).unwrap();
        let mut rows = (0..5000).map(|id| vec![Value::Integer(id)]).collect::<Vec<_>>();
        rows.extend((0..1000).map(|id| vec![Value::Integer(id)]));
        rows.push(vec![Value::Null]);
        let batch = Batch::from_rows(schema, rows).unwrap();
        let estimate = approximate_distinct(&batch, "user_id").unwrap();
        assert!((estimate - 5000.0).abs() / 5000.0 < 0.15);
        assert!(approximate_distinct(&batch, "missing").is_err());
    }
}


/// Approximate distinct keys over an arbitrary row selection without copying values.
pub fn approximate_distinct_selected(
    batch: &crate::batch::Batch, column: &str, selected: &[usize]
) -> Result<f64> {
    let index = batch.schema().column(column)?;
    let values = batch.column(index)?;
    let mut sketch = HyperLogLog::new();
    sketch.add_selected(values, selected)?;
    Ok(sketch.estimate())
}

/// Merge distinct-count sketches across compatible input batches.
pub fn approximate_distinct_batches(
    batches: &[crate::batch::Batch],
    column: &str,
) -> Result<f64> {
    let mut combined = HyperLogLog::new();
    let mut expected_type = None;
    for batch in batches {
        let index = batch.schema().column(column)?;
        let kind = batch.schema().field(index)?.data_type;
        if expected_type.is_some_and(|previous| previous != kind) {
            return Err(Error::Type("approximate distinct column types differ".into()));
        }
        expected_type = Some(kind);
        add_value_run(&mut combined, batch.column(index)?.iter());
    }
    Ok(combined.estimate())
}

#[cfg(test)]
mod approximate_distinct_batch_tests {
    use super::*;
    use crate::{batch::Batch, schema::{DataType, Field, Schema}};

    #[test]
    fn counts_unique_keys_across_overlapping_batch_ranges() {
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, true)]).unwrap();
        let first = Batch::from_rows(schema.clone(),
            (0..2000).map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let second = Batch::from_rows(schema,
            (1000..3000).map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let estimate = approximate_distinct_batches(&[first, second], "key").unwrap();
        assert!((estimate - 3000.0).abs() / 3000.0 < 0.15);
        assert_eq!(approximate_distinct_batches(&[], "key").unwrap(), 0.0);
    }
}

#[cfg(test)]
mod hll_direct_batch_update_perf_tests {
    use super::*;
    use crate::{batch::Batch, schema::{DataType, Field, Schema}};

    #[test]
    fn many_small_batches_match_one_large_sketch() {
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, true)]).unwrap();
        let mut parts = Vec::new();
        let mut expected = HyperLogLog::new();
        for n in 0..350 {
            let values = vec![Value::Integer(n % 200), Value::Null, Value::Integer(n % 200)];
            for value in &values { expected.add(value); }
            parts.push(Batch::from_rows(schema.clone(), values.into_iter().map(|value| vec![value]).collect()).unwrap());
        }
        assert_eq!(approximate_distinct_batches(&parts, "key").unwrap(), expected.estimate());
    }
}

#[cfg(test)]
mod hll_estimation_lookup_tests {
    use super::*;
    #[test]
    fn reciprocal_weights_are_exact_for_every_reachable_rank() {
        for rank in 0..=65 - PRECISION {
            let expected = 2_f64.powi(-(rank as i32));
            assert_eq!(RECIPROCAL_POWERS[rank as usize], expected);
        }
    }
    #[test]
    fn estimate_agrees_with_reference_harmonic_computation() {
        let mut sketch = HyperLogLog::new();
        for value in 0..30_000 { sketch.add(&Value::Integer(value)); }
        let zeros = sketch.registers.iter().filter(|&&rank| rank == 0).count();
        let m = REGISTERS as f64;
        let harmonic: f64 = sketch.registers.iter().map(|&rank| 2_f64.powi(-(rank as i32))).sum();
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let raw = alpha * m * m / harmonic;
        let reference = if raw <= 2.5 * m && zeros > 0 { m * (m / zeros as f64).ln() } else { raw };
        // The fixed-point sum is exact; converting the whole sum to f64
        // can round differently from 4096 individual floating additions.
        assert!((sketch.estimate() - reference).abs() <= reference * 1e-12);
        assert_eq!(HyperLogLog::new().estimate(), 0.0);
    }
}

#[cfg(test)]
mod cached_nonempty_register_count_tests {
    use super::*;
    #[test]
    fn tracked_nonempty_count_matches_registers_after_merge_and_repeats() {
        let mut a = HyperLogLog::new();
        let mut b = HyperLogLog::new();
        for n in 0..50_000 {
            if n % 2 == 0 { a.add(&Value::Integer(n)); }
            else { b.add(&Value::Integer(n)); }
        }
        for _ in 0..10 { a.add(&Value::Integer(2)); }
        a.merge(&b);
        assert_eq!(a.nonempty_registers(), a.registers.iter().filter(|&&rank| rank != 0).count());
        assert!(a.nonempty_registers() > 4000);
        a.merge(&b);
        assert_eq!(a.nonempty_registers(), a.registers.iter().filter(|&&rank| rank != 0).count());
    }
}

#[cfg(test)]
mod hll_cached_zero_count_estimate_tests {
    use super::*;
    #[test]
    fn cached_zero_count_agrees_after_adding_and_merging() {
        let mut a = HyperLogLog::new();
        let mut b = HyperLogLog::new();
        for n in 0..10_000 {
            if n % 2 == 0 { a.add(&Value::Integer(n)); }
            else { b.add(&Value::Integer(n)); }
        }
        a.merge(&b);
        let zeros = a.registers.iter().filter(|&&rank| rank == 0).count();
        assert_eq!(zeros, REGISTERS - a.nonempty_registers());
        assert!(a.estimate() > 0.0);
    }
}

#[cfg(test)]
mod hll_empty_merge_shortcuts_tests {
    use super::*;

    #[test]
    fn merging_empty_and_populated_sketches_keeps_registers() {
        let mut populated = HyperLogLog::new();
        for n in 0..1000 { populated.add(&Value::Integer(n)); }
        let baseline = populated.clone();
        let empty = HyperLogLog::new();
        populated.merge(&empty);
        assert_eq!(populated, baseline);
        let mut receiver = HyperLogLog::new();
        receiver.merge(&baseline);
        assert_eq!(receiver, baseline);
        receiver.merge(&baseline);
        assert_eq!(receiver, baseline);
        assert_eq!(receiver.nonempty_registers(), baseline.nonempty_registers());
    }
}

#[cfg(test)]
mod native_selected_distinct_estimates_tests {
    use super::*;
    use crate::{batch::Batch, schema::{DataType, Field, Schema}};

    #[test]
    fn selected_rows_match_a_materialized_subset_exactly() {
        let source = Batch::from_rows(Schema::new(vec![
            Field::new("account", DataType::Integer, true)
        ]).unwrap(), (0..2000).map(|n| vec![if n % 5 == 0 {
            Value::Null
        } else { Value::Integer(n % 271) }]).collect()).unwrap();
        let indices = (0..source.num_rows()).step_by(3).collect::<Vec<_>>();
        let subset = source.take(&indices).unwrap();
        assert_eq!(approximate_distinct_selected(&source, "account", &indices).unwrap(),
            approximate_distinct(&subset, "account").unwrap());
        assert_eq!(approximate_distinct_selected(&source, "account", &[]).unwrap(), 0.0);
        assert!(approximate_distinct_selected(&source, "account", &[2001]).is_err());
        assert!(approximate_distinct_selected(&source, "missing", &[]).is_err());
    }
}

#[cfg(test)]
mod hll_borrowed_merge_tests {
    use super::*;
    #[test]
    fn merge_matches_registerwise_max_after_repeated_merges() {
        let mut first = HyperLogLog::new();
        let mut second = HyperLogLog::new();
        for n in 0..8000 { first.add(&Value::Integer(n)); second.add(&Value::Integer(n + 4000)); }
        let expected = first.registers.iter().zip(&second.registers)
            .map(|(&left, &right)| left.max(right)).collect::<Vec<_>>();
        first.merge(&second);
        assert_eq!(first.registers.as_slice(), expected.as_slice());
        let once = first.clone();
        first.merge(&second);
        assert_eq!(first, once);
    }
}

#[cfg(test)]
mod cached_hll_harmonic_regressions {
    use super::*;

    fn verify(sketch: &HyperLogLog) {
        let expected: u128 = sketch.registers.iter().map(|&rank| harmonic_weight(rank)).sum();
        assert_eq!(sketch.harmonic_scaled, expected);
        assert_eq!(sketch.nonempty_registers(), sketch.registers.iter().filter(|&&rank| rank > 0).count());
        assert!(sketch.estimate().is_finite());
    }

    #[test]
    fn cached_harmonic_matches_full_register_scan_after_merges_and_duplicates() {
        let mut first = HyperLogLog::new();
        let mut second = HyperLogLog::new();
        verify(&first);
        for n in 0..32_000 {
            first.add(&Value::Integer(n));
            second.add(&Value::Integer(n + 8_000));
        }
        verify(&first);
        verify(&second);
        first.merge(&second);
        verify(&first);
        let baseline = first.clone();
        first.merge(&second);
        assert_eq!(first, baseline);
        let mut copied = HyperLogLog::new();
        copied.merge(&first);
        assert_eq!(copied, first);
        verify(&copied);
    }
}

#[cfg(test)]
mod batched_hll_merge_regressions {
    use super::*;

    fn reference_merge(target: &mut HyperLogLog, source: &HyperLogLog) {
        for (index, &rank) in source.registers.iter().enumerate() {
            target.update_register(index, rank);
        }
    }

    #[test]
    fn batched_merges_match_reference_for_dense_sparse_and_duplicate_sketches() {
        let mut sources = (0..8).map(|_| HyperLogLog::new()).collect::<Vec<_>>();
        for value in 0..120_000 {
            let owner = (value as usize / 5) % sources.len();
            sources[owner].add(&Value::Integer(value % 85_000));
        }
        let mut merged = HyperLogLog::new();
        let mut reference = HyperLogLog::new();
        for sketch in sources.iter().chain(sources.iter().rev()) {
            merged.merge(sketch);
            reference_merge(&mut reference, sketch);
            assert_eq!(merged, reference);
            assert_eq!(merged.harmonic_scaled,
                merged.registers.iter().map(|&rank| harmonic_weight(rank)).sum::<u128>());
            assert_eq!(merged.nonempty, merged.registers.iter()
                .filter(|&&rank| rank != 0).count());
        }
        let before = merged.clone();
        merged.merge(&HyperLogLog::new());
        assert_eq!(merged, before);
    }

    #[test]
    fn individual_register_updates_match_merge_with_disjoint_registers() {
        let mut a = HyperLogLog::new();
        let mut b = HyperLogLog::new();
        for index in 0..REGISTERS {
            if index % 3 == 0 {
                a.update_register(index, ((index % 31) + 1) as u8);
            } else {
                b.update_register(index, ((index % 37) + 1) as u8);
            }
        }
        let mut expected = a.clone();
        reference_merge(&mut expected, &b);
        a.merge(&b);
        assert_eq!(a, expected);
    }
}

#[cfg(test)]
mod hll_wire_exchange_regressions {
    use super::*;

    #[test]
    fn roundtrips_sparse_dense_and_merged_worker_sketches() {
        let mut a = HyperLogLog::new();
        let mut b = HyperLogLog::new();
        for n in 0..40_000 {
            a.add(&Value::Integer(n));
            b.add(&Value::Integer(n + 20_000));
        }
        let empty = HyperLogLog::new();
        for sketch in [&empty, &a, &b] {
            let wire = sketch.to_wire_bytes();
            assert!(wire.len() <= WIRE_BYTES);
            assert_eq!(HyperLogLog::from_wire_bytes(&wire).unwrap(), *sketch);
        }
        let mut merged = HyperLogLog::from_wire_bytes(&a.to_wire_bytes()).unwrap();
        merged.merge(&HyperLogLog::from_wire_bytes(&b.to_wire_bytes()).unwrap());
        a.merge(&b);
        assert_eq!(merged, a);
        assert_eq!(merged.nonempty_registers(), a.nonempty_registers());
        assert_eq!(merged.estimate(), a.estimate());
    }

    #[test]
    fn rejects_bad_version_lengths_and_unreachable_register_ranks() {
        let mut wire = HyperLogLog::new().to_wire_bytes();
        assert!(HyperLogLog::from_wire_bytes(&wire[..wire.len() - 1]).is_err());
        let mut trailing = wire.clone();
        trailing.push(0);
        assert!(HyperLogLog::from_wire_bytes(&trailing).is_err());
        wire[0] ^= 0xff;
        assert!(HyperLogLog::from_wire_bytes(&wire).is_err());
        wire = [WIRE_HEADER.as_slice(), &[0u8; REGISTERS]].concat();
        wire[WIRE_HEADER.len()] = (65 - PRECISION + 1) as u8;
        assert!(HyperLogLog::from_wire_bytes(&wire).is_err());
    }
}

#[cfg(test)]
mod adjacent_hll_run_dedup_regressions {
    use super::*;
    use crate::{batch::Batch, schema::{DataType, Field, Schema}};

    #[test]
    fn adjacent_duplicates_match_reference_updates_including_special_floats() {
        let values = vec![
            Value::Null, Value::Null,
            Value::Float(-0.0), Value::Float(0.0),
            Value::Float(f64::NAN), Value::Float(-f64::NAN),
            Value::Float(7.0), Value::Float(7.0),
            Value::Float(-2.5), Value::Float(7.0),
        ];
        let mut expected = HyperLogLog::new();
        for value in &values { expected.add(value); }
        let mut actual = HyperLogLog::new();
        add_value_run(&mut actual, values.iter());
        assert_eq!(actual, expected);
    }

    #[test]
    fn column_selected_and_multiple_batches_match_naive_hashing() {
        let schema = Schema::new(vec![Field::new("key", DataType::Text, true)]).unwrap();
        let values = (0..3000).map(|i| match i % 13 {
            0 => Value::Null,
            _ => Value::Text(format!("key-{}", i / 25)),
        }).collect::<Vec<_>>();
        let batch = Batch::from_rows(schema, values.iter().cloned().map(|v| vec![v]).collect()).unwrap();
        let mut expected = HyperLogLog::new();
        for value in &values { expected.add(value); }
        assert_eq!(approximate_distinct(&batch, "key").unwrap(), expected.estimate());
        let selections = (0..values.len()).step_by(3).collect::<Vec<_>>();
        let mut selected = HyperLogLog::new();
        for &i in &selections { selected.add(&values[i]); }
        assert_eq!(approximate_distinct_selected(&batch, "key", &selections).unwrap(), selected.estimate());
        let first = batch.slice_rows(0, 1700).unwrap();
        let second = batch.slice_rows(1700, 3000).unwrap();
        assert_eq!(approximate_distinct_batches(&[first, second], "key").unwrap(), expected.estimate());
    }
}


#[cfg(test)]
mod sparse_worker_wire_tests {
    use super::*;

    #[test]
    fn compact_sketches_and_legacy_dense_payloads_roundtrip() {
        let mut small = HyperLogLog::new();
        assert_eq!(small.to_wire_bytes().len(), SPARSE_HEADER_BYTES);
        for n in 0..200 {
            small.add(&Value::Integer(n));
        }
        let sparse = small.to_wire_bytes();
        assert!(sparse.starts_with(SPARSE_WIRE_HEADER));
        assert!(sparse.len() < WIRE_BYTES);
        assert_eq!(HyperLogLog::from_wire_bytes(&sparse).unwrap(), small);
        let legacy = [WIRE_HEADER.as_slice(), small.registers.as_slice()].concat();
        assert_eq!(HyperLogLog::from_wire_bytes(&legacy).unwrap(), small);

        let mut dense = HyperLogLog::new();
        for n in 0..30_000 {
            dense.add(&Value::Integer(n));
        }
        assert_eq!(dense.to_wire_bytes().len(), WIRE_BYTES);
        assert_eq!(HyperLogLog::from_wire_bytes(&dense.to_wire_bytes()).unwrap(), dense);
    }

    #[test]
    fn rejects_malformed_sparse_register_streams() {
        let empty = HyperLogLog::new().to_wire_bytes();
        assert!(HyperLogLog::from_wire_bytes(&empty[..5]).is_err());
        let mut wrong_count = empty.clone();
        wrong_count[4] = 1;
        assert!(HyperLogLog::from_wire_bytes(&wrong_count).is_err());
        let encode = |entries: &[(u16, u8)]| {
            let mut bytes = SPARSE_WIRE_HEADER.to_vec();
            bytes.extend_from_slice(&(entries.len() as u16).to_le_bytes());
            for &(index, rank) in entries {
                bytes.extend_from_slice(&index.to_le_bytes());
                bytes.push(rank);
            }
            bytes
        };
        for entries in [
            vec![(0, 0)], vec![(0, 54)], vec![(REGISTERS as u16, 1)],
            vec![(1, 1), (1, 2)], vec![(10, 1), (9, 2)],
        ] {
            assert!(HyperLogLog::from_wire_bytes(&encode(&entries)).is_err());
        }
        assert_eq!(
            HyperLogLog::from_wire_bytes(&encode(&[(1, 2), (9, 3)]))
                .unwrap().nonempty_registers(), 2,
        );
    }
}

#[cfg(test)]
mod transactional_wire_merge_regressions {
    use super::*;

    #[test]
    fn direct_sparse_and_dense_merges_match_decoded_register_max() {
        let mut source = HyperLogLog::new();
        let mut target = HyperLogLog::new();
        for n in 0..300 { source.add(&Value::Integer(n)); }
        for n in 150..1000 { target.add(&Value::Integer(n)); }
        let sparse = source.to_wire_bytes();
        assert!(sparse.starts_with(SPARSE_WIRE_HEADER));
        let mut expected = target.clone();
        expected.merge(&HyperLogLog::from_wire_bytes(&sparse).unwrap());
        target.merge_wire_bytes(&sparse).unwrap();
        assert_eq!(target, expected);
        // Merge the same worker's dense sketch after it has filled registers.
        for n in 4000..30000 { source.add(&Value::Integer(n)); }
        let dense = source.to_wire_bytes();
        assert!(dense.starts_with(WIRE_HEADER));
        expected.merge(&HyperLogLog::from_wire_bytes(&dense).unwrap());
        target.merge_wire_bytes(&dense).unwrap();
        assert_eq!(target, expected);
        target.merge_wire_bytes(&dense).unwrap();
        assert_eq!(target, expected, "repeated worker frames are idempotent");
    }

    #[test]
    fn invalid_late_registers_leave_all_accumulators_unchanged() {
        let mut original = HyperLogLog::new();
        for n in 0..500 { original.add(&Value::Integer(n)); }
        let mut bad_sparse = HyperLogLog::new();
        for n in 0..100 { bad_sparse.add(&Value::Integer(n + 5000)); }
        let mut sparse = bad_sparse.to_wire_bytes();
        assert!(sparse.starts_with(SPARSE_WIRE_HEADER));
        *sparse.last_mut().unwrap() = u8::MAX;
        let mut target = original.clone();
        assert!(target.merge_wire_bytes(&sparse).is_err());
        assert_eq!(target, original);
        let mut dense = vec![0; WIRE_BYTES];
        dense[..4].copy_from_slice(WIRE_HEADER);
        dense[4] = 1;
        *dense.last_mut().unwrap() = u8::MAX;
        assert!(target.merge_wire_bytes(&dense).is_err());
        assert_eq!(target, original);
    }
}

#[cfg(test)]
mod selected_hll_update_tests {
    use super::*;

    #[test]
    fn selected_updates_match_reference_hashing_and_sparse_wire_format() {
        let values = vec![
            Value::Null, Value::Integer(7), Value::Integer(8), Value::Text("é🙂".into()),
            Value::Float(-0.0), Value::Float(0.0), Value::Float(f64::NAN),
            Value::Integer(7), Value::Text("Berlin".into()),
        ];
        let indices = [1, 1, 3, 4, 5, 0, 2, 6, 6, 7, 8];
        let mut actual = HyperLogLog::new();
        let mut reference = HyperLogLog::new();
        actual.add_selected(&values, &indices).unwrap();
        for &index in &indices { reference.add(&values[index]); }
        assert_eq!(actual, reference);
        assert_eq!(actual.to_wire_bytes(), reference.to_wire_bytes());
        assert!(actual.estimate() > 0.0);
        assert!(actual.add_selected(&values, &[]).is_ok());
    }

    #[test]
    fn out_of_bounds_selection_is_transactional() {
        let values = [Value::Integer(5), Value::Integer(6)];
        let mut sketch = HyperLogLog::new();
        sketch.add(&Value::Text("already present".into()));
        let before = sketch.clone();
        assert!(sketch.add_selected(&values, &[0, 2]).is_err());
        assert_eq!(sketch, before);
        assert!(sketch.add_selected(&values, &[usize::MAX]).is_err());
        assert_eq!(sketch, before);
    }
}
