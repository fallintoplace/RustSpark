//! Mergeable HyperLogLog sketch with fixed 4 KiB register memory.
use std::{collections::hash_map::DefaultHasher, hash::{Hash, Hasher}};
use crate::{scalar::Value, Error, Result};

const PRECISION: u32 = 12;
const REGISTERS: usize = 1 << PRECISION;
// Represent reciprocal rank weights exactly as scaled integers.
const HARMONIC_SHIFT: u32 = 65 - PRECISION;
const HARMONIC_SCALE: u128 = 1_u128 << HARMONIC_SHIFT;

fn scaled_weight(rank: u8) -> u128 {
    1_u128 << (HARMONIC_SHIFT - u32::from(rank))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HyperLogLog {
    registers: [u8; REGISTERS],
    nonempty: usize,
    harmonic_scaled: u128,
}

impl HyperLogLog {
    pub fn new() -> Self {
        Self {
            registers: [0; REGISTERS],
            nonempty: 0,
            harmonic_scaled: REGISTERS as u128 * HARMONIC_SCALE,
        }
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
        let previous = self.registers[index];
        if rank > previous {
            self.harmonic_scaled -= scaled_weight(previous) - scaled_weight(rank);
            if previous == 0 { self.nonempty += 1; }
            self.registers[index] = rank;
        }
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
        for (dest, source) in self.registers.iter_mut().zip(other.registers.iter().copied()) {
            if source > *dest {
                self.harmonic_scaled -= scaled_weight(*dest) - scaled_weight(source);
                if *dest == 0 { self.nonempty += 1; }
                *dest = source;
            }
        }
    }

    pub fn estimate(&self) -> f64 {
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

 
/// Estimate non-NULL distinct values in a native column, without materializing a set.
pub fn approximate_distinct(batch: &crate::batch::Batch, column: &str) -> Result<f64> {
    let index = batch.schema().column(column)?;
    let mut sketch = HyperLogLog::new();
    for value in batch.column(index)? {
        sketch.add(value);
    }
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
    for &row in selected {
        let value = values.get(row)
            .ok_or_else(|| Error::Execution("selected HLL row out of bounds".into()))?;
        sketch.add(value);
    }
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
        for value in batch.column(index)? {
            combined.add(value);
        }
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
            assert_eq!(scaled_weight(rank as u8) as f64 / HARMONIC_SCALE as f64, expected);
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
mod cached_harmonic_sum_tests {
    use super::*;

    #[test]
    fn updates_preserve_exact_harmonic_total_across_merges() {
        let mut first = HyperLogLog::new();
        let mut second = HyperLogLog::new();
        for n in 0..12_000 {
            first.add(&Value::Integer(n));
            second.add(&Value::Integer(n + 6_000));
        }
        for _ in 0..100 {
            first.add(&Value::Integer(7));
        }
        first.merge(&second);
        first.merge(&second);
        let expected: u128 = first.registers.iter().map(|&rank| scaled_weight(rank)).sum();
        assert_eq!(first.harmonic_scaled, expected);
        assert_eq!(first.nonempty, first.registers.iter().filter(|&&rank| rank != 0).count());

        let mut fresh = HyperLogLog::new();
        fresh.merge(&first);
        assert_eq!(fresh, first);
        assert_eq!(fresh.harmonic_scaled, expected);
        let before = fresh.harmonic_scaled;
        fresh.add(&Value::Integer(7));
        assert_eq!(fresh.harmonic_scaled, before);
    }
}
