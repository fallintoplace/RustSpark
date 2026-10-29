//! Mergeable HyperLogLog sketch with fixed 4 KiB register memory.
use std::{collections::hash_map::DefaultHasher, hash::{Hash, Hasher}};
use crate::{scalar::Value, Error, Result};

const PRECISION: u32 = 12;
const REGISTERS: usize = 1 << PRECISION;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HyperLogLog {
    registers: [u8; REGISTERS],
}

impl HyperLogLog {
    pub fn new() -> Self { Self { registers: [0; REGISTERS] } }

    /// NULL is ignored, matching COUNT(DISTINCT expression) semantics.
    pub fn add(&mut self, value: &Value) {
        if value.is_null() { return; }
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        let hash = hasher.finish();
        let index = (hash >> (64 - PRECISION)) as usize;
        let rank = ((hash << PRECISION).leading_zeros() + 1)
            .min(65 - PRECISION) as u8;
        self.registers[index] = self.registers[index].max(rank);
    }

    pub fn merge(&mut self, other: &Self) {
        for (dest, source) in self.registers.iter_mut().zip(other.registers) {
            *dest = (*dest).max(source);
        }
    }

    pub fn estimate(&self) -> f64 {
        let m = REGISTERS as f64;
        let zeros = self.registers.iter().filter(|&&rank| rank == 0).count();
        let harmonic: f64 = self.registers.iter()
            .map(|&rank| 2_f64.powi(-(rank as i32))).sum();
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let raw = alpha * m * m / harmonic;
        if raw <= 2.5 * m && zeros > 0 {
            m * (m / zeros as f64).ln()
        } else {
            raw
        }
    }

    pub fn nonempty_registers(&self) -> usize {
        self.registers.iter().filter(|&&rank| rank != 0).count()
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

