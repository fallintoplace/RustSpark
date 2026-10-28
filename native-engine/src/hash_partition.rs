//! Deterministic exchange hash partitioning for composite SQL keys.
use crate::{batch::Batch, scalar::Value, Error, Result};

const OFFSET: u64 = 0xcbf29ce484222325;
const PRIME: u64 = 0x100000001b3;

fn push_bytes(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash = (*hash ^ u64::from(*byte)).wrapping_mul(PRIME);
    }
}

fn hash_value(hash: &mut u64, value: &Value) {
    match value {
        Value::Null => push_bytes(hash, &[0]),
        Value::Boolean(v) => push_bytes(hash, &[1, u8::from(*v)]),
        Value::Integer(v) => {
            push_bytes(hash, &[2]);
            push_bytes(hash, &v.to_le_bytes());
        }
        Value::Float(v) => {
            push_bytes(hash, &[3]);
            let bits = if v.is_nan() { f64::NAN.to_bits() }
                       else if *v == 0.0 { 0.0_f64.to_bits() }
                       else { v.to_bits() };
            push_bytes(hash, &bits.to_le_bytes());
        }
        Value::Text(v) => {
            push_bytes(hash, &[4]);
            push_bytes(hash, &(v.len() as u64).to_le_bytes());
            push_bytes(hash, v.as_bytes());
        }
    }
}

/// Same typed keys always map to the same partition across processes.
pub fn partition_for_key(values: &[Value], count: usize) -> Result<usize> {
    if count == 0 || count > 65_536 {
        return Err(Error::Execution("partition count must be in 1..=65536".into()));
    }
    let mut hash = OFFSET;
    for value in values {
        hash_value(&mut hash, value);
    }
    Ok((hash % count as u64) as usize)
}

/// Exchange a batch by one or more columns, retaining row order per partition.
pub fn partition(batch: &Batch, keys: &[&str], count: usize) -> Result<Vec<Batch>> {
    if count == 0 || count > 65_536 {
        return Err(Error::Execution("partition count must be in 1..=65536".into()));
    }
    let indices = keys.iter().map(|key| batch.schema().column(key)).collect::<Result<Vec<_>>>()?;
    let mut positions = vec![Vec::new(); count];
    for row in 0..batch.num_rows() {
        let mut hash = OFFSET;
        for &column in &indices {
            hash_value(&mut hash, batch.value(row, column)?);
        }
        positions[(hash % count as u64) as usize].push(row);
    }
    positions.iter().map(|rows| batch.take(rows)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn canonical_float_keys_have_stable_partition_ids() {
        assert_eq!(partition_for_key(&[Value::Float(-0.0)], 127).unwrap(),
                   partition_for_key(&[Value::Float(0.0)], 127).unwrap());
        assert_eq!(partition_for_key(&[Value::Float(f64::NAN)], 127).unwrap(),
                   partition_for_key(&[Value::Float(-f64::NAN)], 127).unwrap());
        assert_eq!(partition_for_key(&[Value::Null], 127).unwrap(),
                   partition_for_key(&[Value::Null], 127).unwrap());
    }

    #[test]
    fn partition_preserves_every_input_row() {
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("k", DataType::Integer, true)]).unwrap(),
            (0..150).map(|n| vec![Value::Integer(n % 7)]).collect()
        ).unwrap();
        let parts = partition(&batch, &["k"], 13).unwrap();
        assert_eq!(parts.iter().map(Batch::num_rows).sum::<usize>(), 150);
        for (p, part) in parts.iter().enumerate() {
            for row in part.rows().unwrap() {
                assert_eq!(partition_for_key(&row, 13).unwrap(), p);
            }
        }
        assert!(partition(&batch, &["missing"], 13).is_err());
        assert!(partition(&batch, &["k"], 0).is_err());
    }
}
