use std::hash::{Hash, Hasher};

use crate::{batch::Batch, Error, Result};

#[derive(Default)]
struct PartitionHasher(u64);

impl Hasher for PartitionHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        if self.0 == 0 {
            self.0 = 0xcbf29ce484222325;
        }
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
    }
}

pub fn partition(batch: &Batch, keys: &[&str], partitions: usize) -> Result<Vec<Batch>> {
    if partitions == 0 || keys.is_empty() {
        return Err(Error::Execution("partition count and hash keys must be nonempty".into()));
    }
    let indices = keys.iter().map(|key| batch.schema().column(key))
        .collect::<Result<Vec<_>>>()?;
    let mut groups = vec![Vec::new(); partitions];
    for row in 0..batch.num_rows() {
        let mut hasher = PartitionHasher::default();
        for &column in &indices {
            batch.value(row, column)?.hash(&mut hasher);
        }
        let bucket = (hasher.finish() % partitions as u64) as usize;
        groups[bucket].push(row);
    }
    groups.into_iter().map(|rows| batch.take(&rows)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn repeated_and_null_keys_land_together() {
        let batch = Batch::from_rows(
            Schema::new(vec![
                Field::new("id", DataType::Integer, true),
                Field::new("payload", DataType::Text, true),
            ]).unwrap(),
            vec![
                vec![Value::Null, Value::Text("a".into())],
                vec![Value::Integer(3), Value::Text("b".into())],
                vec![Value::Null, Value::Text("c".into())],
                vec![Value::Integer(3), Value::Text("d".into())],
            ],
        ).unwrap();
        let parts = partition(&batch, &["id"], 7).unwrap();
        assert_eq!(parts.iter().map(Batch::num_rows).sum::<usize>(), 4);
        assert!(parts.iter().any(|p| p.num_rows() == 2 &&
            p.column(0).unwrap() == [Value::Null, Value::Null]));
        assert_eq!(parts, partition(&batch, &["id"], 7).unwrap());
        assert!(partition(&batch, &[], 7).is_err());
        assert!(partition(&batch, &["id"], 0).is_err());
    }
}
