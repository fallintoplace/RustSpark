use std::hash::{Hash, Hasher};

use crate::{batch::Batch, Error, Result};

#[derive(Default)]
pub(crate) struct PartitionHasher(u64);

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
    if partitions == 0 || partitions > 4096 || keys.is_empty() {
        return Err(Error::Execution("hash partitions must be in 1..=4096 with nonempty keys".into()));
    }
    let indices = keys.iter().map(|key| batch.schema().column(key))
        .collect::<Result<Vec<_>>>()?;
    let columns = indices.iter().map(|&index| batch.column(index))
        .collect::<Result<Vec<_>>>()?;
    let mut groups = vec![Vec::new(); partitions];
    let mask = partitions.is_power_of_two().then_some(partitions - 1);
    for row in 0..batch.num_rows() {
        let mut hasher = PartitionHasher::default();
        for column in &columns {
            column[row].hash(&mut hasher);
        }
        let hash = hasher.finish();
        let bucket = match mask {
            Some(mask) => (hash as usize) & mask,
            None => (hash % partitions as u64) as usize,
        };
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

 
/// Partition successive batches using identical hash keys and concatenate each bucket.
pub fn partition_batches(input: &[Batch], keys: &[&str], partitions: usize) -> Result<Vec<Batch>> {
    if partitions == 0 || partitions > 4096 || keys.is_empty() {
        return Err(Error::Execution("hash partitions must be in 1..=4096 with nonempty keys".into()));
    }
    let first = input.first().ok_or_else(|| Error::Schema("partition input needs a schema".into()))?;
    let schema = first.schema();
    let key_columns = keys.iter().map(|key| schema.column(key)).collect::<Result<Vec<_>>>()?;
    for batch in input {
        if batch.schema() != schema {
            return Err(Error::Schema("partition input schemas differ".into()));
        }
    }

    // Keep only row coordinates until all source batches have been partitioned.
    let mask = partitions.is_power_of_two().then_some(partitions - 1);
    let mut buckets: Vec<Vec<(usize, usize)>> = vec![Vec::new(); partitions];
    for (batch_index, batch) in input.iter().enumerate() {
        for row in 0..batch.num_rows() {
            let mut hasher = PartitionHasher::default();
            for &column in &key_columns {
                batch.value(row, column)?.hash(&mut hasher);
            }
            let hashed = hasher.finish();
            let bucket = match mask {
                Some(mask) => (hashed as usize) & mask,
                None => (hashed % partitions as u64) as usize,
            };
            buckets[bucket].push((batch_index, row));
        }
    }
    buckets.into_iter().map(|entries| {
        let count = entries.len();
        let mut columns = (0..schema.len()).map(|_| Vec::with_capacity(count))
            .collect::<Vec<_>>();
        for (batch_index, row) in entries {
            for (index, column) in columns.iter_mut().enumerate() {
                column.push(input[batch_index].value(row, index)?.clone());
            }
        }
        Batch::new_with_row_count(schema.clone(), columns, count)
    }).collect()
}

#[cfg(test)]
mod partition_batches_feature_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn keys_remain_colocated_across_batch_boundaries() {
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, true)]).unwrap();
        let first = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(9)], vec![Value::Null]
        ]).unwrap();
        let second = Batch::from_rows(schema, vec![
            vec![Value::Integer(9)], vec![Value::Null]
        ]).unwrap();
        let result = partition_batches(&[first, second], &["key"], 8).unwrap();
        assert_eq!(result.iter().map(Batch::num_rows).sum::<usize>(), 4);
        assert!(result.iter().any(|bucket| bucket.column(0).unwrap() ==
            [Value::Integer(9), Value::Integer(9)]));
        assert!(result.iter().any(|bucket| bucket.column(0).unwrap() ==
            [Value::Null, Value::Null]));
        assert!(partition_batches(&[], &["key"], 2).is_err());
    }
}


#[cfg(test)]
mod partition_allocation_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn refuses_excessive_buckets_without_allocating_them() {
        let batch = Batch::empty(
            Schema::new(vec![Field::new("id", DataType::Integer, true)]).unwrap(),
        );
        assert!(partition(&batch, &["id"], usize::MAX).is_err());
        assert!(partition_batches(&[batch.clone()], &["id"], usize::MAX).is_err());
        assert!(partition_batches(&[batch], &["id"], 0).is_err());
    }
}

#[cfg(test)]
mod direct_multi_batch_partition_perf_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn direct_partition_matches_individual_source_partitions() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, true),
            Field::new("payload", DataType::Text, false),
        ]).unwrap();
        let inputs = (0..6).map(|part| {
            Batch::from_rows(schema.clone(), (0..27).map(|n| vec![
                if n % 9 == 0 { Value::Null } else { Value::Integer(n % 7) },
                Value::Text(format!("{part}:{n}:{}", "x".repeat(120))),
            ]).collect()).unwrap()
        }).collect::<Vec<_>>();
        for count in [1, 2, 7, 16] {
            let combined = partition_batches(&inputs, &["id"], count).unwrap();
            let reference = (0..count).map(|bucket| {
                inputs.iter().flat_map(|input| partition(input, &["id"], count).unwrap()[bucket].rows().unwrap())
                    .collect::<Vec<_>>()
            }).collect::<Vec<_>>();
            for (batch, expected) in combined.iter().zip(reference) {
                assert_eq!(batch.rows().unwrap(), expected);
            }
        }
    }
}

#[cfg(test)]
mod bitmasked_bucket_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};
    #[test]
    fn power_of_two_and_irregular_fanout_match_remainder_reference() {
        let schema = Schema::new(vec![Field::new("k", DataType::Integer, true)]).unwrap();
        let rows = (0..1_200).map(|n| vec![if n % 29 == 0 { Value::Null } else { Value::Integer(n % 97) }]).collect();
        let batch = Batch::from_rows(schema, rows).unwrap();
        for fanout in [1, 2, 8, 32, 64, 127, 256, 1024, 4096] {
            let actual = partition(&batch, &["k"], fanout).unwrap();
            let mut expected = vec![Vec::new(); fanout];
            for row in 0..batch.num_rows() {
                let mut hasher = PartitionHasher::default();
                batch.value(row, 0).unwrap().hash(&mut hasher);
                expected[(hasher.finish() % fanout as u64) as usize].push(row);
            }
            for (part, indices) in actual.iter().zip(expected) {
                assert_eq!(part, &batch.take(&indices).unwrap());
            }
        }
    }
}

#[cfg(test)]
mod multibatch_power_two_partition_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};
    #[test]
    fn power_of_two_fast_path_matches_single_batch_buckets() {
        let schema = Schema::new(vec![Field::new("k", DataType::Integer, true)]).unwrap();
        let inputs = (0..3).map(|part| Batch::from_rows(schema.clone(),
            (0..67).map(|n| vec![if n % 17 == 0 { Value::Null } else {
                Value::Integer(n % 19 + part) }]).collect()).unwrap()).collect::<Vec<_>>();
        for count in [1, 2, 8, 64, 127] {
            let actual = partition_batches(&inputs, &["k"], count).unwrap();
            for bucket in 0..count {
                let expected = inputs.iter().flat_map(|input|
                    partition(input, &["k"], count).unwrap()[bucket].rows().unwrap()
                ).collect::<Vec<_>>();
                assert_eq!(actual[bucket].rows().unwrap(), expected);
            }
        }
    }
}
