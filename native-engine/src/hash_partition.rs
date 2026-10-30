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
    if partitions == 1 {
        // Every row hashes to partition zero. Keep the validated input batch
        // directly instead of allocating key groups and hashing all rows.
        return Ok(vec![batch.clone()]);
    }
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

    if partitions == 1 {
        // A single destination needs no hashing or row-coordinate buffers.
        let total = input.iter().try_fold(0usize, |rows, batch| {
            rows.checked_add(batch.num_rows())
                .ok_or_else(|| Error::Execution("partition row count overflow".into()))
        })?;
        let mut columns = (0..schema.len()).map(|_| Vec::with_capacity(total))
            .collect::<Vec<_>>();
        for batch in input {
            for (index, target) in columns.iter_mut().enumerate() {
                target.extend_from_slice(batch.column(index)?);
            }
        }
        return Ok(vec![Batch::new_with_row_count(schema.clone(), columns, total)?]);
    }

    // Keep only row coordinates until all source batches have been partitioned.
    let mut buckets: Vec<Vec<(usize, usize)>> = vec![Vec::new(); partitions];
    let mask = partitions.is_power_of_two().then_some(partitions - 1);
    for (batch_index, batch) in input.iter().enumerate() {
        let key_slices = key_columns.iter().map(|&column| batch.column(column))
            .collect::<Result<Vec<_>>>()?;
        for row in 0..batch.num_rows() {
            let mut hasher = PartitionHasher::default();
            for values in &key_slices {
                values[row].hash(&mut hasher);
            }
            let hash = hasher.finish();
            let bucket = match mask {
                Some(mask) => (hash as usize) & mask,
                None => (hash % partitions as u64) as usize,
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
mod one_partition_hash_shortcut_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn one_partition_preserves_source_and_validates_keys() {
        let schema = Schema::new(vec![
            Field::new("k", DataType::Text, true),
            Field::new("v", DataType::Integer, false),
        ]).unwrap();
        let input = Batch::from_rows(schema, vec![
            vec![Value::Text("é🙂".repeat(1024)), Value::Integer(1)],
            vec![Value::Null, Value::Integer(2)],
        ]).unwrap();
        assert_eq!(partition(&input, &["k"], 1).unwrap(), vec![input.clone()]);
        assert!(partition(&input, &["missing"], 1).is_err());
        assert!(partition(&input, &[], 1).is_err());
        assert_eq!(partition(&Batch::empty(input.schema().clone()), &["k"], 1)
            .unwrap()[0].num_rows(), 0);
    }
}

#[cfg(test)]
mod single_bucket_multi_batch_fast_path_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn skips_hashing_but_keeps_every_row_in_input_order() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("text", DataType::Text, true),
        ]).unwrap();
        let first = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1), Value::Null],
            vec![Value::Integer(2), Value::Text("é".into())],
        ]).unwrap();
        let second = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1), Value::Text("long".repeat(500))],
        ]).unwrap();
        let output = partition_batches(&[first.clone(), second.clone()], &["id"], 1).unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].rows().unwrap(), first.rows().unwrap()
            .into_iter().chain(second.rows().unwrap()).collect::<Vec<_>>());
        assert_eq!(output[0].schema(), &schema);
        assert!(partition_batches(&[first], &["missing"], 1).is_err());
    }
}

#[cfg(test)]
mod multi_batch_bitmask_regressions {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};
    #[test]
    fn bucket_assignment_matches_single_batch_for_both_fanouts() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, true)]).unwrap();
        let parts = (0..3).map(|part| Batch::from_rows(schema.clone(),
            (0..45).map(|n| vec![if n % 11 == 0 { Value::Null }
                else { Value::Integer((part + n) % 13) }]).collect()).unwrap()).collect::<Vec<_>>();
        for fanout in [1, 2, 8, 32, 63, 127] {
            let actual = partition_batches(&parts, &["id"], fanout).unwrap();
            for bucket in 0..fanout {
                let expected = parts.iter().flat_map(|part| partition(part, &["id"], fanout).unwrap()[bucket].rows().unwrap()).collect::<Vec<_>>();
                assert_eq!(actual[bucket].rows().unwrap(), expected);
            }
        }
    }
}

#[cfg(test)]
mod prebound_multi_batch_keys_tests {
    use super::*;
    use crate::{schema::{Schema, Field, DataType}, scalar::Value};

    #[test]
    fn hashes_multiple_nullable_keys_into_identical_buckets() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Integer, true),
            Field::new("b", DataType::Text, true),
        ]).unwrap();
        let data = (0..25).map(|n| vec![
            if n % 3 == 0 { Value::Null } else { Value::Integer(n % 7) },
            if n % 5 == 0 { Value::Null } else { Value::Text(format!("s{}", n % 4)) },
        ]).collect::<Vec<_>>();
        let first = Batch::from_rows(schema.clone(), data[..12].to_vec()).unwrap();
        let second = Batch::from_rows(schema, data[12..].to_vec()).unwrap();
        for count in [2, 8, 9] {
            let actual = partition_batches(&[first.clone(), second.clone()], &["a", "b"], count).unwrap();
            let reference = (0..count).map(|bucket| {
                [first.clone(), second.clone()].iter().flat_map(|batch|
                    partition(batch, &["a", "b"], count).unwrap()[bucket].rows().unwrap()
                ).collect::<Vec<_>>()
            }).collect::<Vec<_>>();
            for (batch, expected) in actual.iter().zip(reference) {
                assert_eq!(batch.rows().unwrap(), expected);
            }
        }
    }
}
