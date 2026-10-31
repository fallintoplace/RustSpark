use std::hash::{Hash, Hasher};

use crate::{batch::Batch, Error, Result};

pub(crate) struct PartitionHasher(u64);

impl Default for PartitionHasher {
    fn default() -> Self {
        // FNV-1a's offset basis is the hash of an empty byte sequence.
        // write(&[]) must be a no-op, not an implicit initializer.
        Self(0xcbf29ce484222325)
    }
}

impl Hasher for PartitionHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
    }

    // Hash::hash invokes typed Hasher methods for numeric keys. The default
    // methods use native-endian bytes, which can send the same SQL key to
    // different shuffle buckets on mixed-endian worker architectures.
    fn write_u16(&mut self, value: u16) { self.write(&value.to_le_bytes()); }
    fn write_u32(&mut self, value: u32) { self.write(&value.to_le_bytes()); }
    fn write_u64(&mut self, value: u64) { self.write(&value.to_le_bytes()); }
    fn write_u128(&mut self, value: u128) { self.write(&value.to_le_bytes()); }
    fn write_usize(&mut self, value: usize) { self.write(&(value as u64).to_le_bytes()); }
    fn write_i16(&mut self, value: i16) { self.write(&value.to_le_bytes()); }
    fn write_i32(&mut self, value: i32) { self.write(&value.to_le_bytes()); }
    fn write_i64(&mut self, value: i64) { self.write(&value.to_le_bytes()); }
    fn write_i128(&mut self, value: i128) { self.write(&value.to_le_bytes()); }
    fn write_isize(&mut self, value: isize) { self.write(&(value as i64).to_le_bytes()); }
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

    // Bind each source column once. Hashing and output reconstruction can
    // access borrowed slices without repeated Batch::value bounds checks.
    let sources = input.iter().map(|batch| (0..schema.len())
        .map(|index| batch.column(index)).collect::<Result<Vec<_>>>()
    ).collect::<Result<Vec<_>>>()?;
    // Construct each destination column on the first matching row.
    // This avoids retaining O(total_rows) (batch, row) coordinates and a
    // separate second pass to materialize the partition payloads.
    let mut buckets: Vec<Option<Vec<Vec<crate::scalar::Value>>>> =
        std::iter::repeat_with(|| None).take(partitions).collect();
    let mut row_counts = vec![0usize; partitions];
    let mask = partitions.is_power_of_two().then_some(partitions - 1);
    for (batch_index, batch) in input.iter().enumerate() {
        let bound = &sources[batch_index];
        for row in 0..batch.num_rows() {
            let mut hasher = PartitionHasher::default();
            for &column in &key_columns {
                bound[column][row].hash(&mut hasher);
            }
            let hash = hasher.finish();
            let bucket = match mask {
                Some(mask) => (hash as usize) & mask,
                None => (hash % partitions as u64) as usize,
            };
            let columns = buckets[bucket].get_or_insert_with(|| {
                (0..schema.len()).map(|_| Vec::new()).collect::<Vec<_>>()
            });
            for (destination, source) in columns.iter_mut().zip(bound) {
                destination.push(source[row].clone());
            }
            row_counts[bucket] += 1;
        }
    }
    buckets.into_iter().zip(row_counts).map(|(columns, rows)| {
        let columns = columns.unwrap_or_else(|| {
            (0..schema.len()).map(|_| Vec::new()).collect()
        });
        Batch::new_with_row_count(schema.clone(), columns, rows)
    }).collect()
}


/** Hash partition owned input batches while moving rather than cloning rows.
 *
 * Compute destination counts before moving input columns, so each bucket
 * allocates exactly the number of retained cells. The placement vector is
 * bounded by input row count and is reused across all source columns.
 */
pub fn partition_batches_owned(
    input: Vec<Batch>, keys: &[&str], partitions: usize,
) -> Result<Vec<Batch>> {
    if partitions == 0 || partitions > 4096 || keys.is_empty() {
        return Err(Error::Execution(
            "hash partitions must be in 1..=4096 with nonempty keys".into(),
        ));
    }
    let first = input.first()
        .ok_or_else(|| Error::Schema("partition input needs a schema".into()))?;
    let schema = first.schema().clone();
    let key_columns = keys.iter().map(|key| schema.column(key))
        .collect::<Result<Vec<_>>>()?;
    if input.iter().any(|batch| batch.schema() != &schema) {
        return Err(Error::Schema("partition input schemas differ".into()));
    }
    let total = input.iter().try_fold(0usize, |sum, batch| {
        sum.checked_add(batch.num_rows())
            .ok_or_else(|| Error::Execution("partition row count overflow".into()))
    })?;
    if partitions == 1 {
        if input.len() == 1 {
            return Ok(input);
        }
        let mut combined = (0..schema.len())
            .map(|_| Vec::with_capacity(total)).collect::<Vec<_>>();
        for batch in input {
            for (destination, source) in combined.iter_mut().zip(batch.into_columns()) {
                destination.extend(source);
            }
        }
        return Ok(vec![Batch::new_with_row_count(schema, combined, total)?]);
    }

    let mut counts = vec![0usize; partitions];
    let mut placements = Vec::with_capacity(input.len());
    let mask = partitions.is_power_of_two().then_some(partitions - 1);
    for batch in &input {
        let columns = key_columns.iter().map(|&index| batch.column(index))
            .collect::<Result<Vec<_>>>()?;
        let mut destinations = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let mut hasher = PartitionHasher::default();
            for column in &columns { column[row].hash(&mut hasher); }
            let hash = hasher.finish();
            let bucket = match mask {
                Some(mask) => (hash as usize) & mask,
                None => (hash % partitions as u64) as usize,
            };
            counts[bucket] = counts[bucket].checked_add(1)
                .ok_or_else(|| Error::Execution("partition row count overflow".into()))?;
            destinations.push(bucket);
        }
        placements.push(destinations);
    }

    let width = schema.len();
    let mut output = counts.iter().map(|&count| {
        (count != 0).then(|| (0..width)
            .map(|_| Vec::with_capacity(count)).collect::<Vec<_>>())
    }).collect::<Vec<_>>();
    for (batch, destinations) in input.into_iter().zip(placements) {
        for (column, source) in batch.into_columns().into_iter().enumerate() {
            for (bucket, value) in destinations.iter().copied().zip(source) {
                output[bucket].as_mut().expect("destination has nonzero rows")[column]
                    .push(value);
            }
        }
    }
    output.into_iter().zip(counts).map(|(columns, rows)| {
        Batch::new_with_row_count(schema.clone(),
            columns.unwrap_or_else(|| vec![Vec::new(); width]), rows)
    }).collect()
}

/// Consume a single input batch without duplicating heap-backed payloads.
pub fn partition_owned(
    batch: Batch, keys: &[&str], partitions: usize,
) -> Result<Vec<Batch>> {
    partition_batches_owned(vec![batch], keys, partitions)
}

#[cfg(test)]
mod owned_hash_partition_regressions {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("key", DataType::Integer, true),
            Field::new("text", DataType::Text, true),
        ]).unwrap()
    }

    #[test]
    fn all_fanouts_match_borrowed_partitioning_and_move_text() {
        let rows = (0..320).map(|row| vec![
            if row % 23 == 0 { Value::Null } else { Value::Integer(row % 53) },
            if row == 119 { Value::Text("é🙂".repeat(4096)) }
            else if row % 17 == 0 { Value::Null }
            else { Value::Text(format!("item-{row}")) },
        ]).collect::<Vec<_>>();
        let input = Batch::from_rows(schema(), rows).unwrap();
        let ptr = match input.value(119, 1).unwrap() {
            Value::Text(text) => text.as_ptr(),
            _ => unreachable!(),
        };
        for count in [1, 2, 7, 16, 257] {
            let expected = partition(&input, &["key"], count).unwrap();
            let output = partition_owned(input.clone(), &["key"], count).unwrap();
            assert_eq!(output, expected, "fanout {count}");
        }
        let actual = partition_owned(input, &["key"], 7).unwrap();
        let moved = actual.iter().flat_map(|batch| batch.column(1).unwrap())
            .find(|cell| matches!(cell, Value::Text(text) if text.len() > 4096))
            .unwrap();
        assert_eq!(match moved { Value::Text(text) => text.as_ptr(), _ => unreachable!() }, ptr);
    }

    #[test]
    fn across_batches_moving_matches_borrowed_and_preserves_errors() {
        let batches = (0..5).map(|part| Batch::from_rows(schema(),
            (0..37).map(|row| vec![
                Value::Integer((row % 13) as i64),
                Value::Text(format!("{part}:{row}")),
            ]).collect()).unwrap()).collect::<Vec<_>>();
        for count in [1, 3, 8, 64] {
            let expected = partition_batches(&batches, &["key"], count).unwrap();
            assert_eq!(partition_batches_owned(batches.clone(), &["key"], count).unwrap(), expected);
        }
        assert!(partition_batches_owned(vec![], &["key"], 4).is_err());
        assert!(partition_batches_owned(batches.clone(), &[], 4).is_err());
        assert!(partition_batches_owned(batches.clone(), &["unknown"], 4).is_err());
        assert!(partition_batches_owned(batches, &["key"], 4097).is_err());
    }
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

#[cfg(test)]
mod borrowed_shuffle_column_sources_tests {
    use super::*;
    use crate::{schema::{DataType, Field, Schema}, scalar::Value};

    #[test]
    fn wide_mixed_columns_match_partitioning_each_batch_separately() {
        let schema = Schema::new(vec![
            Field::new("integer", DataType::Integer, true),
            Field::new("text", DataType::Text, true),
            Field::new("flag", DataType::Boolean, true),
            Field::new("payload", DataType::Text, false),
        ]).unwrap();
        let inputs = (0..5).map(|part| Batch::from_rows(schema.clone(),
            (0..31).map(|row| vec![
                if row % 11 == 0 { Value::Null }
                    else { Value::Integer((row + part) % 7) },
                if row % 5 == 0 { Value::Null }
                    else { Value::Text(format!("segment-{}", (row + part) % 9)) },
                if row % 3 == 0 { Value::Null } else { Value::Boolean(row % 2 == 0) },
                Value::Text(format!("{part}:{row}:{}", "É🙂".repeat(128))),
            ]).collect()).unwrap()).collect::<Vec<_>>();
        for fanout in [2, 7, 16, 127] {
            let output = partition_batches(&inputs, &["integer", "text"], fanout).unwrap();
            for shard in 0..fanout {
                let expected = inputs.iter().flat_map(|batch|
                    partition(batch, &["integer", "text"], fanout).unwrap()[shard]
                        .rows().unwrap()
                ).collect::<Vec<_>>();
                assert_eq!(output[shard].rows().unwrap(), expected);
            }
        }
    }
}

#[cfg(test)]
mod direct_column_partition_regressions {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn sparse_and_dense_fanouts_match_per_batch_partitioning() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Text, true),
            Field::new("id", DataType::Integer, false),
            Field::new("payload", DataType::Text, false),
        ]).unwrap();
        let batches = (0..17).map(|part| {
            Batch::from_rows(schema.clone(), (0..61).map(|row| vec![
                if row % 7 == 0 { Value::Null } else {
                    Value::Text(format!("key-{}", row % 11)) },
                Value::Integer(part * 100 + row),
                Value::Text(format!("{part}:{row}:{}", "é".repeat(20))),
            ]).collect()).unwrap()
        }).collect::<Vec<_>>();
        for partitions in [2, 7, 64, 1024] {
            let direct = partition_batches(&batches, &["key"], partitions).unwrap();
            assert_eq!(direct.len(), partitions);
            assert_eq!(direct.iter().map(Batch::num_rows).sum::<usize>(), 17 * 61);
            let pieces = batches.iter().map(|batch| partition(batch, &["key"], partitions).unwrap())
                .collect::<Vec<_>>();
            for index in 0..partitions {
                let expected = pieces.iter().flat_map(|set| set[index].rows().unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(direct[index].rows().unwrap(), expected);
            }
        }
    }
}

#[cfg(test)]
mod partition_hasher_state_regressions {
    use super::PartitionHasher;
    use std::hash::Hasher;

    #[test]
    fn empty_writes_are_noops_and_default_hashes_empty_input() {
        let empty = PartitionHasher::default().finish();
        assert_eq!(empty, 0xcbf29ce484222325);
        let mut hasher = PartitionHasher::default();
        hasher.write(&[]);
        hasher.write(&[]);
        assert_eq!(hasher.finish(), empty);
    }

    #[test]
    fn hash_is_independent_of_write_boundaries() {
        let value = b"native-shuffle-partition-key";
        let mut whole = PartitionHasher::default();
        whole.write(value);
        let mut fragments = PartitionHasher::default();
        fragments.write(&[]);
        fragments.write(&value[..4]);
        fragments.write(&value[4..15]);
        fragments.write(&[]);
        fragments.write(&value[15..]);
        assert_eq!(fragments.finish(), whole.finish());
    }
}

#[cfg(test)]
mod portable_typed_partition_hash_tests {
    use super::*;

    #[test]
    fn integer_hash_matches_canonical_little_endian_bytes() {
        let value = 0x0123_4567_89ab_cdef_u64;
        let mut typed = PartitionHasher::default();
        value.hash(&mut typed);
        let mut bytes = PartitionHasher::default();
        bytes.write(&value.to_le_bytes());
        assert_eq!(typed.finish(), bytes.finish());

        let mut typed = PartitionHasher::default();
        (-0x123456_i64).hash(&mut typed);
        let mut bytes = PartitionHasher::default();
        bytes.write(&(-0x123456_i64).to_le_bytes());
        assert_eq!(typed.finish(), bytes.finish());
    }

    #[test]
    fn hash_is_stable_across_numeric_write_widths_and_repeated_calls() {
        let mut typed = PartitionHasher::default();
        typed.write_u16(0x0102);
        typed.write_i32(-3);
        typed.write_usize(17);
        let mut bytes = PartitionHasher::default();
        bytes.write(&0x0102_u16.to_le_bytes());
        bytes.write(&(-3_i32).to_le_bytes());
        bytes.write(&(17_u64).to_le_bytes());
        assert_eq!(typed.finish(), bytes.finish());
    }
}
