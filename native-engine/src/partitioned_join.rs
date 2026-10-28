//! Partition-local hash joins for independent native exchange stages.
use crate::{
    batch::Batch,
    hash_partition,
    join::{self, JoinKind},
    Error, Result,
};

/// Join equal-key partitions independently. Output partition order is
/// deterministic, but SQL consumers must not rely on row ordering without
/// an explicit ORDER BY.
pub fn join_partitions(
    left: &Batch,
    right: &Batch,
    left_key: &str,
    right_key: &str,
    kind: JoinKind,
    partitions: usize,
) -> Result<Vec<Batch>> {
    let l = left.schema().field(left.schema().column(left_key)?)?.data_type;
    let r = right.schema().field(right.schema().column(right_key)?)?.data_type;
    if l != r {
        return Err(Error::Type("partitioned join keys must have equal types".into()));
    }
    let left_parts = hash_partition::partition(left, &[left_key], partitions)?;
    let right_parts = hash_partition::partition(right, &[right_key], partitions)?;
    left_parts.iter().zip(right_parts.iter())
        .map(|(l, r)| join::hash_join(l, r, left_key, right_key, kind))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        scalar::Value,
        schema::{DataType, Field, Schema},
    };

    fn table(name: &str, values: Vec<Value>) -> Batch {
        Batch::from_rows(
            Schema::new(vec![Field::new(name, DataType::Integer, true)]).unwrap(),
            values.into_iter().map(|value| vec![value]).collect(),
        ).unwrap()
    }

    fn sorted_rows(batch: &Batch) -> Vec<String> {
        let mut rows = batch.rows().unwrap().into_iter()
            .map(|row| format!("{row:?}")).collect::<Vec<_>>();
        rows.sort();
        rows
    }

    #[test]
    fn partitioned_join_matches_reference_hash_join() {
        let left = table("lid", vec![
            Value::Integer(2), Value::Integer(1), Value::Integer(1), Value::Null,
        ]);
        let right = table("rid", vec![
            Value::Integer(1), Value::Integer(1), Value::Integer(3), Value::Null,
        ]);
        for kind in [JoinKind::Inner, JoinKind::Left] {
            let expected = join::hash_join(&left, &right, "lid", "rid", kind).unwrap();
            let parts = join_partitions(&left, &right, "lid", "rid", kind, 11).unwrap();
            let mut rows = parts.iter().flat_map(sorted_rows).collect::<Vec<_>>();
            rows.sort();
            assert_eq!(rows, sorted_rows(&expected));
            assert_eq!(parts.iter().map(Batch::num_rows).sum::<usize>(), expected.num_rows());
        }
    }

    #[test]
    fn bad_partition_count_is_rejected() {
        let a = table("id", vec![Value::Integer(1)]);
        assert!(join_partitions(&a, &a, "id", "id", JoinKind::Inner, 0).is_err());
    }
}
