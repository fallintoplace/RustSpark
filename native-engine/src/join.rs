use std::collections::{HashMap, HashSet};

use crate::{
    batch::Batch,
    scalar::Value,
    schema::{Field, Schema},
    Error,
    Result,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
}

const MAX_JOIN_ROWS: usize = 1_000_000;
const MAX_MATERIALIZED_JOIN_BYTES: usize = 256 * 1024 * 1024;

fn joinable(key: &Value) -> bool {
    !key.is_null()
}


/// Cartesian product with an output bound before materialization.
/// This is a separate SQL operator from an equality hash join.
pub fn cross_join(left: &Batch, right: &Batch) -> Result<Batch> {
    cross_join_with_max_rows(left, right, MAX_JOIN_ROWS)
}

/// Enforce the query's output limit before allocating the Cartesian product.
pub fn cross_join_with_max_rows(
    left: &Batch, right: &Batch, max_output_rows: usize,
) -> Result<Batch> {
    let count = left.num_rows().checked_mul(right.num_rows())
        .ok_or_else(|| Error::Execution("CROSS JOIN output cardinality overflow".into()))?;
    if count > MAX_JOIN_ROWS.min(max_output_rows) {
        return Err(Error::Execution("CROSS JOIN output exceeds safety limit".into()));
    }
    let mut used: HashSet<String> = left.schema().fields().iter()
        .map(|field| field.name.to_ascii_lowercase()).collect();
    let mut fields = left.schema().fields().to_vec();
    for field in right.schema().fields() {
        let mut name = field.name.clone();
        while used.contains(&name.to_ascii_lowercase()) {
            name = format!("right_{name}");
        }
        used.insert(name.to_ascii_lowercase());
        fields.push(Field::new(name, field.data_type, field.nullable));
    }
    let schema = Schema::new(fields)?;
    if count == 0 {
        return Ok(Batch::empty(schema));
    }
    // Row-count limits alone permit huge allocations when both sides have
    // wide TEXT values. Preflight the retained output before cloning cells.
    let fixed_width = schema.len().checked_mul(std::mem::size_of::<Value>())
        .ok_or_else(|| Error::Execution("CROSS JOIN output byte estimate overflow".into()))?;
    let mut output_bytes = count.checked_mul(fixed_width)
        .filter(|bytes| *bytes <= MAX_MATERIALIZED_JOIN_BYTES)
        .ok_or_else(|| Error::Execution("CROSS JOIN output exceeds byte limit".into()))?;
    for (source, repeats) in [(left, right.num_rows()), (right, left.num_rows())] {
        for (column, field) in source.schema().fields().iter().enumerate() {
            if field.data_type != crate::schema::DataType::Text { continue; }
            for value in source.column(column)? {
                if let Value::Text(text) = value {
                    let bytes = text.len().checked_mul(repeats)
                        .ok_or_else(|| Error::Execution("CROSS JOIN output byte estimate overflow".into()))?;
                    output_bytes = output_bytes.checked_add(bytes)
                        .filter(|total| *total <= MAX_MATERIALIZED_JOIN_BYTES)
                        .ok_or_else(|| Error::Execution("CROSS JOIN output exceeds byte limit".into()))?;
                }
            }
        }
    }
    let mut columns = Vec::with_capacity(schema.len());
    for index in 0..left.schema().len() {
        let source = left.column(index)?;
        let mut output = Vec::with_capacity(count);
        for value in source {
            output.extend(std::iter::repeat_n(value.clone(), right.num_rows()));
        }
        columns.push(output);
    }
    for index in 0..right.schema().len() {
        let source = right.column(index)?;
        let mut output = Vec::with_capacity(count);
        for _ in 0..left.num_rows() {
            output.extend_from_slice(source);
        }
        columns.push(output);
    }
    Batch::from_validated_columns(schema, columns, count)
}

// Count the retained output, not merely row cardinality. One joined key can
// duplicate a large TEXT value millions of times.
fn join_text_columns(batch: &Batch) -> Result<Vec<&[Value]>> {
    batch.schema().fields().iter().enumerate()
        .filter(|(_, field)| field.data_type == crate::schema::DataType::Text)
        .map(|(column, _)| batch.column(column))
        .collect()
}

fn join_fixed_row_bytes(schema: &Schema) -> Result<usize> {
    schema.len().checked_mul(std::mem::size_of::<Value>())
        .ok_or_else(|| Error::Execution("join output byte estimate overflow".into()))
}

fn charge_join_row(
    used: &mut usize, fixed: usize,
    left_text: &[&[Value]], left_row: Option<usize>,
    right_text: &[&[Value]], right_row: Option<usize>,
    limit: usize,
) -> Result<()> {
    let mut bytes = fixed;
    for (columns, row) in [(left_text, left_row), (right_text, right_row)] {
        if let Some(row) = row {
            for column in columns {
                if let Value::Text(value) = &column[row] {
                    bytes = bytes.checked_add(value.len())
                        .ok_or_else(|| Error::Execution("join output byte estimate overflow".into()))?;
                }
            }
        }
    }
    *used = used.checked_add(bytes).filter(|size| *size <= limit)
        .ok_or_else(|| Error::Execution("join output exceeds byte limit".into()))?;
    Ok(())
}

// RIGHT/FULL append build-only rows after probing. Reclaim the exact budget
// already spent by the matched prefix before considering the unmatched tail.
fn materialized_join_bytes(batch: &Batch) -> Result<usize> {
    let mut used = batch.num_rows()
        .checked_mul(join_fixed_row_bytes(batch.schema())?)
        .filter(|size| *size <= MAX_MATERIALIZED_JOIN_BYTES)
        .ok_or_else(|| Error::Execution("join output exceeds byte limit".into()))?;
    for column in join_text_columns(batch)? {
        for value in column {
            if let Value::Text(text) = value {
                used = used.checked_add(text.len())
                    .filter(|size| *size <= MAX_MATERIALIZED_JOIN_BYTES)
                    .ok_or_else(|| Error::Execution("join output exceeds byte limit".into()))?;
            }
        }
    }
    Ok(used)
}

pub fn hash_join(
    left: &Batch,
    right: &Batch,
    left_key: &str,
    right_key: &str,
    kind: JoinKind,
) -> Result<Batch> {
    let left_column = left.schema().column(left_key)?;
    let right_column = right.schema().column(right_key)?;
    if left.schema().field(left_column)?.data_type != right.schema().field(right_column)?.data_type {
        return Err(Error::Type("join key types must match".into()));
    }
    let left_keys = left.column(left_column)?;
    let right_keys = right.column(right_column)?;
    if left.num_rows() > 0 && right.num_rows() > 0
        && matches!(kind, JoinKind::Right | JoinKind::Full) {
        // RIGHT and FULL joins can use the same columnar hash probe as
        // INNER and LEFT. Track matched build rows, then emit the unmatched
        // right suffix without allocating one Vec per joined output row.
        let probe_kind = if kind == JoinKind::Right { JoinKind::Inner } else { JoinKind::Left };
        let prepared = PreparedHashJoin::new(
            left.schema().clone(), right, left_key, right_key, probe_kind,
        )?;
        let mut matched_right = vec![false; right.num_rows()];
        let probed = prepared.probe_with_matches(left, Some(&mut matched_right))?;
        let schema = hash_join(
            &Batch::empty(left.schema().clone()),
            &Batch::empty(right.schema().clone()),
            left_key, right_key, kind,
        )?.schema().clone();
        let mut count = probed.num_rows();
        let mut bytes = materialized_join_bytes(&probed)?;
        let fixed = join_fixed_row_bytes(&schema)?;
        let right_text = join_text_columns(right)?;
        let mut columns = probed.into_columns();
        for (right_row, matched) in matched_right.into_iter().enumerate() {
            if matched { continue; }
            if count >= MAX_JOIN_ROWS {
                return Err(Error::Execution("join output exceeds safety limit".into()));
            }
            charge_join_row(&mut bytes, fixed, &[], None, &right_text,
                Some(right_row), MAX_MATERIALIZED_JOIN_BYTES)?;
            for destination in columns.iter_mut().take(left.schema().len()) {
                destination.push(Value::Null);
            }
            for (index, destination) in columns.iter_mut()
                .skip(left.schema().len()).enumerate() {
                destination.push(right.value(right_row, index)?.clone());
            }
            count += 1;
        }
        return Batch::from_validated_columns(schema, columns, count);
    }
    if left.num_rows() > 0 && right.num_rows() > 0
        && matches!(kind, JoinKind::Inner | JoinKind::Left | JoinKind::Semi | JoinKind::Anti) {
        // Prepared joins build columnar output directly instead of allocating
        // a row vector for every match. Empty inputs keep the schema-only path.
        return PreparedHashJoin::new(left.schema().clone(), right, left_key, right_key, kind)?
            .probe(left);
    }
    if matches!(kind, JoinKind::Semi | JoinKind::Anti) {
        let mut keys: HashSet<&Value> = HashSet::with_capacity(right.num_rows());
        for row in 0..right.num_rows() {
            let key = &right_keys[row];
            if joinable(key) { keys.insert(key); }
        }
        let mut indices = Vec::new();
        for row in 0..left.num_rows() {
            let key = &left_keys[row];
            let found = joinable(key) && keys.contains(key);
            if (kind == JoinKind::Semi && found) || (kind == JoinKind::Anti && !found) {
                if indices.len() >= MAX_JOIN_ROWS {
                    return Err(Error::Execution("join output exceeds safety limit".into()));
                }
                indices.push(row);
            }
        }

        let fixed = join_fixed_row_bytes(left.schema())?;
        let text = join_text_columns(left)?;
        let mut bytes = 0usize;
        for &row in &indices {
            charge_join_row(&mut bytes, fixed, &text, Some(row), &[], None,
                MAX_MATERIALIZED_JOIN_BYTES)?;
        }
        return left.take(&indices);
    }
    let mut used: HashSet<String> = left
        .schema()
        .fields()
        .iter()
        .map(|field| field.name.to_ascii_lowercase())
        .collect();
    let mut fields = left.schema().fields().iter().map(|field| {
        Field::new(&field.name, field.data_type,
            field.nullable || matches!(kind, JoinKind::Right | JoinKind::Full))
    }).collect::<Vec<_>>();
    for field in right.schema().fields() {
        let mut name = field.name.clone();
        while used.contains(&name.to_ascii_lowercase()) {
            name = format!("right_{name}");
        }
        used.insert(name.to_ascii_lowercase());
        fields.push(Field::new(name, field.data_type,
            field.nullable || matches!(kind, JoinKind::Left | JoinKind::Full)));
    }
    let schema = Schema::new(fields)?;
    if kind == JoinKind::Inner && (left.num_rows() == 0 || right.num_rows() == 0) {
        return Ok(Batch::empty(schema));
    }
    let mut right_hash: HashMap<&Value, Vec<usize>> = HashMap::new();
    for row in 0..right.num_rows() {
        let key = &right_keys[row];
        if joinable(key) {
            right_hash.entry(key).or_default().push(row);
        }
    }
    let mut output = Vec::new();
    let mut output_bytes = 0usize;
    let fixed = join_fixed_row_bytes(&schema)?;
    let left_text = join_text_columns(left)?;
    let right_text = join_text_columns(right)?;
    let mut matched_right = if matches!(kind, JoinKind::Right | JoinKind::Full) {
        vec![false; right.num_rows()]
    } else {
        Vec::new()
    };
    for left_row in 0..left.num_rows() {
        let key = &left_keys[left_row];
        let right_rows = if joinable(key) { right_hash.get(&key) } else { None };
        if let Some(right_rows) = right_rows {
            let mut left_values = left.row(left_row)?;
            for (match_index, &right_row) in right_rows.iter().enumerate() {
                if !matched_right.is_empty() {
                    matched_right[right_row] = true;
                }
                if output.len() >= MAX_JOIN_ROWS {
                    return Err(Error::Execution("join output exceeds safety limit".into()));
                }
                charge_join_row(&mut output_bytes, fixed, &left_text, Some(left_row),
                    &right_text, Some(right_row), MAX_MATERIALIZED_JOIN_BYTES)?;
                let mut row = if match_index + 1 == right_rows.len() {
                    std::mem::take(&mut left_values)
                } else {
                    left_values.clone()
                };
                row.extend(right.row(right_row)?);
                output.push(row);
            }
        } else if matches!(kind, JoinKind::Left | JoinKind::Full) {
            if output.len() >= MAX_JOIN_ROWS {
                return Err(Error::Execution("join output exceeds safety limit".into()));
            }
            charge_join_row(&mut output_bytes, fixed, &left_text, Some(left_row),
                &right_text, None, MAX_MATERIALIZED_JOIN_BYTES)?;
            let mut row = left.row(left_row)?;
            row.extend(vec![Value::Null; right.schema().len()]);
            output.push(row);
        }
    }
    if matches!(kind, JoinKind::Right | JoinKind::Full) {
        for right_row in 0..right.num_rows() {
            if !matched_right[right_row] {
                if output.len() >= MAX_JOIN_ROWS {
                    return Err(Error::Execution("join output exceeds safety limit".into()));
                }
                charge_join_row(&mut output_bytes, fixed, &left_text, None,
                    &right_text, Some(right_row), MAX_MATERIALIZED_JOIN_BYTES)?;
                let mut row = vec![Value::Null; left.schema().len()];
                row.extend(right.row(right_row)?);
                output.push(row);
            }
        }
    }
    Batch::from_rows(schema, output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, Field};

    fn table(values: &[Value], name: &str) -> Batch {
        Batch::from_rows(
            Schema::new(vec![Field::new(name, DataType::Integer, true)]).unwrap(),
            values.iter().cloned().map(|value| vec![value]).collect(),
        )
        .unwrap()
    }

    #[test]
    fn full_outer_join_preserves_both_unmatched_sides() {
        let left = table(&[Value::Integer(1), Value::Integer(2), Value::Null], "id");
        let right = table(&[Value::Integer(2), Value::Integer(3), Value::Null], "id");
        let result = hash_join(&left, &right, "id", "id", JoinKind::Full).unwrap();
        assert_eq!(result.rows().unwrap(), vec![
            vec![Value::Integer(1), Value::Null],
            vec![Value::Integer(2), Value::Integer(2)],
            vec![Value::Null, Value::Null],
            vec![Value::Null, Value::Integer(3)],
            vec![Value::Null, Value::Null],
        ]);
    }

    #[test]
    fn right_join_includes_unmatched_right_rows_and_null_keys() {
        let left = table(&[Value::Integer(1)], "id");
        let right = table(&[Value::Integer(1), Value::Integer(2), Value::Null], "id");
        let result = hash_join(&left, &right, "id", "id", JoinKind::Right).unwrap();
        assert_eq!(result.rows().unwrap(), vec![
            vec![Value::Integer(1), Value::Integer(1)],
            vec![Value::Null, Value::Integer(2)],
            vec![Value::Null, Value::Null],
        ]);
    }

    #[test]
    fn left_join_preserves_unmatched_rows_and_never_matches_nulls() {
        let left = table(&[Value::Integer(1), Value::Null, Value::Integer(2)], "id");
        let right = table(&[Value::Integer(1), Value::Null], "id");
        let result = hash_join(&left, &right, "id", "id", JoinKind::Left).unwrap();
        assert_eq!(result.rows().unwrap(), vec![
            vec![Value::Integer(1), Value::Integer(1)],
            vec![Value::Null, Value::Null],
            vec![Value::Integer(2), Value::Null],
        ]);
        assert_eq!(result.schema().column("right_id").unwrap(), 1);
    }
    #[test]
    fn empty_inner_join_returns_typed_empty_output() {
        let left = table(&[], "id");
        let right = table(&[Value::Integer(1), Value::Null], "id");
        let result = hash_join(&left, &right, "id", "id", JoinKind::Inner).unwrap();
        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.schema().len(), 2);
        assert_eq!(hash_join(&right, &left, "id", "id", JoinKind::Inner).unwrap().num_rows(), 0);
    }
}

#[cfg(test)]
mod nan_join_key_tests {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn nan_keys_match_in_inner_semi_and_left_joins_like_spark() {
        let schema = Schema::new(vec![Field::new("n", DataType::Float, true)]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Float(f64::NAN)], vec![Value::Float(1.0)],
        ]).unwrap();
        let right = Batch::from_rows(schema, vec![
            vec![Value::Float(f64::NAN)], vec![Value::Float(1.0)],
        ]).unwrap();
        assert_eq!(hash_join(&left, &right, "n", "n", JoinKind::Inner).unwrap().num_rows(), 2);
        assert_eq!(hash_join(&left, &right, "n", "n", JoinKind::Semi).unwrap().num_rows(), 2);
        assert_eq!(hash_join(&left, &right, "n", "n", JoinKind::Anti).unwrap().num_rows(), 0);
        assert_eq!(hash_join(&left, &right, "n", "n", JoinKind::Left).unwrap().num_rows(), 2);
    }
}

#[cfg(test)]
mod one_to_many_join_perf_tests {
    use super::*;
    use crate::schema::{DataType, Field};
    #[test]
    fn repeated_match_rows_preserve_text_payloads_and_order() {
        let left = Batch::from_rows(Schema::new(vec![
            Field::new("id", DataType::Integer, false), Field::new("payload", DataType::Text, false),
        ]).unwrap(), vec![vec![Value::Integer(4), Value::Text("hello".into())]]).unwrap();
        let right = Batch::from_rows(Schema::new(vec![Field::new("key", DataType::Integer, false)]).unwrap(),
            vec![vec![Value::Integer(4)], vec![Value::Integer(4)], vec![Value::Integer(4)]]).unwrap();
        let joined = hash_join(&left, &right, "id", "key", JoinKind::Inner).unwrap();
        assert_eq!(joined.num_rows(), 3);
        assert!(joined.rows().unwrap().iter().all(|row| row[1] == Value::Text("hello".into())));
    }
}

#[cfg(test)]
mod borrowed_semi_join_keys_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};
    #[test]
    fn avoids_materialization_without_changing_null_or_duplicate_results() {
        let schema = Schema::new(vec![Field::new("k", DataType::Text, true)]).unwrap();
        let long = "large-key".repeat(500);
        let left = Batch::from_rows(schema.clone(), vec![vec![Value::Text(long.clone())], vec![Value::Null], vec![Value::Text("other".into())], vec![Value::Text(long.clone())]]).unwrap();
        let right = Batch::from_rows(schema, vec![vec![Value::Null], vec![Value::Text(long.clone())], vec![Value::Text(long.clone())]]).unwrap();
        assert_eq!(hash_join(&left, &right, "k", "k", JoinKind::Semi).unwrap().rows().unwrap(),
            vec![vec![Value::Text(long.clone())], vec![Value::Text(long.clone())]]);
        assert_eq!(hash_join(&left, &right, "k", "k", JoinKind::Anti).unwrap().rows().unwrap(),
            vec![vec![Value::Null], vec![Value::Text("other".into())]]);
    }
}

#[cfg(test)]
mod borrowed_join_key_perf_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn repeated_long_text_build_keys_match_and_keep_outer_rows() {
        let schema = Schema::new(vec![Field::new("key", DataType::Text, true)]).unwrap();
        let key = Value::Text("large-shared-key".repeat(128));
        let left = Batch::from_rows(schema.clone(), vec![vec![key.clone()], vec![Value::Null]]).unwrap();
        let right = Batch::from_rows(schema, vec![vec![key.clone()], vec![key], vec![Value::Null]]).unwrap();
        let inner = hash_join(&left, &right, "key", "key", JoinKind::Inner).unwrap();
        assert_eq!(inner.num_rows(), 2);
        let outer = hash_join(&left, &right, "key", "key", JoinKind::Left).unwrap();
        assert_eq!(outer.num_rows(), 3);
        assert_eq!(outer.row(2).unwrap(), vec![Value::Null, Value::Null]);
    }
}

pub struct PreparedHashJoin<'a> {
    right: &'a Batch,
    left_schema: Schema,
    left_column: usize,
    kind: JoinKind,
    output_schema: Schema,
    right_hash: HashMap<&'a Value, Vec<usize>>,
}

impl<'a> PreparedHashJoin<'a> {
    pub fn new(
        left_schema: Schema, right: &'a Batch,
        left_key: &str, right_key: &str, kind: JoinKind,
    ) -> Result<Self> {
        if matches!(kind, JoinKind::Right | JoinKind::Full) {
            return Err(Error::Unsupported("prepared hash join needs a probe-side join kind".into()));
        }
        let left_column = left_schema.column(left_key)?;
        let right_column = right.schema().column(right_key)?;
        if left_schema.field(left_column)?.data_type != right.schema().field(right_column)?.data_type {
            return Err(Error::Type("join key types must match".into()));
        }
        let output_schema = hash_join(
            &Batch::empty(left_schema.clone()), &Batch::empty(right.schema().clone()),
            left_key, right_key, kind,
        )?.schema().clone();
        let mut right_hash: HashMap<&Value, Vec<usize>> = HashMap::new();
        for (row, key) in right.column(right_column)?.iter().enumerate() {
            if joinable(key) { right_hash.entry(key).or_default().push(row); }
        }
        Ok(Self { right, left_schema, left_column, kind, output_schema, right_hash })
    }

    pub fn probe(&self, left: &Batch) -> Result<Batch> {
        self.probe_with_matches(left, None)
    }

    /// Reuse build-side hash lookup results to mark matched build rows during
    /// RIGHT/FULL outer joins without retaining a second set of cloned keys.
    pub fn probe_with_matches(&self, left: &Batch, matched: Option<&mut [bool]>) -> Result<Batch> {
        self.probe_range_with_matches(left, 0, left.num_rows(), matched)
    }

    /// Reuse the original columnar input when processing a bounded probe range.
    pub fn probe_range_with_matches(
        &self, left: &Batch, start: usize, end: usize,
        mut matched: Option<&mut [bool]>,
    ) -> Result<Batch> {
        if start > end || end > left.num_rows() {
            return Err(Error::Execution("prepared join probe row range out of bounds".into()));
        }
        if let Some(flags) = matched.as_deref() {
            if flags.len() != self.right.num_rows() {
                return Err(Error::Execution("prepared join match bitmap size mismatch".into()));
            }
        }
        if left.schema() != &self.left_schema {
            return Err(Error::Schema("prepared join probe schema mismatch".into()));
        }
        if matches!(self.kind, JoinKind::Semi | JoinKind::Anti) {
            let mut selected = Vec::new();
            for row in start..end {
                let key = left.value(row, self.left_column)?;
                let matched = joinable(key) && self.right_hash.contains_key(key);
                if (self.kind == JoinKind::Semi && matched) ||
                    (self.kind == JoinKind::Anti && !matched) {
                    if selected.len() >= MAX_JOIN_ROWS {
                        return Err(Error::Execution("join output exceeds safety limit".into()));
                    }
                    selected.push(row);
                }
            }

            let fixed = join_fixed_row_bytes(left.schema())?;
            let text = join_text_columns(left)?;
            let mut bytes = 0usize;
            for &row in &selected {
                charge_join_row(&mut bytes, fixed, &text, Some(row), &[], None,
                    MAX_MATERIALIZED_JOIN_BYTES)?;
            }
            return left.take(&selected);
        }
        // On high-fanout joins, reject oversized output before cloning a
        // million wide rows or mutating the caller's match bitmap. Skip the
        // extra hash probes when a cheap cardinality bound proves safety.
        let probe_rows = end - start;
        let possible_rows = probe_rows.checked_mul(self.right.num_rows().max(1));
        if possible_rows.is_none_or(|limit| limit > MAX_JOIN_ROWS) {
            let mut planned_rows = 0usize;
            for row in start..end {
                let key = left.value(row, self.left_column)?;
                let matches = if joinable(key) {
                    self.right_hash.get(key).map_or(0, Vec::len)
                } else {
                    0
                };
                let output = if matches == 0 && self.kind == JoinKind::Left {
                    1
                } else {
                    matches
                };
                planned_rows = planned_rows.checked_add(output)
                    .filter(|&rows| rows <= MAX_JOIN_ROWS)
                    .ok_or_else(|| Error::Execution("join output exceeds safety limit".into()))?;
            }
        }
        let left_columns = (0..self.left_schema.len())
            .map(|col| left.column(col)).collect::<Result<Vec<_>>>()?;
        let right_columns = (0..self.right.schema().len())
            .map(|col| self.right.column(col)).collect::<Result<Vec<_>>>()?;
        let mut columns: Vec<Vec<Value>> =
            (0..self.output_schema.len()).map(|_| Vec::new()).collect();
        let fixed = join_fixed_row_bytes(&self.output_schema)?;
        let left_text = join_text_columns(left)?;
        let right_text = join_text_columns(self.right)?;
        let mut bytes = 0usize;
        let mut rows = 0usize;
        for left_row in start..end {
            let key = &left_columns[self.left_column][left_row];
            let matches = if joinable(key) { self.right_hash.get(key) } else { None };
            if let Some(right_rows) = matches {
                if let Some(flags) = matched.as_deref_mut() {
                    for &right_row in right_rows { flags[right_row] = true; }
                }
                for &right_row in right_rows {
                    if rows >= MAX_JOIN_ROWS {
                        return Err(Error::Execution("join output exceeds safety limit".into()));
                    }
                    charge_join_row(&mut bytes, fixed, &left_text, Some(left_row),
                        &right_text, Some(right_row), MAX_MATERIALIZED_JOIN_BYTES)?;
                    for (source, dest) in left_columns.iter().zip(&mut columns) {
                        dest.push(source[left_row].clone());
                    }
                    for (source, dest) in right_columns.iter()
                        .zip(columns[self.left_schema.len()..].iter_mut()) {
                        dest.push(source[right_row].clone());
                    }
                    rows += 1;
                }
            } else if self.kind == JoinKind::Left {
                if rows >= MAX_JOIN_ROWS {
                    return Err(Error::Execution("join output exceeds safety limit".into()));
                }
                charge_join_row(&mut bytes, fixed, &left_text, Some(left_row),
                    &right_text, None, MAX_MATERIALIZED_JOIN_BYTES)?;
                for (source, dest) in left_columns.iter().zip(&mut columns) {
                    dest.push(source[left_row].clone());
                }
                for dest in &mut columns[self.left_schema.len()..] {
                    dest.push(Value::Null);
                }
                rows += 1;
            }
        }
        Batch::from_validated_columns(self.output_schema.clone(), columns, rows)
    }
}

#[cfg(test)]
mod prepared_hash_join_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn prepared_batches_match_regular_join_for_all_probe_kinds() {
        let schema = Schema::new(vec![Field::new("key", DataType::Text, true)]).unwrap();
        let key = "repeated-long-key".repeat(30);
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text(key.clone())], vec![Value::Null],
            vec![Value::Text("absent".into())], vec![Value::Text(key.clone())],
            vec![Value::Text("other".into())],
        ]).unwrap();
        let right = Batch::from_rows(schema, vec![
            vec![Value::Text(key.clone())], vec![Value::Text(key.clone())],
            vec![Value::Null], vec![Value::Text("other".into())],
        ]).unwrap();
        for kind in [JoinKind::Inner, JoinKind::Left, JoinKind::Semi, JoinKind::Anti] {
            let prepared = PreparedHashJoin::new(left.schema().clone(), &right, "key", "key", kind).unwrap();
            let a = left.slice_rows(0, 2).unwrap();
            let b = left.slice_rows(2, 5).unwrap();
            let mut actual = prepared.probe(&a).unwrap().rows().unwrap();
            actual.extend(prepared.probe(&b).unwrap().rows().unwrap());
            let expected = hash_join(&left, &right, "key", "key", kind).unwrap();
            assert_eq!(actual, expected.rows().unwrap(), "kind: {kind:?}");
            assert!(prepared.probe(&Batch::empty(Schema::new(vec![]).unwrap())).is_err());
        }
    }
}

#[cfg(test)]
mod bitmap_outer_join_probe_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};
    #[test]
    fn tracks_duplicate_and_nan_build_matches_but_not_nulls() {
        let schema = Schema::new(vec![Field::new("n", DataType::Float, true)]).unwrap();
        let build = Batch::from_rows(schema.clone(), vec![vec![Value::Float(f64::NAN)], vec![Value::Null],
            vec![Value::Float(1.0)], vec![Value::Float(f64::NAN)]]).unwrap();
        let probe = Batch::from_rows(schema.clone(), vec![vec![Value::Float(f64::NAN)], vec![Value::Null]]).unwrap();
        let prepared = PreparedHashJoin::new(schema, &build, "n", "n", JoinKind::Inner).unwrap();
        let mut flags = vec![false; build.num_rows()];
        assert_eq!(prepared.probe_with_matches(&probe, Some(&mut flags)).unwrap().num_rows(), 2);
        assert_eq!(flags, vec![true, false, false, true]);
        assert!(prepared.probe_with_matches(&probe, Some(&mut [])).is_err());
    }
}

#[cfg(test)]
mod prepared_join_range_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};
    #[test]
    fn row_ranges_match_full_and_chunked_probe_results() {
        let schema = Schema::new(vec![Field::new("k", DataType::Integer, true)]).unwrap();
        let build = Batch::from_rows(schema.clone(), vec![vec![Value::Integer(1)], vec![Value::Integer(1)], vec![Value::Null]]).unwrap();
        let left = Batch::from_rows(schema, vec![vec![Value::Integer(1)], vec![Value::Null], vec![Value::Integer(2)], vec![Value::Integer(1)]]).unwrap();
        for kind in [JoinKind::Inner, JoinKind::Left, JoinKind::Semi, JoinKind::Anti] {
            let prepared = PreparedHashJoin::new(left.schema().clone(), &build, "k", "k", kind).unwrap();
            let mut rows = prepared.probe_range_with_matches(&left, 0, 2, None).unwrap().rows().unwrap();
            rows.extend(prepared.probe_range_with_matches(&left, 2, 4, None).unwrap().rows().unwrap());
            assert_eq!(rows, prepared.probe(&left).unwrap().rows().unwrap());
            assert!(prepared.probe_range_with_matches(&left, 2, 5, None).is_err());
        }
    }
}

#[cfg(test)]
mod prepared_columnar_results_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};
    #[test]
    fn output_columns_match_row_materialization_for_wide_duplicate_keys() {
        let schema = Schema::new(vec![Field::new("k", DataType::Integer, true),
            Field::new("payload", DataType::Text, false)]).unwrap();
        let wide = "value".repeat(200);
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(5), Value::Text(wide.clone())],
            vec![Value::Null, Value::Text("missing".into())],
            vec![Value::Integer(7), Value::Text(wide.clone())],
        ]).unwrap();
        let right = Batch::from_rows(schema, vec![
            vec![Value::Integer(5), Value::Text("right1".into())],
            vec![Value::Integer(5), Value::Text("right2".into())],
        ]).unwrap();
        for kind in [JoinKind::Inner, JoinKind::Left] {
            let prepared = PreparedHashJoin::new(left.schema().clone(), &right, "k", "k", kind).unwrap();
            let actual = prepared.probe(&left).unwrap();
            let baseline = hash_join(&left, &right, "k", "k", kind).unwrap();
            assert_eq!(actual, baseline);
        }
    }
}

#[cfg(test)]
mod cross_join_budget_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn enforces_query_budget_before_materializing_product() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let left = Batch::from_rows(schema.clone(),
            (0..100).map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let right = Batch::from_rows(schema,
            (0..100).map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        assert!(cross_join_with_max_rows(&left, &right, 50).is_err());
        let two = left.slice_rows(0, 2).unwrap();
        assert_eq!(cross_join_with_max_rows(&two, &two, 4).unwrap().num_rows(), 4);
        assert!(cross_join_with_max_rows(&two, &two, 3).is_err());
        let empty = left.slice_rows(0, 0).unwrap();
        assert_eq!(cross_join_with_max_rows(&empty, &right, 0).unwrap().num_rows(), 0);
    }
}

#[cfg(test)]
mod default_columnar_hash_join_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn nonempty_probe_uses_columnar_engine_for_all_supported_join_kinds() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Integer, true),
            Field::new("payload", DataType::Text, false),
        ]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(2), Value::Text("a".repeat(100))],
            vec![Value::Null, Value::Text("n".into())],
            vec![Value::Integer(9), Value::Text("missing".into())],
        ]).unwrap();
        let right = Batch::from_rows(schema, vec![
            vec![Value::Integer(2), Value::Text("x".into())],
            vec![Value::Integer(2), Value::Text("y".into())],
            vec![Value::Null, Value::Text("z".into())],
        ]).unwrap();
        assert_eq!(hash_join(&left, &right, "key", "key", JoinKind::Inner)
            .unwrap().num_rows(), 2);
        assert_eq!(hash_join(&left, &right, "key", "key", JoinKind::Left)
            .unwrap().num_rows(), 4);
        assert_eq!(hash_join(&left, &right, "key", "key", JoinKind::Semi)
            .unwrap().num_rows(), 1);
        assert_eq!(hash_join(&left, &right, "key", "key", JoinKind::Anti)
            .unwrap().num_rows(), 2);
    }
}

#[cfg(test)]
mod empty_cross_join_output_fast_path_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn empty_cartesian_side_preserves_joined_schema() {
        let empty = Batch::empty(Schema::new(vec![
            Field::new("id", DataType::Integer, false),
        ]).unwrap());
        let right = Batch::from_rows(Schema::new(vec![
            Field::new("id", DataType::Text, true),
        ]).unwrap(), vec![vec![Value::Text("a".into())]]).unwrap();
        for (a, b) in [(&empty, &right), (&right, &empty)] {
            let result = cross_join(a, b).unwrap();
            assert_eq!(result.num_rows(), 0);
            assert_eq!(result.schema().len(), 2);
            assert_eq!(result.schema().fields()[1].name, "right_id");
        }
    }
}

#[cfg(test)]
mod preflight_join_cardinality_regressions {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn oversized_fanout_fails_before_cloning_or_mutating_bitmap() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Integer, false),
            Field::new("payload", DataType::Text, false),
        ]).unwrap();
        let left = Batch::from_rows(schema.clone(), (0..2_000).map(|_| vec![
            Value::Integer(7), Value::Text("large".repeat(100)),
        ]).collect()).unwrap();
        let right = Batch::from_rows(schema, (0..600).map(|_| vec![
            Value::Integer(7), Value::Text("right".repeat(100)),
        ]).collect()).unwrap();
        let prepared = PreparedHashJoin::new(left.schema().clone(), &right,
            "key", "key", JoinKind::Inner).unwrap();
        let mut flags = vec![false; right.num_rows()];
        assert!(prepared.probe_with_matches(&left, Some(&mut flags)).unwrap_err()
            .to_string().contains("safety limit"));
        assert!(flags.iter().all(|&flag| !flag));
    }

    #[test]
    fn high_theoretical_fanout_keeps_sparse_matches_and_left_rows() {
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, true)]).unwrap();
        let left = Batch::from_rows(schema.clone(), (0..2_000).map(|index| vec![
            if index == 100 { Value::Integer(7) } else { Value::Integer(8) },
        ]).collect()).unwrap();
        let right = Batch::from_rows(schema, (0..600).map(|index| vec![
            Value::Integer(if index == 0 { 7 } else { 9 }),
        ]).collect()).unwrap();
        let prepared = PreparedHashJoin::new(left.schema().clone(), &right,
            "key", "key", JoinKind::Inner).unwrap();
        let mut flags = vec![false; right.num_rows()];
        let result = prepared.probe_with_matches(&left, Some(&mut flags)).unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.value(0, 0).unwrap(), &Value::Integer(7));
        assert!(flags[0]);
        assert!(flags[1..].iter().all(|&flag| !flag));

        let left_join = PreparedHashJoin::new(left.schema().clone(), &right,
            "key", "key", JoinKind::Left).unwrap();
        assert_eq!(left_join.probe(&left).unwrap().num_rows(), 2_000);
    }
}


#[cfg(test)]
mod columnar_outer_hash_join_regressions {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    fn build(rows: Vec<(Value, Value)>) -> Batch {
        Batch::from_rows(Schema::new(vec![
            Field::new("id", DataType::Integer, true),
            Field::new("payload", DataType::Text, true),
        ]).unwrap(), rows.into_iter().map(|(id, text)| vec![id, text]).collect()).unwrap()
    }

    #[test]
    fn right_and_full_matches_include_duplicate_null_and_unmatched_keys() {
        let long = "🙂".repeat(2048);
        let left = build(vec![
            (Value::Integer(2), Value::Text(long.clone())),
            (Value::Null, Value::Text("null-left".into())),
            (Value::Integer(2), Value::Text("left-duplicate".into())),
            (Value::Integer(4), Value::Null),
        ]);
        let right = build(vec![
            (Value::Integer(2), Value::Text("right-a".into())),
            (Value::Integer(3), Value::Text("right-b".into())),
            (Value::Integer(2), Value::Text("right-c".into())),
            (Value::Null, Value::Text("null-right".into())),
        ]);
        let right_join = hash_join(&left, &right, "id", "id", JoinKind::Right).unwrap();
        assert_eq!(right_join.num_rows(), 6);
        assert_eq!(right_join.rows().unwrap(), vec![
            vec![Value::Integer(2), Value::Text(long.clone()), Value::Integer(2), Value::Text("right-a".into())],
            vec![Value::Integer(2), Value::Text(long), Value::Integer(2), Value::Text("right-c".into())],
            vec![Value::Integer(2), Value::Text("left-duplicate".into()), Value::Integer(2), Value::Text("right-a".into())],
            vec![Value::Integer(2), Value::Text("left-duplicate".into()), Value::Integer(2), Value::Text("right-c".into())],
            vec![Value::Null, Value::Null, Value::Integer(3), Value::Text("right-b".into())],
            vec![Value::Null, Value::Null, Value::Null, Value::Text("null-right".into())],
        ]);
        let full = hash_join(&left, &right, "id", "id", JoinKind::Full).unwrap();
        assert_eq!(full.num_rows(), 8);
        assert_eq!(full.row(2).unwrap(), vec![
            Value::Null, Value::Text("null-left".into()), Value::Null, Value::Null,
        ]);
        assert_eq!(full.row(5).unwrap(), vec![
            Value::Integer(4), Value::Null, Value::Null, Value::Null,
        ]);
        assert_eq!(full.row(6).unwrap(), right_join.row(4).unwrap());
        assert_eq!(full.row(7).unwrap(), right_join.row(5).unwrap());
    }

    #[test]
    fn outer_join_widens_nullability_for_nonnullable_source_fields() {
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, false)]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![vec![Value::Integer(1)]]).unwrap();
        let right = Batch::from_rows(schema, vec![vec![Value::Integer(2)]]).unwrap();
        for kind in [JoinKind::Right, JoinKind::Full] {
            let result = hash_join(&left, &right, "key", "key", kind).unwrap();
            assert!(result.schema().field(0).unwrap().nullable);
            assert_eq!(result.schema().field(1).unwrap().name, "right_key");
            assert_eq!(result.row(result.num_rows() - 1).unwrap(),
                vec![Value::Null, Value::Integer(2)]);
        }
    }
}

#[cfg(test)]
mod validated_join_output_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn typed_cartesian_and_prepared_join_keep_zero_width_and_nullable_rows() {
        let empty_schema = Schema::new(vec![]).unwrap();
        let a = Batch::new_with_row_count(empty_schema.clone(), vec![], 2).unwrap();
        let b = Batch::new_with_row_count(empty_schema, vec![], 3).unwrap();
        assert_eq!(cross_join(&a, &b).unwrap().num_rows(), 6);

        let schema = Schema::new(vec![
            Field::new("k", DataType::Integer, true),
            Field::new("payload", DataType::Text, true),
        ]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1), Value::Text("界".repeat(512))],
            vec![Value::Null, Value::Null],
        ]).unwrap();
        let right = Batch::from_rows(schema, vec![
            vec![Value::Integer(1), Value::Text("right".into())],
        ]).unwrap();
        let prepared = PreparedHashJoin::new(
            left.schema().clone(), &right, "k", "k", JoinKind::Left
        ).unwrap();
        let result = prepared.probe(&left).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.row(1).unwrap(),
            vec![Value::Null, Value::Null, Value::Null, Value::Null]);
        assert_eq!(result, hash_join(&left, &right, "k", "k", JoinKind::Left).unwrap());
    }
}


#[cfg(test)]
mod materialized_cross_join_byte_budget_regressions {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn refuses_wide_text_cartesian_product_before_allocating_output() {
        let left = Batch::from_rows(Schema::new(vec![
            Field::new("payload", DataType::Text, false),
        ]).unwrap(), vec![vec![Value::Text("x".repeat(1024 * 1024))]]).unwrap();
        let right = Batch::from_rows(Schema::new(vec![
            Field::new("id", DataType::Integer, false),
        ]).unwrap(), (0..400).map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let error = cross_join(&left, &right).unwrap_err();
        assert!(error.to_string().contains("byte limit"));
    }

    #[test]
    fn modest_wide_text_and_nulls_still_keep_exact_cartesian_rows() {
        let left = Batch::from_rows(Schema::new(vec![
            Field::new("payload", DataType::Text, true),
        ]).unwrap(), vec![
            vec![Value::Text("🙂".repeat(256))], vec![Value::Null],
        ]).unwrap();
        let right = Batch::from_rows(Schema::new(vec![
            Field::new("id", DataType::Integer, false),
        ]).unwrap(), vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]).unwrap();
        let output = cross_join(&left, &right).unwrap();
        assert_eq!(output.num_rows(), 4);
        assert_eq!(output.value(0, 0).unwrap(), left.value(0, 0).unwrap());
        assert_eq!(output.value(3, 0).unwrap(), &Value::Null);
        assert_eq!(output.value(3, 1).unwrap(), &Value::Integer(2));
    }
}

#[cfg(test)]
mod bounded_hash_join_output_bytes_tests {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn charges_exact_text_lengths_and_null_extended_cells() {
        let left = [Value::Text("long left".into()), Value::Null];
        let right = [Value::Text("right".into()), Value::Text("R".into())];
        let left_text: &[&[Value]] = &[&left];
        let right_text: &[&[Value]] = &[&right];
        let fixed = 2 * std::mem::size_of::<Value>();
        let cost = fixed + "long left".len() + "right".len();
        let mut bytes = 0usize;
        charge_join_row(&mut bytes, fixed, left_text, Some(0),
            right_text, Some(0), cost).unwrap();
        assert_eq!(bytes, cost);
        assert!(charge_join_row(&mut bytes, fixed, left_text, Some(0),
            right_text, Some(1), cost).is_err());
        assert_eq!(bytes, cost, "rejected output must not consume budget");
        let mut tail = 0usize;
        charge_join_row(&mut tail, fixed, &[], None, right_text, Some(0),
            fixed + "right".len()).unwrap();
        assert_eq!(tail, fixed + "right".len());
        let mut all_null = 0usize;
        charge_join_row(&mut all_null, fixed, left_text, Some(1), &[], None,
            fixed).unwrap();
        assert_eq!(all_null, fixed);
    }

    #[test]
    fn prepared_and_outer_joins_preserve_results_with_nullable_text() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Integer, true),
            Field::new("text", DataType::Text, true),
        ]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1), Value::Text("l".into())],
            vec![Value::Null, Value::Text("n".into())],
            vec![Value::Integer(2), Value::Null],
        ]).unwrap();
        let right = Batch::from_rows(schema, vec![
            vec![Value::Integer(1), Value::Text("r".into())],
            vec![Value::Integer(3), Value::Text("extra".into())],
        ]).unwrap();
        for (kind, expected_rows) in [
            (JoinKind::Inner, 1), (JoinKind::Left, 3),
            (JoinKind::Right, 2), (JoinKind::Full, 4),
            (JoinKind::Semi, 1), (JoinKind::Anti, 2),
        ] {
            let joined = hash_join(&left, &right, "key", "key", kind).unwrap();
            assert_eq!(joined.num_rows(), expected_rows, "kind={kind:?}");
            assert!(materialized_join_bytes(&joined).unwrap() <= MAX_MATERIALIZED_JOIN_BYTES);
        }
    }

    #[test]
    fn bounded_helper_rejects_fixed_width_overflows_before_cloning() {
        let mut used = usize::MAX - 5;
        let rejected = charge_join_row(&mut used, 10, &[], None, &[], None, usize::MAX);
        assert!(rejected.is_err());
        assert_eq!(used, usize::MAX - 5);
    }
}
