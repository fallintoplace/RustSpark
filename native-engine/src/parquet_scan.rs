//! Bounded Parquet scans using Apache Parquet and Arrow, never DataFusion.
use std::{fs::File, path::Path};

use parquet::arrow::{ProjectionMask, arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder}};

use crate::{arrow_bridge, batch::Batch, Error, Result};

// Bound native Value slots per decoded batch, even for a very wide Parquet
// schema. The caller's batch_size remains an upper limit, not a target.
const MAX_NATIVE_VALUE_SLOT_BYTES: usize = 64 * 1024 * 1024;

fn bounded_native_batch_rows(requested: usize, columns: usize) -> usize {
    let per_row = columns.max(1).saturating_mul(std::mem::size_of::<crate::scalar::Value>());
    requested.min((MAX_NATIVE_VALUE_SLOT_BYTES / per_row.max(1)).max(1))
}

pub struct ParquetScan {
    inner: ParquetRecordBatchReader,
    finished: bool,
}

/// File metadata for planning scans without decoding any row batches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParquetSummary {
    pub rows: u64,
    pub row_groups: usize,
    pub columns: usize,
}

/// Read the native schema without decoding any Parquet data pages.
pub fn file_schema(path: impl AsRef<Path>) -> Result<crate::schema::Schema> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| Error::Execution(format!("Parquet metadata: {e}")))?;
    arrow_bridge::schema_from_arrow(builder.schema().as_ref())
}

pub fn file_summary(path: impl AsRef<Path>) -> Result<ParquetSummary> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| Error::Execution(format!("Parquet metadata: {e}")))?;
    let rows = u64::try_from(builder.metadata().file_metadata().num_rows())
        .map_err(|_| Error::Execution("negative Parquet row count".into()))?;
    Ok(ParquetSummary {
        rows,
        row_groups: builder.metadata().num_row_groups(),
        columns: builder.schema().fields().len(),
    })
}

pub fn scan(path: impl AsRef<Path>, batch_size: usize) -> Result<ParquetScan> {
    scan_with_expected_schema(path, batch_size, None)
}

/// Validate a registered table's schema using the same file handle used for
/// scanning. Replacing a Parquet file must not silently change result columns.
pub fn scan_checked(
    path: impl AsRef<Path>, batch_size: usize, expected: &crate::schema::Schema,
) -> Result<ParquetScan> {
    scan_with_expected_schema(path, batch_size, Some(expected))
}

fn scan_with_expected_schema(
    path: impl AsRef<Path>, batch_size: usize,
    expected: Option<&crate::schema::Schema>,
) -> Result<ParquetScan> {
    if batch_size == 0 || batch_size > 1_000_000 {
        return Err(Error::Execution("Parquet batch size must be in 1..=1000000".into()));
    }
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| Error::Execution(format!("Parquet metadata: {e}")))?;
    if let Some(expected) = expected {
        let actual = arrow_bridge::schema_from_arrow(builder.schema().as_ref())?;
        if &actual != expected {
            return Err(Error::Schema(
                "registered Parquet table schema changed since registration".into()
            ));
        }
    }
    let rows = bounded_native_batch_rows(batch_size, builder.schema().fields().len());
    let reader = builder.with_batch_size(rows)
        .build()
        .map_err(|e| Error::Execution(format!("Parquet reader: {e}")))?;
    Ok(ParquetScan { inner: reader, finished: false })
}

/// Compose Parquet decoder pushdowns without decoding discarded columns,
/// row groups, or row windows in separate intermediate stages.
#[derive(Clone, Copy, Debug, Default)]
pub struct ParquetScanOptions<'a> {
    /// None selects every file column. Some retains the requested columns in
    /// file order and rejects empty or duplicate column lists.
    pub columns: Option<&'a [&'a str]>,
    /// None reads all row groups. Some selects precisely the supplied groups.
    pub row_groups: Option<&'a [usize]>,
    /// Number of rows to skip after applying the row-group selection.
    pub offset: usize,
    /// Optional number of rows to decode after the offset.
    pub limit: Option<usize>,
}

/// Resolve requested Parquet root fields once for wide projections. Keep
/// linear lookups for narrow schemas to avoid unnecessary hash allocations.
fn projection_indices(
    fields: &[arrow_schema::FieldRef], columns: &[&str],
) -> Result<Vec<usize>> {
    if columns.is_empty() {
        return Err(Error::Schema("Parquet projection must have at least one column".into()));
    }
    // More requests than available physical fields must contain an unknown
    // or duplicate name. Reject before allocating selection hash tables.
    if columns.len() > fields.len() {
        return Err(Error::Schema("Parquet projection exceeds available columns".into()));
    }
    let indexed = if columns.len().saturating_mul(fields.len()) >= 2048 {
        let mut indices = std::collections::HashMap::with_capacity(fields.len());
        for (position, field) in fields.iter().enumerate() {
            // A case-insensitive collision must not silently read a different
            // physical column. None marks an ambiguous root field name.
            match indices.entry(field.name().to_ascii_lowercase()) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(Some(position));
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    *entry.get_mut() = None;
                }
            }
        }
        Some(indices)
    } else {
        None
    };
    let mut selected = Vec::with_capacity(columns.len());
    let mut seen = std::collections::HashSet::with_capacity(columns.len());
    for &name in columns {
        let position = if let Some(indices) = &indexed {
            match indices.get(&name.to_ascii_lowercase()) {
                Some(Some(index)) => Some(*index),
                Some(None) => return Err(Error::Schema(format!(
                    "ambiguous Parquet column: {name}"
                ))),
                None => None,
            }
        } else {
            let mut matches = fields.iter().enumerate()
                .filter(|(_, field)| field.name().eq_ignore_ascii_case(name));
            let first = matches.next().map(|(index, _)| index);
            if matches.next().is_some() {
                return Err(Error::Schema(format!("ambiguous Parquet column: {name}")));
            }
            first
        }.ok_or_else(|| Error::Schema(format!("unknown Parquet column: {name}")))?;
        if !seen.insert(position) {
            return Err(Error::Schema(format!("duplicate Parquet projection column: {name}")));
        }
        selected.push(position);
    }
    Ok(selected)
}

fn validate_row_groups(indices: &[usize], available: usize) -> Result<()> {
    // Unique selected groups cannot outnumber file groups. This preflight
    // also prevents a huge untrusted list from preallocating a huge HashSet.
    if indices.len() > available {
        return Err(Error::Execution("invalid or repeated Parquet row group".into()));
    }
    // Most pushed-down scans select only one or a few row groups. Avoid
    // initializing a hash table for a tiny selection. At 16 entries the
    // bounded linear check is cheaper than hash allocation and probing.
    if indices.len() <= 16 {
        for (position, &index) in indices.iter().enumerate() {
            if index >= available || indices[..position].contains(&index) {
                return Err(Error::Execution("invalid or repeated Parquet row group".into()));
            }
        }
        return Ok(());
    }
    let mut seen = std::collections::HashSet::with_capacity(indices.len());
    for &index in indices {
        if index >= available || !seen.insert(index) {
            return Err(Error::Execution("invalid or repeated Parquet row group".into()));
        }
    }
    Ok(())
}

pub fn scan_with_options(
    path: impl AsRef<Path>, batch_size: usize, options: ParquetScanOptions<'_>,
) -> Result<ParquetScan> {
    if batch_size == 0 || batch_size > 1_000_000 {
        return Err(Error::Execution("Parquet batch size must be in 1..=1000000".into()));
    }
    if let Some(limit) = options.limit {
        options.offset.checked_add(limit)
            .ok_or_else(|| Error::Execution("Parquet OFFSET + LIMIT overflow".into()))?;
    }
    let file = File::open(path)?;
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| Error::Execution(format!("Parquet metadata: {e}")))?;
    if let Some(columns) = options.columns {
        let selected = projection_indices(builder.schema().fields(), columns)?;
        let mask = ProjectionMask::roots(builder.parquet_schema(), selected);
        builder = builder.with_projection(mask);
    }
    if let Some(row_groups) = options.row_groups {
        validate_row_groups(row_groups, builder.metadata().num_row_groups())?;
        builder = builder.with_row_groups(row_groups.to_vec());
    }
    builder = builder.with_offset(options.offset);
    if let Some(limit) = options.limit {
        builder = builder.with_limit(limit);
    }
    let projected_width = options.columns.map_or(builder.schema().fields().len(), |columns| columns.len());
    let rows = bounded_native_batch_rows(batch_size, projected_width);
    let inner = builder.with_batch_size(rows)
        .build().map_err(|e| Error::Execution(format!("Parquet reader: {e}")))?;
    Ok(ParquetScan { inner, finished: false })
}

/// Decode only named Parquet columns. Output follows the file's column order.
pub fn scan_columns(
    path: impl AsRef<Path>, batch_size: usize, columns: &[&str],
) -> Result<ParquetScan> {
    scan_with_options(path, batch_size, ParquetScanOptions {
        columns: Some(columns), ..ParquetScanOptions::default()
    })
}

/// Restrict decoding to validated row-group indices.
pub fn scan_row_groups(
    path: impl AsRef<Path>, batch_size: usize, row_groups: &[usize],
) -> Result<ParquetScan> {
    scan_with_options(path, batch_size, ParquetScanOptions {
        row_groups: Some(row_groups), ..ParquetScanOptions::default()
    })
}

/// Skip and limit decoded rows in the Parquet reader instead of materializing
/// and slicing unwanted native batches afterward.
pub fn scan_window(
    path: impl AsRef<Path>, batch_size: usize, offset: usize, limit: usize,
) -> Result<ParquetScan> {
    scan_with_options(path, batch_size, ParquetScanOptions {
        offset, limit: Some(limit), ..ParquetScanOptions::default()
    })
}

impl Iterator for ParquetScan {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished { return None; }
        let result = self.inner.next().map(|result| {
            let record = result
                .map_err(|e| Error::Execution(format!("Parquet decode: {e}")))?;
            arrow_bridge::from_arrow(&record)
        });
        if result.as_ref().is_none_or(Result::is_err) {
            self.finished = true;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};
    use parquet::arrow::ArrowWriter;

    #[test]
    fn scan_parquet_in_bounded_native_batches() {
        let path = std::env::temp_dir().join(format!(
            "rustspark-native-{}-{}.parquet",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let native = Batch::from_rows(
            Schema::new(vec![
                Field::new("id", DataType::Integer, false),
                Field::new("city", DataType::Text, true),
            ]).unwrap(),
            vec![
                vec![Value::Integer(1), Value::Text("Berlin".into())],
                vec![Value::Integer(2), Value::Null],
                vec![Value::Integer(3), Value::Text("Paris".into())],
            ],
        ).unwrap();
        let arrow = arrow_bridge::to_arrow(&native).unwrap();
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), arrow.schema(), None).unwrap();
        writer.write(&arrow).unwrap();
        writer.close().unwrap();

        let parts = scan(&path, 2).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(parts.iter().map(Batch::num_rows).collect::<Vec<_>>(), vec![2, 1]);
        let rows: Vec<_> = parts.into_iter().flat_map(|batch| batch.rows().unwrap()).collect();
        assert_eq!(rows, native.rows().unwrap());
        assert!(scan(&path, 0).is_err());
        std::fs::remove_file(path).unwrap();
    }
}

 
/// A Parquet scan cursor that stops after a global row limit.
pub struct LimitedParquetScan {
    inner: ParquetScan,
    remaining: usize,
}

pub fn scan_limited(path: impl AsRef<Path>, batch_size: usize, max_rows: usize) -> Result<LimitedParquetScan> {
    let read_rows = batch_size.min(max_rows.max(1));
    // Push the global row limit into the Parquet decoder. The wrapper still
    // enforces its contract, but the reader need not decode the trailing pages.
    Ok(LimitedParquetScan {
        inner: scan_window(path, read_rows, 0, max_rows)?,
        remaining: max_rows,
    })
}

impl Iterator for LimitedParquetScan {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 { return None; }
        let item = self.inner.next()?;
        match item {
            Ok(batch) => {
                let keep = self.remaining.min(batch.num_rows());
                self.remaining -= keep;
                if keep == batch.num_rows() {
                    Some(Ok(batch))
                } else {
                    Some(batch.slice_rows(0, keep))
                }
            }
            Err(error) => {
                self.remaining = 0;
                Some(Err(error))
            }
        }
    }
}

#[cfg(test)]
mod limited_parquet_feature_tests {
    use super::*;
    use crate::{batch::Batch, scalar::Value, schema::{DataType, Field, Schema}};
    use parquet::arrow::ArrowWriter;

    #[test]
    fn limits_rows_even_when_arrow_batches_are_larger() {
        let path = std::env::temp_dir().join(format!("native-parquet-limited-{}-{}.parquet",
            std::process::id(), std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let input = Batch::from_rows(schema, (0..10).map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let arrow = arrow_bridge::to_arrow(&input).unwrap();
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), arrow.schema(), None).unwrap();
        writer.write(&arrow).unwrap();
        writer.close().unwrap();
        let parts = scan_limited(&path, 4, 6).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(parts.iter().map(Batch::num_rows).sum::<usize>(), 6);
        assert_eq!(scan_limited(&path, 4, 0).unwrap().count(), 0);
        std::fs::remove_file(path).unwrap();
    }
}


#[cfg(test)]
mod parquet_limited_batch_reuse_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};
    use parquet::arrow::ArrowWriter;

    #[test]
    fn full_batches_and_partial_tail_keep_data_and_cardinality() {
        let path = std::env::temp_dir().join(format!("rustspark-limited-perf-{}-{}.parquet",
            std::process::id(), std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        let batch = Batch::from_rows(Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap(),
            (0..7).map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let arrow = arrow_bridge::to_arrow(&batch).unwrap();
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), arrow.schema(), None).unwrap();
        writer.write(&arrow).unwrap(); writer.close().unwrap();
        let collected = scan_limited(&path, 3, 5).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(collected.iter().map(Batch::num_rows).collect::<Vec<_>>(), [3, 2]);
        assert_eq!(collected.iter().flat_map(|b| b.rows().unwrap()).collect::<Vec<_>>(),
            batch.slice_rows(0, 5).unwrap().rows().unwrap());
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(test)]
mod parquet_file_summary_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

    #[test]
    fn reports_metadata_without_reading_rows() {
        let path = std::env::temp_dir().join(format!("rustspark-summary-{}-{}.parquet",
            std::process::id(), std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        let input = Batch::from_rows(Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("label", DataType::Text, true),
        ]).unwrap(), (0..5).map(|n| vec![Value::Integer(n),
            if n == 0 { Value::Null } else { Value::Text(format!("v{n}")) },
        ]).collect()).unwrap();
        let arrow = arrow_bridge::to_arrow(&input).unwrap();
        let props = WriterProperties::builder().set_max_row_group_row_count(Some(2)).build();
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(),
            arrow.schema(), Some(props)).unwrap();
        writer.write(&arrow).unwrap();
        writer.close().unwrap();
        assert_eq!(file_summary(&path).unwrap(), ParquetSummary {
            rows: 5, row_groups: 3, columns: 2,
        });
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(test)]
mod parquet_projection_pushdown_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};
    use parquet::arrow::ArrowWriter;

    #[test]
    fn reads_only_requested_columns_and_preserves_nulls() {
        let path = std::env::temp_dir().join(format!("rustspark-projection-{}-{}.parquet",
            std::process::id(), std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        let input = Batch::from_rows(Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("label", DataType::Text, true),
            Field::new("flag", DataType::Boolean, true),
        ]).unwrap(), vec![
            vec![Value::Integer(1), Value::Text("é🙂".into()), Value::Boolean(true)],
            vec![Value::Integer(2), Value::Null, Value::Null],
        ]).unwrap();
        let arrow = arrow_bridge::to_arrow(&input).unwrap();
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(),
            arrow.schema(), None).unwrap();
        writer.write(&arrow).unwrap();
        writer.close().unwrap();
        let output = scan_columns(&path, 1, &["LABEL"]).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(output[0].schema().fields()[0].name, "label");
        assert_eq!(output[0].row(0).unwrap(), vec![Value::Text("é🙂".into())]);
        assert_eq!(output[1].row(0).unwrap(), vec![Value::Null]);
        assert!(scan_columns(&path, 1, &[]).is_err());
        assert!(scan_columns(&path, 1, &["label", "LABEL"]).is_err());
        assert!(scan_columns(&path, 1, &["missing"]).is_err());
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(test)]
mod parquet_row_group_selection_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

    #[test]
    fn skips_unselected_row_groups_before_reading() {
        let path = std::env::temp_dir().join(format!("rustspark-groups-{}-{}.parquet",
            std::process::id(), std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        let batch = Batch::from_rows(Schema::new(vec![
            Field::new("id", DataType::Integer, false),
        ]).unwrap(), (0..6).map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let arrow = arrow_bridge::to_arrow(&batch).unwrap();
        let props = WriterProperties::builder().set_max_row_group_row_count(Some(2)).build();
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(),
            arrow.schema(), Some(props)).unwrap();
        writer.write(&arrow).unwrap();
        writer.close().unwrap();
        let output = scan_row_groups(&path, 2, &[0, 2]).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        let rows = output.iter().flat_map(|batch| batch.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(rows, vec![
            vec![Value::Integer(0)], vec![Value::Integer(1)],
            vec![Value::Integer(4)], vec![Value::Integer(5)],
        ]);
        assert!(scan_row_groups(&path, 2, &[3]).is_err());
        assert!(scan_row_groups(&path, 2, &[1, 1]).is_err());
        assert!(scan_row_groups(&path, 0, &[0]).is_err());
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(test)]
mod parquet_native_offset_limit_pushdown_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};
    use parquet::arrow::ArrowWriter;

    #[test]
    fn reads_only_requested_window_across_batch_boundaries() {
        let path = std::env::temp_dir().join(format!("rustspark-window-{}-{}.parquet",
            std::process::id(), std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        let batch = Batch::from_rows(Schema::new(vec![
            Field::new("id", DataType::Integer, false),
        ]).unwrap(), (0..12).map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let arrow = arrow_bridge::to_arrow(&batch).unwrap();
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(),
            arrow.schema(), None).unwrap();
        writer.write(&arrow).unwrap();
        writer.close().unwrap();
        let output = scan_window(&path, 2, 3, 5).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(output.iter().flat_map(|batch| batch.rows().unwrap())
            .collect::<Vec<_>>(), (3..8).map(|n| vec![Value::Integer(n)]).collect::<Vec<_>>());
        assert_eq!(scan_window(&path, 3, 0, 0).unwrap().count(), 0);
        assert_eq!(scan_window(&path, 3, 100, 5).unwrap().count(), 0);
        assert!(scan_window(&path, 3, usize::MAX, 1).is_err());
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(test)]
mod streaming_parquet_schema_tests {
    use super::*;
    use crate::{schema::{DataType, Field, Schema}, scalar::Value, expr::{Expr, BinaryOp},
        pipeline::Pipeline};
    use parquet::arrow::ArrowWriter;

    #[test]
    fn metadata_schema_drives_bounded_native_parquet_pipeline() {
        let path = std::env::temp_dir().join(format!(
            "native-parquet-stream-cli-{}-{}.parquet", std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let schema = Schema::new(vec![
            Field::new("n", DataType::Integer, false),
            Field::new("label", DataType::Text, true),
        ]).unwrap();
        let data = Batch::from_rows(schema.clone(),
            (0..10).map(|n| vec![Value::Integer(n), Value::Text("x".into())]).collect()
        ).unwrap();
        let arrow = arrow_bridge::to_arrow(&data).unwrap();
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(),
            arrow.schema(), None).unwrap();
        writer.write(&arrow).unwrap();
        writer.close().unwrap();
        assert_eq!(file_schema(&path).unwrap(), schema);
        let rows = Pipeline::new(scan(&path, 3).unwrap())
            .with_filter(Expr::binary(Expr::Column("n".into()), BinaryOp::GreaterEqual,
                Expr::Literal(Value::Integer(5))))
            .with_limit(1, Some(2))
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(rows.into_iter().flat_map(|batch| batch.rows().unwrap())
            .map(|row| row[0].clone()).collect::<Vec<_>>(),
            vec![Value::Integer(6), Value::Integer(7)]);
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(test)]
mod parquet_scan_terminal_decode_errors_tests {
    use super::*;
    use arrow_array::{ArrayRef, Date32Array, RecordBatch};
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    #[test]
    fn unsupported_arrow_columns_fail_once_and_stop_the_scan() {
        let path = std::env::temp_dir().join(format!(
            "native-parquet-fuse-{}-{}.parquet",
            std::process::id(), std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
        ));
        let record = RecordBatch::try_from_iter(vec![
            ("date", Arc::new(Date32Array::from(vec![1, 2])) as ArrayRef),
        ]).unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create(&path).unwrap(), record.schema(), None,
        ).unwrap();
        writer.write(&record).unwrap();
        writer.close().unwrap();

        let mut source = scan(&path, 1).unwrap();
        assert!(source.next().unwrap().is_err());
        assert!(source.next().is_none());
        assert!(source.next().is_none());
        drop(source);
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(test)]
mod parquet_limited_row_pushdown_regressions {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};
    use parquet::arrow::ArrowWriter;

    #[test]
    fn pushed_limit_matches_prefix_of_full_scan_for_all_batch_sizes() {
        let path = std::env::temp_dir().join(format!(
            "rustspark-parquet-limit-pushdown-{}-{}.parquet",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let schema = Schema::new(vec![
            Field::new("n", DataType::Integer, false),
            Field::new("label", DataType::Text, true),
        ]).unwrap();
        let source = Batch::from_rows(schema, (0..73).map(|i| vec![
            Value::Integer(i),
            if i % 7 == 0 { Value::Null } else { Value::Text(format!("row-{i}")) },
        ]).collect()).unwrap();
        let arrow = arrow_bridge::to_arrow(&source).unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create(&path).unwrap(), arrow.schema(), None
        ).unwrap();
        writer.write(&arrow).unwrap();
        writer.close().unwrap();

        let full = scan(&path, 11).unwrap().collect::<Result<Vec<_>>>().unwrap()
            .into_iter().flat_map(|batch| batch.rows().unwrap()).collect::<Vec<_>>();
        for batch_size in [1, 7, 17, 100] {
            for limit in [0, 1, 6, 17, 32, 73, 120] {
                let result = scan_limited(&path, batch_size, limit).unwrap()
                    .collect::<Result<Vec<_>>>().unwrap()
                    .into_iter().flat_map(|batch| batch.rows().unwrap()).collect::<Vec<_>>();
                assert_eq!(result, full[..limit.min(full.len())]);
            }
        }
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(test)]
mod combined_parquet_scan_pushdown_regressions {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

    #[test]
    fn combines_column_row_group_and_window_pushdown_without_changing_values() {
        let path = std::env::temp_dir().join(format!(
            "rustspark-combined-scan-{}-{}.parquet",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
        ));
        let input = Batch::from_rows(Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("payload", DataType::Text, true),
            Field::new("flag", DataType::Boolean, true),
        ]).unwrap(), (0..12).map(|n| vec![
            Value::Integer(n),
            if n % 4 == 0 { Value::Null }
            else { Value::Text(format!("row-{n}-{}", "é🙂".repeat(30))) },
            Value::Boolean(n % 2 == 0),
        ]).collect()).unwrap();
        let record = crate::arrow_bridge::to_arrow(&input).unwrap();
        let props = WriterProperties::builder().set_max_row_group_row_count(Some(2)).build();
        let mut writer = ArrowWriter::try_new(
            File::create(&path).unwrap(), record.schema(), Some(props)).unwrap();
        writer.write(&record).unwrap();
        writer.close().unwrap();

        let output = scan_with_options(&path, 2, ParquetScanOptions {
            columns: Some(&["payload", "ID"]),
            row_groups: Some(&[0, 2, 4]),
            offset: 1, limit: Some(4),
        }).unwrap().collect::<Result<Vec<_>>>().unwrap();
        let expected = vec![1, 4, 5, 8];
        let rows = output.iter().flat_map(|batch| batch.rows().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), expected.len());
        // Parquet projection preserves file column order, not selector order.
        for (row, n) in rows.iter().zip(expected) {
            assert_eq!(row[0], Value::Integer(n));
            assert_eq!(row[1], if n % 4 == 0 { Value::Null }
                else { Value::Text(format!("row-{n}-{}", "é🙂".repeat(30))) });
        }
        assert_eq!(output[0].schema().fields()[0].name, "id");
        assert!(output.iter().all(|batch| batch.num_rows() <= 2));
        let all = scan_with_options(&path, 5, ParquetScanOptions::default())
            .unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(all.iter().flat_map(|batch| batch.rows().unwrap())
            .collect::<Vec<_>>(), input.rows().unwrap());
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn combined_options_check_duplicate_fields_groups_and_offset_overflow() {
        let path = std::env::temp_dir().join(format!(
            "rustspark-options-errors-{}-{}.parquet",
            std::process::id(), std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
        ));
        let batch = Batch::from_rows(Schema::new(vec![
            Field::new("id", DataType::Integer, false),
        ]).unwrap(), vec![vec![Value::Integer(1)]]).unwrap();
        let record = crate::arrow_bridge::to_arrow(&batch).unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create(&path).unwrap(), record.schema(), None).unwrap();
        writer.write(&record).unwrap(); writer.close().unwrap();
        assert!(scan_with_options(&path, 4, ParquetScanOptions {
            columns: Some(&["id", "ID"]), ..ParquetScanOptions::default()
        }).is_err());
        assert!(scan_with_options(&path, 4, ParquetScanOptions {
            row_groups: Some(&[0, 0]), ..ParquetScanOptions::default()
        }).is_err());
        assert!(scan_with_options(&path, 4, ParquetScanOptions {
            offset: usize::MAX, limit: Some(1), ..ParquetScanOptions::default()
        }).is_err());
        assert!(scan_with_options(&path, 0, ParquetScanOptions::default()).is_err());
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(test)]
mod wide_parquet_projection_lookup_regressions {
    use super::*;
    use crate::{schema::{DataType, Field, Schema}, scalar::Value};
    use parquet::arrow::ArrowWriter;

    #[test]
    fn resolves_many_out_of_order_columns_without_changing_file_order() {
        let path = std::env::temp_dir().join(format!(
            "rustspark-wide-projection-{}-{}.parquet",
            std::process::id(), std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
        ));
        let mut fields = (0..128).map(|index|
            Field::new(format!("col_{index:03}"), DataType::Integer, false)
        ).collect::<Vec<_>>();
        fields.push(Field::new("東京A", DataType::Integer, false));
        let native = Batch::from_rows(Schema::new(fields).unwrap(),
            vec![(0..129).map(Value::Integer).collect()]).unwrap();
        let record = arrow_bridge::to_arrow(&native).unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create(&path).unwrap(), record.schema(), None,
        ).unwrap();
        writer.write(&record).unwrap();
        writer.close().unwrap();
        let mut reader = scan_columns(&path, 4, &["東京a", "COL_127", "col_000"]).unwrap();
        let output = reader.next().unwrap().unwrap();
        assert_eq!(output.schema().fields().iter().map(|field| field.name.as_str())
            .collect::<Vec<_>>(), ["col_000", "col_127", "東京A"]);
        assert_eq!(output.row(0).unwrap(), vec![
            Value::Integer(0), Value::Integer(127), Value::Integer(128),
        ]);
        assert!(reader.next().is_none());
        assert!(scan_columns(&path, 4, &["col_001", "COL_001"]).is_err());
        assert!(scan_columns(&path, 4, &["unknown"]).is_err());
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(test)]
mod parquet_projection_index_regressions {
    use super::*;
    use std::sync::Arc;

    fn fields(count: usize) -> Vec<arrow_schema::FieldRef> {
        (0..count).map(|n| Arc::new(arrow_schema::Field::new(
            format!("Column_{n}"), arrow_schema::DataType::Int64, true,
        ))).collect()
    }

    #[test]
    fn wide_projection_matches_linear_lookup_with_reverse_and_case_insensitive_names() {
        let fields = fields(256);
        let names = (0..256).rev().map(|n| format!("cOlUmN_{n}")).collect::<Vec<_>>();
        let wanted = names.iter().map(String::as_str).collect::<Vec<_>>();
        assert_eq!(projection_indices(&fields, &wanted).unwrap(),
            (0..256).rev().collect::<Vec<_>>());
        assert!(projection_indices(&fields, &["Column_17", "column_17"]).is_err());
        assert!(projection_indices(&fields, &["missing"]).is_err());
        assert!(projection_indices(&fields, &[]).is_err());
    }

    #[test]
    fn sparse_and_case_colliding_schemas_preserve_first_match_rule() {
        let mut fields = fields(128);
        fields.insert(3, Arc::new(arrow_schema::Field::new(
            "COLUMN_0", arrow_schema::DataType::Int64, true,
        )));
        let names = (0..128).map(|n| format!("column_{n}")).collect::<Vec<_>>();
        let wanted = names.iter().map(String::as_str).collect::<Vec<_>>();
        let expected = wanted.iter().map(|name| fields.iter()
            .position(|field| field.name().eq_ignore_ascii_case(name)).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(projection_indices(&fields, &wanted).unwrap(), expected);
        assert_eq!(projection_indices(&fields, &["column_0"]).unwrap(), vec![0]);
    }
}

#[cfg(test)]
mod parquet_selection_preflight_tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn rejects_impossible_projection_width_before_building_lookup_tables() {
        let fields: Vec<arrow_schema::FieldRef> = vec![
            Arc::new(arrow_schema::Field::new(
                "ID", arrow_schema::DataType::Int64, false)),
            Arc::new(arrow_schema::Field::new(
                "payload", arrow_schema::DataType::Utf8, true)),
        ];
        assert_eq!(projection_indices(&fields, &["id", "PAYLOAD"]).unwrap(), [0, 1]);
        assert!(projection_indices(&fields, &["id", "ID", "payload"]).is_err());
        assert!(projection_indices(&fields, &["id", "id"]).is_err());
        assert!(projection_indices(&fields, &["missing"]).is_err());
    }

    #[test]
    fn validates_oversized_row_group_windows_without_allocating_hash_tables() {
        assert!(validate_row_groups(&[], 0).is_ok());
        assert!(validate_row_groups(&[0, 2], 3).is_ok());
        assert!(validate_row_groups(&[0, 0], 3).is_err());
        assert!(validate_row_groups(&[3], 3).is_err());
        assert!(validate_row_groups(&[0, 1, 2, 3, 4], 2).is_err());
    }
}

#[cfg(test)]
mod bounded_wide_parquet_batch_regressions {
    use super::*;

    #[test]
    fn huge_row_requests_are_capped_by_decoded_column_width() {
        let requested = 1_000_000;
        let one = bounded_native_batch_rows(requested, 1);
        let wide = bounded_native_batch_rows(requested, 1024);
        assert_eq!(one, requested);
        assert!(wide < one);
        assert!(wide > 0);
        for columns in [1, 8, 128, 1024, 4096] {
            let rows = bounded_native_batch_rows(requested, columns);
            assert!(rows <= requested);
            assert!(rows > 0);
            let slots = rows * columns * std::mem::size_of::<crate::scalar::Value>();
            assert!(slots <= MAX_NATIVE_VALUE_SLOT_BYTES);
        }
        assert_eq!(bounded_native_batch_rows(5, 1024), 5);
        assert_eq!(bounded_native_batch_rows(5, 0), 5);
    }
}

#[cfg(test)]
mod ambiguous_parquet_projection_tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn rejects_casefold_collisions_in_small_and_indexed_schemas() {
        let make_field = |name: String| Arc::new(arrow_schema::Field::new(
            name, arrow_schema::DataType::Int64, false));
        let small = vec![make_field("UserID".into()), make_field("userid".into())];
        let error = projection_indices(&small, &["USERID"]).unwrap_err();
        assert!(error.to_string().contains("ambiguous"));
        assert!(projection_indices(&small, &["missing"]).is_err());

        let mut fields = (0..64).map(|n| make_field(format!("column{n}")))
            .collect::<Vec<_>>();
        fields.push(make_field("Amount".into()));
        fields.push(make_field("AMOUNT".into()));
        let names = (0..32).map(|n| format!("column{n}"))
            .collect::<Vec<_>>();
        let mut requested = names.iter().map(String::as_str).collect::<Vec<_>>();
        requested.push("amount");
        assert!(requested.len() * fields.len() >= 2048);
        let error = projection_indices(&fields, &requested).unwrap_err();
        assert!(error.to_string().contains("ambiguous"));
        assert_eq!(projection_indices(&fields, &["column3"]).unwrap(), [3]);
    }
}

#[cfg(test)]
mod row_group_small_selection_admission_tests {
    use super::*;

    #[test]
    fn small_and_large_paths_agree_on_validity() {
        for length in [0usize, 1, 2, 15, 16, 17, 32] {
            let distinct = (0..length).collect::<Vec<_>>();
            assert!(validate_row_groups(&distinct, length).is_ok());
            if length > 1 {
                let mut duplicate = distinct.clone();
                duplicate[length - 1] = duplicate[0];
                assert!(validate_row_groups(&duplicate, length).is_err());
            }
            if length > 0 {
                let mut out_of_bounds = distinct.clone();
                out_of_bounds[length - 1] = length;
                assert!(validate_row_groups(&out_of_bounds, length).is_err());
            }
            assert!(validate_row_groups(&distinct, length.saturating_sub(1))
                .is_err() || length == 0);
        }
    }

    #[test]
    fn noncontiguous_groups_preserve_caller_order() {
        for count in [2, 16, 17, 64] {
            let indices = (0..count).rev().map(|index| index * 2)
                .collect::<Vec<_>>();
            assert!(validate_row_groups(&indices, count * 2).is_ok());
        }
    }
}
