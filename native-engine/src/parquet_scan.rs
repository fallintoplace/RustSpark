//! Bounded Parquet scans using Apache Parquet and Arrow, never DataFusion.
use std::{fs::File, path::Path};

use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

use crate::{arrow_bridge, batch::Batch, Error, Result};

pub struct ParquetScan {
    inner: ParquetRecordBatchReader,
}

pub fn scan(path: impl AsRef<Path>, batch_size: usize) -> Result<ParquetScan> {
    if batch_size == 0 || batch_size > 1_000_000 {
        return Err(Error::Execution("Parquet batch size must be in 1..=1000000".into()));
    }
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| Error::Execution(format!("Parquet metadata: {e}")))?
        .with_batch_size(batch_size)
        .build()
        .map_err(|e| Error::Execution(format!("Parquet reader: {e}")))?;
    Ok(ParquetScan { inner: reader })
}

impl Iterator for ParquetScan {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|result| {
            let record = result
                .map_err(|e| Error::Execution(format!("Parquet decode: {e}")))?;
            arrow_bridge::from_arrow(&record)
        })
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
    Ok(LimitedParquetScan { inner: scan(path, batch_size)?, remaining: max_rows })
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
                Some(batch.slice_rows(0, keep))
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

