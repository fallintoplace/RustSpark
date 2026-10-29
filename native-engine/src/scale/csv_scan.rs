//! Bounded CSV record reader. Schemas are explicit to avoid full-file inference.
use std::{fs::File, io::{BufRead, BufReader}, path::Path};
use crate::{
    csv::parse_records,
    scalar::Value,
    schema::{DataType, Schema},
    Error, Result,
};

pub struct CsvRows<R: BufRead> {
    reader: R,
    schema: Schema,
    max_record_bytes: usize,
    finished: bool,
}

impl<R: BufRead> CsvRows<R> {
    pub fn new(reader: R, schema: Schema, max_record_bytes: usize) -> Result<Self> {
        if schema.is_empty() || max_record_bytes == 0 {
            return Err(Error::Schema("CSV scan requires a schema and record limit".into()));
        }
        let mut scan = Self { reader, schema, max_record_bytes, finished: false };
        let header = scan.read_raw_record()?
            .ok_or_else(|| Error::Parse("CSV input has no header".into()))?;
        let parsed = parse_records(header.trim_start_matches('\u{feff}'))?;
        if parsed.len() != 1 || parsed[0].len() != scan.schema.len() {
            return Err(Error::Schema("CSV header differs from declared schema".into()));
        }
        for (name, field) in parsed[0].iter().zip(scan.schema.fields()) {
            if !name.eq_ignore_ascii_case(&field.name) {
                return Err(Error::Schema(format!("CSV header mismatch at {}", field.name)));
            }
        }
        Ok(scan)
    }

    fn read_raw_record(&mut self) -> Result<Option<String>> {
        let mut bytes = Vec::new();
        let mut quoted = false;
        loop {
            let available = self.reader.fill_buf()?;
            if available.is_empty() {
                if bytes.is_empty() { return Ok(None); }
                if quoted { return Err(Error::Parse("unterminated quoted CSV field".into())); }
                break;
            }
            let mut consumed = 0;
            let mut complete = false;
            for &byte in available {
                if bytes.len() >= self.max_record_bytes {
                    return Err(Error::Execution("CSV record exceeds byte limit".into()));
                }
                bytes.push(byte);
                consumed += 1;
                if byte == b'"' { quoted = !quoted; }
                if (byte == b'\n' || byte == b'\r') && !quoted {
                    complete = true;
                    break;
                }
            }
            self.reader.consume(consumed);
            if complete { break; }
        }
        String::from_utf8(bytes).map(Some)
            .map_err(|_| Error::Parse("CSV input is not UTF-8".into()))
    }

    fn parse_row(&self, text: &str) -> Result<Option<Vec<Value>>> {
        let records = parse_records(text)?;
        if records.is_empty() { return Ok(None); }
        if records.len() != 1 || records[0].len() != self.schema.len() {
            return Err(Error::Parse("CSV row width differs from declared schema".into()));
        }
        let row = records.into_iter().next().unwrap();
        let values = row.into_iter().zip(self.schema.fields())
            .map(|(value, field)| {
                if value.is_empty() { return Ok(Value::Null); }
                match field.data_type {
                    DataType::Text => Ok(Value::Text(value)),
                    DataType::Boolean => {
                        if value.eq_ignore_ascii_case("true") { Ok(Value::Boolean(true)) }
                        else if value.eq_ignore_ascii_case("false") { Ok(Value::Boolean(false)) }
                        else { Err(Error::Parse("invalid CSV boolean".into())) }
                    }
                    DataType::Integer => value.parse::<i64>().map(Value::Integer)
                        .map_err(|_| Error::Parse("invalid CSV integer".into())),
                    DataType::Float => value.parse::<f64>()
                        .map_err(|_| Error::Parse("invalid CSV float".into()))
                        .and_then(|number| {
                            if number.is_finite() { Ok(Value::Float(number)) }
                            else { Err(Error::Parse("nonfinite CSV float".into())) }
                        }),
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(values))
    }
}

impl<R: BufRead> Iterator for CsvRows<R> {
    type Item = Result<Vec<Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished { return None; }
        loop {
            match self.read_raw_record() {
                Ok(Some(text)) => match self.parse_row(&text) {
                    Ok(Some(row)) => return Some(Ok(row)),
                    Ok(None) => continue,
                    Err(error) => {
                        self.finished = true;
                        return Some(Err(error));
                    }
                },
                Ok(None) => {
                    self.finished = true;
                    return None;
                }
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

pub fn open_csv(
    path: impl AsRef<Path>, schema: Schema, max_record_bytes: usize
) -> Result<CsvRows<BufReader<File>>> {
    CsvRows::new(BufReader::new(File::open(path)?), schema, max_record_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{scale::chunks::RowBatcher, schema::{DataType, Field}};
    use std::io::Cursor;

    #[test]
    fn multiline_records_and_column_types_stream_in_small_batches() {
        let schema = Schema::new(vec![
            Field::new("city", DataType::Text, false),
            Field::new("qty", DataType::Integer, true),
            Field::new("active", DataType::Boolean, true),
        ]).unwrap();
        let input = "city,qty,active\r\n\"New\nYork\",10,true\r\nParis,,false\r\nRome,3,true\r\n";
        let rows = CsvRows::new(Cursor::new(input), schema.clone(), 128).unwrap();
        let batches = RowBatcher::new(rows, schema, 2).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].row(0).unwrap()[0], Value::Text("New\nYork".into()));
        assert_eq!(batches[0].row(1).unwrap()[1], Value::Null);
        assert_eq!(batches[1].row(0).unwrap()[1], Value::Integer(3));
    }

    #[test]
    fn reject_wrong_header_and_oversized_record() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        assert!(CsvRows::new(Cursor::new("wrong\n1\n"), schema.clone(), 100).is_err());
        let mut rows = CsvRows::new(Cursor::new("id\n1234567890\n"), schema, 5).unwrap();
        assert!(rows.next().unwrap().is_err());
    }
}

#[cfg(test)]
mod cr_only_csv_scan_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};
    use std::io::Cursor;

    #[test]
    fn scans_cr_only_records_without_splitting_quoted_carriage_returns() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("text", DataType::Text, false),
        ]).unwrap();
        let input = "id,text\r1,\"a\rb\"\r2,ok\r";
        let rows = CsvRows::new(Cursor::new(input), schema, 1024).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(rows, vec![
            vec![Value::Integer(1), Value::Text("a\rb".into())],
            vec![Value::Integer(2), Value::Text("ok".into())],
        ]);
    }
}
