use std::io::BufRead;

use crate::{
    batch::Batch,
    scalar::Value,
    schema::{DataType, Field, Schema},
    Error, Result,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Start,
    Unquoted,
    Quoted,
    AfterQuote,
}

struct Records<R> {
    input: R,
    max_record_bytes: usize,
    pending: Option<u8>,
    skip_lf: bool,
}

impl<R: BufRead> Records<R> {
    fn byte(&mut self) -> Result<Option<u8>> {
        if let Some(byte) = self.pending.take() {
            return Ok(Some(byte));
        }
        let slice = self.input.fill_buf()?;
        let byte = slice.first().copied();
        if byte.is_some() {
            self.input.consume(1);
        }
        Ok(byte)
    }

    fn record(&mut self) -> Result<Option<Vec<String>>> {
        if self.skip_lf {
            if let Some(byte) = self.byte()? {
                if byte != b'\n' {
                    self.pending = Some(byte);
                }
            }
            self.skip_lf = false;
        }
        let mut state = State::Start;
        let mut field = Vec::new();
        let mut fields = Vec::new();
        let mut size = 0usize;
        let mut seen = false;
        loop {
            let byte = match self.byte()? {
                Some(byte) => byte,
                None => {
                    if !seen {
                        return Ok(None);
                    }
                    if state == State::Quoted {
                        return Err(Error::Parse("unterminated CSV quote".into()));
                    }
                    fields.push(decode(field)?);
                    return Ok(Some(fields));
                }
            };
            seen = true;
            size += 1;
            if size > self.max_record_bytes {
                return Err(Error::Execution("CSV record exceeds configured byte limit".into()));
            }
            match (state, byte) {
                (State::Start, b'"') => state = State::Quoted,
                (State::Quoted, b'"') => state = State::AfterQuote,
                (State::AfterQuote, b'"') => {
                    field.push(b'"');
                    state = State::Quoted;
                }
                (State::Quoted, byte) => field.push(byte),
                (State::Start | State::Unquoted | State::AfterQuote, b',') => {
                    fields.push(decode(std::mem::take(&mut field))?);
                    state = State::Start;
                }
                (State::Start | State::Unquoted | State::AfterQuote, b'\n' | b'\r') => {
                    if byte == b'\r' {
                        self.skip_lf = true;
                    }
                    fields.push(decode(field)?);
                    return Ok(Some(fields));
                }
                (State::Start | State::Unquoted, b'"') => {
                    return Err(Error::Parse("quote in unquoted CSV field".into()));
                }
                (State::AfterQuote, _) => {
                    return Err(Error::Parse("invalid character after CSV quote".into()));
                }
                (State::Start | State::Unquoted, byte) => {
                    field.push(byte);
                    state = State::Unquoted;
                }
            }
        }
    }
}

fn decode(bytes: Vec<u8>) -> Result<String> {
    String::from_utf8(bytes).map_err(|_| Error::Parse("invalid UTF-8 in CSV".into()))
}

pub struct CsvStream<R> {
    records: Records<R>,
    schema: Schema,
    batch_rows: usize,
    exhausted: bool,
    pending_error: Option<Error>,
}

impl<R: BufRead> CsvStream<R> {
    pub fn new(input: R, batch_rows: usize, max_record_bytes: usize) -> Result<Self> {
        Self::open(input, batch_rows, max_record_bytes, None)
    }

    pub fn new_typed(
        input: R, batch_rows: usize, max_record_bytes: usize, schema: Schema,
    ) -> Result<Self> {
        Self::open(input, batch_rows, max_record_bytes, Some(schema))
    }

    fn open(
        input: R, batch_rows: usize, max_record_bytes: usize, typed: Option<Schema>,
    ) -> Result<Self> {
        if batch_rows == 0 || max_record_bytes == 0 {
            return Err(Error::Execution("CSV batch rows and record bytes must be positive".into()));
        }
        let mut records = Records {
            input, max_record_bytes, pending: None, skip_lf: false,
        };
        let mut names = records.record()?.ok_or_else(|| Error::Parse("CSV header missing".into()))?;
        if let Some(first) = names.first_mut() {
            if let Some(clean) = first.strip_prefix('\u{feff}') {
                *first = clean.to_owned();
            }
        }
        let schema = if let Some(schema) = typed {
            if schema.len() != names.len() ||
                !schema.fields().iter().zip(&names).all(|(field, name)|
                    field.name.eq_ignore_ascii_case(name)) {
                return Err(Error::Schema("typed CSV schema does not match header".into()));
            }
            schema
        } else {
            Schema::new(names.into_iter()
                .map(|name| Field::new(name, DataType::Text, true)).collect())?
        };
        Ok(Self { records, schema, batch_rows, exhausted: false, pending_error: None })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl<R: BufRead> Iterator for CsvStream<R> {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted { return None; }
        if let Some(error) = self.pending_error.take() {
            self.exhausted = true;
            return Some(Err(error));
        }
        // Avoid a large allocation when a short stream requests huge batches.
        // Let row storage grow when a batch actually contains many records.
        let mut rows = Vec::with_capacity(self.batch_rows.min(1_024));
        for _ in 0..self.batch_rows.min(65_536) {
            match self.records.record() {
                Ok(Some(fields)) => {
                    if fields.len() != self.schema.len() {
                        let error = Error::Parse("CSV row width differs from header".into());
                        if rows.is_empty() {
                            self.exhausted = true;
                            return Some(Err(error));
                        }
                        self.pending_error = Some(error);
                        break;
                    }
                    let converted = fields.into_iter().zip(self.schema.fields()).map(|(text, field)| {
                        if text.is_empty() {
                            return Ok(Value::Null);
                        }
                        let kind = field.data_type;
                        match kind {
                            DataType::Text => Ok(Value::Text(text)),
                            DataType::Boolean => {
                                if text.eq_ignore_ascii_case("true") {
                                    Ok(Value::Boolean(true))
                                } else if text.eq_ignore_ascii_case("false") {
                                    Ok(Value::Boolean(false))
                                } else {
                                    Err(Error::Parse("invalid boolean in CSV stream".into()))
                                }
                            }
                            DataType::Integer => text.parse::<i64>()
                                .map(Value::Integer)
                                .map_err(|_| Error::Parse("invalid integer in CSV stream".into())),
                            DataType::Float => {
                                let value = text.parse::<f64>()
                                    .map_err(|_| Error::Parse("invalid float in CSV stream".into()))?;
                                if !value.is_finite() {
                                    return Err(Error::Parse("nonfinite float in CSV stream".into()));
                                }
                                Ok(Value::Float(value))
                            }
                        }
                    }).collect::<Result<Vec<_>>>();
                    match converted {
                        Ok(row) => rows.push(row),
                        Err(error) => {
                            if rows.is_empty() {
                                self.exhausted = true;
                                return Some(Err(error));
                            }
                            self.pending_error = Some(error);
                            break;
                        }
                    }
                }
                Ok(None) => {
                    self.exhausted = true;
                    break;
                }
                Err(error) => {
                    if rows.is_empty() {
                        self.exhausted = true;
                        return Some(Err(error));
                    }
                    self.pending_error = Some(error);
                    break;
                }
            }
        }
        if rows.is_empty() {
            None
        } else {
            let result = Batch::from_rows(self.schema.clone(), rows);
            if result.is_err() {
                self.exhausted = true;
            }
            Some(result)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn streams_quoted_multiline_records_across_batches() {
        let data = b"id,note\r\n1,\"line 1\nline 2\"\r\n2,\"a\"\"b\"\r\n3,\r\n";
        let stream = CsvStream::new(Cursor::new(data), 1, 100).unwrap();
        let chunks = stream.collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].row(0).unwrap(), vec![
            Value::Text("1".into()), Value::Text("line 1\nline 2".into()),
        ]);
        assert_eq!(chunks[1].value(0, 1).unwrap(), &Value::Text("a\"b".into()));
        assert_eq!(chunks[2].value(0, 1).unwrap(), &Value::Null);
    }

    #[test]
    fn limits_records_and_rejects_invalid_csv() {
        assert!(CsvStream::new(Cursor::new(b"a\n123456\n"), 1, 3)
            .unwrap().next().unwrap().is_err());
        let mut stream = CsvStream::new(Cursor::new(b"a,b\n1\n"), 1, 100).unwrap();
        assert!(stream.next().unwrap().is_err());
    }
    #[test]
    fn schema_failure_is_terminal_for_typed_streams() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let mut stream = CsvStream::new_typed(
            Cursor::new(b"id\n\n7\n"), 1, 64, schema,
        ).unwrap();
        assert!(stream.next().unwrap().is_err());
        assert!(stream.next().is_none());
    }

    #[test]
    fn typed_scan_preserves_integer_and_float_values() {
        let schema = Schema::new(vec![
            Field::new("city", DataType::Text, false),
            Field::new("amount", DataType::Integer, true),
            Field::new("ratio", DataType::Float, true),
        ]).unwrap();
        let mut source = CsvStream::new_typed(
            Cursor::new(b"city,amount,ratio\nBerlin,12,1.25\nParis,,2.5\n"),
            1, 256, schema,
        ).unwrap();
        assert_eq!(source.next().unwrap().unwrap().row(0).unwrap(), vec![
            Value::Text("Berlin".into()), Value::Integer(12), Value::Float(1.25),
        ]);
        assert_eq!(source.next().unwrap().unwrap().row(0).unwrap()[1], Value::Null);
        assert!(source.next().is_none());
    }

    #[test]
    fn typed_scan_rejects_inconsistent_schema_and_bad_numbers() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        assert!(CsvStream::new_typed(
            Cursor::new(b"other\n3\n"), 2, 256, schema.clone(),
        ).is_err());
        let mut stream = CsvStream::new_typed(
            Cursor::new(b"n\nabc\n"), 2, 256, schema,
        ).unwrap();
        assert!(stream.next().unwrap().is_err());
    }

}

#[cfg(test)]
mod csv_bom_engine_tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn typed_and_inferred_streams_drop_bom_from_first_header() {
        let source = "\u{feff}id,city\n1,Paris\n";
        let inferred = CsvStream::new(Cursor::new(source), 2, 128).unwrap();
        assert_eq!(inferred.schema().column("id").unwrap(), 0);
        let typed_schema = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("city", DataType::Text, false),
        ]).unwrap();
        let mut typed = CsvStream::new_typed(
            Cursor::new(source), 2, 128, typed_schema,
        ).unwrap();
        assert_eq!(typed.next().unwrap().unwrap().row(0).unwrap(), vec![
            Value::Integer(1), Value::Text("Paris".into()),
        ]);
    }
}

#[cfg(test)]
mod consolidated_csv_prefix_regressions {
    use super::*;
    use std::io::Cursor;
    #[test]
    fn valid_rows_emit_before_terminal_conversion_error() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let mut stream = CsvStream::new_typed(Cursor::new(b"id\n1\n2\nbad\n3\n"), 10, 128, schema).unwrap();
        assert_eq!(stream.next().unwrap().unwrap().rows().unwrap(), vec![
            vec![Value::Integer(1)], vec![Value::Integer(2)]]);
        assert!(stream.next().unwrap().is_err());
        assert!(stream.next().is_none());
    }
    #[test]
    fn malformed_width_preserves_valid_prefix() {
        let mut stream = CsvStream::new(Cursor::new(b"id,name\n1,ok\n2\n"), 10, 128).unwrap();
        assert_eq!(stream.next().unwrap().unwrap().num_rows(), 1);
        assert!(stream.next().unwrap().is_err());
        assert!(stream.next().is_none());
    }
}

#[cfg(test)]
mod bounded_csv_stream_batch_tests {
    use super::*;
    use std::io::Cursor;
    #[test]
    fn huge_requested_batch_is_chunked_without_losing_rows() {
        let input = format!("n\n{}", "x\n".repeat(65_537));
        let batches = CsvStream::new(Cursor::new(input), usize::MAX, 32)
            .unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(batches.iter().map(Batch::num_rows).collect::<Vec<_>>(), [65_536, 1]);
    }
}

#[cfg(test)]
mod modest_initial_csv_batch_capacity_tests {
    use super::*;

    #[test]
    fn short_and_large_batches_preserve_all_csv_rows() {
        let mut short = CsvStream::new(
            std::io::Cursor::new(b"id\na\nb\n"),
            usize::MAX, 64,
        ).unwrap();
        let batch = short.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert!(short.next().is_none());

        let mut csv = String::from("id\n");
        for n in 0..1_025 { csv.push_str(&format!("{n}\n")); }
        let batches = CsvStream::new(
            std::io::Cursor::new(csv.into_bytes()), usize::MAX, 64,
        ).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(batches.iter().map(Batch::num_rows).sum::<usize>(), 1_025);
    }
}
