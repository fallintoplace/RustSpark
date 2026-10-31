use std::io::BufRead;

const MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;

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

const READ_CHUNK_BYTES: usize = 8 * 1024;

struct Records<R> {
    input: R,
    max_record_bytes: usize,
    pending: Option<u8>,
    skip_lf: bool,
    buffer: [u8; READ_CHUNK_BYTES],
    cursor: usize,
    filled: usize,
}

impl<R: BufRead> Records<R> {
    fn byte(&mut self) -> Result<Option<u8>> {
        if let Some(byte) = self.pending.take() {
            return Ok(Some(byte));
        }
        if self.cursor == self.filled {
            // The CSV parser consumes one byte at a time, but the underlying
            // reader must not be polled separately for every byte in a row.
            self.filled = loop {
                match std::io::Read::read(&mut self.input, &mut self.buffer) {
                    Ok(count) => break count,
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(error) => return Err(error.into()),
                }
            };
            self.cursor = 0;
            if self.filled == 0 { return Ok(None); }
        }
        let byte = self.buffer[self.cursor];
        self.cursor += 1;
        Ok(Some(byte))
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
                (State::Unquoted, b'"') => {
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
    pending_row: Option<Vec<Value>>,
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
            buffer: [0; READ_CHUNK_BYTES], cursor: 0, filled: 0,
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
        Ok(Self { records, schema, batch_rows, exhausted: false, pending_error: None, pending_row: None })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl<R: BufRead> CsvStream<R> {
    // Reuse one row buffer per output batch, then drain its cells directly
    // into columns. Avoid allocating a Vec<Value> for every CSV record.
    fn read_row_into(&mut self, row: &mut Vec<Value>) -> Result<bool> {
        let Some(fields) = self.records.record()? else { return Ok(false); };
        if fields.len() != self.schema.len() {
            return Err(Error::Parse("CSV row width differs from header".into()));
        }
        row.clear();
        for (text, field) in fields.into_iter().zip(self.schema.fields()) {
            let value = if text.is_empty() {
                Value::Null
            } else {
                match field.data_type {
                    DataType::Text => Value::Text(text),
                    DataType::Boolean if text.eq_ignore_ascii_case("true") =>
                        Value::Boolean(true),
                    DataType::Boolean if text.eq_ignore_ascii_case("false") =>
                        Value::Boolean(false),
                    DataType::Boolean =>
                        return Err(Error::Parse("invalid boolean in CSV stream".into())),
                    DataType::Integer => Value::Integer(text.parse::<i64>()
                        .map_err(|_| Error::Parse("invalid integer in CSV stream".into()))?),
                    DataType::Float => {
                        let value = text.parse::<f64>()
                            .map_err(|_| Error::Parse("invalid float in CSV stream".into()))?;
                        if !value.is_finite() {
                            return Err(Error::Parse("nonfinite float in CSV stream".into()));
                        }
                        Value::Float(value)
                    }
                }
            };
            row.push(value);
        }
        Ok(true)
    }

    fn next_with_byte_budget(&mut self, byte_budget: usize) -> Option<Result<Batch>> {
        if self.exhausted { return None; }
        if let Some(error) = self.pending_error.take() {
            self.exhausted = true;
            return Some(Err(error));
        }
        let mut columns = (0..self.schema.len()).map(|_|
            Vec::with_capacity(self.batch_rows.min(1_024))
        ).collect::<Vec<_>>();
        let mut row = Vec::with_capacity(self.schema.len());
        let mut row_count = 0usize;
        let mut used = 0usize;
        let row_width = self.schema.len().saturating_mul(std::mem::size_of::<Value>());
        for _ in 0..self.batch_rows.min(65_536) {
            let next = if let Some(pending) = self.pending_row.take() {
                row = pending;
                Ok(true)
            } else {
                self.read_row_into(&mut row)
            };
            match next {
                Ok(true) => {
                    let row_bytes = row.iter().fold(row_width, |total, value|
                        total.saturating_add(match value {
                            Value::Text(text) => text.len(), _ => 0,
                        })
                    );
                    if row_bytes > byte_budget {
                        let error = Error::Execution("CSV row exceeds batch byte budget".into());
                        if row_count == 0 {
                            self.exhausted = true;
                            return Some(Err(error));
                        }
                        self.pending_error = Some(error);
                        break;
                    }
                    if row_count != 0 && used.saturating_add(row_bytes) > byte_budget {
                        self.pending_row = Some(row);
                        break;
                    }
                    used += row_bytes;
                    row_count += 1;
                    for (column, value) in columns.iter_mut().zip(row.drain(..)) {
                        column.push(value);
                    }
                }
                Ok(false) => {
                    self.exhausted = true;
                    break;
                }
                Err(error) => {
                    if row_count == 0 {
                        self.exhausted = true;
                        return Some(Err(error));
                    }
                    self.pending_error = Some(error);
                    break;
                }
            }
        }
        if row_count == 0 { return None; }
        // Preserve nullable and type validation while skipping row-major
        // buffering and a second row-to-column transposition.
        let result = Batch::new_with_row_count(self.schema.clone(), columns, row_count);
        if result.is_err() { self.exhausted = true; }
        Some(result)
    }
}

impl<R: BufRead> Iterator for CsvStream<R> {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_with_byte_budget(MAX_BATCH_BYTES)
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

#[cfg(test)]
mod csv_byte_budget_regressions {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn large_rows_split_before_exceeding_a_batch_byte_budget() {
        let data = format!("payload\n{}\n{}\n{}\n",
            "a".repeat(50), "b".repeat(50), "c".repeat(50));
        let mut stream = CsvStream::new(Cursor::new(data), 100, 128).unwrap();
        let per_row = std::mem::size_of::<Value>() + 50;
        let budget = per_row * 2;
        let first = stream.next_with_byte_budget(budget).unwrap().unwrap();
        let second = stream.next_with_byte_budget(budget).unwrap().unwrap();
        assert_eq!(first.num_rows(), 2);
        assert_eq!(second.num_rows(), 1);
        assert_eq!(second.row(0).unwrap(), vec![Value::Text("c".repeat(50))]);
        assert!(stream.next_with_byte_budget(budget).is_none());
    }

    #[test]
    fn oversized_row_is_terminal_but_prior_rows_are_preserved() {
        let data = format!("text\na\n{}\n", "x".repeat(80));
        let mut stream = CsvStream::new(Cursor::new(data), 100, 128).unwrap();
        let budget = std::mem::size_of::<Value>() + 16;
        assert_eq!(stream.next_with_byte_budget(budget).unwrap().unwrap().num_rows(), 1);
        assert!(stream.next_with_byte_budget(budget).unwrap().is_err());
        assert!(stream.next_with_byte_budget(budget).is_none());
    }
}

#[cfg(test)]
mod buffered_csv_record_boundary_tests {
    use super::*;
    use std::io::{BufReader, Cursor, Read};

    struct Fragmented<R>(R);
    impl<R: Read> Read for Fragmented<R> {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            let max = bytes.len().min(3);
            self.0.read(&mut bytes[..max])
        }
    }

    #[test]
    fn quoted_utf8_and_crlf_survive_fragmented_input() {
        let input = "name,notes\r\n\"é🙂\",\"hello,\"\"quoted\"\"\r\nnext\"\r\nlast,x\n";
        let stream = CsvStream::new(BufReader::with_capacity(2,
            Fragmented(Cursor::new(input.as_bytes()))), 1, 4096).unwrap();
        let actual = stream.collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(actual.len(), 2);
        assert_eq!(actual[0].row(0).unwrap(), vec![
            Value::Text("é🙂".into()), Value::Text("hello,\"quoted\"\r\nnext".into())
        ]);
        assert_eq!(actual[1].row(0).unwrap(), vec![
            Value::Text("last".into()), Value::Text("x".into())
        ]);
    }

    #[test]
    fn long_records_and_batched_rows_cross_internal_read_boundaries() {
        let long = "a".repeat(READ_CHUNK_BYTES * 2 + 13);
        let input = format!("payload\n{long}\nsmall\n");
        let rows = CsvStream::new(BufReader::new(Cursor::new(input)), 2,
            READ_CHUNK_BYTES * 3).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].rows().unwrap(), vec![
            vec![Value::Text(long)], vec![Value::Text("small".into())]
        ]);
    }
}

#[cfg(test)]
mod column_direct_csv_batch_tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn large_nullable_quoted_rows_match_across_batch_boundaries() {
        let mut csv = String::from("id,payload,flag\\n");
        let mut expected = Vec::new();
        for n in 0..257 {
            let text = if n % 11 == 0 { String::new() } else {
                format!("payload-{n}-{}", "é".repeat(32))
            };
            csv.push_str(&format!("{n},\\\"{text}\\\",{}\\n", n % 2 == 0));
            expected.push(vec![
                Value::Integer(n),
                if text.is_empty() { Value::Null } else { Value::Text(text) },
                Value::Boolean(n % 2 == 0),
            ]);
        }
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("payload", DataType::Text, true),
            Field::new("flag", DataType::Boolean, false),
        ]).unwrap();
        for batch_rows in [1, 7, 64, 500] {
            let batches = CsvStream::new_typed(Cursor::new(csv.as_bytes()), batch_rows, 4096,
                schema.clone()).unwrap().collect::<Result<Vec<_>>>().unwrap();
            let rows = batches.iter().flat_map(|batch| batch.rows().unwrap()).collect::<Vec<_>>();
            assert_eq!(rows, expected);
        }
    }
}
