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
    skip_lf: bool,
}

impl<R: BufRead> CsvRows<R> {
    pub fn new(reader: R, schema: Schema, max_record_bytes: usize) -> Result<Self> {
        if schema.is_empty() || max_record_bytes == 0 {
            return Err(Error::Schema("CSV scan requires a schema and record limit".into()));
        }
        let mut scan = Self { reader, schema, max_record_bytes, finished: false, skip_lf: false };
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
        // The previous record already consumed CR; absorb its optional LF
        // without producing a blank extra record or allocating a parser row.
        if self.skip_lf {
            self.skip_lf = false;
            if self.reader.fill_buf()?.first() == Some(&b'\n') {
                self.reader.consume(1);
            }
        }
        // Match the strict CSV parser's field states while scanning records.
        // A quote inside an unquoted field must never hide a line separator.
        #[derive(Clone, Copy, PartialEq, Eq)]
        enum QuoteState { Start, Unquoted, Quoted, AfterQuote }
        let mut bytes = Vec::new();
        let mut state = QuoteState::Start;
        loop {
            let available = self.reader.fill_buf()?;
            if available.is_empty() {
                if bytes.is_empty() { return Ok(None); }
                if state == QuoteState::Quoted {
                    return Err(Error::Parse("unterminated quoted CSV field".into()));
                }
                break;
            }
            // Ordinary unquoted CSV fields dominate wide fact tables.
            // Bulk-copy runs until a quote or record terminator. Retain the
            // exact field-start state so quoted fields remain validated.
            if matches!(state, QuoteState::Start | QuoteState::Unquoted) {
                let plain = available.iter()
                    .position(|&byte| matches!(byte, b'"' | b'\n' | b'\r'))
                    .unwrap_or(available.len());
                if plain > 0 {
                    if bytes.len().saturating_add(plain) > self.max_record_bytes {
                        return Err(Error::Execution("CSV record exceeds byte limit".into()));
                    }
                    let after_separator = available[plain - 1] == b',';
                    bytes.extend_from_slice(&available[..plain]);
                    self.reader.consume(plain);
                    state = if after_separator { QuoteState::Start }
                        else { QuoteState::Unquoted };
                    continue;
                }
            }
            let mut consumed = 0;
            let mut complete = false;
            for &byte in available {
                if bytes.len().saturating_add(consumed) >= self.max_record_bytes {
                    return Err(Error::Execution("CSV record exceeds byte limit".into()));
                }
                consumed += 1;
                match state {
                    QuoteState::Start => match byte {
                        b'"' => state = QuoteState::Quoted,
                        b',' => {},
                        b'\n' | b'\r' => complete = true,
                        _ => state = QuoteState::Unquoted,
                    },
                    QuoteState::Unquoted => match byte {
                        b',' => state = QuoteState::Start,
                        b'"' => return Err(Error::Parse("quote in unquoted CSV field".into())),
                        b'\n' | b'\r' => complete = true,
                        _ => {},
                    },
                    QuoteState::Quoted => {
                        if byte == b'"' { state = QuoteState::AfterQuote; }
                    },
                    QuoteState::AfterQuote => match byte {
                        b'"' => state = QuoteState::Quoted,
                        b',' => state = QuoteState::Start,
                        b'\n' | b'\r' => complete = true,
                        _ => return Err(Error::Parse("unexpected characters after CSV quote".into())),
                    },
                }
                if complete {
                    self.skip_lf = byte == b'\r';
                    break;
                }
            }
            bytes.extend_from_slice(&available[..consumed]);
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
        let Some(row) = records.into_iter().next() else { return Ok(None); };
        let values = row.into_iter().zip(self.schema.fields())
            .map(|(value, field)| {
                if value.is_empty() {
                    if !field.nullable {
                        return Err(Error::Type(format!(
                            "NULL in required column {}", field.name
                        )));
                    }
                    return Ok(Value::Null);
                }
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

#[cfg(test)]
mod crlf_fast_scan_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};
    use std::io::{BufReader, Cursor};

    #[test]
    fn skips_crlf_separator_even_across_tiny_read_buffers() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("note", DataType::Text, false),
        ]).unwrap();
        let input = "id,note\r\n1,\"a\r\nb\"\r\n2,plain\r\n3,end\r";
        let source = BufReader::with_capacity(1, Cursor::new(input.as_bytes()));
        let rows = CsvRows::new(source, schema, 128).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(rows, vec![
            vec![Value::Integer(1), Value::Text("a\r\nb".into())],
            vec![Value::Integer(2), Value::Text("plain".into())],
            vec![Value::Integer(3), Value::Text("end".into())],
        ]);
    }

    #[test]
    fn standalone_cr_does_not_consume_first_byte_of_next_record() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let rows = CsvRows::new(Cursor::new("id\r1\r2\r"), schema, 16).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(rows, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
    }
}

#[cfg(test)]
mod streamed_required_csv_column_regressions {
    use super::*;
    use crate::schema::{DataType, Field, Schema};
    use std::io::Cursor;

    #[test]
    fn direct_csv_row_stream_rejects_missing_required_values() {
        let required = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("note", DataType::Text, false),
        ]).unwrap();
        for input in ["id,note\n1,\n", "id,note\n,hello\n"] {
            let mut stream = CsvRows::new(Cursor::new(input), required.clone(), 128).unwrap();
            assert!(stream.next().unwrap().unwrap_err().to_string().contains("required"));
            assert!(stream.next().is_none());
        }
        let nullable = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("note", DataType::Text, true),
        ]).unwrap();
        let values = CsvRows::new(Cursor::new("id,note\n1,\n"), nullable, 128)
            .unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(values, vec![vec![Value::Integer(1), Value::Null]]);
    }
}

#[cfg(test)]
mod strict_csv_record_boundary_regressions {
    use super::*;
    use crate::schema::{DataType, Field, Schema};
    use std::io::{BufReader, Cursor};

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("note", DataType::Text, false),
        ]).unwrap()
    }

    #[test]
    fn malformed_quotes_do_not_hide_following_records() {
        for input in [
            "id,note\n1,bad\"quote\n2,safe\n",
            "id,note\n1,\"quoted\"extra\n2,safe\n",
        ] {
            let mut reader = CsvRows::new(Cursor::new(input), schema(), 1024).unwrap();
            assert!(matches!(reader.next(), Some(Err(Error::Parse(_)))));
            assert!(reader.next().is_none());
        }
    }

    #[test]
    fn doubled_quotes_and_multiline_records_survive_single_byte_buffers() {
        let input = "id,note\r\n1,\"a\"\"b\r\nc\"\r\n2,plain\r\n";
        let reader = BufReader::with_capacity(1, Cursor::new(input.as_bytes()));
        let actual = CsvRows::new(reader, schema(), 256).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(actual, vec![
            vec![Value::Integer(1), Value::Text("a\"b\r\nc".into())],
            vec![Value::Integer(2), Value::Text("plain".into())],
        ]);
    }

    #[test]
    fn unterminated_quote_is_reported_at_eof() {
        let mut reader = CsvRows::new(Cursor::new("id,note\n1,\"unfinished"), schema(), 256).unwrap();
        assert!(matches!(reader.next(), Some(Err(Error::Parse(_)))));
        assert!(reader.next().is_none());
    }
}

#[cfg(test)]
mod bulk_plain_csv_scan_regressions {
    use super::*;
    use std::io::{BufReader, Cursor};
    use crate::schema::{Field, DataType};

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("text", DataType::Text, true),
            Field::new("number", DataType::Integer, false),
        ]).unwrap()
    }

    #[test]
    fn handles_unquoted_runs_across_small_buffers_and_crlf() {
        let wide = "text_value".repeat(400);
        let input = format!("text,number\r\n{wide},1\r\nx,2\n\"quoted\",3\r\n");
        let reader = BufReader::with_capacity(11, Cursor::new(input));
        let rows = CsvRows::new(reader, schema(), 16_384).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(rows, vec![
            vec![Value::Text(wide), Value::Integer(1)],
            vec![Value::Text("x".into()), Value::Integer(2)],
            vec![Value::Text("quoted".into()), Value::Integer(3)],
        ]);
    }

    #[test]
    fn quote_in_plain_field_is_still_an_error() {
        let input = "text,number\na\"b,1\n";
        let mut rows = CsvRows::new(Cursor::new(input), schema(), 1024).unwrap();
        assert!(matches!(rows.next(), Some(Err(Error::Parse(_)))));
        assert!(rows.next().is_none());
    }

    #[test]
    fn exact_record_byte_budget_matches_scalar_path() {
        let schema = Schema::new(vec![Field::new("x", DataType::Text, true)]).unwrap();
        let rows = CsvRows::new(Cursor::new("x\na\n"), schema.clone(), 2).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(rows, vec![vec![Value::Text("a".into())]]);
        let mut rows = CsvRows::new(Cursor::new("x\nabc\n"), schema, 2).unwrap();
        assert!(matches!(rows.next(), Some(Err(Error::Execution(_)))));
    }
}
