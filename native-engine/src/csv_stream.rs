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
}

impl<R: BufRead> CsvStream<R> {
    pub fn new(input: R, batch_rows: usize, max_record_bytes: usize) -> Result<Self> {
        if batch_rows == 0 || max_record_bytes == 0 {
            return Err(Error::Execution("CSV batch rows and record bytes must be positive".into()));
        }
        let mut records = Records {
            input, max_record_bytes, pending: None, skip_lf: false,
        };
        let names = records.record()?.ok_or_else(|| Error::Parse("CSV header missing".into()))?;
        let schema = Schema::new(names.into_iter()
            .map(|name| Field::new(name, DataType::Text, true)).collect())?;
        Ok(Self { records, schema, batch_rows, exhausted: false })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl<R: BufRead> Iterator for CsvStream<R> {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        let mut rows = Vec::new();
        for _ in 0..self.batch_rows {
            match self.records.record() {
                Ok(Some(fields)) => {
                    if fields.len() != self.schema.len() {
                        self.exhausted = true;
                        return Some(Err(Error::Parse("CSV row width differs from header".into())));
                    }
                    rows.push(fields.into_iter().map(|value| {
                        if value.is_empty() { Value::Null } else { Value::Text(value) }
                    }).collect());
                }
                Ok(None) => {
                    self.exhausted = true;
                    break;
                }
                Err(error) => {
                    self.exhausted = true;
                    return Some(Err(error));
                }
            }
        }
        if rows.is_empty() {
            None
        } else {
            Some(Batch::from_rows(self.schema.clone(), rows))
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
}
