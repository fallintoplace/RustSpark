use std::{
    fs::File,
    io::Read,
    path::Path,
};

use crate::{
    batch::Batch,
    scalar::Value,
    schema::{DataType, Field, Schema},
    Error,
    Result,
};

const MAX_CSV_BYTES: u64 = 64 * 1024 * 1024;
const MAX_CSV_ROWS: usize = 1_000_000;

fn finish_field(row: &mut Vec<String>, field: &mut String) {
    row.push(std::mem::take(field));
}

fn finish_record(records: &mut Vec<Vec<String>>, row: &mut Vec<String>, field: &mut String) {
    if row.is_empty() && field.is_empty() {
        return;
    }
    finish_field(row, field);
    records.push(std::mem::take(row));
}

/// CSV reader for small local datasets. Supports UTF-8, CRLF, escaped quotes,
/// embedded commas and multiline quoted fields. It intentionally has limits.
pub fn parse_records(input: &str) -> Result<Vec<Vec<String>>> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum State {
        Start,
        Unquoted,
        Quoted,
        AfterQuote,
    }

    let mut state = State::Start;
    let mut chars = input.chars().peekable();
    let mut row = Vec::new();
    let mut field = String::new();
    let mut records = Vec::new();
    while let Some(character) = chars.next() {
        match state {
            State::Quoted => {
                if character == '"' {
                    if chars.peek() == Some(&'"') {
                        field.push('"');
                        let _ = chars.next();
                    } else {
                        state = State::AfterQuote;
                    }
                } else {
                    field.push(character);
                }
            }
            State::AfterQuote | State::Start | State::Unquoted => match character {
                ',' => {
                    finish_field(&mut row, &mut field);
                    state = State::Start;
                }
                '\n' | '\r' => {
                    if character == '\r' && chars.peek() == Some(&'\n') {
                        let _ = chars.next();
                    }
                    finish_record(&mut records, &mut row, &mut field);
                    state = State::Start;
                }
                '"' if state == State::Start => state = State::Quoted,
                '"' => return Err(Error::Parse("quote in unquoted CSV field".into())),
                _ if state == State::AfterQuote => {
                    return Err(Error::Parse("unexpected characters after CSV quote".into()));
                }
                _ => {
                    field.push(character);
                    state = State::Unquoted;
                }
            },
        }
        if records.len() > MAX_CSV_ROWS {
            return Err(Error::Execution("CSV row count limit exceeded".into()));
        }
    }
    if state == State::Quoted {
        return Err(Error::Parse("unterminated CSV quote".into()));
    }
    if !row.is_empty() || !field.is_empty() || state == State::AfterQuote {
        finish_record(&mut records, &mut row, &mut field);
    }
    Ok(records)
}

fn kind(values: &[Vec<String>], column: usize) -> DataType {
    let nonempty = values.iter().map(|row| row[column].as_str()).filter(|v| !v.is_empty());
    let samples: Vec<_> = nonempty.collect();
    if samples.is_empty() {
        DataType::Text
    } else if samples.iter().all(|value| value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false")) {
        DataType::Boolean
    } else if samples.iter().all(|value| value.parse::<i64>().is_ok()) {
        DataType::Integer
    } else if samples.iter().all(|value| value.parse::<f64>().is_ok_and(f64::is_finite)) {
        DataType::Float
    } else {
        DataType::Text
    }
}

pub fn from_csv_str(input: &str) -> Result<Batch> {
    let mut records = parse_records(input)?;
    if records.is_empty() {
        return Err(Error::Parse("CSV input has no header".into()));
    }
    let header = records.remove(0);
    if header.is_empty() {
        return Err(Error::Schema("CSV input has no columns".into()));
    }
    if records.len() > MAX_CSV_ROWS {
        return Err(Error::Execution("CSV row count limit exceeded".into()));
    }
    for row in &records {
        if row.len() != header.len() {
            return Err(Error::Parse("CSV row width differs from header".into()));
        }
    }
    let schema = Schema::new(
        header
            .iter()
            .enumerate()
            .map(|(column, name)| Field::new(name, kind(&records, column), true))
            .collect(),
    )?;
    let rows = records
        .into_iter()
        .map(|record| {
            record
                .into_iter()
                .enumerate()
                .map(|(index, text)| {
                    if text.is_empty() {
                        return Ok(Value::Null);
                    }
                    match schema.field(index)?.data_type {
                        DataType::Boolean => Ok(Value::Boolean(text.eq_ignore_ascii_case("true"))),
                        DataType::Integer => text
                            .parse::<i64>()
                            .map(Value::Integer)
                            .map_err(|_| Error::Parse("invalid integer in CSV".into())),
                        DataType::Float => text
                            .parse::<f64>()
                            .map(Value::Float)
                            .map_err(|_| Error::Parse("invalid float in CSV".into())),
                        DataType::Text => Ok(Value::Text(text)),
                    }
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    Batch::from_rows(schema, rows)
}

pub fn read_csv(path: impl AsRef<Path>) -> Result<Batch> {
    let file = File::open(path)?;
    if file.metadata()?.len() > MAX_CSV_BYTES {
        return Err(Error::Execution("CSV file exceeds 64 MiB input limit".into()));
    }
    let mut text = String::new();
    file.take(MAX_CSV_BYTES + 1).read_to_string(&mut text)?;
    if text.len() as u64 > MAX_CSV_BYTES {
        return Err(Error::Execution("CSV file exceeds 64 MiB input limit".into()));
    }
    from_csv_str(text.strip_prefix('\u{feff}').unwrap_or(&text))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoted_fields_nulls_and_types() {
        let batch = from_csv_str("city,amount,active\r\n\"New York\",12,true\r\n\"Pa\"\"ris\",,false\r\n").unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.row(0).unwrap(), vec![
            Value::Text("New York".into()),
            Value::Integer(12),
            Value::Boolean(true),
        ]);
        assert_eq!(batch.row(1).unwrap(), vec![
            Value::Text("Pa\"ris".into()),
            Value::Null,
            Value::Boolean(false),
        ]);
    }

    #[test]
    fn embedded_newlines_and_bad_csv() {
        let batch = from_csv_str("note\n\"hello\nworld\"\n").unwrap();
        assert_eq!(batch.row(0).unwrap(), vec![Value::Text("hello\nworld".into())]);
        assert!(from_csv_str("a,b\n1").is_err());
        assert!(from_csv_str("a\n\"open").is_err());
    }
}
