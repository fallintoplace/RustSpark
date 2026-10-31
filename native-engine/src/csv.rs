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
    parse_records_with_limit(input, MAX_CSV_ROWS)
}

fn parse_records_with_limit(input: &str, max_records: usize) -> Result<Vec<Vec<String>>> {
    if input.len() as u64 > MAX_CSV_BYTES {
        return Err(Error::Execution("CSV input exceeds 64 MiB limit".into()));
    }
    // Unquoted LF files can be split a record at a time without per-character
    // parser state. Empty physical lines are ignored by the general scanner.
    if !input.as_bytes().contains(&b'"') && !input.as_bytes().contains(&b'\r') {
        let mut records = Vec::new();
        for line in input.split('\n').filter(|line| !line.is_empty()) {
            records.push(line.split(',').map(str::to_owned).collect());
            if records.len() > max_records {
                return Err(Error::Execution("CSV row count limit exceeded".into()));
            }
        }
        return Ok(records);
    }
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
                    if records.len() > max_records {
                        return Err(Error::Execution("CSV row count limit exceeded".into()));
                    }
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
    }
    if state == State::Quoted {
        return Err(Error::Parse("unterminated CSV quote".into()));
    }
    if !row.is_empty() || !field.is_empty() || state == State::AfterQuote {
        finish_record(&mut records, &mut row, &mut field);
    }
    if records.len() > max_records {
        return Err(Error::Execution("CSV row count limit exceeded".into()));
    }
    Ok(records)
}

fn kind(values: &[Vec<String>], column: usize) -> DataType {
    let mut nonempty = false;
    let mut boolean = true;
    let mut integer = true;
    let mut float = true;
    for row in values {
        let value = row[column].as_str();
        if value.is_empty() { continue; }
        nonempty = true;
        if boolean {
            boolean = value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false");
        }
        if integer { integer = value.parse::<i64>().is_ok(); }
        if float { float = value.parse::<f64>().is_ok_and(f64::is_finite); }
        if !boolean && !integer && !float { return DataType::Text; }
    }
    if !nonempty { DataType::Text }
    else if boolean { DataType::Boolean }
    else if integer { DataType::Integer }
    else if float { DataType::Float }
    else { DataType::Text }
}

pub fn from_csv_str(input: &str) -> Result<Batch> {
    // Count the CSV header separately from the table's data-row admission budget.
    let mut records = parse_records_with_limit(
        input.strip_prefix('\u{feff}').unwrap_or(input), MAX_CSV_ROWS + 1,
    )?;
    if records.is_empty() {
        return Err(Error::Parse("CSV input has no header".into()));
    }
    let header = std::mem::take(&mut records[0]);
    if header.is_empty() {
        return Err(Error::Schema("CSV input has no columns".into()));
    }
    if records.len() - 1 > MAX_CSV_ROWS {
        return Err(Error::Execution("CSV row count limit exceeded".into()));
    }
    for row in records.iter().skip(1) {
        if row.len() != header.len() {
            return Err(Error::Parse("CSV row width differs from header".into()));
        }
    }
    let schema = Schema::new(
        header
            .iter()
            .enumerate()
            .map(|(column, name)| Field::new(name, kind(&records[1..], column), true))
            .collect(),
    )?;
    let row_count = records.len() - 1;
    // Build column vectors directly. Previously the importer allocated a
    // Vec<Value> for every row, then Batch::from_rows transposed them again.
    let mut columns = (0..schema.len())
        .map(|_| Vec::with_capacity(row_count)).collect::<Vec<_>>();
    for record in records.into_iter().skip(1) {
        for (index, text) in record.into_iter().enumerate() {
            let value = if text.is_empty() {
                Value::Null
            } else {
                match schema.field(index)?.data_type {
                    DataType::Boolean => Value::Boolean(text.eq_ignore_ascii_case("true")),
                    DataType::Integer => text.parse::<i64>().map(Value::Integer)
                        .map_err(|_| Error::Parse("invalid integer in CSV".into()))?,
                    DataType::Float => text.parse::<f64>().map(Value::Float)
                        .map_err(|_| Error::Parse("invalid float in CSV".into()))?,
                    DataType::Text => Value::Text(text),
                }
            };
            columns[index].push(value);
        }
    }
    Batch::new_with_row_count(schema, columns, row_count)
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
        let bom = from_csv_str("\u{feff}id,name\n1,\u{feff}Jane\n").unwrap();
        assert_eq!(bom.schema().column("id").unwrap(), 0);
        assert_eq!(bom.row(0).unwrap(), vec![Value::Integer(1), Value::Text("\u{feff}Jane".into())]);
    }
}

 
/// Decode bounded CSV input using a declared schema and matching header order.
pub fn from_csv_str_typed(input: &str, schema: Schema) -> Result<Batch> {
    // Admission counts data rows; the header needs its own record slot.
    let mut records = parse_records_with_limit(
        input.strip_prefix('\u{feff}').unwrap_or(input), MAX_CSV_ROWS + 1,
    )?;
    if records.is_empty() {
        return Err(Error::Parse("CSV input has no header".into()));
    }
    let header = std::mem::take(&mut records[0]);
    if header.len() != schema.len() || header.iter()
        .zip(schema.fields()).any(|(name, field)| !name.eq_ignore_ascii_case(&field.name)) {
        return Err(Error::Schema("CSV header differs from declared schema".into()));
    }
    if records.len() - 1 > MAX_CSV_ROWS {
        return Err(Error::Execution("CSV row count limit exceeded".into()));
    }
    let row_count = records.len() - 1;
    let mut columns = (0..schema.len())
        .map(|_| Vec::with_capacity(row_count)).collect::<Vec<_>>();
    for record in records.into_iter().skip(1) {
        if record.len() != schema.len() {
            return Err(Error::Parse("CSV row width differs from header".into()));
        }
        for (index, text) in record.into_iter().enumerate() {
            let value = if text.is_empty() {
                Value::Null
            } else {
                match schema.field(index)?.data_type {
                    DataType::Text => Value::Text(text),
                    DataType::Integer => text.parse::<i64>().map(Value::Integer)
                        .map_err(|_| Error::Parse("invalid typed CSV integer".into()))?,
                    DataType::Float => text.parse::<f64>().ok().filter(|n| n.is_finite())
                        .map(Value::Float)
                        .ok_or_else(|| Error::Parse("invalid typed CSV float".into()))?,
                    DataType::Boolean if text.eq_ignore_ascii_case("true") => Value::Boolean(true),
                    DataType::Boolean if text.eq_ignore_ascii_case("false") => Value::Boolean(false),
                    DataType::Boolean => return Err(Error::Parse("invalid typed CSV boolean".into())),
                }
            };
            columns[index].push(value);
        }
    }
    Batch::new_with_row_count(schema, columns, row_count)
}

#[cfg(test)]
mod typed_csv_feature_tests {
    use super::*;

    #[test]
    fn strict_csv_preserves_type_and_required_column() {
        // The record scanner accounts for the header separately from data rows.
        assert!(parse_records_with_limit("id\n1\n2\n", 3).is_ok());
        assert!(parse_records_with_limit("id\n1\n2\n", 2).is_err());
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("label", DataType::Text, true),
        ]).unwrap();
        let input = "\u{feff}ID,label\n1,001\n2,\n";
        let parsed = from_csv_str_typed(input, schema.clone()).unwrap();
        assert_eq!(parsed.rows().unwrap(), vec![
            vec![Value::Integer(1), Value::Text("001".into())],
            vec![Value::Integer(2), Value::Null]
        ]);
        assert!(from_csv_str_typed("id,label\n,missing\n", schema.clone()).is_err());
        assert!(from_csv_str_typed("id,label\nx,z\n", schema.clone()).is_err());
        assert!(from_csv_str_typed("label,id\nx,1\n", schema).is_err());
    }
}


#[cfg(test)]
mod streaming_type_inference_tests {
    use super::*;

    #[test]
    fn nullable_inference_uses_all_nonempty_values() {
        let data = from_csv_str("id,flag,measure,note\n1,true,1.2,hello\n2,false,2.5,world\n3,,,\n").unwrap();
        assert_eq!(data.schema().field(0).unwrap().data_type, DataType::Integer);
        assert_eq!(data.schema().field(1).unwrap().data_type, DataType::Boolean);
        assert_eq!(data.schema().field(2).unwrap().data_type, DataType::Float);
        assert_eq!(data.schema().field(3).unwrap().data_type, DataType::Text);
    }
}

#[cfg(test)]
mod csv_unquoted_fast_path_tests {
    use super::*;

    #[test]
    fn simple_record_handles_empty_fields_utf8_and_line_ending() {
        assert_eq!(parse_records("one,,東京,\\n").unwrap(),
            vec![vec!["one".to_string(), "".into(), "東京".into(), "\\n".into()]]);
        assert_eq!(parse_records("a,b,\n").unwrap(),
            vec![vec!["a".to_string(), "b".into(), "".into()]]);
        assert!(parse_records("\n").unwrap().is_empty());
        assert_eq!(parse_records("a,\nb,\n").unwrap().len(), 2);
        assert_eq!(parse_records("\"a,b\",c\n").unwrap(),
            vec![vec!["a,b".to_string(), "c".into()]]);
    }
}

#[cfg(test)]
mod in_memory_csv_byte_limit_tests {
    use super::*;
    #[test]
    fn direct_csv_parse_rejects_oversized_input_before_allocating_rows() {
        let oversized = "a".repeat(MAX_CSV_BYTES as usize + 1);
        assert!(parse_records(&oversized).is_err());
    }
}

#[cfg(test)]
mod csv_inference_single_pass_perf_tests {
    use super::*;
    #[test]
    fn retains_type_precedence_for_nulls_numbers_and_nonfinite_values() {
        assert_eq!(from_csv_str("v\n\n").unwrap().schema().field(0).unwrap().data_type, DataType::Text);
        assert_eq!(from_csv_str("v\ntrue\nfalse\n").unwrap().schema().field(0).unwrap().data_type, DataType::Boolean);
        assert_eq!(from_csv_str("v\n1\n-8\n").unwrap().schema().field(0).unwrap().data_type, DataType::Integer);
        assert_eq!(from_csv_str("v\n1\n2.5\n").unwrap().schema().field(0).unwrap().data_type, DataType::Float);
        assert_eq!(from_csv_str("v\n1\nNaN\n").unwrap().schema().field(0).unwrap().data_type, DataType::Text);
    }
}

#[cfg(test)]
mod csv_data_row_limit_accounting_tests {
    use super::*;

    #[test]
    fn raw_record_limit_and_header_inclusive_table_limit_differ() {
        let input = "id\n1\n2\n";
        assert!(parse_records_with_limit(input, 2).is_err());
        assert_eq!(parse_records_with_limit(input, 3).unwrap().len(), 3);
        assert_eq!(from_csv_str(input).unwrap().num_rows(), 2);
    }
}

#[cfg(test)]
mod direct_columnar_csv_decode_regressions {
    use super::*;

    #[test]
    fn untyped_inference_preserves_nullable_mixed_columns_and_unicode() {
        let csv = "id,active,label,score\r\n1,true,\"é,🙂\",1.5\r\n2,false,,2.5\r\n3,,東京,\r\n";
        let batch = from_csv_str(csv).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.schema().field(0).unwrap().data_type, DataType::Integer);
        assert_eq!(batch.schema().field(1).unwrap().data_type, DataType::Boolean);
        assert_eq!(batch.schema().field(2).unwrap().data_type, DataType::Text);
        assert_eq!(batch.schema().field(3).unwrap().data_type, DataType::Float);
        assert_eq!(batch.column(2).unwrap(), &[
            Value::Text("é,🙂".into()), Value::Null, Value::Text("東京".into())
        ]);
        assert_eq!(from_csv_str("id,label\n").unwrap().num_rows(), 0);
    }

    #[test]
    fn declared_types_still_validate_required_cells_and_row_width() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("tag", DataType::Text, true),
        ]).unwrap();
        let input = from_csv_str_typed("id,tag\n1,é🙂\n2,\n", schema.clone()).unwrap();
        assert_eq!(input.rows().unwrap(), vec![
            vec![Value::Integer(1), Value::Text("é🙂".into())],
            vec![Value::Integer(2), Value::Null],
        ]);
        assert!(from_csv_str_typed("id,tag\n,missing\n", schema.clone()).is_err());
        assert!(from_csv_str_typed("id,tag\n1\n", schema.clone()).is_err());
        assert!(from_csv_str_typed("id,tag\n1,extra,third\n", schema).is_err());
    }
}

#[cfg(test)]
mod fast_multiline_csv_regressions {
    use super::*;

    #[test]
    fn matches_general_scanner_for_unquoted_multiline_data() {
        for input in [
            "", "\n", "a,b\n", "a,b\n1,2\n3,4\n",
            ",,\n\n,\n", "café,東京\n🙂,\n",
            "a,b\n\n\n1,2", "a,b,\n,,\n",
        ] {
            let slow = input.replace('\n', "\r\n");
            assert_eq!(parse_records(input).unwrap(), parse_records(&slow).unwrap(),
                "unquoted LF and CRLF scanner differ for {input:?}");
        }
    }

    #[test]
    fn enforces_record_budget_without_quotes_or_carriage_returns() {
        assert!(parse_records_with_limit("a,b\n1,2\n", 2).is_ok());
        assert!(parse_records_with_limit("a,b\n1,2\n3,4\n", 2).is_err());
        assert!(parse_records_with_limit("a,b", 0).is_err());
        assert!(parse_records_with_limit("\n\n", 0).unwrap().is_empty());
        assert!(parse_records_with_limit("a,b\r\n1,2\r\n3,4\r\n", 2).is_err());
    }
}
