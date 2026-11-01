use std::{
    env, fs,
    io::{self, Read, Write},
    path::PathBuf,
    process::ExitCode,
};
use rustspark_native::{
    batch::Batch, scalar::Value,
    schema::{DataType, Field, Schema},
    sql, Error, Result,
    scale::{cancel::QueryControl, catalog_manifest, file_catalog::FileCatalog,
        stream_execute::{self, StreamOptions}},
};

const MAX_SQL_FILE_BYTES: u64 = 1024 * 1024;

fn read_sql_file(path: &std::path::Path) -> Result<String> {
    let file = fs::File::open(path)?;
    if file.metadata()?.len() > MAX_SQL_FILE_BYTES {
        return Err(Error::Execution("SQL file exceeds 1 MiB limit".into()));
    }
    let mut sql = String::new();
    file.take(MAX_SQL_FILE_BYTES + 1).read_to_string(&mut sql)?;
    if sql.len() as u64 > MAX_SQL_FILE_BYTES {
        return Err(Error::Execution("SQL file exceeds 1 MiB limit".into()));
    }
    Ok(sql)
}

struct Args {
    csv: Option<PathBuf>, parquet: Option<PathBuf>, catalog_manifest: Option<PathBuf>,
    table: String, schema: Option<Schema>, sql: String,
    batch_rows: usize, record_bytes: usize, scratch: PathBuf,
}

fn parse_schema(text: &str) -> Result<Schema> {
    let mut fields = Vec::new();
    for column in text.split(',') {
        let (name, kind) = column.split_once(':')
            .ok_or_else(|| Error::Parse("schema uses NAME:TYPE entries".into()))?;
        let (kind, nullable) = match kind.strip_suffix('!') {
            Some(kind) => (kind, false),
            None => (kind, true),
        };
        let kind = match kind.to_ascii_lowercase().as_str() {
            "int" | "integer" => DataType::Integer,
            "float" | "double" => DataType::Float,
            "bool" | "boolean" => DataType::Boolean,
            "text" | "string" => DataType::Text,
            _ => return Err(Error::Parse(format!("unsupported type: {kind}"))),
        };
        fields.push(Field::new(name, kind, nullable));
    }
    if fields.is_empty() { return Err(Error::Schema("schema must not be empty".into())); }
    Schema::new(fields)
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Option<Args>> {
    let mut csv = None;
    let mut parquet = None;
    let mut catalog_manifest = None;
    let mut table = "data".to_string();
    let mut schema = None;
    let mut sql = None;
    let mut sql_file = None;
    let mut batch_rows = 4096;
    let mut record_bytes = 8 * 1024 * 1024;
    let mut scratch = env::temp_dir();
    let mut args = args.into_iter();
    while let Some(flag) = args.next() {
        if flag == "--help" || flag == "-h" { return Ok(None); }
        let value = args.next()
            .ok_or_else(|| Error::Parse(format!("missing value for {flag}")))?;
        match flag.as_str() {
            "--csv" => {
                if csv.replace(PathBuf::from(value)).is_some() {
                    return Err(Error::Parse("duplicate --csv input".into()));
                }
            },
            "--parquet" => {
                if parquet.replace(PathBuf::from(value)).is_some() {
                    return Err(Error::Parse("duplicate --parquet input".into()));
                }
            },
            "--table" => table = value,
            "--catalog-manifest" => {
                if catalog_manifest.replace(PathBuf::from(value)).is_some() {
                    return Err(Error::Parse("duplicate --catalog-manifest".into()));
                }
            },
            "--schema" => schema = Some(parse_schema(&value)?),
            "--sql" => sql = Some(value),
            "--sql-file" => sql_file = Some(PathBuf::from(value)),
            "--batch-rows" => batch_rows = value.parse()
                .map_err(|_| Error::Parse("invalid --batch-rows".into()))?,
            "--max-record-bytes" => record_bytes = value.parse()
                .map_err(|_| Error::Parse("invalid --max-record-bytes".into()))?,
            "--scratch-dir" => scratch = PathBuf::from(value),
            _ => return Err(Error::Parse(format!("unknown option: {flag}"))),
        }
    }
    if batch_rows == 0 || batch_rows > 65_536 ||
        record_bytes == 0 || record_bytes > 64 * 1024 * 1024 {
        return Err(Error::Execution("streaming input limit out of bounds".into()));
    }
    let sql = match (sql, sql_file) {
        (Some(query), None) => query,
        (None, Some(path)) => read_sql_file(&path)?,
        _ => return Err(Error::Parse("choose --sql or --sql-file".into())),
    };
    if sql.trim().is_empty() { return Err(Error::Parse("SQL cannot be empty".into())); }
    let inputs = usize::from(csv.is_some()) + usize::from(parquet.is_some())
        + usize::from(catalog_manifest.is_some());
    if inputs != 1 {
        return Err(Error::Parse("choose --csv, --parquet, or --catalog-manifest".into()));
    }
    if csv.is_some() && schema.is_none() {
        return Err(Error::Parse("--schema required for CSV".into()));
    }
    if (parquet.is_some() || catalog_manifest.is_some()) && schema.is_some() {
        return Err(Error::Parse("Parquet schema comes from file metadata".into()));
    }
    Ok(Some(Args {
        csv, parquet, catalog_manifest, table, schema, sql, batch_rows, record_bytes, scratch,
    }))
}


fn write_csv_text(output: &mut impl Write, text: &str) -> Result<()> {
    let bytes = text.as_bytes();
    if !bytes.iter().any(|byte| matches!(byte, b',' | b'"' | b'\r' | b'\n')) {
        output.write_all(bytes)?;
        return Ok(());
    }
    output.write_all(b"\"")?;
    let mut start = 0;
    for (index, &byte) in bytes.iter().enumerate() {
        if byte == b'"' {
            output.write_all(&bytes[start..index])?;
            output.write_all(b"\"\"")?;
            start = index + 1;
        }
    }
    output.write_all(&bytes[start..])?;
    output.write_all(b"\"")?;
    Ok(())
}

fn write_csv_value(output: &mut impl Write, value: &Value) -> Result<()> {
    match value {
        Value::Null => Ok(()),
        Value::Boolean(value) => { write!(output, "{value}")?; Ok(()) },
        Value::Integer(value) => { write!(output, "{value}")?; Ok(()) },
        Value::Float(value) => { write!(output, "{value}")?; Ok(()) },
        Value::Text(value) => write_csv_text(output, value),
    }
}

fn write_csv(
    schema: &Schema, input: impl IntoIterator<Item = Result<Batch>>,
    output: &mut impl Write,
) -> Result<()> {
    for (index, field) in schema.fields().iter().enumerate() {
        if index != 0 { output.write_all(b",")?; }
        write_csv_text(output, &field.name)?;
    }
    output.write_all(b"\n")?;
    for batch in input {
        let batch = batch?;
        if batch.schema() != schema {
            return Err(Error::Schema("streaming output schema changed".into()));
        }
        // Access column slices once. Converting to rows clones every STRING
        // before formatting, even though CSV only needs to borrow the text.
        let columns = (0..schema.len())
            .map(|index| batch.column(index)).collect::<Result<Vec<_>>>()?;
        for row in 0..batch.num_rows() {
            for (index, column) in columns.iter().enumerate() {
                if index != 0 { output.write_all(b",")?; }
                write_csv_value(output, &column[row])?;
            }
            output.write_all(b"\n")?;
        }
    }
    output.flush()?;
    Ok(())
}

fn run() -> Result<()> {
    let Some(args) = parse_args(env::args().skip(1))? else {
        println!("Usage: rustspark-stream (--csv FILE --schema 'id:int!,city:text' | --parquet FILE) --sql QUERY");
        return Ok(());
    };
    let mut catalog = if let Some(path) = &args.catalog_manifest {
        catalog_manifest::load(path)?
    } else {
        FileCatalog::new()
    };
    if let Some(path) = &args.csv {
        catalog.register_csv(&args.table, path,
            args.schema.ok_or_else(|| Error::Parse("CSV schema required".into()))?, args.record_bytes)?;
    } else if let Some(path) = &args.parquet {
        catalog.register_parquet(&args.table, path)?;
    }
    let plan = sql::parse_sql(&args.sql)?;
    let options = StreamOptions {
        batch_rows: args.batch_rows, scratch_directory: args.scratch,
        ..StreamOptions::default()
    };
    let schema = stream_execute::output_schema(&plan, &catalog, &options)?;
    let input = stream_execute::execute(&plan, &catalog, &options, QueryControl::default())?;
    let stdout = io::stdout();
    let mut output = io::BufWriter::new(stdout.lock());
    write_csv(&schema, input, &mut output)
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => { eprintln!("{error}"); ExitCode::FAILURE }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn schema_and_csv_output_preserve_nulls_quotes_and_required_columns() {
        let schema = parse_schema("id:int!,city:text").unwrap();
        assert!(!schema.field(0).unwrap().nullable);
        let data = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1), Value::Text("A,\"B\"".into())],
            vec![Value::Integer(2), Value::Null],
        ]).unwrap();
        let mut output = Vec::new();
        write_csv(&schema, [Ok(data)], &mut output).unwrap();
        assert_eq!(String::from_utf8(output).unwrap(), "id,city\n1,\"A,\"\"B\"\"\"\n2,\n");
    }


    #[test]
    fn streaming_sql_file_admission_bounds_memory() {
        let path = std::env::temp_dir().join(format!(
            "rustspark-stream-sql-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
                .unwrap().as_nanos()
        ));
        fs::write(&path, vec![b'x'; MAX_SQL_FILE_BYTES as usize + 1]).unwrap();
        assert!(read_sql_file(&path).is_err());
        fs::write(&path, "SELECT * FROM data").unwrap();
        assert_eq!(read_sql_file(&path).unwrap(), "SELECT * FROM data");
        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn catalog_manifest_is_a_distinct_streaming_source() {
        let valid = ["--catalog-manifest", "catalog.json",
            "--sql", "SELECT * FROM events"];
        assert!(parse_args(valid.into_iter().map(String::from))
            .unwrap().unwrap().catalog_manifest.is_some());
        let invalid = ["--catalog-manifest", "catalog.json", "--csv", "events.csv",
            "--schema", "id:int", "--sql", "SELECT * FROM events"];
        assert!(parse_args(invalid.into_iter().map(String::from)).is_err());
    }

    #[test]
    fn reject_bad_streaming_flags() {
        let args = ["--csv","x.csv","--schema","n:int","--sql","SELECT * FROM data",
            "--sql-file","q.sql"];
        assert!(parse_args(args.into_iter().map(String::from)).is_err());
        assert!(parse_schema("n:missing").is_err());
        let parquet = ["--parquet", "data.parquet", "--sql", "SELECT * FROM data"];
        assert!(parse_args(parquet.into_iter().map(String::from)).unwrap().unwrap().parquet.is_some());
        let invalid = ["--parquet", "data.parquet", "--schema", "n:int", "--sql", "SELECT * FROM data"];
        assert!(parse_args(invalid.into_iter().map(String::from)).is_err());
    }
}

#[cfg(test)]
mod borrowed_csv_output_tests {
    use super::*;

    #[test]
    fn wide_unicode_and_quotes_stream_with_exact_csv_escaping() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("label", DataType::Text, true),
        ]).unwrap();
        let big = "🙂".repeat(8_000);
        let input = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1), Value::Text(format!("{big},\"quoted\"\r\nnext"))],
            vec![Value::Integer(2), Value::Null],
            vec![Value::Integer(3), Value::Text("plain".into())],
        ]).unwrap();
        let mut output = Vec::new();
        write_csv(&schema, [Ok(input)], &mut output).unwrap();
        let output = String::from_utf8(output).unwrap();
        assert_eq!(output, format!(
            "id,label\n1,\"{big},\"\"quoted\"\"\r\nnext\"\n2,\n3,plain\n"
        ));
    }

    #[test]
    fn zero_column_batches_preserve_row_count_without_row_vectors() {
        let schema = Schema::new(vec![]).unwrap();
        let input = Batch::new_with_row_count(schema.clone(), vec![], 4).unwrap();
        let mut output = Vec::new();
        write_csv(&schema, [Ok(input)], &mut output).unwrap();
        assert_eq!(output, b"\n\n\n\n\n");
    }
}
