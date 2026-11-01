use std::{
    env, fs,
    io::{self, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::ExitCode,
};

use rustspark_native::{
    Error, Result,
    catalog::Catalog,
    csv, csv_stream::CsvStream,
    parquet_scan,
    executor,
    batch::Batch,
    expr::Expr,
    pipeline::Pipeline,
    plan::Plan,
    project::{self, Selection},
    scalar::Value,
    schema::{DataType, Field, Schema},
    sql,
};

struct Args {
    csv: Option<PathBuf>,
    parquet: Option<PathBuf>,
    sources: Vec<(String, PathBuf)>,
    table: String,
    sql: String,
    stream: bool,
    csv_schema: Option<String>,
    jsonl: bool,
    json_array: bool,
    max_rows: Option<usize>,
}

const MAX_SQL_FILE_BYTES: u64 = 1024 * 1024;

fn read_sql_file(path: &Path) -> Result<String> {
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

fn usage() -> &'static str {
    "Usage: rustspark-native (--csv FILE | --parquet FILE | --source TABLE=FILE [--source TABLE=FILE ...]) [--table NAME] (--sql QUERY | --sql-file FILE) [--stream] [--csv-schema NAME:TYPE,...] [--jsonl]\n     Native --stream supports SELECT, WHERE, and LIMIT over bounded CSV or Parquet batches.\n     Parquet execution uses bounded in-memory batches (up to 1,000,000 rows).\n     Supported native scalar types: text, bool, int64, float64."
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Option<Args>> {
    let mut csv = None;
    let mut parquet = None;
    let mut sources = Vec::new();
    let mut table = "data".to_string();
    let mut sql = None;
    let mut sql_file = None;
    let mut stream = false;
    let mut csv_schema = None;
    let mut jsonl = false;
    let mut json_array = false;
    let mut max_rows = None;
    let mut stdout_format_seen = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        if arg == "--help" || arg == "-h" {
            return Ok(None);
        }
        if arg == "--jsonl" {
            if jsonl || stdout_format_seen {
                return Err(Error::Parse("duplicate output format".into()));
            }
            jsonl = true;
            continue;
        }
        if arg == "--stream" {
            if stream {
                return Err(Error::Parse("duplicate --stream".into()));
            }
            stream = true;
            continue;
        }
        let value = iter.next().ok_or_else(|| Error::Parse(format!("missing value for {arg}")))?;
        match arg.as_str() {
            "--csv" => {
                if csv.replace(PathBuf::from(value)).is_some() {
                    return Err(Error::Parse("duplicate --csv".into()));
                }
            }
            "--parquet" => {
                if parquet.replace(PathBuf::from(value)).is_some() {
                    return Err(Error::Parse("duplicate --parquet".into()));
                }
            }
            "--source" | "--csv-table" | "--parquet-table" => {
                let (table_name, file) = value.split_once('=')
                    .ok_or_else(|| Error::Parse(format!("{arg} expects TABLE=FILE")))?;
                if table_name.is_empty() || file.is_empty() {
                    return Err(Error::Parse(format!("{arg} requires a table and file path")));
                }
                let path = PathBuf::from(file);
                let extension = path.extension().and_then(|ext| ext.to_str())
                    .map(|ext| ext.to_ascii_lowercase());
                let supported = match arg.as_str() {
                    "--csv-table" => extension.as_deref() == Some("csv"),
                    "--parquet-table" => extension.as_deref() == Some("parquet"),
                    _ => matches!(extension.as_deref(), Some("csv" | "parquet")),
                };
                if !supported {
                    return Err(Error::Unsupported(format!(
                        "{arg} requires a .csv or .parquet file of the requested type"
                    )));
                }
                sources.push((table_name.to_string(), path));
            },
            "--table" => table = value,
            "--max-rows" => {
                let rows = value.parse::<usize>()
                    .map_err(|_| Error::Parse("--max-rows requires a nonnegative integer".into()))?;
                if max_rows.replace(rows).is_some() {
                    return Err(Error::Parse("duplicate --max-rows".into()));
                }
            },
            "--stdout-format" => {
                if stdout_format_seen || jsonl {
                    return Err(Error::Parse("duplicate output format".into()));
                }
                stdout_format_seen = true;
                match value.as_str() {
                    "csv" => {},
                    "json" | "jsonl" => jsonl = true,
                    "json-array" => json_array = true,
                    _ => return Err(Error::Parse("unsupported output format".into())),
                }
            },
            "--csv-schema" => {
                if csv_schema.replace(value).is_some() {
                    return Err(Error::Parse("duplicate --csv-schema".into()));
                }
            },
            "--sql" => {
                if sql.replace(value).is_some() {
                    return Err(Error::Parse("duplicate --sql".into()));
                }
            }
            "--sql-file" => {
                if sql_file.replace(PathBuf::from(value)).is_some() {
                    return Err(Error::Parse("duplicate --sql-file".into()));
                }
            }
            _ => return Err(Error::Parse(format!("unknown option: {arg}"))),
        }
    }
    if csv_schema.is_some() && !stream {
        return Err(Error::Parse("--csv-schema requires --stream".into()));
    }
    if csv_schema.is_some() && parquet.is_some() {
        return Err(Error::Parse("--csv-schema cannot be used with Parquet input".into()));
    }
    if sql.is_some() && sql_file.is_some() {
        return Err(Error::Parse("use either --sql or --sql-file".into()));
    }
    if sources.is_empty() {
        if csv.is_some() == parquet.is_some() {
            return Err(Error::Parse("choose --csv, --parquet, or --source".into()));
        }
    } else if csv.is_some() || parquet.is_some() {
        return Err(Error::Parse("--source cannot be mixed with --csv or --parquet".into()));
    }
    if stream && !sources.is_empty() {
        return Err(Error::Unsupported("--stream does not yet support multiple file sources".into()));
    }
    let sql = match (sql, sql_file) {
        (Some(query), None) => query,
        (None, Some(path)) => read_sql_file(&path)?,
        _ => return Err(Error::Parse("--sql or --sql-file is required".into())),
    };
    if sql.trim().is_empty() {
        return Err(Error::Parse("SQL query cannot be empty".into()));
    }
    Ok(Some(Args { csv, parquet, sources, table, sql, stream, csv_schema, jsonl, json_array, max_rows }))
}

fn escape_field(input: &str) -> String {
    if input.chars().any(|character| matches!(character, ',' | '"' | '\r' | '\n')) {
        format!("\"{}\"", input.replace('"', "\"\""))
    } else {
        input.to_string()
    }
}

fn csv_value(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Boolean(value) => value.to_string(),
        Value::Integer(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::Text(value) => escape_field(value),
    }
}

fn print_header(schema: &Schema, output: &mut impl Write) -> Result<()> {
    let header = schema
        .fields()
        .iter()
        .map(|field| escape_field(&field.name))
        .collect::<Vec<_>>()
        .join(",");
    writeln!(output, "{header}")?;
    Ok(())
}

fn print_csv_rows(batch: &Batch, output: &mut impl Write) -> Result<()> {
    for row_idx in 0..batch.num_rows() {
        let row = batch.row(row_idx)?;
        let line = row.iter().map(csv_value).collect::<Vec<_>>().join(",");
        writeln!(output, "{line}")?;
    }
    Ok(())
}

fn print_csv(batch: &Batch, output: &mut impl Write) -> Result<()> {
    print_header(batch.schema(), output)?;
    print_csv_rows(batch, output)
}

fn print_jsonl_rows(batch: &Batch, output: &mut impl Write) -> Result<()> {
    for row_index in 0..batch.num_rows() {
        let row = batch.row(row_index)?;
        let mut record = serde_json::Map::new();
        for (field, value) in batch.schema().fields().iter().zip(&row) {
            let json = match value {
                Value::Null => serde_json::Value::Null,
                Value::Boolean(value) => serde_json::Value::Bool(*value),
                Value::Integer(value) => serde_json::Value::Number((*value).into()),
                Value::Float(value) => serde_json::Number::from_f64(*value)
                    .map(serde_json::Value::Number)
                    .ok_or_else(|| Error::Execution(
                        "nonfinite SQL float cannot be encoded as JSON".into()
                    ))?,
                Value::Text(value) => serde_json::Value::String(value.clone()),
            };
            record.insert(field.name.clone(), json);
        }
        writeln!(output, "{}", serde_json::Value::Object(record))?;
    }
    Ok(())
}

struct JsonArrayPrinter<'a, W: Write> {
    output: &'a mut W,
    first: bool,
}

impl<'a, W: Write> JsonArrayPrinter<'a, W> {
    fn new(output: &'a mut W) -> Result<Self> {
        output.write_all(b"[")?;
        Ok(Self { output, first: true })
    }

    fn print_batch(&mut self, batch: &Batch) -> Result<()> {
        for row_index in 0..batch.num_rows() {
            let row = batch.row(row_index)?;
            let mut record = serde_json::Map::new();
            for (field, value) in batch.schema().fields().iter().zip(&row) {
                let json = match value {
                    Value::Null => serde_json::Value::Null,
                    Value::Boolean(value) => serde_json::Value::Bool(*value),
                    Value::Integer(value) => serde_json::Value::Number((*value).into()),
                    Value::Float(value) => serde_json::Number::from_f64(*value)
                        .map(serde_json::Value::Number)
                        .ok_or_else(|| Error::Execution(
                            "nonfinite SQL float cannot be encoded as JSON".into()
                        ))?,
                    Value::Text(value) => serde_json::Value::String(value.clone()),
                };
                record.insert(field.name.clone(), json);
            }
            if !self.first { self.output.write_all(b",")?; }
            write!(self.output, "{}", serde_json::Value::Object(record))?;
            self.first = false;
        }
        Ok(())
    }

    fn finish(self) -> Result<()> {
        writeln!(self.output, "]")?;
        Ok(())
    }
}

fn parse_stream_schema(spec: &str) -> Result<Schema> {
    let fields = spec.split(',').map(|part| {
        let (name, kind) = part.split_once(':')
            .ok_or_else(|| Error::Parse("CSV schema fields must use NAME:TYPE".into()))?;
        let kind = match kind.trim().to_ascii_lowercase().as_str() {
            "text" => DataType::Text,
            "bool" | "boolean" => DataType::Boolean,
            "int64" | "integer" => DataType::Integer,
            "float64" | "float" => DataType::Float,
            _ => return Err(Error::Parse(format!("unsupported CSV schema type: {kind}"))),
        };
        Ok(Field::new(name.trim(), kind, true))
    }).collect::<Result<Vec<_>>>()?;
    Schema::new(fields)
}

type StreamPlan = (String, Option<Expr>, Option<Vec<Selection>>, Option<(usize, Option<usize>)>);

fn stream_plan(plan: &Plan) -> Result<StreamPlan> {
    let (limit, plan) = match plan {
        Plan::Limit { input, offset, count } => (Some((*offset, *count)), input.as_ref()),
        _ => (None, plan),
    };
    let (projection, plan) = match plan {
        Plan::Project { input, selections } => (Some(selections.clone()), input.as_ref()),
        _ => (None, plan),
    };
    let (predicate, plan) = match plan {
        Plan::Filter { input, predicate } => (Some(predicate.clone()), input.as_ref()),
        _ => (None, plan),
    };
    match plan {
        Plan::Scan { table } => Ok((table.clone(), predicate, projection, limit)),
        _ => Err(Error::Unsupported(
            "--stream supports only scan, WHERE, SELECT, and LIMIT".into(),
        )),
    }
}

fn run_stream(args: &Args) -> Result<()> {
    let plan = sql::parse_sql(&args.sql)?;
    let (table, predicate, projection, limit) = stream_plan(&plan)?;
    if !table.eq_ignore_ascii_case(&args.table) {
        return Err(Error::Schema(format!("unknown table: {table}")));
    }

    let (source_schema, batches): (Schema, Box<dyn Iterator<Item = Result<Batch>>>) =
        if let Some(path) = &args.parquet {
            (parquet_scan::file_schema(path)?, Box::new(parquet_scan::scan(path, 4096)?))
        } else {
            let path = args.csv.as_ref()
                .ok_or_else(|| Error::Unsupported("streaming requires CSV or Parquet input".into()))?;
            let input = BufReader::new(fs::File::open(path)?);
            let csv = if let Some(spec) = &args.csv_schema {
                CsvStream::new_typed(input, 4_096, 1024 * 1024, parse_stream_schema(spec)?)?
            } else {
                CsvStream::new(input, 4_096, 1024 * 1024)?
            };
            (csv.schema().clone(), Box::new(csv))
        };
    let output_schema = if let Some(ref selections) = projection {
        project::project(&Batch::empty(source_schema.clone()), selections)?
            .schema().clone()
    } else {
        source_schema
    };
    let mut pipeline = Pipeline::new(batches);
    if let Some(predicate) = predicate {
        pipeline = pipeline.with_filter(predicate);
    }
    if let Some(selections) = projection {
        pipeline = pipeline.with_projection(selections);
    }
    if let Some((offset, count)) = limit {
        pipeline = pipeline.with_limit(offset, count);
    }
    if let Some(rows) = args.max_rows {
        pipeline = pipeline.with_limit(0, Some(rows));
    }

    let stdout = io::stdout();
    let mut output = io::BufWriter::new(stdout.lock());
    if args.json_array {
        let mut printer = JsonArrayPrinter::new(&mut output)?;
        for batch in pipeline { printer.print_batch(&batch?)?; }
        printer.finish()?;
    } else {
        if !args.jsonl { print_header(&output_schema, &mut output)?; }
        for batch in pipeline {
            let batch = batch?;
            if args.jsonl { print_jsonl_rows(&batch, &mut output)?; }
            else { print_csv_rows(&batch, &mut output)?; }
        }
    }
    output.flush()?;
    Ok(())
}


const MAX_PARQUET_ROWS: usize = 1_000_000;
const MAX_PARQUET_RETAINED_BYTES: usize = 256 * 1024 * 1024;

fn parquet_batch_retained_bytes(batch: &Batch) -> Result<usize> {
    let fixed = batch.schema().len().checked_mul(std::mem::size_of::<Value>())
        .and_then(|bytes| bytes.checked_add(64))
        .ok_or_else(|| Error::Execution("Parquet retained byte estimate overflow".into()))?;
    let mut bytes = batch.num_rows().checked_mul(fixed)
        .ok_or_else(|| Error::Execution("Parquet retained byte estimate overflow".into()))?;
    for column in 0..batch.schema().len() {
        for value in batch.column(column)? {
            if let Value::Text(text) = value {
                bytes = bytes.checked_add(text.capacity())
                    .ok_or_else(|| Error::Execution("Parquet retained byte estimate overflow".into()))?;
            }
        }
    }
    Ok(bytes)
}

// Reassemble decoded column buffers by ownership instead of first cloning
// them into row vectors and cloning the TEXT cells again into new columns.
fn collect_parquet_batches<I>(
    schema: Schema, input: I, max_rows: usize, max_bytes: usize,
) -> Result<Batch>
where I: IntoIterator<Item = Result<Batch>>
{
    let mut output = Batch::empty(schema.clone());
    let mut total_rows = 0usize;
    let mut retained_bytes = 0usize;
    for incoming in input {
        let batch = incoming?;
        if batch.schema() != &schema {
            return Err(Error::Schema("Parquet batch schema changed".into()));
        }
        let rows = total_rows.checked_add(batch.num_rows())
            .filter(|&rows| rows <= max_rows)
            .ok_or_else(|| Error::Execution("Parquet input exceeds native row limit".into()))?;
        let bytes = retained_bytes.checked_add(parquet_batch_retained_bytes(&batch)?)
            .filter(|&bytes| bytes <= max_bytes)
            .ok_or_else(|| Error::Execution("Parquet input exceeds native byte limit".into()))?;
        output.append_owned(batch)?;
        total_rows = rows;
        retained_bytes = bytes;
    }
    Ok(output)
}

fn read_parquet_bounded(path: &Path) -> Result<Batch> {
    let schema = parquet_scan::file_schema(path)?;
    let scan = parquet_scan::scan(path, 4_096)?;
    collect_parquet_batches(schema, scan, MAX_PARQUET_ROWS, MAX_PARQUET_RETAINED_BYTES)
}

fn run() -> Result<()> {
    let Some(args) = parse_args(env::args().skip(1))? else {
        println!("{}", usage());
        return Ok(());
    };
    if args.stream {
        return run_stream(&args);
    }
    let mut catalog = Catalog::new();
    if args.sources.is_empty() {
        let data = if let Some(path) = &args.csv {
            csv::read_csv(path)?
        } else if let Some(path) = &args.parquet {
            read_parquet_bounded(path)?
        } else {
            return Err(Error::Parse("missing input file".into()));
        };
        catalog.register(&args.table, data)?;
    } else {
        for (table, path) in &args.sources {
            let data = match path.extension().and_then(|ext| ext.to_str())
                .map(|ext| ext.to_ascii_lowercase()).as_deref() {
                Some("csv") => csv::read_csv(path)?,
                Some("parquet") => read_parquet_bounded(path)?,
                _ => return Err(Error::Unsupported("unsupported native table format".into())),
            };
            catalog.register(table, data)?;
        }
    }
    let plan = sql::parse_sql(&args.sql)?;
    let batch = executor::execute(&plan, &catalog)?;
    let batch = if let Some(rows) = args.max_rows {
        batch.slice_rows(0, rows.min(batch.num_rows()))?
    } else { batch };
    let stdout = io::stdout();
    let mut output = io::BufWriter::new(stdout.lock());
    if args.json_array {
        let mut printer = JsonArrayPrinter::new(&mut output)?;
        printer.print_batch(&batch)?;
        printer.finish()?;
    } else if args.jsonl { print_jsonl_rows(&batch, &mut output)?; }
    else { print_csv(&batch, &mut output)?; }
    output.flush()?;
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reject_invalid_cli_combinations() {
        let args = ["--csv", "data.csv", "--sql", "SELECT * FROM data", "--sql-file", "q.sql"];
        assert!(parse_args(args.into_iter().map(String::from)).is_err());
        let args = ["--csv", "data.csv", "--sql", "SELECT * FROM data"];
        assert!(parse_args(args.into_iter().map(String::from)).unwrap().is_some());
        let args = ["--parquet", "data.parquet", "--sql", "SELECT * FROM data"];
        assert!(parse_args(args.into_iter().map(String::from)).unwrap().is_some());
        let args = ["--parquet", "data.parquet", "--csv", "data.csv", "--sql", "SELECT * FROM data"];
        assert!(parse_args(args.into_iter().map(String::from)).is_err());
        let args = ["--parquet", "data.parquet", "--sql", "SELECT * FROM data", "--stream"];
        assert!(parse_args(args.into_iter().map(String::from)).unwrap().unwrap().stream);
        let bad = ["--parquet", "data.parquet", "--stream", "--csv-schema", "x:int64", "--sql", "SELECT * FROM data"];
        assert!(parse_args(bad.into_iter().map(String::from)).is_err());
    }

    #[test]
    fn multiple_native_sources_can_be_registered_without_datafusion() {
        let args = parse_args([
            "--source", "left=orders.csv",
            "--source", "right=payments.parquet",
            "--sql", "SELECT * FROM left",
        ].into_iter().map(String::from)).unwrap().unwrap();
        assert_eq!(args.sources.len(), 2);
        assert_eq!(args.sources[0].0, "left");
        assert_eq!(args.sources[1].1, PathBuf::from("payments.parquet"));
        for options in [
            vec!["--source", "missing", "--sql", "SELECT * FROM t"],
            vec!["--source", "t=table.json", "--sql", "SELECT * FROM t"],
            vec!["--source", "t=a.csv", "--csv", "a.csv", "--sql", "SELECT * FROM t"],
            vec!["--source", "t=a.csv", "--sql", "SELECT * FROM t", "--stream"],
        ] {
            assert!(parse_args(options.into_iter().map(String::from)).is_err());
        }
    }

    #[test]
    fn json_lines_output_preserves_types_and_escapes_text() {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Text, true),
            Field::new("count", DataType::Integer, true),
        ]).unwrap();
        let batch = Batch::from_rows(schema, vec![
            vec![Value::Text("a\\\"b".into()), Value::Integer(4)],
            vec![Value::Null, Value::Null],
        ]).unwrap();
        let mut bytes = Vec::new();
        print_jsonl_rows(&batch, &mut bytes).unwrap();
        let content = String::from_utf8(bytes).unwrap();
        let rows: Vec<serde_json::Value> = content.lines().map(|line|
            serde_json::from_str(line).unwrap()
        ).collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["name"], "a\\\"b");
        assert_eq!(rows[0]["count"], 4);
        assert!(rows[1]["count"].is_null());
    }


    #[test]
    fn sql_file_admission_checks_size_before_parsing() {
        use std::time::{SystemTime, UNIX_EPOCH};
        let unique = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!(
            "rustspark-sql-file-budget-{}-{unique}.sql", std::process::id()
        ));
        fs::write(&path, vec![b'x'; MAX_SQL_FILE_BYTES as usize + 1]).unwrap();
        let args = vec![
            "--csv".into(), "unused.csv".into(),
            "--sql-file".into(), path.to_string_lossy().into_owned(),
        ];
        assert!(parse_args(args).is_err());
        fs::write(&path, "SELECT 1 FROM data").unwrap();
        let args = vec![
            "--csv".into(), "unused.csv".into(),
            "--sql-file".into(), path.to_string_lossy().into_owned(),
        ];
        assert_eq!(parse_args(args).unwrap().unwrap().sql, "SELECT 1 FROM data");
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn json_lines_reject_nonfinite_floats_instead_of_losing_values() {
        let schema = Schema::new(vec![Field::new("measure", DataType::Float, true)]).unwrap();
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let batch = Batch::from_rows(
                schema.clone(), vec![vec![Value::Float(value)]]
            ).unwrap();
            let mut output = Vec::new();
            let error = print_jsonl_rows(&batch, &mut output).unwrap_err();
            assert!(error.to_string().contains("nonfinite SQL float"));
            assert!(output.is_empty());
        }
        let valid = Batch::from_rows(
            schema, vec![vec![Value::Float(0.5)], vec![Value::Null]]
        ).unwrap();
        let mut output = Vec::new();
        print_jsonl_rows(&valid, &mut output).unwrap();
        assert_eq!(String::from_utf8(output).unwrap(), "{\"measure\":0.5}\n{\"measure\":null}\n");
    }

    #[test]
    fn json_array_streams_across_batches_and_rejects_nan() {
        let schema = Schema::new(vec![Field::new("n", DataType::Float, true)]).unwrap();
        let first = Batch::from_rows(schema.clone(), vec![vec![Value::Float(1.5)]]).unwrap();
        let second = Batch::from_rows(schema.clone(), vec![vec![Value::Null]]).unwrap();
        let mut output = Vec::new();
        let mut writer = JsonArrayPrinter::new(&mut output).unwrap();
        writer.print_batch(&first).unwrap();
        writer.print_batch(&second).unwrap();
        writer.finish().unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(parsed.as_array().unwrap().len(), 2);
        assert_eq!(parsed[0]["n"], 1.5);
        assert!(parsed[1]["n"].is_null());
        let invalid = Batch::from_rows(schema, vec![vec![Value::Float(f64::INFINITY)]]).unwrap();
        let mut output = Vec::new();
        let mut writer = JsonArrayPrinter::new(&mut output).unwrap();
        assert!(writer.print_batch(&invalid).is_err());
    }

    #[test]
    fn stdout_format_accepts_json_array_and_rejects_conflicts() {
        let args = ["--csv", "data.csv", "--sql", "SELECT * FROM data",
            "--stdout-format", "json-array"];
        assert!(parse_args(args.into_iter().map(String::from))
            .unwrap().unwrap().json_array);
        for args in [
            vec!["--csv", "data.csv", "--sql", "SELECT * FROM data",
                "--stdout-format", "xml"],
            vec!["--csv", "data.csv", "--sql", "SELECT * FROM data",
                "--jsonl", "--stdout-format", "csv"],
        ] {
            assert!(parse_args(args.into_iter().map(String::from)).is_err());
        }
    }

    #[test]
    fn native_cli_row_cap_parses_and_rejects_invalid_counts() {
        let valid = parse_args([
            "--csv", "unused.csv", "--sql", "SELECT * FROM data", "--max-rows", "0"
        ].into_iter().map(String::from)).unwrap().unwrap();
        assert_eq!(valid.max_rows, Some(0));
        for count in ["-1", "not-a-number", "1.5"] {
            let args = ["--csv", "unused.csv", "--sql", "SELECT * FROM data",
                "--max-rows", count];
            assert!(parse_args(args.into_iter().map(String::from)).is_err());
        }
        let duplicate = ["--csv", "unused.csv", "--sql", "SELECT * FROM data",
            "--max-rows", "1", "--max-rows", "2"];
        assert!(parse_args(duplicate.into_iter().map(String::from)).is_err());
    }

    #[test]
    fn client_style_named_file_source_flags_register_tables() {
        let args = parse_args([
            "--csv-table", "sales=first.csv",
            "--parquet-table", "users=second.PARQUET",
            "--sql", "SELECT * FROM sales",
        ].into_iter().map(String::from)).unwrap().unwrap();
        assert_eq!(args.sources.len(), 2);
        assert_eq!(args.sources[0].0, "sales");
        assert_eq!(args.sources[1].0, "users");
        for source in [
            vec!["--csv-table", "x=bad.parquet"],
            vec!["--parquet-table", "x=bad.csv"],
            vec!["--csv-table", "x"],
        ] {
            let mut flags = source;
            flags.extend(["--sql", "SELECT 1 FROM x"]);
            assert!(parse_args(flags.into_iter().map(String::from)).is_err());
        }
    }

    #[test]
    fn quote_text_fields_for_csv_export() {
        assert_eq!(csv_value(&Value::Text("a,\"b\"".into())), "\"a,\"\"b\"\"\"");
        assert_eq!(csv_value(&Value::Null), "");
    }
}

#[cfg(test)]
mod bounded_owned_parquet_materialization_tests {
    use super::*;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("text", DataType::Text, true),
        ]).unwrap()
    }

    #[test]
    fn moves_nullable_wide_text_and_preserves_input_row_order() {
        let schema = schema();
        let wide = "🙂é".repeat(4096);
        let a = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1), Value::Text(wide.clone())],
            vec![Value::Integer(2), Value::Null],
        ]).unwrap();
        let b = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(3), Value::Text(wide.clone())]
        ]).unwrap();
        let expected = vec![
            vec![Value::Integer(1), Value::Text(wide.clone())],
            vec![Value::Integer(2), Value::Null],
            vec![Value::Integer(3), Value::Text(wide)],
        ];
        let result = collect_parquet_batches(schema.clone(), [Ok(a), Ok(b)],
            3, 1024 * 1024).unwrap();
        assert_eq!(result.schema(), &schema);
        assert_eq!(result.rows().unwrap(), expected);
    }

    #[test]
    fn enforces_row_and_retained_byte_budgets_without_partial_results() {
        let schema = schema();
        let make_batch = || {
            let mut huge = String::with_capacity(8192);
            huge.push('a');
            Batch::from_rows(schema.clone(), vec![
                vec![Value::Integer(1), Value::Text(huge)]
            ]).unwrap()
        };
        assert!(collect_parquet_batches(schema.clone(),
            [Ok(make_batch()), Ok(make_batch())], 1, 100_000).is_err());
        let oversized = collect_parquet_batches(schema.clone(),
            [Ok(make_batch())], 10, 4096).err().unwrap();
        assert!(oversized.to_string().contains("byte limit"));
        let empty = collect_parquet_batches(
            schema.clone(), Vec::<Result<Batch>>::new(), 10, 4096).unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.schema(), &schema);
    }

    #[test]
    fn rejects_schema_changes_before_assembling_mismatched_columns() {
        let base = schema();
        let different = Schema::new(vec![Field::new(
            "id", DataType::Integer, false,
        )]).unwrap();
        let batch = Batch::from_rows(different, vec![
            vec![Value::Integer(3)]
        ]).unwrap();
        assert!(collect_parquet_batches(base, [Ok(batch)], 10, 10000).is_err());
    }
}
