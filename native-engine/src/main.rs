use std::{
    env, fs,
    io::{self, BufReader, Write},
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
}

fn usage() -> &'static str {
    "Usage: rustspark-native (--csv FILE | --parquet FILE | --source TABLE=FILE [--source TABLE=FILE ...]) [--table NAME] (--sql QUERY | --sql-file FILE) [--stream] [--csv-schema NAME:TYPE,...] [--jsonl]\n     Native --stream supports SELECT, WHERE, and LIMIT over CSV batches only.\n     Parquet execution uses bounded in-memory batches (up to 1,000,000 rows).\n     Supported native scalar types: text, bool, int64, float64."
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
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        if arg == "--help" || arg == "-h" {
            return Ok(None);
        }
        if arg == "--jsonl" {
            if jsonl { return Err(Error::Parse("duplicate --jsonl".into())); }
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
            "--source" => {
                let (table_name, file) = value.split_once('=')
                    .ok_or_else(|| Error::Parse("--source expects TABLE=FILE".into()))?;
                if table_name.is_empty() || file.is_empty() {
                    return Err(Error::Parse("--source requires a table and file path".into()));
                }
                let path = PathBuf::from(file);
                if !matches!(path.extension().and_then(|ext| ext.to_str())
                    .map(|ext| ext.to_ascii_lowercase()).as_deref(), Some("csv" | "parquet")) {
                    return Err(Error::Unsupported("--source supports .csv or .parquet files".into()));
                }
                sources.push((table_name.to_string(), path));
            },
            "--table" => table = value,
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
    if stream && parquet.is_some() {
        return Err(Error::Unsupported("--stream currently supports CSV input only".into()));
    }
    let sql = match (sql, sql_file) {
        (Some(query), None) => query,
        (None, Some(path)) => fs::read_to_string(path)?,
        _ => return Err(Error::Parse("--sql or --sql-file is required".into())),
    };
    if sql.trim().is_empty() {
        return Err(Error::Parse("SQL query cannot be empty".into()));
    }
    Ok(Some(Args { csv, parquet, sources, table, sql, stream, csv_schema, jsonl }))
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
                    .unwrap_or(serde_json::Value::Null),
                Value::Text(value) => serde_json::Value::String(value.clone()),
            };
            record.insert(field.name.clone(), json);
        }
        writeln!(output, "{}", serde_json::Value::Object(record))?;
    }
    Ok(())
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

    let path = args.csv.as_ref().ok_or_else(|| Error::Unsupported("streaming requires CSV input".into()))?;
    let input = BufReader::new(fs::File::open(path)?);
    let csv = if let Some(spec) = &args.csv_schema {
        CsvStream::new_typed(input, 4_096, 1024 * 1024, parse_stream_schema(spec)?)?
    } else {
        CsvStream::new(input, 4_096, 1024 * 1024)?
    };
    let output_schema = if let Some(ref selections) = projection {
        project::project(&Batch::empty(csv.schema().clone()), selections)?
            .schema().clone()
    } else {
        csv.schema().clone()
    };
    let mut pipeline = Pipeline::new(csv);
    if let Some(predicate) = predicate {
        pipeline = pipeline.with_filter(predicate);
    }
    if let Some(selections) = projection {
        pipeline = pipeline.with_projection(selections);
    }
    if let Some((offset, count)) = limit {
        pipeline = pipeline.with_limit(offset, count);
    }

    let stdout = io::stdout();
    let mut output = io::BufWriter::new(stdout.lock());
    if !args.jsonl { print_header(&output_schema, &mut output)?; }
    for batch in pipeline {
        let batch = batch?;
        if args.jsonl { print_jsonl_rows(&batch, &mut output)?; }
        else { print_csv_rows(&batch, &mut output)?; }
    }
    output.flush()?;
    Ok(())
}

const MAX_PARQUET_ROWS: usize = 1_000_000;

fn read_parquet_bounded(path: &Path) -> Result<Batch> {
    let mut schema: Option<Schema> = None;
    let mut rows = Vec::new();
    for next in parquet_scan::scan(path, 4_096)? {
        let batch = next?;
        if let Some(current) = &schema {
            if current != batch.schema() {
                return Err(Error::Schema("Parquet batch schema changed".into()));
            }
        } else {
            schema = Some(batch.schema().clone());
        }
        let total = rows.len().checked_add(batch.num_rows())
            .ok_or_else(|| Error::Execution("Parquet row count overflow".into()))?;
        if total > MAX_PARQUET_ROWS {
            return Err(Error::Execution("Parquet input exceeds native 1000000 row limit".into()));
        }
        rows.extend(batch.rows()?);
    }
    let schema = schema.ok_or_else(|| Error::Unsupported("empty Parquet data cannot yet be queried in the native CLI".into()))?;
    Batch::from_rows(schema, rows)
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
    let stdout = io::stdout();
    let mut output = io::BufWriter::new(stdout.lock());
    if args.jsonl { print_jsonl_rows(&batch, &mut output)?; }
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
        assert!(parse_args(args.into_iter().map(String::from)).is_err());
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
    fn quote_text_fields_for_csv_export() {
        assert_eq!(csv_value(&Value::Text("a,\"b\"".into())), "\"a,\"\"b\"\"\"");
        assert_eq!(csv_value(&Value::Null), "");
    }
}
