use std::{
    env, fs,
    io::{self, BufReader, Write},
    path::PathBuf,
    process::ExitCode,
};

use rustspark_native::{
    Error, Result,
    catalog::Catalog,
    csv, csv_stream::CsvStream,
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
    csv: PathBuf,
    table: String,
    sql: String,
    stream: bool,
    csv_schema: Option<String>,
}

fn usage() -> &'static str {
    "Usage: rustspark-native --csv FILE [--table NAME] (--sql QUERY | --sql-file FILE) [--stream] [--csv-schema NAME:TYPE,...]\n     Native --stream supports SELECT, WHERE, and LIMIT on bounded CSV record batches.\n     Supported types: text, bool, int64, float64. Other SQL runs in the bounded legacy mode."
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Option<Args>> {
    let mut csv = None;
    let mut table = "data".to_string();
    let mut sql = None;
    let mut sql_file = None;
    let mut stream = false;
    let mut csv_schema = None;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        if arg == "--help" || arg == "-h" {
            return Ok(None);
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
    let csv = csv.ok_or_else(|| Error::Parse("--csv is required".into()))?;
    let sql = match (sql, sql_file) {
        (Some(query), None) => query,
        (None, Some(path)) => fs::read_to_string(path)?,
        _ => return Err(Error::Parse("--sql or --sql-file is required".into())),
    };
    if sql.trim().is_empty() {
        return Err(Error::Parse("SQL query cannot be empty".into()));
    }
    Ok(Some(Args { csv, table, sql, stream, csv_schema }))
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

    let input = BufReader::new(fs::File::open(&args.csv)?);
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
    print_header(&output_schema, &mut output)?;
    for batch in pipeline {
        print_csv_rows(&batch?, &mut output)?;
    }
    output.flush()?;
    Ok(())
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
    catalog.register(&args.table, csv::read_csv(&args.csv)?)?;
    let plan = sql::parse_sql(&args.sql)?;
    let batch = executor::execute(&plan, &catalog)?;
    let stdout = io::stdout();
    let mut output = io::BufWriter::new(stdout.lock());
    print_csv(&batch, &mut output)?;
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
    }

    #[test]
    fn quote_text_fields_for_csv_export() {
        assert_eq!(csv_value(&Value::Text("a,\"b\"".into())), "\"a,\"\"b\"\"\"");
        assert_eq!(csv_value(&Value::Null), "");
    }
}
