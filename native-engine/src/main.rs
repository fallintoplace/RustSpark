use std::{
    env, fs,
    io::{self, Write},
    path::PathBuf,
    process::ExitCode,
};

use rustspark_native::{
    Error, Result,
    catalog::Catalog,
    csv,
    executor,
    scalar::Value,
    sql,
};

struct Args {
    csv: PathBuf,
    table: String,
    sql: String,
}

fn usage() -> &'static str {
    "Usage: rustspark-native --csv FILE [--table NAME] (--sql QUERY | --sql-file FILE)\n     Supported SQL: SELECT, WHERE, GROUP BY, COUNT/SUM/MIN/MAX, DISTINCT,\n     ORDER BY, LIMIT and OFFSET. This is an experimental native engine."
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Option<Args>> {
    let mut csv = None;
    let mut table = "data".to_string();
    let mut sql = None;
    let mut sql_file = None;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        if arg == "--help" || arg == "-h" {
            return Ok(None);
        }
        let value = iter.next().ok_or_else(|| Error::Parse(format!("missing value for {arg}")))?;
        match arg.as_str() {
            "--csv" => {
                if csv.replace(PathBuf::from(value)).is_some() {
                    return Err(Error::Parse("duplicate --csv".into()));
                }
            }
            "--table" => table = value,
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
    Ok(Some(Args { csv, table, sql }))
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

fn print_csv(batch: &rustspark_native::batch::Batch, output: &mut impl Write) -> Result<()> {
    let header = batch
        .schema()
        .fields()
        .iter()
        .map(|field| escape_field(&field.name))
        .collect::<Vec<_>>()
        .join(",");
    writeln!(output, "{header}")?;
    for row in batch.rows()? {
        let line = row.iter().map(csv_value).collect::<Vec<_>>().join(",");
        writeln!(output, "{line}")?;
    }
    Ok(())
}

fn run() -> Result<()> {
    let Some(args) = parse_args(env::args().skip(1))? else {
        println!("{}", usage());
        return Ok(());
    };
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
