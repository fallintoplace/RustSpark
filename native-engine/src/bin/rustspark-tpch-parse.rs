//! Report which of the 22 numbered TPC-H SQL files parse in the native engine.
//! This checks syntax only, not table binding or runtime correctness.
use std::{env, fs, path::{Path, PathBuf}, process::ExitCode};
use rustspark_native::sql::parse_sql;
use serde::Serialize;

const MAX_SQL_BYTES: u64 = 1024 * 1024;

#[derive(Serialize)]
struct QueryStatus {
    id: u8,
    status: &'static str,
    error: Option<String>,
}

fn check_query(id: u8, dir: &Path) -> QueryStatus {
    let filename = dir.join(format!("{id:02}.sql"));
    let sql = (|| -> Result<String, String> {
        let size = fs::metadata(&filename).map_err(|e| e.to_string())?.len();
        if size > MAX_SQL_BYTES {
            return Err("SQL query exceeds 1 MiB limit".into());
        }
        fs::read_to_string(&filename).map_err(|e| e.to_string())
    })();
    match sql.and_then(|source| parse_sql(&source).map(|_| ())
        .map_err(|e| e.to_string())) {
        Ok(()) => QueryStatus { id, status: "parse_ok", error: None },
        Err(error) => QueryStatus {
            id,
            status: "parse_error",
            error: Some(error.chars().take(400).collect()),
        },
    }
}

#[derive(Serialize)]
struct Report {
    scope: &'static str,
    parsed: usize,
    total: usize,
    queries: Vec<QueryStatus>,
}

fn report(dir: &Path) -> Report {
    let queries = (1..=22).map(|id| check_query(id, dir)).collect::<Vec<_>>();
    let parsed = queries.iter().filter(|q| q.status == "parse_ok").count();
    Report {
        scope: "native SQL parsing only, not TPC-H query compatibility",
        parsed, total: 22, queries,
    }
}

fn main() -> ExitCode {
    let mut args = env::args_os().skip(1);
    let Some(dir) = args.next().map(PathBuf::from) else {
        eprintln!("Usage: rustspark-tpch-parse <directory-with-01.sql-through-22.sql>");
        return ExitCode::FAILURE;
    };
    if args.next().is_some() || !dir.is_dir() {
        eprintln!("expected exactly one existing SQL directory");
        return ExitCode::FAILURE;
    }
    let result = report(&dir);
    match serde_json::to_string_pretty(&result) {
        Ok(json) => println!("{json}"),
        Err(error) => {
            eprintln!("could not encode parser report: {error}");
            return ExitCode::FAILURE;
        }
    }
    if result.parsed == result.total {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_parser_success_and_distinguishes_missing_queries() {
        let dir = std::env::temp_dir().join(format!(
            "rustspark-tpch-parse-{}-{:?}", std::process::id(), std::thread::current().id()
        ));
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("01.sql"), "SELECT 1;").unwrap();
        fs::write(dir.join("02.sql"), "SELECT FROM t;").unwrap();
        let summary = report(&dir);
        assert_eq!(summary.parsed, 1);
        assert_eq!(summary.total, 22);
        assert_eq!(summary.queries[0].status, "parse_ok");
        assert_eq!(summary.queries[1].status, "parse_error");
        assert_eq!(summary.queries[21].status, "parse_error");
        fs::remove_dir_all(dir).unwrap();
    }
}
