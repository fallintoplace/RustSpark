use std::{
    error::Error,
    io::{self, BufRead, IsTerminal, Write},
};

use datafusion::sql::parser::DFParser;
use rustspark::RustSparkSession;

use super::{Args, cache_tables, execute_script, register_derived_tables};

mod scanner;
use scanner::SqlTerminatorScanner;

async fn execute_interactive_statement(
    session: &RustSparkSession,
    args: &Args,
    query: &str,
) -> Result<(), Box<dyn Error>> {
    super::with_query_timeout(args.query_timeout, execute_script(session, query, args)).await
}

async fn execute_pending_sql(session: &RustSparkSession, args: &Args, sql: &str, timer_on: bool) {
    // Invalid SQL is reported without terminating the interactive session.
    let statements = match DFParser::parse_sql(sql) {
        Ok(statements) => statements,
        Err(error) => {
            eprintln!("SQL error: {error}");
            return;
        }
    };
    for statement in statements {
        let query = statement.to_string();
        let started = std::time::Instant::now();
        let result = execute_interactive_statement(session, args, &query).await;
        if timer_on {
            eprintln!("Time: {:.3} ms", started.elapsed().as_secs_f64() * 1_000.0);
        }
        if let Err(error) = result {
            eprintln!("SQL error: {error}");
        }
    }
}

fn shell_arguments<'a>(command: &'a str, keyword: &str) -> Option<&'a str> {
    let rest = command.strip_prefix(keyword)?;
    if rest.is_empty() || rest.starts_with(' ') || rest.starts_with('\t') {
        Some(rest.trim())
    } else {
        None
    }
}

fn is_single_table_argument(name: &str) -> bool {
    !name.is_empty() && name.split_whitespace().count() == 1
}

// Execute SQL files in the current session without replacing pending input.
async fn handle_read_command(
    session: &RustSparkSession,
    args: &Args,
    command: &str,
    timer_on: bool,
) -> bool {
    let Some(path) = shell_arguments(command, ".read") else {
        return false;
    };
    if path.is_empty() {
        eprintln!("Shell error: .read requires a file path");
        return true;
    }
    match std::fs::read_to_string(path) {
        Ok(contents) => {
            let script = contents.strip_prefix('\u{feff}').unwrap_or(&contents);
            if !script.trim().is_empty() {
                execute_pending_sql(session, args, script, timer_on).await;
            }
        }
        Err(error) => eprintln!("Shell error: Cannot read {path}: {error}"),
    }
    true
}

// Print table column names without row scans or full type metadata.
async fn handle_columns_command(session: &RustSparkSession, command: &str) -> bool {
    let Some(name) = shell_arguments(command, ".columns") else {
        return false;
    };
    if !is_single_table_argument(name) {
        eprintln!("Shell error: .columns requires exactly one table name");
    } else {
        match session.describe_table(name).await {
            Ok(columns) => {
                for (column, _, _) in columns {
                    println!("{column}");
                }
            }
            Err(error) => eprintln!("Shell error: {error}"),
        }
    }
    true
}

// Handle the schema-specific shell command without changing pending SQL.
async fn handle_schema_command(session: &RustSparkSession, command: &str) -> bool {
    let Some(name) = shell_arguments(command, ".schema") else {
        return false;
    };
    if !is_single_table_argument(name) {
        eprintln!("Shell error: .schema requires exactly one table name");
    } else {
        match session.describe_table(name).await {
            Ok(columns) => {
                for (column, data_type, nullable) in columns {
                    let nullability = if nullable { "NULL" } else { "NOT NULL" };
                    println!("{column}\t{data_type}\t{nullability}");
                }
            }
            Err(error) => eprintln!("Shell error: {error}"),
        }
    }
    true
}

// Count a registered table without requiring a SQL statement.
async fn handle_count_command(session: &RustSparkSession, args: &Args, command: &str) -> bool {
    let Some(name) = shell_arguments(command, ".count") else {
        return false;
    };
    if !is_single_table_argument(name) {
        eprintln!("Shell error: .count requires exactly one table name");
    } else {
        let result = super::with_query_timeout(args.query_timeout, async {
            let count = session.table(name).await?.count().await?;
            println!("{count}");
            Ok::<(), Box<dyn Error>>(())
        })
        .await;
        if let Err(error) = result {
            eprintln!("Shell error: {error}");
        }
    }
    true
}

fn parse_preview_selection(arguments: &str) -> Option<(&str, usize)> {
    let parts: Vec<_> = arguments.split_whitespace().collect();
    match parts.as_slice() {
        [table] => Some((*table, 10)),
        [table, count] => count
            .parse::<usize>()
            .ok()
            .filter(|rows| (1..=1000).contains(rows))
            .map(|rows| (*table, rows)),
        _ => None,
    }
}

fn quote_shell_table(table: &str) -> String {
    table
        .split('.')
        .map(|part| format!("\"{}\"", part.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(".")
}

// Use quoted identifiers so a shell table name cannot change the SQL plan.
async fn handle_preview_command(session: &RustSparkSession, args: &Args, command: &str) -> bool {
    let Some(arguments) = shell_arguments(command, ".preview") else {
        return false;
    };
    let usage = "Shell error: .preview requires TABLE [ROWS] (ROWS must be 1..=1000)";
    let (table, rows) = match parse_preview_selection(arguments) {
        Some(selection) => selection,
        None => {
            eprintln!("{usage}");
            return true;
        }
    };
    if table.split('.').any(str::is_empty) {
        eprintln!("Shell error: .preview requires a valid table name");
        return true;
    }
    let source = quote_shell_table(table);
    let query = format!("SELECT * FROM {source} LIMIT {rows}");
    if let Err(error) = execute_interactive_statement(session, args, &query).await {
        eprintln!("Shell error: {error}");
    }
    true
}

enum ShellAction {
    Continue,
    Exit,
    Sql,
}

async fn dispatch_shell_command(
    session: &RustSparkSession,
    args: &Args,
    command: &str,
) -> ShellAction {
    match command {
        "\\q" | ".quit" | ".exit" => return ShellAction::Exit,
        "\\h" | ".help" => {
            eprintln!("Enter SQL ending with ';'. Use \\q to quit.");
            eprintln!("Read SQL from a file with .read PATH.");
            eprintln!(
                "Shell commands: .tables, .schemas, .schema TABLE, .count TABLE, .preview TABLE [ROWS], .quit."
            );
            return ShellAction::Continue;
        }
        ".tables" => {
            for table in session.list_tables() {
                println!("{table}");
            }
            return ShellAction::Continue;
        }
        ".catalogs" => {
            for catalog in session.list_catalogs() {
                println!("{catalog}");
            }
            return ShellAction::Continue;
        }
        ".schemas" => {
            for schema in session.list_schemas() {
                println!("{schema}");
            }
            return ShellAction::Continue;
        }
        _ => {}
    }
    if handle_preview_command(session, args, command).await
        || handle_columns_command(session, command).await
        || handle_schema_command(session, command).await
        || handle_count_command(session, args, command).await
    {
        ShellAction::Continue
    } else {
        ShellAction::Sql
    }
}

/// Run SQL statements in one session so views and DDL survive between prompts.
pub(super) async fn run_interactive(
    session: &RustSparkSession,
    args: &Args,
) -> Result<(), Box<dyn Error>> {
    register_derived_tables(session, args).await?;
    cache_tables(session, args).await?;

    let stdin = io::stdin();
    let show_prompt = stdin.is_terminal();
    let mut reader = stdin.lock();
    let mut pending = String::new();
    let mut line = String::new();
    let mut terminator = SqlTerminatorScanner::default();
    let mut timer_on = false;

    loop {
        if show_prompt {
            let prompt = if pending.is_empty() {
                "rustspark> "
            } else {
                "       -> "
            };
            eprint!("{prompt}");
            io::stderr().flush()?;
        }
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        if pending.is_empty() && line.starts_with('\u{feff}') {
            line = line.trim_start_matches('\u{feff}').to_owned();
        }
        if line.trim() == ".cancel" && terminator.outside_multiline_constructs() {
            if pending.trim().is_empty() {
                eprintln!("No pending SQL");
            } else {
                pending.clear();
                terminator.reset();
                eprintln!("Pending SQL cancelled");
            }
            continue;
        }
        if pending.trim().is_empty() {
            let command = line.trim();
            if let Some(arguments) = shell_arguments(command, ".timer") {
                match arguments {
                    "on" => timer_on = true,
                    "off" => timer_on = false,
                    "" => eprintln!("Timer: {}", if timer_on { "on" } else { "off" }),
                    _ => eprintln!("Shell error: use .timer on or .timer off"),
                }
                continue;
            }
            if handle_read_command(session, args, command, timer_on).await {
                continue;
            }
            match dispatch_shell_command(session, args, command).await {
                ShellAction::Exit => break,
                ShellAction::Continue => continue,
                ShellAction::Sql => {}
            }
        }
        pending.push_str(&line);
        if !terminator.push(&line) {
            // A standalone comment does not start a SQL statement. Clear it
            // once all block comments are closed so shell commands still work.
            if terminator.outside_multiline_constructs() && terminator.last_significant.is_none() {
                pending.clear();
                terminator.reset();
            }
            continue;
        }

        let query = std::mem::take(&mut pending);
        terminator.reset();
        execute_pending_sql(session, args, &query, timer_on).await;
    }

    if !pending.trim().is_empty() {
        return Err("SQL input ended before a terminating semicolon".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        SqlTerminatorScanner, is_single_table_argument, parse_preview_selection, quote_shell_table,
        shell_arguments,
    };

    #[test]
    fn shell_commands_require_an_exact_keyword_boundary() {
        assert_eq!(shell_arguments(".read", ".read"), Some(""));
        assert_eq!(
            shell_arguments(".read query.sql", ".read"),
            Some("query.sql")
        );
        assert_eq!(
            shell_arguments(".read\tquery.sql", ".read"),
            Some("query.sql")
        );
        assert_eq!(shell_arguments(".readme", ".read"), None);
        assert_eq!(shell_arguments(".timer on", ".timer"), Some("on"));
        assert_eq!(shell_arguments(".previewer x", ".preview"), None);
    }

    #[test]
    fn single_table_commands_reject_missing_and_multiple_names() {
        assert!(is_single_table_argument("events"));
        assert!(!is_single_table_argument(""));
        assert!(!is_single_table_argument("events logs"));
        assert!(!is_single_table_argument("  "));
    }

    #[test]
    fn preview_table_quoting_preserves_components_and_embedded_quotes() {
        assert_eq!(quote_shell_table("db.events"), "\"db\".\"events\"");
        assert_eq!(quote_shell_table("d\"b.events"), "\"d\"\"b\".\"events\"");
    }

    #[test]
    fn preview_selection_defaults_and_rejects_invalid_limits() {
        assert_eq!(parse_preview_selection("data"), Some(("data", 10)));
        assert_eq!(parse_preview_selection("db.table 1000"), Some(("db.table", 1000)));
        assert_eq!(parse_preview_selection("data 0"), None);
        assert_eq!(parse_preview_selection("data 1001"), None);
        assert_eq!(parse_preview_selection("data NaN"), None);
        assert_eq!(parse_preview_selection("data 1 extra"), None);
    }

    #[test]
    fn incremental_scan_keeps_quote_and_comment_state_between_lines() {
        let mut scanner = SqlTerminatorScanner::default();
        assert!(!scanner.push("SELECT 'semi;\n"));
        assert!(!scanner.push("quoted''value' /* outer /* nested ; */\n"));
        assert!(scanner.push(" still commented */ AS value; -- trailing ;\n"));
    }

    #[test]
    fn incremental_scan_ignores_comment_semicolons() {
        let mut scanner = SqlTerminatorScanner::default();
        assert!(!scanner.push("SELECT 1 -- ; comment\n"));
        assert!(!scanner.push("/* ; */\n"));
        assert!(scanner.push(";\n"));
    }

    #[test]
    fn incremental_scan_scales_with_many_lines_and_can_reset() {
        let mut scanner = SqlTerminatorScanner::default();
        for _ in 0..10_000 {
            assert!(!scanner.push("SELECT 1\n"));
        }
        assert!(scanner.push(";\n"));
        scanner.reset();
        assert!(!scanner.push("SELECT 2\n"));
        assert!(scanner.push(";\n"));
    }
}
