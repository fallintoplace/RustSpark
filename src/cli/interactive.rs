use std::{
    error::Error,
    io::{self, BufRead, IsTerminal, Write},
};

use datafusion::sql::parser::DFParser;
use rustspark::RustSparkSession;

use super::{Args, cache_tables, execute_script, register_derived_tables};

// Scan only the newly read line. Re-scanning the accumulated SQL on every
// line made large interactive statements quadratic in their line count.
#[derive(Default)]
struct SqlTerminatorScanner {
    quote: Option<char>,
    block_depth: usize,
    line_comment: bool,
    last_significant: Option<char>,
}

impl SqlTerminatorScanner {
    fn push(&mut self, fragment: &str) -> bool {
        let mut chars = fragment.chars().peekable();
        while let Some(ch) = chars.next() {
            if self.line_comment {
                if ch == '\n' {
                    self.line_comment = false;
                }
                continue;
            }
            if self.block_depth > 0 {
                if ch == '/' && chars.peek() == Some(&'*') {
                    chars.next();
                    self.block_depth += 1;
                } else if ch == '*' && chars.peek() == Some(&'/') {
                    chars.next();
                    self.block_depth -= 1;
                }
                continue;
            }
            if let Some(delimiter) = self.quote {
                if ch == delimiter {
                    if chars.peek() == Some(&delimiter) {
                        chars.next();
                    } else {
                        self.quote = None;
                    }
                }
                continue;
            }
            match ch {
                '-' if chars.peek() == Some(&'-') => {
                    chars.next();
                    self.line_comment = true;
                }
                '/' if chars.peek() == Some(&'*') => {
                    chars.next();
                    self.block_depth = 1;
                }
                '\'' | '"' | '\u{60}' => {
                    self.quote = Some(ch);
                    self.last_significant = Some(ch);
                }
                other if !other.is_whitespace() => self.last_significant = Some(other),
                _ => {}
            }
        }
        self.quote.is_none() && self.block_depth == 0 && self.last_significant == Some(';')
    }
}

async fn execute_interactive_statement(
    session: &RustSparkSession,
    args: &Args,
    query: &str,
) -> Result<(), Box<dyn Error>> {
    super::with_query_timeout(args.query_timeout, execute_script(session, query, args)).await
}

async fn execute_pending_sql(session: &RustSparkSession, args: &Args, sql: &str) {
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
        if let Err(error) = execute_interactive_statement(session, args, &query).await {
            eprintln!("SQL error: {error}");
        }
    }
}

// Handle the schema-specific shell command without changing pending SQL.
async fn handle_schema_command(session: &RustSparkSession, command: &str) -> bool {
    if command != ".schema" && !command.starts_with(".schema ") {
        return false;
    }
    let name = command.strip_prefix(".schema").unwrap_or_default().trim();
    if name.is_empty() || name.split_whitespace().count() != 1 {
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
async fn handle_count_command(session: &RustSparkSession, command: &str) -> bool {
    if command != ".count" && !command.starts_with(".count ") {
        return false;
    }
    let name = command.strip_prefix(".count").unwrap_or_default().trim();
    if name.is_empty() || name.split_whitespace().count() != 1 {
        eprintln!("Shell error: .count requires exactly one table name");
    } else {
        match session.table(name).await {
            Ok(table) => match table.count().await {
                Ok(count) => println!("{count}"),
                Err(error) => eprintln!("Shell error: {error}"),
            },
            Err(error) => eprintln!("Shell error: {error}"),
        }
    }
    true
}

enum ShellAction {
    Continue,
    Exit,
    Sql,
}

async fn dispatch_shell_command(session: &RustSparkSession, command: &str) -> ShellAction {
    match command {
        "\\q" | ".quit" | ".exit" => return ShellAction::Exit,
        "\\h" | ".help" => {
            eprintln!("Enter SQL ending with ';'. Use \\q to quit.");
            eprintln!("Shell commands: .tables, .schemas, .schema TABLE, .count TABLE, .quit.");
            return ShellAction::Continue;
        }
        ".tables" => {
            for table in session.list_tables() {
                println!("{table}");
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
    if handle_schema_command(session, command).await || handle_count_command(session, command).await {
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
        if pending.trim().is_empty() {
            match dispatch_shell_command(session, line.trim()).await {
                ShellAction::Exit => break,
                ShellAction::Continue => continue,
                ShellAction::Sql => {}
            }
        }
        pending.push_str(&line);
        if !terminator.push(&line) {
            continue;
        }

        let query = std::mem::take(&mut pending);
        terminator = SqlTerminatorScanner::default();
        execute_pending_sql(session, args, &query).await;
    }

    if !pending.trim().is_empty() {
        return Err("SQL input ended before a terminating semicolon".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::SqlTerminatorScanner;

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
        scanner = SqlTerminatorScanner::default();
        assert!(!scanner.push("SELECT 2\n"));
        assert!(scanner.push(";\n"));
    }
}
