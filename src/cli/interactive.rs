use std::{
    error::Error,
    io::{self, BufRead, IsTerminal, Write},
    time::Duration,
};

use datafusion::sql::parser::DFParser;
use rustspark::RustSparkSession;

use super::{Args, cache_tables, execute_script, register_derived_tables};

// Detect a semicolon outside quoted strings and comments, including a final
// SQL comment on the same line as the terminator.
fn has_sql_terminator(sql: &str) -> bool {
    let mut chars = sql.chars().peekable();
    let mut quote = None;
    let mut block_depth = 0usize;
    let mut line_comment = false;
    let mut last_significant = None;
    while let Some(ch) = chars.next() {
        if line_comment {
            if ch == '\n' {
                line_comment = false;
            }
            continue;
        }
        if block_depth > 0 {
            if ch == '/' && chars.peek() == Some(&'*') {
                chars.next();
                block_depth += 1;
            } else if ch == '*' && chars.peek() == Some(&'/') {
                chars.next();
                block_depth -= 1;
            }
            continue;
        }
        if let Some(delimiter) = quote {
            if ch == delimiter {
                if chars.peek() == Some(&delimiter) {
                    chars.next();
                } else {
                    quote = None;
                }
            }
            continue;
        }
        match ch {
            '-' if chars.peek() == Some(&'-') => {
                chars.next();
                line_comment = true;
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                block_depth = 1;
            }
            '\'' | '"' | '\u{60}' => {
                quote = Some(ch);
                last_significant = Some(ch);
            }
            other if !other.is_whitespace() => last_significant = Some(other),
            _ => {}
        }
    }
    quote.is_none() && block_depth == 0 && last_significant == Some(';')
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
        if pending.trim().is_empty() {
            match line.trim() {
                "\\q" | ".quit" | ".exit" => break,
                "\\h" | ".help" => {
                    eprintln!("Enter SQL ending with ';'. Use \\q to quit.");
                    continue;
                }
                _ => {}
            }
        }
        pending.push_str(&line);
        if !has_sql_terminator(&pending) {
            continue;
        }

        // The DataFusion parser understands SQL quotes and comments. An invalid
        // statement is reported without terminating the interactive process.
        let statements = match DFParser::parse_sql(&pending) {
            Ok(statements) => statements,
            Err(error) => {
                eprintln!("SQL error: {error}");
                pending.clear();
                continue;
            }
        };
        pending.clear();
        for statement in statements {
            let query = statement.to_string();
            let execution = execute_script(session, &query, args);
            let result = if let Some(seconds) = args.query_timeout {
                match tokio::time::timeout(Duration::from_secs(seconds), execution).await {
                    Ok(result) => result,
                    Err(_) => Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("SQL query timed out after {seconds} seconds"),
                    )
                    .into()),
                }
            } else {
                execution.await
            };
            if let Err(error) = result {
                eprintln!("SQL error: {error}");
            }
        }
    }

    if !pending.trim().is_empty() {
        return Err("SQL input ended before a terminating semicolon".into());
    }
    Ok(())
}
