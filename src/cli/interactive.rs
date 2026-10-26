use std::{
    error::Error,
    io::{self, BufRead, IsTerminal, Write},
    time::Duration,
};

use datafusion::sql::parser::DFParser;
use rustspark::RustSparkSession;

use super::{Args, cache_tables, execute_script, register_derived_tables};

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
            let prompt = if pending.is_empty() { "rustspark> " } else { "       -> " };
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
        if !pending.trim_end().ends_with(';') {
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
