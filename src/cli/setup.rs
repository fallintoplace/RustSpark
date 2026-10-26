use std::{collections::HashSet, io::Read};

use rustspark::RustSparkSession;

use super::{Args, parse_named_view, parse_union_by_name, plan_query};

pub(super) async fn register_derived_tables(
    session: &RustSparkSession,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error>> {
    for declaration in &args.unions_by_name {
        let (name, sources) = parse_union_by_name(declaration)?;
        let union = session.union_tables_by_name(&sources).await?;
        session.register_dataframe(name, union)?;
    }
    for definition in &args.sql_views {
        let (name, sql) = parse_named_view(definition)?;
        let dataframe = plan_query(session, sql, args).await?;
        session.register_dataframe(name, dataframe)?;
    }
    for definition in &args.materialized_sql_views {
        let (name, sql) = parse_named_view(definition)?;
        let dataframe = plan_query(session, sql, args).await?.cache().await?;
        session.register_dataframe(name, dataframe)?;
    }
    Ok(())
}

pub(super) async fn read_query(args: &Args) -> Result<String, Box<dyn std::error::Error>> {
    let query = if let Some(sql) = args.sql.as_ref() {
        sql.clone()
    } else if let Some(path) = args.sql_file.as_ref() {
        if path.as_os_str() == "-" {
            read_sql_stdin().await?
        } else {
            std::fs::read_to_string(path)?
        }
    } else {
        unreachable!("clap requires either --sql or --sql-file")
    };
    Ok(query)
}

pub(super) async fn cache_tables(
    session: &RustSparkSession,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cached_names = HashSet::new();
    for name in &args.cache_tables {
        if !cached_names.insert(name.to_ascii_lowercase()) {
            return Err(format!("Duplicate cached table: {name}").into());
        }
        session.cache_table(name).await?;
    }
    Ok(())
}

// Use a detached reader thread so a blocked stdin read cannot stall the query timer.
async fn read_sql_stdin() -> Result<String, Box<dyn std::error::Error>> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let mut query = String::new();
        let result = std::io::stdin().read_to_string(&mut query).map(|_| query);
        let _ = sender.send(result);
    });
    Ok(receiver.await??)
}
