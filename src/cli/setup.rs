use std::{
    collections::{HashMap, HashSet},
    io::Read,
};

use rustspark::RustSparkSession;

use super::{Args, parse_named_view, parse_union_by_name};

fn ensure_unique_name(
    names: &mut HashSet<String>,
    name: &str,
    kind: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if !names.insert(name.to_ascii_lowercase()) {
        return Err(format!("Duplicate {kind}: {name}").into());
    }
    Ok(())
}

// A view is a read-only query, not an opportunity to execute SQL mutations.
async fn plan_view_query(
    session: &RustSparkSession,
    sql: &str,
    args: &Args,
) -> Result<datafusion::prelude::DataFrame, Box<dyn std::error::Error>> {
    let dataframe = if !args.named_params.is_empty() {
        let params: HashMap<_, _> = args.named_params.iter().cloned().collect();
        session.sql_read_only_with_params(sql, params).await?
    } else if !args.params.is_empty() {
        session
            .sql_read_only_with_params(sql, args.params.clone())
            .await?
    } else {
        session.sql_read_only(sql).await?
    };
    Ok(dataframe)
}

async fn register_unions_by_name(
    session: &RustSparkSession,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut union_names = HashSet::new();
    for declaration in args
        .unions_by_name
        .iter()
        .chain(&args.unions_distinct_by_name)
    {
        let (name, _) = parse_union_by_name(declaration)?;
        ensure_unique_name(&mut union_names, name, "union destination")?;
    }
    let unions = args
        .unions_by_name
        .iter()
        .map(|definition| (definition, false))
        .chain(
            args.unions_distinct_by_name
                .iter()
                .map(|definition| (definition, true)),
        );
    for (definition, distinct) in unions {
        let (name, sources) = parse_union_by_name(definition)?;
        let union = if distinct {
            session.union_tables_distinct_by_name(&sources).await?
        } else {
            session.union_tables_by_name(&sources).await?
        };
        session.register_dataframe(name, union)?;
    }
    Ok(())
}

async fn register_sql_views(
    session: &RustSparkSession,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut view_names = HashSet::new();
    let definitions = args
        .sql_views
        .iter()
        .map(|definition| (definition, false))
        .chain(
            args.materialized_sql_views
                .iter()
                .map(|definition| (definition, true)),
        );
    for (definition, materialized) in definitions {
        let (name, sql) = parse_named_view(definition)?;
        ensure_unique_name(&mut view_names, name, "SQL view")?;
        let dataframe = plan_view_query(session, sql, args).await?;
        let dataframe = if materialized {
            dataframe.cache().await?
        } else {
            dataframe
        };
        session.register_dataframe(name, dataframe)?;
    }
    Ok(())
}

pub(super) async fn register_derived_tables(
    session: &RustSparkSession,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error>> {
    register_unions_by_name(session, args).await?;
    register_sql_views(session, args).await
}

pub(super) async fn read_query(
    args: &Args,
) -> Result<std::borrow::Cow<'_, str>, Box<dyn std::error::Error>> {
    if let Some(sql) = args.sql.as_deref() {
        // A potentially large --sql script already lives in Args.
        return Ok(std::borrow::Cow::Borrowed(sql));
    }
    if let Some(path) = args.sql_file.as_ref() {
        let sql = if path.as_os_str() == "-" {
            read_sql_stdin().await?
        } else {
            std::fs::read_to_string(path)?
        };
        return Ok(std::borrow::Cow::Owned(sql));
    }
    unreachable!("clap requires either --sql or --sql-file")
}

pub(super) async fn cache_tables(
    session: &RustSparkSession,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cached_names = HashSet::new();
    for name in &args.cache_tables {
        ensure_unique_name(&mut cached_names, name, "cached table")?;
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
