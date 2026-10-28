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

fn union_definitions(args: &Args) -> impl Iterator<Item = (&str, bool)> {
    args.unions_by_name
        .iter()
        .map(|definition| (definition.as_str(), false))
        .chain(
            args.unions_distinct_by_name
                .iter()
                .map(|definition| (definition.as_str(), true)),
        )
}

type ParsedUnionDefinition<'a> = (&'a str, Vec<&'a str>, bool);

fn parsed_union_definitions(
    args: &Args,
) -> Result<Vec<ParsedUnionDefinition<'_>>, Box<dyn std::error::Error>> {
    let mut names = HashSet::new();
    union_definitions(args)
        .map(
            |(declaration, distinct)| -> Result<_, Box<dyn std::error::Error>> {
                let (name, sources) = parse_union_by_name(declaration)?;
                ensure_unique_name(&mut names, name, "union destination")?;
                Ok((name, sources, distinct))
            },
        )
        .collect()
}

async fn register_unions_by_name(
    session: &RustSparkSession,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error>> {
    for (name, sources, distinct) in parsed_union_definitions(args)? {
        let union = if distinct {
            session.union_tables_distinct_by_name(&sources).await?
        } else {
            session.union_tables_by_name(&sources).await?
        };
        session.register_dataframe(name, union)?;
    }
    Ok(())
}

fn view_definitions(args: &Args) -> impl Iterator<Item = (&str, bool)> {
    args.sql_views
        .iter()
        .map(|definition| (definition.as_str(), false))
        .chain(
            args.materialized_sql_views
                .iter()
                .map(|definition| (definition.as_str(), true)),
        )
}

fn validate_view_names(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let mut names = HashSet::new();
    for (definition, _) in view_definitions(args) {
        let (name, _) = parse_named_view(definition)?;
        ensure_unique_name(&mut names, name, "SQL view")?;
    }
    Ok(())
}

async fn register_sql_views(
    session: &RustSparkSession,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error>> {
    validate_view_names(args)?;
    for (definition, materialized) in view_definitions(args) {
        let (name, sql) = parse_named_view(definition)?;
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
        let mut sql = if path.as_os_str() == "-" {
            read_sql_stdin().await?
        } else {
            std::fs::read_to_string(path)?
        };
        if sql.starts_with('\u{feff}') {
            sql.replace_range(..'\u{feff}'.len_utf8(), "");
        }
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

#[cfg(test)]
mod view_validation_tests {
    use super::{Args, parsed_union_definitions, validate_view_names};
    use clap::Parser;

    #[test]
    fn validates_all_union_names_before_registration() {
        let args = Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--union-by-name",
            "combined=left,right",
            "--union-distinct-by-name",
            "COMBINED=other,table",
        ])
        .unwrap();
        let error = parsed_union_definitions(&args).unwrap_err();
        assert!(error.to_string().contains("Duplicate union destination"));
    }

    #[test]
    fn preserves_union_order_and_distinct_mode() {
        let args = Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--union-by-name",
            "first=one,two",
            "--union-distinct-by-name",
            "second=three,four",
        ])
        .unwrap();
        let parsed = parsed_union_definitions(&args).unwrap();
        assert_eq!(parsed[0], ("first", vec!["one", "two"], false));
        assert_eq!(parsed[1], ("second", vec!["three", "four"], true));
    }

    #[tokio::test]
    async fn sql_files_accept_utf8_bom_without_changing_query_text() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("query.sql");
        std::fs::write(&file, "\u{feff}SELECT 'é' AS value").unwrap();
        let args =
            Args::try_parse_from(["rustspark", "--sql-file", file.to_str().unwrap()]).unwrap();
        let sql = super::read_query(&args).await.unwrap();
        assert_eq!(sql.as_ref(), "SELECT 'é' AS value");
    }

    #[test]
    fn rejects_duplicate_lazy_and_materialized_view_names() {
        let args = Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--sql-view",
            "events=SELECT 1",
            "--materialized-sql-view",
            "EVENTS=SELECT 2",
        ])
        .unwrap();
        let error = validate_view_names(&args).expect_err("duplicate views");
        assert!(error.to_string().contains("Duplicate SQL view"), "{error}");
    }
}
