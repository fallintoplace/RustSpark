use std::{collections::HashSet, path::Path, sync::Arc};

use rustspark::RustSparkSession;

use super::{
    Args, csv_read_options, json_read_options, parquet_read_options, parse_named_table,
    register_csv_with_null_value,
};

async fn reject_duplicate_csv_headers(
    session: &RustSparkSession,
    name: &str,
) -> datafusion::error::Result<()> {
    let dataframe = session.table(name).await?;
    let mut names = HashSet::new();
    for field in dataframe.schema().fields() {
        if !names.insert(field.name().to_ascii_lowercase()) {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "Duplicate CSV column: {}",
                field.name()
            )));
        }
    }
    Ok(())
}

fn parse_unique_named_table<'a>(
    mapping: &'a str,
    names: &mut HashSet<String>,
) -> Result<(&'a str, &'a str), Box<dyn std::error::Error>> {
    let (name, path) = parse_named_table(mapping)?;
    if !names.insert(name.to_ascii_lowercase()) {
        return Err(format!("Duplicate table name: {name}").into());
    }
    Ok((name, path))
}

async fn register_csv_input(
    session: &RustSparkSession,
    args: &Args,
    name: &str,
    path: &Path,
) -> datafusion::error::Result<()> {
    let options = csv_read_options(args);
    if let Some(marker) = args.csv_null_value.as_deref() {
        register_csv_with_null_value(session, name, path, options, marker).await?;
    } else {
        session
            .register_csv_with_options(name, path, options)
            .await?;
    }
    reject_duplicate_csv_headers(session, name).await
}

async fn finish_registration_when_full(
    jobs: &mut tokio::task::JoinSet<datafusion::error::Result<()>>,
) -> Result<(), Box<dyn std::error::Error>> {
    if jobs.len() >= 4 {
        jobs.join_next()
            .await
            .expect("registration task exists")??;
    }
    Ok(())
}

pub(super) async fn register_inputs(
    session: &Arc<RustSparkSession>,
    args: &Arc<Args>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(csv) = args.csv.as_ref() {
        register_csv_input(session, args, &args.table, csv.as_path()).await?;
    } else if let Some(parquet) = args.parquet.as_ref() {
        let options = parquet_read_options(args);
        session
            .register_parquet_with_options(&args.table, parquet, options)
            .await?;
    } else if let Some(json) = args.json.as_ref() {
        let options = json_read_options(args);
        session
            .register_json_with_options(&args.table, json, options)
            .await?;
    } else {
        let mut names = HashSet::new();
        let mut registration_jobs = tokio::task::JoinSet::new();
        for mapping in &args.csv_tables {
            let (name, path) = parse_unique_named_table(mapping, &mut names)?;
            let session = Arc::clone(session);
            let args = Arc::clone(args);
            let name = name.to_owned();
            let path = path.to_owned();
            registration_jobs.spawn(async move {
                register_csv_input(&session, &args, &name, Path::new(&path)).await
            });
            finish_registration_when_full(&mut registration_jobs).await?;
        }
        for mapping in &args.parquet_tables {
            let (name, path) = parse_unique_named_table(mapping, &mut names)?;
            let session = Arc::clone(session);
            let args = Arc::clone(args);
            let name = name.to_owned();
            let path = path.to_owned();
            registration_jobs.spawn(async move {
                let options = parquet_read_options(&args);
                session
                    .register_parquet_with_options(&name, &path, options)
                    .await
            });
            finish_registration_when_full(&mut registration_jobs).await?;
        }
        for mapping in &args.json_tables {
            let (name, path) = parse_unique_named_table(mapping, &mut names)?;
            let session = Arc::clone(session);
            let args = Arc::clone(args);
            let name = name.to_owned();
            let path = path.to_owned();
            registration_jobs.spawn(async move {
                let options = json_read_options(&args);
                session
                    .register_json_with_options(&name, &path, options)
                    .await
            });
            finish_registration_when_full(&mut registration_jobs).await?;
        }
        while let Some(result) = registration_jobs.join_next().await {
            result??;
        }
    }
    Ok(())
}
