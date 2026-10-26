use std::{collections::HashSet, sync::Arc};

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

pub(super) async fn register_inputs(
    session: &Arc<RustSparkSession>,
    args: &Arc<Args>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(csv) = args.csv.as_ref() {
        let options = csv_read_options(args);
        if let Some(marker) = args.csv_null_value.as_deref() {
            register_csv_with_null_value(session, &args.table, csv, options, marker).await?;
        } else {
            session
                .register_csv_with_options(&args.table, csv, options)
                .await?;
        }
        reject_duplicate_csv_headers(session, &args.table).await?;
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
            let (name, path) = parse_named_table(mapping)?;
            if !names.insert(name.to_ascii_lowercase()) {
                return Err(format!("Duplicate table name: {name}").into());
            }
            let session = Arc::clone(session);
            let args = Arc::clone(args);
            let name = name.to_owned();
            let path = path.to_owned();
            registration_jobs.spawn(async move {
                let options = csv_read_options(&args);
                if let Some(marker) = args.csv_null_value.as_deref() {
                    register_csv_with_null_value(&session, &name, &path, options, marker).await?;
                } else {
                    session
                        .register_csv_with_options(&name, &path, options)
                        .await?;
                }
                reject_duplicate_csv_headers(&session, &name).await
            });
            if registration_jobs.len() >= 4 {
                registration_jobs
                    .join_next()
                    .await
                    .expect("registration task exists")??;
            }
        }
        for mapping in &args.parquet_tables {
            let (name, path) = parse_named_table(mapping)?;
            if !names.insert(name.to_ascii_lowercase()) {
                return Err(format!("Duplicate table name: {name}").into());
            }
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
            if registration_jobs.len() >= 4 {
                registration_jobs
                    .join_next()
                    .await
                    .expect("registration task exists")??;
            }
        }
        for mapping in &args.json_tables {
            let (name, path) = parse_named_table(mapping)?;
            if !names.insert(name.to_ascii_lowercase()) {
                return Err(format!("Duplicate table name: {name}").into());
            }
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
            if registration_jobs.len() >= 4 {
                registration_jobs
                    .join_next()
                    .await
                    .expect("registration task exists")??;
            }
        }
        while let Some(result) = registration_jobs.join_next().await {
            result??;
        }
    }
    Ok(())
}
