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
    max_parallel: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    if jobs.len() >= max_parallel {
        jobs.join_next()
            .await
            .expect("registration task exists")??;
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum NamedInputKind {
    Csv,
    Parquet,
    Json,
}

const INPUT_KINDS: [NamedInputKind; 3] = [
    NamedInputKind::Csv,
    NamedInputKind::Parquet,
    NamedInputKind::Json,
];

impl NamedInputKind {
    fn unnamed_path(self, args: &Args) -> Option<&Path> {
        match self {
            Self::Csv => args.csv.as_deref(),
            Self::Parquet => args.parquet.as_deref(),
            Self::Json => args.json.as_deref(),
        }
    }

    fn named_tables(self, args: &Args) -> &[String] {
        match self {
            Self::Csv => &args.csv_tables,
            Self::Parquet => &args.parquet_tables,
            Self::Json => &args.json_tables,
        }
    }
}

async fn register_named_input(
    session: &RustSparkSession,
    args: &Args,
    name: &str,
    path: &Path,
    kind: NamedInputKind,
) -> datafusion::error::Result<()> {
    match kind {
        NamedInputKind::Csv => register_csv_input(session, args, name, path).await,
        NamedInputKind::Parquet => {
            session
                .register_parquet_with_options(name, path, parquet_read_options(args))
                .await
        }
        NamedInputKind::Json => {
            session
                .register_json_with_options(name, path, json_read_options(args))
                .await
        }
    }
}

async fn register_unnamed_input(
    session: &RustSparkSession,
    args: &Args,
) -> datafusion::error::Result<bool> {
    let input = INPUT_KINDS
        .into_iter()
        .find_map(|kind| kind.unnamed_path(args).map(|path| (path, kind)));
    let Some((path, kind)) = input else {
        return Ok(false);
    };
    register_named_input(session, args, &args.table, path, kind).await?;
    Ok(true)
}

pub(super) async fn register_inputs(
    session: &Arc<RustSparkSession>,
    args: &Arc<Args>,
) -> Result<(), Box<dyn std::error::Error>> {
    if register_unnamed_input(session, args).await? {
        return Ok(());
    }
    let mut names = HashSet::new();
    let mut registration_jobs = tokio::task::JoinSet::new();
    // Parallel metadata and CSV inference work scales with available cores,
    // but bound outstanding jobs to avoid loading hundreds of schemas at once.
    let max_parallel =
        std::thread::available_parallelism().map_or(4, |cores| cores.get().clamp(4, 16));
    for kind in INPUT_KINDS {
        for mapping in kind.named_tables(args) {
            let (name, path) = parse_unique_named_table(mapping, &mut names)?;
            let session = Arc::clone(session);
            let args = Arc::clone(args);
            let name = name.to_owned();
            let path = path.to_owned();
            registration_jobs.spawn(async move {
                register_named_input(&session, &args, &name, Path::new(&path), kind).await
            });
            finish_registration_when_full(&mut registration_jobs, max_parallel).await?;
        }
    }
    while let Some(result) = registration_jobs.join_next().await {
        result??;
    }
    Ok(())
}
