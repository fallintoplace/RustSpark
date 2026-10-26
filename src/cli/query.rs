use std::{
    collections::{HashMap, HashSet},
    fs, io,
    path::Path,
};

use datafusion::{
    config::TableParquetOptions,
    dataframe::DataFrameWriteOptions,
    logical_expr::sqlparser::ast::{
        AlterTableOperation, ObjectType, RenameTableNameKind, Statement as SqlStatement,
    },
    prelude::DataFrame,
    sql::parser::{DFParser, Statement as DFStatement},
};
use futures::StreamExt;
use rustspark::RustSparkSession;

use super::output::is_broken_pipe;
use super::{Args, write_arrow_ipc, write_stdout};

fn output_write_options(single_file: bool) -> DataFrameWriteOptions {
    let options = DataFrameWriteOptions::new();
    if single_file {
        options.with_single_file_output(true)
    } else {
        options
    }
}

enum FileExport {
    Parquet(Box<Option<TableParquetOptions>>),
    Csv,
    Json,
}

/// Keep the old destination untouched until the entire single-file export
/// completes. The temporary directory is deleted on every error path.
async fn write_file_export(
    dataframe: DataFrame,
    destination: &Path,
    format: FileExport,
    single_file: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let staging = if single_file {
        Some(
            tempfile::Builder::new()
                .prefix(".rustspark-export-")
                .tempdir_in(parent)?,
        )
    } else {
        None
    };
    let path = staging
        .as_ref()
        .map(|directory| directory.path().join("result"))
        .unwrap_or_else(|| destination.to_path_buf());
    let output = path.to_str().ok_or("Output path must be valid UTF-8")?;
    let options = output_write_options(single_file);
    match format {
        FileExport::Parquet(parquet_options) => {
            dataframe
                .write_parquet(output, options, *parquet_options)
                .await?;
        }
        FileExport::Csv => {
            dataframe.write_csv(output, options, None).await?;
        }
        FileExport::Json => {
            dataframe.write_json(output, options, None).await?;
        }
    }
    if single_file {
        // NamedTempFile::persist replaces an existing destination across
        // platforms. Copying through it keeps the completed file on the same
        // filesystem and avoids publishing a partial result on failure.
        let mut completed = tempfile::NamedTempFile::new_in(parent)?;
        io::copy(&mut fs::File::open(&path)?, completed.as_file_mut())?;
        completed.as_file_mut().sync_all()?;
        completed.persist(destination)?;
    }
    Ok(())
}

/// Fully execute intermediate SQL without buffering rows that the CLI discards.
async fn drain_dataframe(dataframe: DataFrame) -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = dataframe.execute_stream().await?;
    while let Some(batch) = stream.next().await {
        batch?;
    }
    Ok(())
}

// Parse with DataFusion's dialect to avoid splitting on semicolons in SQL literals
// or comments. Execute setup statements before presenting the final result.
pub(super) async fn execute_script(
    session: &RustSparkSession,
    script: &str,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error>> {
    let script = script.strip_prefix('\u{feff}').unwrap_or(script);
    let mut statements = DFParser::parse_sql(script)?;
    let last = statements
        .pop_back()
        .ok_or("SQL script must contain at least one statement")?;
    for statement in statements {
        if let Some((source, destination)) = persistent_rename_table(&statement, args)? {
            let root = args
                .database_dir
                .as_deref()
                .ok_or("Missing database directory")?;
            super::persistence::rename_saved_table(session, root, &source, &destination).await?;
            continue;
        }
        validate_persistent_table_name(&statement, args)?;
        let dropped = persistent_drop_table(&statement, args)?;
        if let Some(target) = persistent_insert_table(&statement, args)? {
            execute_staged_insert(session, &statement.to_string(), args, &target, false).await?;
        } else {
            drain_dataframe(plan_query(session, &statement.to_string(), args).await?).await?;
        }
        persist_created_table(session, &statement, args).await?;
        persist_dropped_table(dropped, args)?;
    }
    if let Some((source, destination)) = persistent_rename_table(&last, args)? {
        let root = args
            .database_dir
            .as_deref()
            .ok_or("Missing database directory")?;
        return super::persistence::rename_saved_table(session, root, &source, &destination).await;
    }
    validate_persistent_table_name(&last, args)?;
    let dropped = persistent_drop_table(&last, args)?;
    if let Some(target) = persistent_insert_table(&last, args)? {
        execute_staged_insert(session, &last.to_string(), args, &target, true).await?;
    } else {
        execute_query(session, &last.to_string(), args).await?;
    }
    persist_created_table(session, &last, args).await?;
    persist_dropped_table(dropped, args)
}

/// Route a persistent INSERT to a staged copy, never the visible table files.
/// Unquoted, local table names only. External registrations keep DataFusion's
/// normal INSERT behavior.
fn persistent_insert_table(
    statement: &DFStatement,
    args: &Args,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let Some(root) = args.database_dir.as_deref() else {
        return Ok(None);
    };
    let DFStatement::Statement(statement) = statement else {
        return Ok(None);
    };
    let SqlStatement::Insert(insert) = statement.as_ref() else {
        return Ok(None);
    };
    let name = insert.table.to_string().to_ascii_lowercase();
    if !super::valid_identifier(&name) {
        return Err("Persistent INSERT requires a simple unquoted table name".into());
    }
    if insert.overwrite && root.join(&name).is_dir() {
        return Err("Persistent INSERT OVERWRITE is unsupported".into());
    }
    if root.join(&name).is_dir() {
        if args.save_table.is_some() {
            return Err("--save-table cannot be used with persistent INSERT".into());
        }
        Ok(Some(name))
    } else {
        Ok(None)
    }
}

// Deterministically exercise task cancellation while a private INSERT
// provider is registered. Only debug builds support this test hook.
#[cfg(debug_assertions)]
async fn debug_pause_staged_insert() {
    if let Ok(raw) = std::env::var("RUSTSPARK_TEST_INSERT_PAUSE_MS")
        && let Ok(ms) = raw.parse::<u64>()
    {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }
}

#[cfg(not(debug_assertions))]
async fn debug_pause_staged_insert() {}

/// A dropped execution future (for example an interactive query timeout)
/// must restore the original provider before another SQL statement starts.
struct RestoreTableOnCancel<'a> {
    session: &'a RustSparkSession,
    table: &'a str,
    original: std::sync::Arc<dyn datafusion::datasource::TableProvider>,
    active: bool,
}

impl Drop for RestoreTableOnCancel<'_> {
    fn drop(&mut self) {
        if self.active {
            let _ = self.session.deregister_table(self.table);
            let _ = self
                .session
                .register_table_provider(self.table, self.original.clone());
        }
    }
}

async fn execute_staged_insert(
    session: &RustSparkSession,
    sql: &str,
    args: &Args,
    table: &str,
    final_statement: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let root = args
        .database_dir
        .as_deref()
        .ok_or("Missing database directory")?;
    let stage = super::persistence::StagedInsert::prepare(root, table)?;
    let original = session.table_provider(table).await?;
    let mut guard = RestoreTableOnCancel {
        session,
        table,
        original,
        active: true,
    };
    if !session.deregister_table(table)? {
        stage.discard()?;
        return Err(format!("Persistent table disappeared: {table}").into());
    }
    if let Err(error) =
        super::persistence::register_saved_table(session, table, stage.data_path()).await
    {
        let _ = stage.discard();
        return Err(error);
    }

    // Keep the guard alive while awaiting SQL and re-registration. Cancellation
    // cannot leave an interactive session pointed at an unpublished table.
    debug_pause_staged_insert().await;
    let execution: Result<(), Box<dyn std::error::Error>> = async {
        if final_statement {
            execute_query(session, sql, args).await
        } else {
            drain_dataframe(plan_query(session, sql, args).await?).await?;
            Ok(())
        }
    }
    .await;
    if !session.deregister_table(table)? {
        return Err(format!("Staged table disappeared: {table}").into());
    }
    let outcome = match execution {
        Ok(()) => stage.commit(),
        Err(error) => {
            let _ = stage.discard();
            Err(error)
        }
    };
    // The original provider is restored on cancellation or re-registration
    // error. The disk path remains valid before and after commit.
    super::persistence::register_saved_table(session, table, &root.join(table)).await?;
    guard.active = false;
    outcome
}

fn persistent_create_table<'a>(
    statement: &'a DFStatement,
    args: &Args,
) -> Option<&'a datafusion::logical_expr::sqlparser::ast::CreateTable> {
    args.database_dir.as_ref()?;
    let DFStatement::Statement(statement) = statement else {
        return None;
    };
    let SqlStatement::CreateTable(create) = statement.as_ref() else {
        return None;
    };
    Some(create)
}

fn validate_persistent_table_name(
    statement: &DFStatement,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(create) = persistent_create_table(statement, args) {
        let name = create.name.to_string();
        if !super::valid_identifier(&name) {
            return Err(format!(
                "Persistent CREATE TABLE requires a single unquoted SQL identifier: {name}"
            )
            .into());
        }
    }
    Ok(())
}

/// Handle only simple, single-table renames owned by the on-disk database.
/// Other ALTER TABLE operations keep DataFusion's normal unsupported behavior.
fn persistent_rename_table(
    statement: &DFStatement,
    args: &Args,
) -> Result<Option<(String, String)>, Box<dyn std::error::Error>> {
    let Some(root) = args.database_dir.as_deref() else {
        return Ok(None);
    };
    let DFStatement::Statement(statement) = statement else {
        return Ok(None);
    };
    let SqlStatement::AlterTable(alter) = statement.as_ref() else {
        return Ok(None);
    };
    let [AlterTableOperation::RenameTable { table_name }] = alter.operations.as_slice() else {
        return Ok(None);
    };
    let new_name = match table_name {
        RenameTableNameKind::To(name) | RenameTableNameKind::As(name) => {
            name.to_string().to_ascii_lowercase()
        }
    };
    let old_name = alter.name.to_string().to_ascii_lowercase();
    if !super::valid_identifier(&old_name) || !super::valid_identifier(&new_name) {
        return Err("Persistent ALTER TABLE RENAME requires simple SQL table names".into());
    }
    if !root.join(&old_name).is_dir() {
        return Ok(None);
    }
    Ok(Some((old_name, new_name)))
}

/// Remember persistent DROP TABLE targets before DataFusion unregisters them.
/// File deletion follows successful SQL execution, never SQL planning.
fn persistent_drop_table(
    statement: &DFStatement,
    args: &Args,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let Some(root) = args.database_dir.as_deref() else {
        return Ok(None);
    };
    let DFStatement::Statement(statement) = statement else {
        return Ok(None);
    };
    let SqlStatement::Drop {
        object_type: ObjectType::Table,
        names,
        ..
    } = statement.as_ref()
    else {
        return Ok(None);
    };
    if names.len() != 1 {
        return Err("Persistent DROP TABLE requires exactly one table".into());
    }
    let name = names[0].to_string().to_ascii_lowercase();
    if !super::valid_identifier(&name) {
        return Err(format!("Persistent DROP TABLE requires a simple table name: {name}").into());
    }
    if root.join(&name).is_dir() {
        Ok(Some(name))
    } else {
        Ok(None)
    }
}

fn persist_dropped_table(
    name: Option<String>,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(name) = name {
        let root = args
            .database_dir
            .as_deref()
            .ok_or("Missing database directory")?;
        super::persistence::drop_saved_table(root, &name)?;
    }
    Ok(())
}

async fn persist_created_table(
    session: &RustSparkSession,
    statement: &DFStatement,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(create) = persistent_create_table(statement, args) else {
        return Ok(());
    };
    let directory = args
        .database_dir
        .as_deref()
        .ok_or("Missing database directory")?;
    let name = create.name.to_string().to_ascii_lowercase();
    if create.if_not_exists && directory.join(&name).exists() {
        return Ok(());
    }
    super::persistence::save_table(session.table(&name).await?, directory, &name).await?;
    if !session.deregister_table(&name)? {
        return Err(format!("Created table '{name}' disappeared before registration").into());
    }
    super::persistence::register_saved_table(session, &name, &directory.join(&name)).await?;
    Ok(())
}

pub(super) async fn plan_query(
    session: &RustSparkSession,
    query: &str,
    args: &Args,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let dataframe = if !args.named_params.is_empty() {
        let params: HashMap<_, _> = args.named_params.iter().cloned().collect();
        session.sql_with_params(query, params).await?
    } else if !args.params.is_empty() {
        session.sql_with_params(query, args.params.clone()).await?
    } else {
        session.sql(query).await?
    };
    Ok(dataframe)
}

fn reject_duplicate_output_fields(dataframe: &DataFrame) -> Result<(), Box<dyn std::error::Error>> {
    let mut names = HashSet::new();
    for field in dataframe.schema().fields() {
        if !names.insert(field.name()) {
            return Err(format!("Duplicate output column: {}", field.name()).into());
        }
    }
    Ok(())
}

async fn execute_query(
    session: &RustSparkSession,
    query: &str,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error>> {
    let dataframe = plan_query(session, query, args).await?;
    if let Some(name) = args.save_table.as_deref() {
        let directory = args
            .database_dir
            .as_deref()
            .ok_or("--save-table requires --database-dir")?;
        super::persistence::save_table(dataframe, directory, name).await?;
        return Ok(());
    }
    if args.stdout_format.is_some() || args.output_csv.is_some() || args.output_json.is_some() {
        reject_duplicate_output_fields(&dataframe)?;
    }
    if args.describe {
        for field in dataframe.schema().fields() {
            let nullability = if field.is_nullable() {
                "NULL"
            } else {
                "NOT NULL"
            };
            println!("{}\t{}\t{nullability}", field.name(), field.data_type());
        }
    } else if args.explain {
        dataframe.explain(false, false)?.show().await?;
    } else if args.explain_analyze {
        dataframe.explain(true, false)?.show().await?;
    } else if let Some(path) = args.output_parquet.as_ref() {
        let writer_options = args.parquet_compression.as_ref().map(|codec| {
            let mut options = TableParquetOptions::default();
            options.global.compression = Some(codec.clone());
            options
        });
        write_file_export(
            dataframe,
            path,
            FileExport::Parquet(Box::new(writer_options)),
            args.output_single_file,
        )
        .await?;
    } else if let Some(path) = args.output_csv.as_ref() {
        write_file_export(dataframe, path, FileExport::Csv, args.output_single_file).await?;
    } else if let Some(path) = args.output_json.as_ref() {
        write_file_export(dataframe, path, FileExport::Json, args.output_single_file).await?;
    } else if let Some(path) = args.output_arrow.as_ref() {
        write_arrow_ipc(dataframe, path).await?;
    } else if let Some(format) = args.stdout_format {
        let dataframe = if let Some(max_rows) = args.max_rows {
            let offset = args.offset.unwrap_or(0);
            offset
                .checked_add(max_rows)
                .ok_or("--offset plus --max-rows exceeds the supported range")?;
            dataframe.limit(offset, Some(max_rows))?
        } else {
            dataframe
        };
        if let Err(error) = write_stdout(dataframe, format).await
            && !is_broken_pipe(error.as_ref())
        {
            return Err(error);
        }
    } else if args.count_only {
        println!("{}", dataframe.count().await?);
    } else if let Some(max_rows) = args.max_rows {
        if let Some(offset) = args.offset {
            offset
                .checked_add(max_rows)
                .ok_or("--offset plus --max-rows exceeds the supported range")?;
            dataframe
                .limit(offset, Some(max_rows))?
                .show_limit(max_rows)
                .await?;
        } else {
            dataframe.show_limit(max_rows).await?;
        }
    } else {
        dataframe.show().await?;
    }
    Ok(())
}
