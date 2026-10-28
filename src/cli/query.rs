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

// Keep pagination overflow checks identical for streamed and displayed results.
fn limit_for_output(
    dataframe: DataFrame,
    offset: usize,
    max_rows: usize,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    offset
        .checked_add(max_rows)
        .ok_or("--offset plus --max-rows exceeds the supported range")?;
    Ok(dataframe.limit(offset, Some(max_rows))?)
}

enum FileExport {
    Parquet(Box<Option<TableParquetOptions>>),
    Csv,
    Json,
}

fn requested_file_export(args: &Args) -> Option<(&Path, FileExport)> {
    if let Some(path) = args.output_parquet.as_deref() {
        let writer_options = args.parquet_compression.as_ref().map(|codec| {
            let mut options = TableParquetOptions::default();
            options.global.compression = Some(codec.clone());
            options
        });
        Some((path, FileExport::Parquet(Box::new(writer_options))))
    } else if let Some(path) = args.output_csv.as_deref() {
        Some((path, FileExport::Csv))
    } else {
        args.output_json
            .as_deref()
            .map(|path| (path, FileExport::Json))
    }
}

fn temporary_export_staging(
    parent: &Path,
    single_file: bool,
) -> Result<Option<tempfile::TempDir>, Box<dyn std::error::Error>> {
    if !single_file {
        return Ok(None);
    }
    Ok(Some(
        tempfile::Builder::new()
            .prefix(".rustspark-export-")
            .tempdir_in(parent)?,
    ))
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
    let staging = temporary_export_staging(parent, single_file)?;
    let path = staging
        .as_ref()
        .map(|directory| directory.path().join("result"))
        .unwrap_or_else(|| destination.to_path_buf());
    let output = path.to_str().ok_or("Output path must be valid UTF-8")?;
    write_export_contents(dataframe, output, format, single_file).await?;
    if single_file {
        publish_single_file_export(&path, destination, parent)?;
    }
    Ok(())
}

fn publish_single_file_export(
    path: &Path,
    destination: &Path,
    parent: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    // Both paths are on the same filesystem. Sync the finished part
    // before renaming it into place to avoid another full-size copy.
    let completed = fs::OpenOptions::new().write(true).open(path)?;
    completed.sync_all()?;
    drop(completed);
    if let Err(rename_error) = fs::rename(path, destination) {
        // Some platforms cannot rename over an existing destination.
        // Preserve the previous cross-platform atomic replacement path.
        if !destination.exists() {
            return Err(rename_error.into());
        }
        let mut completed = tempfile::NamedTempFile::new_in(parent)?;
        io::copy(&mut fs::File::open(path)?, completed.as_file_mut())?;
        completed.as_file_mut().sync_all()?;
        completed.persist(destination)?;
    }
    Ok(())
}

async fn write_export_contents(
    dataframe: DataFrame,
    output: &str,
    format: FileExport,
    single_file: bool,
) -> Result<(), Box<dyn std::error::Error>> {
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
    if args.describe || args.explain || args.explain_analyze {
        // Every statement in an inspection script must be read-only, not
        // just the final statement whose schema or plan is printed.
        for statement in statements {
            drain_dataframe(plan_read_only_query(session, &statement.to_string(), args).await?)
                .await?;
        }
        return execute_query(session, &last.to_string(), args).await;
    }
    for statement in statements {
        execute_script_statement(session, &statement, args, false).await?;
    }
    execute_script_statement(session, &last, args, true).await
}

async fn execute_script_statement(
    session: &RustSparkSession,
    statement: &DFStatement,
    args: &Args,
    final_statement: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some((source, destination)) = persistent_rename_table(statement, args)? {
        let root = super::database_root(args)?;
        return super::persistence::rename_saved_table(session, root, &source, &destination).await;
    }
    validate_persistent_table_name(statement, args)?;
    let dropped = persistent_drop_table(statement, args)?;
    let sql = statement.to_string();
    if let Some(target) = persistent_insert_table(statement, args)? {
        execute_staged_insert(session, &sql, args, &target, final_statement).await?;
    } else if final_statement {
        execute_query(session, &sql, args).await?;
    } else {
        drain_dataframe(plan_query(session, &sql, args).await?).await?;
    }
    persist_created_table(session, statement, args).await?;
    persist_dropped_table(dropped, args)
}

// Extract DataFusion's SQL AST once for persistent DDL and INSERT handling.
fn sql_statement(statement: &DFStatement) -> Option<&SqlStatement> {
    let DFStatement::Statement(statement) = statement else {
        return None;
    };
    Some(statement.as_ref())
}

fn persistent_table_is_directory(root: &Path, name: &str) -> bool {
    root.join(name).is_dir()
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
    let Some(SqlStatement::Insert(insert)) = sql_statement(statement) else {
        return Ok(None);
    };
    let name = insert.table.to_string().to_ascii_lowercase();
    if !super::valid_identifier(&name) {
        return Err("Persistent INSERT requires a simple unquoted table name".into());
    }
    let persisted = persistent_table_is_directory(root, &name);
    if insert.overwrite && persisted {
        return Err("Persistent INSERT OVERWRITE is unsupported".into());
    }
    if persisted {
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
    use std::sync::atomic::{AtomicBool, Ordering};
    static PAUSED_ONCE: AtomicBool = AtomicBool::new(false);
    if let Ok(raw) = std::env::var("RUSTSPARK_TEST_INSERT_PAUSE_ONCE_MS") {
        if !PAUSED_ONCE.swap(true, Ordering::SeqCst)
            && let Ok(ms) = raw.parse::<u64>()
        {
            tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        }
        return;
    }
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
    let root = super::database_root(args)?;
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
    // DataFusion INSERT produces a small DML result. Materialize that result
    // before publishing, but do not display it until the storage commit has
    // succeeded. This also avoids re-executing the write after publication.
    let execution: Result<Option<DataFrame>, Box<dyn std::error::Error>> = async {
        let dataframe = plan_query(session, sql, args).await?;
        if final_statement {
            let schema = std::sync::Arc::new(dataframe.schema().as_arrow().clone());
            let mut batches = dataframe.collect().await?;
            if batches.is_empty() {
                batches.push(datafusion::arrow::record_batch::RecordBatch::new_empty(
                    schema,
                ));
            }
            Ok(Some(session.read_batches(batches)?))
        } else {
            drain_dataframe(dataframe).await?;
            Ok(None)
        }
    }
    .await;
    if !session.deregister_table(table)? {
        return Err(format!("Staged table disappeared: {table}").into());
    }
    let (outcome, result_to_present) = match execution {
        Ok(result) => (stage.commit(), result),
        Err(error) => {
            let _ = stage.discard();
            (Err(error), None)
        }
    };
    // The original provider is restored on cancellation or re-registration
    // error. The disk path remains valid before and after commit.
    super::persistence::register_saved_table(session, table, &root.join(table)).await?;
    guard.active = false;
    outcome?;
    if let Some(dataframe) = result_to_present {
        present_query_result(dataframe, args).await?;
    }
    Ok(())
}

fn persistent_create_table<'a>(
    statement: &'a DFStatement,
    args: &Args,
) -> Option<&'a datafusion::logical_expr::sqlparser::ast::CreateTable> {
    args.database_dir.as_ref()?;
    let SqlStatement::CreateTable(create) = sql_statement(statement)? else {
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
    let Some(SqlStatement::AlterTable(alter)) = sql_statement(statement) else {
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
    if !persistent_table_is_directory(root, &old_name) {
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
    let Some(SqlStatement::Drop {
        object_type: ObjectType::Table,
        names,
        ..
    }) = sql_statement(statement)
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
    if persistent_table_is_directory(root, &name) {
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
        let root = super::database_root(args)?;
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
    let directory = super::database_root(args)?;
    let name = create.name.to_string().to_ascii_lowercase();
    if create.if_not_exists && directory.join(&name).exists() {
        return Ok(());
    }
    super::persistence::save_table(session.table(&name).await?, directory, &name, None).await?;
    if !session.deregister_table(&name)? {
        return Err(format!("Created table '{name}' disappeared before registration").into());
    }
    super::persistence::register_saved_table(session, &name, &directory.join(&name)).await?;
    Ok(())
}

fn bind_query_params(
    dataframe: DataFrame,
    args: &Args,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    if !args.named_params.is_empty() {
        let params: HashMap<_, _> = args.named_params.iter().cloned().collect();
        Ok(dataframe.with_param_values(params)?)
    } else if !args.params.is_empty() {
        Ok(dataframe.with_param_values(args.params.clone())?)
    } else {
        Ok(dataframe)
    }
}

async fn plan_with_args(
    session: &RustSparkSession,
    query: &str,
    args: &Args,
    read_only: bool,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let dataframe = if read_only {
        session.sql_read_only(query).await?
    } else {
        session.sql(query).await?
    };
    bind_query_params(dataframe, args)
}

pub(super) async fn plan_query(
    session: &RustSparkSession,
    query: &str,
    args: &Args,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    plan_with_args(session, query, args, false).await
}

async fn plan_read_only_query(
    session: &RustSparkSession,
    query: &str,
    args: &Args,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    plan_with_args(session, query, args, true).await
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
    let dataframe = if args.describe || args.explain || args.explain_analyze {
        plan_read_only_query(session, query, args).await?
    } else {
        plan_query(session, query, args).await?
    };
    present_query_result(dataframe, args).await
}

async fn present_query_result(
    dataframe: DataFrame,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(name) = args.save_table.as_deref() {
        let directory = args
            .database_dir
            .as_deref()
            .ok_or("--save-table requires --database-dir")?;
        super::persistence::save_table(
            dataframe,
            directory,
            name,
            args.parquet_compression.as_deref(),
        )
        .await?;
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
    } else if let Some((path, format)) = requested_file_export(args) {
        write_file_export(dataframe, path, format, args.output_single_file).await?;
    } else if let Some(path) = args.output_arrow.as_ref() {
        write_arrow_ipc(dataframe, path, args.arrow_compression).await?;
    } else if let Some(format) = args.stdout_format {
        let dataframe = if let Some(max_rows) = args.max_rows {
            limit_for_output(dataframe, args.offset.unwrap_or(0), max_rows)?
        } else {
            dataframe
        };
        if let Err(error) = write_stdout(dataframe, format, !args.stdout_no_header).await
            && !is_broken_pipe(error.as_ref())
        {
            return Err(error);
        }
    } else if args.count_only {
        println!("{}", dataframe.count().await?);
    } else if let Some(max_rows) = args.max_rows {
        if let Some(offset) = args.offset {
            limit_for_output(dataframe, offset, max_rows)?
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

#[cfg(test)]
mod export_staging_tests {
    use super::temporary_export_staging;

    #[test]
    fn only_single_file_exports_allocate_staging() {
        let root = tempfile::tempdir().unwrap();
        assert!(temporary_export_staging(root.path(), false).unwrap().is_none());
        let staged = temporary_export_staging(root.path(), true).unwrap();
        assert!(staged.unwrap().path().is_dir());
    }
}
