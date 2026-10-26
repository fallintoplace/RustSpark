use std::{io::Write, path::Path};

use datafusion::{arrow::ipc::writer::FileWriter, prelude::DataFrame};
use futures::StreamExt;

use super::StdoutFormat;

// Write one record batch at a time so stdout never requires collecting all results.
pub(super) async fn write_stdout(
    dataframe: DataFrame,
    format: StdoutFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = dataframe.execute_stream().await?;
    let schema = stream.schema();
    let stdout = std::io::stdout();
    let mut output = std::io::BufWriter::new(stdout.lock());
    match format {
        StdoutFormat::Csv => {
            let mut writer = datafusion::arrow::csv::WriterBuilder::new().build(&mut output);
            let mut wrote_batch = false;
            while let Some(batch) = stream.next().await {
                writer.write(&batch?)?;
                wrote_batch = true;
            }
            if !wrote_batch {
                // Include column headers even when the query returns no rows.
                writer.write(&datafusion::arrow::record_batch::RecordBatch::new_empty(
                    schema,
                ))?;
            }
        }
        StdoutFormat::Json => {
            let mut writer = datafusion::arrow::json::writer::WriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, datafusion::arrow::json::writer::LineDelimited>(&mut output);
            while let Some(batch) = stream.next().await {
                writer.write(&batch?)?;
            }
            writer.finish()?;
        }
    }
    output.flush()?;
    Ok(())
}

pub(super) async fn write_arrow_ipc(
    dataframe: DataFrame,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = dataframe.execute_stream().await?;
    let schema = stream.schema();
    let directory = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let temporary = tempfile::NamedTempFile::new_in(directory)?;
    let mut writer = FileWriter::try_new_buffered(temporary.reopen()?, &schema)?;
    while let Some(batch) = stream.next().await {
        writer.write(&batch?)?;
    }
    writer.finish()?;
    drop(writer);
    temporary.persist(path)?;
    Ok(())
}
