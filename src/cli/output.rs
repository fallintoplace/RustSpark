use std::{
    io::Write,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use datafusion::{arrow::ipc::writer::FileWriter, prelude::DataFrame};
use futures::StreamExt;

use super::StdoutFormat;

pub(super) fn is_broken_pipe(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut source = Some(error);
    while let Some(current) = source {
        if current
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::BrokenPipe)
        {
            return true;
        }
        // Arrow CSV/JSON writers may flatten I/O errors to string-only ArrowError values.
        if current
            .downcast_ref::<datafusion::arrow::error::ArrowError>()
            .is_some_and(|arrow| {
                let message = arrow.to_string().to_ascii_lowercase();
                message.contains("broken pipe")
                    || message.contains("pipe is being closed")
                    || message.contains("pipe has been ended")
            })
        {
            return true;
        }
        source = current.source();
    }
    false
}

fn track_broken_pipe<T>(
    result: std::io::Result<T>,
    broken_pipe: &AtomicBool,
) -> std::io::Result<T> {
    if result
        .as_ref()
        .is_err_and(|error| error.kind() == std::io::ErrorKind::BrokenPipe)
    {
        broken_pipe.store(true, Ordering::Relaxed);
    }
    result
}

struct PipeTrackingWriter<W> {
    inner: W,
    broken_pipe: Arc<AtomicBool>,
}

impl<W: Write> Write for PipeTrackingWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        track_broken_pipe(self.inner.write(bytes), &self.broken_pipe)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        track_broken_pipe(self.inner.flush(), &self.broken_pipe)
    }
}

fn preserve_writer_pipe_error(
    error: datafusion::arrow::error::ArrowError,
    broken_pipe: &AtomicBool,
) -> Box<dyn std::error::Error> {
    if broken_pipe.load(Ordering::Relaxed) {
        Box::new(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
    } else {
        Box::new(error)
    }
}

async fn write_delimited<W: Write>(
    stream: &mut datafusion::physical_plan::SendableRecordBatchStream,
    output: &mut W,
    schema: datafusion::arrow::datatypes::SchemaRef,
    delimiter: u8,
    include_header: bool,
    broken_pipe: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = datafusion::arrow::csv::WriterBuilder::new()
        .with_delimiter(delimiter)
        .with_header(include_header)
        .build(output);
    let mut wrote_batch = false;
    while let Some(batch) = stream.next().await {
        writer
            .write(&batch?)
            .map_err(|error| preserve_writer_pipe_error(error, broken_pipe))?;
        wrote_batch = true;
    }
    if !wrote_batch && include_header {
        // Include column headers even when the query returns no rows.
        writer
            .write(&datafusion::arrow::record_batch::RecordBatch::new_empty(
                schema,
            ))
            .map_err(|error| preserve_writer_pipe_error(error, broken_pipe))?;
    }
    Ok(())
}

async fn write_arrow_stream<W: Write>(
    stream: &mut datafusion::physical_plan::SendableRecordBatchStream,
    output: &mut W,
    schema: &datafusion::arrow::datatypes::SchemaRef,
    broken_pipe: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = datafusion::arrow::ipc::writer::StreamWriter::try_new(output, schema)
        .map_err(|error| preserve_writer_pipe_error(error, broken_pipe))?;
    while let Some(batch) = stream.next().await {
        writer
            .write(&batch?)
            .map_err(|error| preserve_writer_pipe_error(error, broken_pipe))?;
    }
    writer
        .finish()
        .map_err(|error| preserve_writer_pipe_error(error, broken_pipe))?;
    Ok(())
}

fn escape_markdown_cell(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('|', "\\|")
        .replace('\r', " ")
        .replace('\n', "<br>")
}

// Keep only one record batch and the current cell in memory.
async fn write_markdown<W: Write>(
    stream: &mut datafusion::physical_plan::SendableRecordBatchStream,
    output: &mut W,
    schema: &datafusion::arrow::datatypes::SchemaRef,
) -> Result<(), Box<dyn std::error::Error>> {
    write!(output, "|")?;
    for field in schema.fields() {
        write!(output, " {} |", escape_markdown_cell(field.name()))?;
    }
    writeln!(output)?;
    write!(output, "|")?;
    for _ in schema.fields() {
        write!(output, " --- |")?;
    }
    writeln!(output)?;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        for row in 0..batch.num_rows() {
            write!(output, "|")?;
            for column in batch.columns() {
                let value = if column.is_null(row) {
                    "NULL".to_owned()
                } else {
                    datafusion::arrow::util::display::array_value_to_string(
                        column.as_ref(),
                        row,
                    )?
                };
                write!(output, " {} |", escape_markdown_cell(&value))?;
            }
            writeln!(output)?;
        }
    }
    Ok(())
}

// Write one record batch at a time so stdout never requires collecting all results.
pub(super) async fn write_stdout(
    dataframe: datafusion::prelude::DataFrame,
    format: StdoutFormat,
    include_header: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = dataframe.execute_stream().await?;
    let schema = stream.schema();
    let stdout = std::io::stdout();
    // Buffer large result streams without accumulating Arrow batches.
    // Fewer writes to stdout improve CSV, JSON and IPC export throughput.
    let mut output = std::io::BufWriter::with_capacity(128 * 1024, stdout.lock());
    let broken_pipe = Arc::new(AtomicBool::new(false));
    let mut tracked = PipeTrackingWriter {
        inner: &mut output,
        broken_pipe: Arc::clone(&broken_pipe),
    };
    match format {
        StdoutFormat::Csv | StdoutFormat::Tsv => {
            let delimiter = if matches!(format, StdoutFormat::Tsv) {
                b'\t'
            } else {
                b','
            };
            write_delimited(
                &mut stream,
                &mut tracked,
                schema,
                delimiter,
                include_header,
                &broken_pipe,
            )
            .await?;
        }
        StdoutFormat::Json => {
            let mut writer = datafusion::arrow::json::writer::WriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, datafusion::arrow::json::writer::LineDelimited>(&mut tracked);
            while let Some(batch) = stream.next().await {
                writer
                    .write(&batch?)
                    .map_err(|error| preserve_writer_pipe_error(error, &broken_pipe))?;
            }
            writer
                .finish()
                .map_err(|error| preserve_writer_pipe_error(error, &broken_pipe))?;
        }
        StdoutFormat::JsonArray => {
            let mut writer = datafusion::arrow::json::writer::WriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, datafusion::arrow::json::writer::JsonArray>(&mut tracked);
            while let Some(batch) = stream.next().await {
                writer
                    .write(&batch?)
                    .map_err(|error| preserve_writer_pipe_error(error, &broken_pipe))?;
            }
            writer
                .finish()
                .map_err(|error| preserve_writer_pipe_error(error, &broken_pipe))?;
        }
        StdoutFormat::Markdown => {
            write_markdown(&mut stream, &mut tracked, &schema).await?;
        }
        StdoutFormat::Arrow => {
            write_arrow_stream(&mut stream, &mut tracked, &schema, &broken_pipe).await?;
        }
    }
    drop(tracked);
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
