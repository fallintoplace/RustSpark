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

struct PipeTrackingWriter<W> {
    inner: W,
    broken_pipe: Arc<AtomicBool>,
}

impl<W: Write> Write for PipeTrackingWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let result = self.inner.write(bytes);
        if result
            .as_ref()
            .is_err_and(|error| error.kind() == std::io::ErrorKind::BrokenPipe)
        {
            self.broken_pipe.store(true, Ordering::Relaxed);
        }
        result
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let result = self.inner.flush();
        if result
            .as_ref()
            .is_err_and(|error| error.kind() == std::io::ErrorKind::BrokenPipe)
        {
            self.broken_pipe.store(true, Ordering::Relaxed);
        }
        result
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

// Write one record batch at a time so stdout never requires collecting all results.
pub(super) async fn write_stdout(
    dataframe: datafusion::prelude::DataFrame,
    format: StdoutFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = dataframe.execute_stream().await?;
    let schema = stream.schema();
    let stdout = std::io::stdout();
    let mut output = std::io::BufWriter::new(stdout.lock());
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
            let mut writer = datafusion::arrow::csv::WriterBuilder::new()
                .with_delimiter(delimiter)
                .build(&mut tracked);
            let mut wrote_batch = false;
            while let Some(batch) = stream.next().await {
                writer
                    .write(&batch?)
                    .map_err(|error| preserve_writer_pipe_error(error, &broken_pipe))?;
                wrote_batch = true;
            }
            if !wrote_batch {
                // Include column headers even when the query returns no rows.
                writer
                    .write(&datafusion::arrow::record_batch::RecordBatch::new_empty(
                        schema,
                    ))
                    .map_err(|error| preserve_writer_pipe_error(error, &broken_pipe))?;
            }
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
        StdoutFormat::Arrow => {
            let mut writer =
                datafusion::arrow::ipc::writer::StreamWriter::try_new(&mut tracked, &schema)
                    .map_err(|error| preserve_writer_pipe_error(error, &broken_pipe))?;
            while let Some(batch) = stream.next().await {
                writer
                    .write(&batch?)
                    .map_err(|error| preserve_writer_pipe_error(error, &broken_pipe))?;
            }
            writer
                .finish()
                .map_err(|error| preserve_writer_pipe_error(error, &broken_pipe))?;
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
