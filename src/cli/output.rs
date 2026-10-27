use std::{
    io::{BufWriter, Write},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use datafusion::{
    arrow::ipc::{
        CompressionType,
        writer::{FileWriter, IpcWriteOptions},
    },
    prelude::DataFrame,
};
use futures::StreamExt;

use super::{ArrowCompression, StdoutFormat};

pub(super) fn is_broken_pipe(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut source = Some(error);
    while let Some(current) = source {
        if current
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::BrokenPipe)
        {
            return true;
        }
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
        writer
            .write(&datafusion::arrow::record_batch::RecordBatch::new_empty(
                schema,
            ))
            .map_err(|error| preserve_writer_pipe_error(error, broken_pipe))?;
    }
    Ok(())
}

fn display_cell(
    column: &datafusion::arrow::array::ArrayRef,
    row: usize,
) -> datafusion::arrow::error::Result<std::borrow::Cow<'static, str>> {
    if column.is_null(row) {
        Ok(std::borrow::Cow::Borrowed("NULL"))
    } else {
        datafusion::arrow::util::display::array_value_to_string(column.as_ref(), row)
            .map(std::borrow::Cow::Owned)
    }
}

fn escape_vertical_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '\r' => escaped.push_str("\\r"),
            '\n' => escaped.push_str("\\n"),
            '\t' => escaped.push_str("\\t"),
            other => escaped.push(other),
        }
    }
    escaped
}

async fn write_vertical<W: Write>(
    stream: &mut datafusion::physical_plan::SendableRecordBatchStream,
    output: &mut W,
    schema: &datafusion::arrow::datatypes::SchemaRef,
) -> Result<(), Box<dyn std::error::Error>> {
    let escaped_headers: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| escape_vertical_value(field.name()))
        .collect();
    let mut row_number = 0_usize;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        for row in 0..batch.num_rows() {
            row_number += 1;
            writeln!(output, "--- Row {row_number} ---")?;
            for (index, header) in escaped_headers.iter().enumerate() {
                let column = batch.column(index);
                let value = display_cell(column, row)?;
                writeln!(output, "{header}: {}", escape_vertical_value(&value))?;
            }
        }
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
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '\\' => escaped.push_str("\\\\"),
            '|' => escaped.push_str("\\|"),
            '\r' => escaped.push(' '),
            '\n' => escaped.push_str("<br>"),
            other => escaped.push(other),
        }
    }
    escaped
}

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
                let value = display_cell(column, row)?;
                write!(output, " {} |", escape_markdown_cell(&value))?;
            }
            writeln!(output)?;
        }
    }
    Ok(())
}

fn escape_html_cell(value: &str) -> std::borrow::Cow<'_, str> {
    if !value
        .bytes()
        .any(|byte| matches!(byte, b'&' | b'<' | b'>' | b'"' | b'\''))
    {
        return std::borrow::Cow::Borrowed(value);
    }
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            other => escaped.push(other),
        }
    }
    std::borrow::Cow::Owned(escaped)
}

async fn write_html<W: Write>(
    stream: &mut datafusion::physical_plan::SendableRecordBatchStream,
    output: &mut W,
    schema: &datafusion::arrow::datatypes::SchemaRef,
) -> Result<(), Box<dyn std::error::Error>> {
    writeln!(output, "<table>")?;
    write!(output, "<thead><tr>")?;
    for field in schema.fields() {
        write!(output, "<th>{}</th>", escape_html_cell(field.name()))?;
    }
    writeln!(output, "</tr></thead>")?;
    writeln!(output, "<tbody>")?;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        for row in 0..batch.num_rows() {
            write!(output, "<tr>")?;
            for column in batch.columns() {
                if column.is_null(row) {
                    write!(output, "<td class=\"null\">NULL</td>")?;
                } else {
                    let value = datafusion::arrow::util::display::array_value_to_string(
                        column.as_ref(),
                        row,
                    )?;
                    write!(output, "<td>{}</td>", escape_html_cell(&value))?;
                }
            }
            writeln!(output, "</tr>")?;
        }
    }
    writeln!(output, "</tbody></table>")?;
    Ok(())
}

pub(super) async fn write_stdout(
    dataframe: datafusion::prelude::DataFrame,
    format: StdoutFormat,
    include_header: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = dataframe.execute_stream().await?;
    let schema = stream.schema();
    let stdout = std::io::stdout();
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
        StdoutFormat::Vertical => {
            write_vertical(&mut stream, &mut tracked, &schema).await?;
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
        StdoutFormat::Html => {
            write_html(&mut stream, &mut tracked, &schema).await?;
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
    compression: Option<ArrowCompression>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = dataframe.execute_stream().await?;
    let schema = stream.schema();
    let directory = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let temporary = tempfile::NamedTempFile::new_in(directory)?;
    let codec = compression.map(|compression| match compression {
        ArrowCompression::Lz4 => CompressionType::LZ4_FRAME,
        ArrowCompression::Zstd => CompressionType::ZSTD,
    });
    let options = IpcWriteOptions::default().try_with_compression(codec)?;
    let mut writer =
        FileWriter::try_new_with_options(BufWriter::new(temporary.reopen()?), &schema, options)?;
    while let Some(batch) = stream.next().await {
        writer.write(&batch?)?;
    }
    writer.finish()?;
    drop(writer);
    temporary.as_file().sync_all()?;
    temporary.persist(path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{display_cell, escape_vertical_value};

    #[test]
    fn escapes_vertical_controls_without_changing_unicode() {
        assert_eq!(escape_vertical_value("a\\b\r\n\té"), "a\\\\b\\r\\n\\té");
        assert_eq!(escape_vertical_value("plain"), "plain");
    }

    #[test]
    fn null_display_reuses_a_borrowed_constant() {
        let column: datafusion::arrow::array::ArrayRef =
            std::sync::Arc::new(datafusion::arrow::array::StringArray::from(vec![
                None,
                Some("value"),
            ]));
        assert!(matches!(
            display_cell(&column, 0).unwrap(),
            std::borrow::Cow::Borrowed("NULL")
        ));
        assert_eq!(display_cell(&column, 1).unwrap(), "value");
    }
}
