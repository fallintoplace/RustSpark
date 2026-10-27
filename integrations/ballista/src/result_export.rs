use std::{fs, io::{self, BufWriter}, path::Path};

use datafusion::arrow::ipc::writer::FileWriter;
use futures::StreamExt;
use rustspark::RustSparkSession;

fn arrow_output_parent(destination: &Path) -> Result<&Path, Box<dyn std::error::Error>> {
    if destination.file_name().is_none() {
        return Err("RUSTSPARK_OUTPUT_ARROW must name a file".into());
    }
    match fs::symlink_metadata(destination) {
        Ok(_) => return Err(format!("Arrow output already exists: {}", destination.display()).into()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new(".")))
}

pub async fn write_arrow(
    session: &RustSparkSession,
    query: &str,
    destination: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let parent = arrow_output_parent(destination)?;
    let mut stream = session.sql_read_only_stream(query).await?;
    let schema = stream.schema();
    let temporary = tempfile::NamedTempFile::new_in(parent)?;
    let output = BufWriter::with_capacity(128 * 1024, temporary.reopen()?);
    let mut writer = FileWriter::try_new(output, &schema)?;
    while let Some(batch) = stream.next().await {
        writer.write(&batch?)?;
    }
    writer.finish()?;
    drop(writer);
    temporary.as_file().sync_all()?;
    temporary.persist_noclobber(destination)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::ipc::reader::FileReader;

    #[tokio::test]
    async fn writes_full_result_to_a_valid_arrow_file() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let destination = directory.path().join("results.arrow");
        write_arrow(
            &RustSparkSession::new(),
            "SELECT 11 AS id UNION ALL SELECT 12 AS id",
            &destination,
        )
        .await?;
        let reader = FileReader::try_new(fs::File::open(destination)?, None)?;
        assert_eq!(reader.schema().field(0).name(), "id");
        let mut row_count = 0;
        for batch in reader {
            row_count += batch?.num_rows();
        }
        assert_eq!(row_count, 2);
        Ok(())
    }

    #[tokio::test]
    async fn empty_results_still_write_the_schema() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let destination = directory.path().join("empty.arrow");
        write_arrow(
            &RustSparkSession::new(),
            "SELECT CAST(1 AS BIGINT) AS id WHERE FALSE",
            &destination,
        )
        .await?;
        let mut reader = FileReader::try_new(fs::File::open(destination)?, None)?;
        assert_eq!(reader.schema().field(0).name(), "id");
        assert!(reader.next().is_none());
        Ok(())
    }

    #[tokio::test]
    async fn refuses_to_overwrite_existing_output() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let destination = directory.path().join("existing.arrow");
        fs::write(&destination, b"keep this file")?;
        let error = write_arrow(&RustSparkSession::new(), "SELECT 1", &destination)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("already exists"));
        assert_eq!(fs::read(destination)?, b"keep this file");
        Ok(())
    }

    #[tokio::test]
    async fn rejects_sql_writes_before_creating_output() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let destination = directory.path().join("write.arrow");
        assert!(write_arrow(&RustSparkSession::new(), "CREATE TABLE x (id INT)", &destination).await.is_err());
        assert!(!destination.exists());
        Ok(())
    }
}
