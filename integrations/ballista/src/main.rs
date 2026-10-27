use rustspark::RustSparkSession;

mod deadline;
mod remote_inputs;
mod result_preview;
mod scheduler;

fn default_if_unset(
    value: Result<String, std::env::VarError>,
    fallback: &str,
) -> Result<String, std::env::VarError> {
    match value {
        Ok(value) => Ok(value),
        Err(std::env::VarError::NotPresent) => Ok(fallback.to_owned()),
        Err(error) => Err(error),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(limit) = deadline::configured_deadline()? {
        match tokio::time::timeout(limit, run()).await {
            Ok(result) => result,
            Err(_) => Err("Ballista client query deadline exceeded; remote jobs may still run".into()),
        }
    } else {
        run().await
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let url = default_if_unset(std::env::var("BALLISTA_URL"), "df://localhost:50050")?;
    let query = default_if_unset(
        std::env::var("RUSTSPARK_SQL"),
        "SELECT COUNT(*) AS n FROM (VALUES (1), (2), (3)) AS input(id)",
    )?;

    let context = scheduler::connect(&url).await?;
    let session = RustSparkSession::from_context(context);

    match std::env::var("RUSTSPARK_PARQUET") {
        Ok(path) => session.register_parquet("data", &path).await?,
        Err(std::env::VarError::NotPresent) => {}
        Err(error) => return Err(error.into()),
    }
    remote_inputs::register(&session).await?;

    let result = session.sql(&query).await?;
    if let Some(rows) = result_preview::configured_rows()? {
        result.show_limit(rows).await?;
    } else {
        result.show().await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::default_if_unset;
    use std::{env::VarError, ffi::OsString};

    #[test]
    fn only_missing_variables_use_default_values() {
        assert_eq!(
            default_if_unset(Err(VarError::NotPresent), "fallback").unwrap(),
            "fallback"
        );
        assert_eq!(
            default_if_unset(Ok("configured".into()), "fallback").unwrap(),
            "configured"
        );
        assert!(matches!(
            default_if_unset(Err(VarError::NotUnicode(OsString::from("raw"))), "fallback"),
            Err(VarError::NotUnicode(_))
        ));
    }
}
