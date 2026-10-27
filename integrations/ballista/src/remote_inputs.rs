use std::{collections::HashSet, env};

use rustspark::RustSparkSession;

#[derive(Debug, PartialEq, Eq)]
enum Format {
    Csv,
    Json,
    Parquet,
}

#[derive(Debug, PartialEq, Eq)]
struct Source {
    format: Format,
    name: String,
    path: String,
}

fn parse_sources(input: &str) -> Result<Vec<Source>, String> {
    if input.trim().is_empty() {
        return Err("RUSTSPARK_INPUTS must not be empty".into());
    }

    let mut seen = HashSet::new();
    let mut sources = Vec::new();
    for item in input.split(';') {
        let (kind_and_name, path) = item
            .split_once('=')
            .ok_or_else(|| format!("Expected format:table=path in RUSTSPARK_INPUTS: {item}"))?;
        let (kind, name) = kind_and_name
            .split_once(':')
            .ok_or_else(|| format!("Expected format:table=path in RUSTSPARK_INPUTS: {item}"))?;
        let format = match kind.trim().to_ascii_lowercase().as_str() {
            "csv" => Format::Csv,
            "json" => Format::Json,
            "parquet" => Format::Parquet,
            other => return Err(format!("Unsupported distributed input format: {other}")),
        };
        let name = name.trim();
        let path = path.trim();
        let mut chars = name.chars();
        if !matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
            || !chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(format!("Invalid SQL table identifier: {name}"));
        }
        if path.is_empty() {
            return Err(format!("Missing path for distributed input: {name}"));
        }
        if !seen.insert(name.to_ascii_lowercase()) {
            return Err(format!("Duplicate distributed table name: {name}"));
        }
        sources.push(Source {
            format,
            name: name.to_owned(),
            path: path.to_owned(),
        });
    }
    Ok(sources)
}

pub async fn register(session: &RustSparkSession) -> Result<(), Box<dyn std::error::Error>> {
    let specification = match env::var("RUSTSPARK_INPUTS") {
        Ok(value) => value,
        Err(env::VarError::NotPresent) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let sources = parse_sources(&specification)?;
    if env::var_os("RUSTSPARK_PARQUET").is_some()
        && sources.iter().any(|source| source.name.eq_ignore_ascii_case("data"))
    {
        return Err("RUSTSPARK_INPUTS cannot redefine the legacy data table".into());
    }

    for source in sources {
        match source.format {
            Format::Csv => session.register_csv(&source.name, &source.path).await?,
            Format::Json => session.register_json(&source.name, &source.path).await?,
            Format::Parquet => session.register_parquet(&source.name, &source.path).await?,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_mixed_sources_for_distributed_joins() {
        let inputs = parse_sources(
            "parquet:orders=/shared/orders.parquet;csv:customers=/shared/customers.csv;json:logs=s3://bucket/logs",
        )
        .unwrap();
        assert_eq!(inputs.len(), 3);
        assert_eq!(inputs[0].format, Format::Parquet);
        assert_eq!(inputs[1].name, "customers");
        assert_eq!(inputs[2].path, "s3://bucket/logs");
    }

    #[test]
    fn rejects_duplicate_names_case_insensitively() {
        let error = parse_sources("parquet:Orders=/tmp/a;json:orders=/tmp/b").unwrap_err();
        assert!(error.contains("Duplicate"));
    }

    #[test]
    fn rejects_malformed_or_empty_sources() {
        for value in [
            "",
            "parquet:orders=",
            "parquet:orders",
            "unknown:orders=/tmp/a",
            "csv:bad-name=/tmp/a",
            "csv:orders=/tmp/a;",
        ] {
            assert!(parse_sources(value).is_err(), "{value}");
        }
    }
}
