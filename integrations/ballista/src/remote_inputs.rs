//! Named data sources for the optional distributed Ballista client.
//! Every location must be readable by the Ballista scheduler and executors.
use std::{collections::HashSet, env, fs};

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
    parse_source_items(input.split(';'))
}

fn parse_source_items<'a>(items: impl IntoIterator<Item = &'a str>) -> Result<Vec<Source>, String> {
    let mut seen = HashSet::new();
    let mut sources = Vec::new();
    for item in items {
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

fn parse_manifest(input: &str) -> Result<Vec<Source>, String> {
    let input = input.strip_prefix('\u{feff}').unwrap_or(input);
    let mut entries = input
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .flat_map(|line| line.split(';'))
        .peekable();
    if entries.peek().is_none() {
        return Err("RUSTSPARK_INPUTS_FILE must contain at least one table".into());
    }
    parse_source_items(entries)
}

fn optional_env(name: &str) -> Result<Option<String>, env::VarError> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error),
    }
}

fn load_sources(
    inline: Option<String>,
    manifest_path: Option<String>,
) -> Result<Option<Vec<Source>>, Box<dyn std::error::Error>> {
    match (inline, manifest_path) {
        (Some(_), Some(_)) => {
            Err("RUSTSPARK_INPUTS and RUSTSPARK_INPUTS_FILE cannot both be set".into())
        }
        (Some(specification), None) => Ok(Some(parse_sources(&specification)?)),
        (None, Some(path)) => {
            if path.trim().is_empty() {
                return Err("RUSTSPARK_INPUTS_FILE must name a file".into());
            }
            Ok(Some(parse_manifest(&fs::read_to_string(path)?)?))
        }
        (None, None) => Ok(None),
    }
}

pub async fn register(session: &RustSparkSession) -> Result<(), Box<dyn std::error::Error>> {
    let inline = optional_env("RUSTSPARK_INPUTS")?;
    let manifest = optional_env("RUSTSPARK_INPUTS_FILE")?;
    let Some(sources) = load_sources(inline, manifest)? else {
        return Ok(());
    };
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
    fn manifest_supports_comments_blanks_and_bom() {
        let sources = parse_manifest(
            "\u{feff}# shared inputs\nparquet:Orders=s3://bucket/orders\n\n  # lookup tables\n csv:rates=/mnt/rates.csv\njson:events=s3://bucket/data?version=2\n",
        )
        .unwrap();
        assert_eq!(sources.len(), 3);
        assert_eq!(sources[0].name, "Orders");
        assert_eq!(sources[1].format, Format::Csv);
        assert_eq!(sources[2].path, "s3://bucket/data?version=2");
    }

    #[test]
    fn manifest_keeps_large_input_order_and_semicolon_syntax() {
        let manifest = (0..512)
            .map(|index| format!("csv:t{index}=/shared/{index}.csv"))
            .collect::<Vec<_>>()
            .join("\n");
        let sources = parse_manifest(&manifest).unwrap();
        assert_eq!(sources.len(), 512);
        assert_eq!(sources[0].name, "t0");
        assert_eq!(sources[511].name, "t511");
        let combined = parse_manifest("csv:left=/shared/left;parquet:right=/shared/right").unwrap();
        assert_eq!(combined.len(), 2);
        assert_eq!(combined[1].format, Format::Parquet);
    }

    #[test]
    fn manifest_rejects_ambiguous_or_empty_sources() {
        for input in ["", "  # nothing\n\n", "csv:Orders=/a\nparquet:orders=/b", "csv:bad-name=/a"] {
            assert!(parse_manifest(input).is_err(), "{input}");
        }
    }

    #[test]
    fn manifest_and_inline_sources_are_mutually_exclusive() {
        assert!(load_sources(None, None).unwrap().is_none());
        assert!(load_sources(Some("csv:t=/a".into()), None).unwrap().is_some());
        let error = load_sources(Some("csv:t=/a".into()), Some("inputs.txt".into())).unwrap_err();
        assert!(error.to_string().contains("cannot both be set"));
        assert!(load_sources(None, Some(" ".into())).is_err());
        assert!(load_sources(None, Some("/path/that/does/not/exist".into())).is_err());
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
