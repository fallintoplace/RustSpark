use std::sync::Arc;
use datafusion::{
    arrow::{array::{Array, StringArray, StringBuilder}, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn extract_part(url: &str, part: &str) -> datafusion::error::Result<Option<String>> {
    if url.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(DataFusionError::Execution("PARSE_URL: invalid URL characters".into()));
    }
    let (scheme, rest) = url.split_once("://").ok_or_else(|| {
        DataFusionError::Execution("PARSE_URL: expected a hierarchical URL".into())
    })?;
    if !scheme.chars().next().is_some_and(|c| c.is_ascii_alphabetic())
        || !scheme.bytes().all(|c| c.is_ascii_alphanumeric() || b"+-.".contains(&c))
    {
        return Err(DataFusionError::Execution("PARSE_URL: invalid protocol".into()));
    }
    let authority_end = rest.find(&['/', '?', '#'][..]).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    if authority.is_empty() {
        return Err(DataFusionError::Execution("PARSE_URL: missing host".into()));
    }
    let host_port = authority.rsplit_once('@').map_or(authority, |(_, host)| host);
    let host = if host_port.starts_with('[') {
        let close = host_port.find(']').ok_or_else(|| {
            DataFusionError::Execution("PARSE_URL: invalid bracketed host".into())
        })?;
        let host = &host_port[..=close];
        let port = &host_port[close + 1..];
        if !port.is_empty() && !port.starts_with(':') {
            return Err(DataFusionError::Execution("PARSE_URL: invalid authority".into()));
        }
        host
    } else {
        host_port.split_once(':').map_or(host_port, |(host, _)| host)
    };
    if host.is_empty() {
        return Err(DataFusionError::Execution("PARSE_URL: missing host".into()));
    }
    let tail = &rest[authority_end..];
    let (no_fragment, reference) = tail.split_once('#')
        .map_or((tail, None), |(before, after)| (before, Some(after)));
    let (path, query) = no_fragment.split_once('?')
        .map_or((no_fragment, None), |(path, query)| (path, Some(query)));
    let userinfo = authority.rsplit_once('@').map(|(user, _)| user);
    Ok(match part.to_ascii_uppercase().as_str() {
        "PROTOCOL" => Some(scheme.to_owned()),
        "HOST" => Some(host.to_owned()),
        "AUTHORITY" => Some(authority.to_owned()),
        "USERINFO" => userinfo.map(str::to_owned),
        "PATH" => Some(path.to_owned()),
        "QUERY" => query.map(str::to_owned),
        "REF" => reference.map(str::to_owned),
        "FILE" => Some(match query {
            Some(query) => format!("{path}?{query}"),
            None => path.to_owned(),
        }),
        _ => None,
    })
}

fn evaluate(url: &str, part: &str) -> datafusion::error::Result<Option<String>> {
    extract_part(url, part)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "parse_url",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let urls = arrays[0].as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("parse_url expects Utf8 URL".into()))?;
            let parts = arrays[1].as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("parse_url expects Utf8 part".into()))?;
            let mut output = StringBuilder::new();
            for row in 0..urls.len() {
                let value = if urls.is_null(row) || parts.is_null(row) {
                    None
                } else {
                    evaluate(urls.value(row), parts.value(row))?
                };
                match value {
                    Some(value) => output.append_value(value),
                    None => output.append_null(),
                }
            }
            let output = output.finish();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    (!output.is_null(0)).then(|| output.value(0).to_owned()),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(output)))
            }
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::evaluate;
    #[test]
    fn parses_basic_url_sections_and_invalid_urls() {
        let url = "https://user:pass@spark.apache.org:443/path?query=1#docs";
        assert_eq!(evaluate(url, "HOST").unwrap().as_deref(), Some("spark.apache.org"));
        assert_eq!(evaluate(url, "QUERY").unwrap().as_deref(), Some("query=1"));
        assert_eq!(evaluate(url, "PROTOCOL").unwrap().as_deref(), Some("https"));
        assert_eq!(evaluate(url, "PATH").unwrap().as_deref(), Some("/path"));
        assert_eq!(evaluate(url, "REF").unwrap().as_deref(), Some("docs"));
        assert_eq!(evaluate(url, "FILE").unwrap().as_deref(), Some("/path?query=1"));
        assert_eq!(evaluate(url, "USERINFO").unwrap().as_deref(), Some("user:pass"));
        assert_eq!(evaluate(url, "AUTHORITY").unwrap().as_deref(), Some("user:pass@spark.apache.org:443"));
        assert_eq!(evaluate("https://spark.apache.org/", "QUERY").unwrap(), None);
        assert!(evaluate("inva lid://spark.apache.org/path", "HOST").is_err());
    }
}
