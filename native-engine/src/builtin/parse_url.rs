use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};
use url::{ParseError, Url};

const MAX_URL_BYTES: usize = 16 * 1024 * 1024;

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)
}

fn find_authority(input: &str) -> Option<&str> {
    let (_, remainder) = input.split_once("://")?;
    Some(remainder.split(['/', '?', '#']).next().unwrap_or(""))
}

fn extract(url: &str, part: &str, key: Option<&str>) -> Result<Value> {
    if url.len() > MAX_URL_BYTES {
        return Err(Error::Execution("PARSE_URL input exceeds 16 MiB budget".into()));
    }
    let parsed = match Url::parse(url) {
        Ok(parsed) => Some(parsed),
        Err(ParseError::RelativeUrlWithoutBase) => {
            // Java URI accepts relative paths. Reject whitespace that a URI
            // parser would not accept as part of the original URL.
            if url.chars().any(char::is_whitespace) {
                return Err(Error::Parse("invalid relative URL".into()));
            }
            None
        }
        Err(error) => return Err(Error::Parse(format!("invalid URL: {error}"))),
    };
    let (without_ref, fragment) = match url.split_once('#') {
        Some((left, right)) => (left, Some(right)),
        None => (url, None),
    };
    let (without_query, query) = match without_ref.split_once('?') {
        Some((left, right)) => (left, Some(right)),
        None => (without_ref, None),
    };
    // Opaque URIs have a scheme-specific part, not a hierarchical path.
    // Keep relative paths intact, but do not report the entire URI as PATH.
    let raw_path = if parsed.as_ref().is_some_and(Url::cannot_be_a_base) {
        None
    } else {
        Some(match find_authority(without_query) {
            Some(authority) => {
                let after_scheme = without_query.split_once("://")
                    .map(|(_, rest)| rest).unwrap_or("");
                after_scheme.strip_prefix(authority).unwrap_or("")
            }
            None => without_query,
        })
    };
    // Avoid allocating an uppercased component name for each input row.
    let result = if part.eq_ignore_ascii_case("PROTOCOL") {
        parsed.as_ref().map(|parsed| parsed.scheme().to_owned())
    } else if part.eq_ignore_ascii_case("HOST") {
        parsed.as_ref().and_then(|parsed| parsed.host_str().map(str::to_owned))
    } else if part.eq_ignore_ascii_case("AUTHORITY") {
        find_authority(url).map(str::to_owned)
    } else if part.eq_ignore_ascii_case("USERINFO") {
        find_authority(url)
            .and_then(|authority| authority.rsplit_once('@').map(|(user, _)| user.to_owned()))
    } else if part.eq_ignore_ascii_case("PATH") {
        raw_path.map(str::to_owned)
    } else if part.eq_ignore_ascii_case("QUERY") {
        if let Some(key) = key {
            query.and_then(|query| query.split('&').find_map(|param| {
                let (name, value) = param.split_once('=')?;
                (name == key).then(|| value.to_owned())
            }))
        } else {
            query.map(str::to_owned)
        }
    } else if part.eq_ignore_ascii_case("REF") {
        fragment.map(str::to_owned)
    } else if part.eq_ignore_ascii_case("FILE") {
        raw_path.map(|path| match query {
            Some(query) => format!("{path}?{query}"),
            None => path.to_owned(),
        })
    } else {
        return Err(Error::Type(format!(
            "unsupported PARSE_URL component: {}",
            part.to_ascii_uppercase()
        )));
    };
    Ok(result.map(Value::Text).unwrap_or(Value::Null))
}

fn evaluate_inner(args: &[Value]) -> Result<Value> {
    let Some(input) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(part) = super::string(&args[1])? else { return Ok(Value::Null); };
    let key = if let Some(value) = args.get(2) {
        let Some(key) = super::string(value)? else { return Ok(Value::Null); };
        Some(key)
    } else { None };
    if key.is_some() && !part.eq_ignore_ascii_case("QUERY") {
        return Err(Error::Type("PARSE_URL query key requires QUERY component".into()));
    }
    extract(input, part, key)
}

fn parse_url(args: &[Value]) -> Result<Value> {
    evaluate_inner(args)
}

fn try_parse_url(args: &[Value]) -> Result<Value> {
    match evaluate_inner(args) {
        Ok(result) => Ok(result),
        Err(Error::Parse(_) | Error::Type(_)) => Ok(Value::Null),
        Err(error) => Err(error),
    }
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "parse_url", min_args: 2, max_args: 3, result_type: output, call: parse_url },
    Function { name: "try_parse_url", min_args: 2, max_args: 3, result_type: output, call: try_parse_url },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, schema::{Field, Schema}, sql};

    fn get(input: &str, part: &str, key: Option<&str>) -> Result<Value> {
        let mut args = vec![Value::Text(input.into()), Value::Text(part.into())];
        if let Some(key) = key { args.push(Value::Text(key.into())); }
        parse_url(&args)
    }

    #[test]
    fn extracts_uri_parts_and_case_sensitive_query_keys() {
        let source = "https://user:pass@spark.apache.org:8080/path?q=1&Q=2#top";
        assert_eq!(get(source, "PROTOCOL", None).unwrap(), Value::Text("https".into()));
        assert_eq!(get(source, "HOST", None).unwrap(), Value::Text("spark.apache.org".into()));
        assert_eq!(get(source, "AUTHORITY", None).unwrap(),
            Value::Text("user:pass@spark.apache.org:8080".into()));
        assert_eq!(get(source, "USERINFO", None).unwrap(), Value::Text("user:pass".into()));
        assert_eq!(get(source, "PATH", None).unwrap(), Value::Text("/path".into()));
        assert_eq!(get(source, "FILE", None).unwrap(), Value::Text("/path?q=1&Q=2".into()));
        assert_eq!(get(source, "QUERY", None).unwrap(), Value::Text("q=1&Q=2".into()));
        assert_eq!(get(source, "REF", None).unwrap(), Value::Text("top".into()));
        assert_eq!(get(source, "QUERY", Some("q")).unwrap(), Value::Text("1".into()));
        assert_eq!(get(source, "QUERY", Some("Q")).unwrap(), Value::Text("2".into()));
        assert_eq!(get(source, "QUERY", Some("absent")).unwrap(), Value::Null);
        assert_eq!(get("not-a-url", "PATH", None).unwrap(),
            Value::Text("not-a-url".into()));
    }

    #[test]
    fn distinguishes_strict_and_try_variants() {
        let invalid = [Value::Text("inva lid://spark.apache.org".into()),
            Value::Text("HOST".into())];
        assert!(parse_url(&invalid).is_err());
        assert_eq!(try_parse_url(&invalid).unwrap(), Value::Null);
        assert_eq!(try_parse_url(&[Value::Null, invalid[1].clone()]).unwrap(), Value::Null);
        assert!(parse_url(&[Value::Text("https://example.org".into()),
            Value::Text("INVALID".into())]).is_err());
        assert_eq!(try_parse_url(&[Value::Text("https://example.org".into()),
            Value::Text("INVALID".into())]).unwrap(), Value::Null);
        assert!(output(&[DataType::Integer, DataType::Text]).is_err());
    }

    #[test]
    fn evaluates_url_parts_from_native_sql_columns() {
        let schema = Schema::new(vec![Field::new("link", DataType::Text, true)]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::from_rows(schema, vec![
            vec![Value::Text("https://spark.apache.org/path?query=1".into())],
            vec![Value::Null],
        ]).unwrap()).unwrap();
        let plan = sql::parse_sql(
            "SELECT PARSE_URL(link, 'HOST') AS host, PARSE_URL(link, 'QUERY', 'query') AS key FROM t"
        ).unwrap();
        assert_eq!(executor::execute(&plan, &catalog).unwrap().rows().unwrap(), vec![
            vec![Value::Text("spark.apache.org".into()), Value::Text("1".into())],
            vec![Value::Null, Value::Null],
        ]);
    }
}

#[cfg(test)]
mod case_insensitive_url_parts_tests {
    use super::*;

    #[test]
    fn mixed_case_components_keep_all_extractors_and_query_keys() {
        let url = "https://u:p@example.com:443/path?q=1&Q=2#frag";
        for part in ["protocol", "hOsT", "authority", "userInfo", "PaTh", "qUeRy", "rEf", "file"] {
            assert_eq!(extract(url, part, None).unwrap(),
                extract(url, &part.to_ascii_uppercase(), None).unwrap());
        }
        assert_eq!(extract(url, "query", Some("Q")).unwrap(), Value::Text("2".into()));
        assert_eq!(extract(url, "QUERY", Some("q")).unwrap(), Value::Text("1".into()));
        assert!(matches!(extract(url, "unsupported", None), Err(Error::Type(_))));
    }
}

#[cfg(test)]
mod opaque_uri_path_regressions {
    use super::*;

    #[test]
    fn opaque_schemes_do_not_report_scheme_specific_data_as_paths() {
        for uri in [
            "mailto:dev@example.org",
            "urn:isbn:9780131103627",
            "data:text/plain,hello",
        ] {
            assert_eq!(extract(uri, "PATH", None).unwrap(), Value::Null);
            assert_eq!(extract(uri, "FILE", None).unwrap(), Value::Null);
        }
        assert_eq!(extract("mailto:dev@example.org", "PROTOCOL", None).unwrap(),
            Value::Text("mailto".into()));
        assert_eq!(extract("relative/file.csv?q=1", "PATH", None).unwrap(),
            Value::Text("relative/file.csv".into()));
        assert_eq!(extract("relative/file.csv?q=1", "FILE", None).unwrap(),
            Value::Text("relative/file.csv?q=1".into()));
    }
}
