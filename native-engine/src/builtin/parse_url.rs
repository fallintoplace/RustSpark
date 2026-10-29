use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)
}

fn part(url: &str, requested: &str, key: Option<&str>) -> Result<Option<String>> {
    if url.len() > 1024 * 1024 {
        return Err(Error::Execution("PARSE_URL input exceeds 1 MiB".into()));
    }
    let (scheme, rest) = url.split_once("://").ok_or_else(||
        Error::Parse("PARSE_URL requires a hierarchical URL".into()))?;
    if scheme.is_empty() || !scheme.chars().next().unwrap().is_ascii_alphabetic()
        || !scheme.bytes().all(|c| c.is_ascii_alphanumeric() || b"+-.".contains(&c))
    {
        return Err(Error::Parse("PARSE_URL has invalid scheme".into()));
    }
    let authority_end = rest.find(|c| c == '/' || c == '?' || c == '#').unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    if authority.is_empty() || authority.bytes().any(|c| c.is_ascii_whitespace()) {
        return Err(Error::Parse("PARSE_URL has invalid authority".into()));
    }
    let (userinfo, hostport) = match authority.rsplit_once('@') {
        Some((user, host)) => (Some(user), host),
        None => (None, authority),
    };
    let host = if let Some(ipv6) = hostport.strip_prefix('[') {
        let closing = ipv6.find(']').ok_or_else(||
            Error::Parse("PARSE_URL has invalid IPv6 authority".into()))?;
        let suffix = &ipv6[closing + 1..];
        if !suffix.is_empty() && !suffix.starts_with(':') {
            return Err(Error::Parse("PARSE_URL has invalid IPv6 port".into()));
        }
        &hostport[..closing + 2]
    } else {
        hostport.split_once(':').map_or(hostport, |(host, _)| host)
    };
    if host.is_empty() {
        return Err(Error::Parse("PARSE_URL has no hostname".into()));
    }
    let tail = &rest[authority_end..];
    let reference = tail.split_once('#').map(|(_, reference)| reference);
    let without_ref = tail.split('#').next().unwrap_or(tail);
    let (path, query) = without_ref.split_once('?').map_or((without_ref, None), |(p, q)| (p, Some(q)));
    let path = if path.starts_with('/') { path } else { "" };
    let file = if let Some(query) = query { format!("{path}?{query}") } else { path.into() };
    let result = match requested.to_ascii_uppercase().as_str() {
        "PROTOCOL" => Some(scheme.to_string()),
        "HOST" => Some(host.to_string()),
        "AUTHORITY" => Some(authority.to_string()),
        "USERINFO" => userinfo.map(str::to_string),
        "PATH" => Some(path.to_string()),
        "FILE" => Some(file),
        "REF" => reference.map(str::to_string),
        "QUERY" => match key {
            None => query.map(str::to_string),
            Some(key) => query.and_then(|query| query.split('&').find_map(|piece| {
                let (name, value) = piece.split_once('=').map_or((piece, ""), |(n, v)| (n, v));
                (name == key).then(|| value.to_string())
            })),
        },
        _ => return Err(Error::Parse("PARSE_URL unknown part".into())),
    };
    if key.is_some() && !requested.eq_ignore_ascii_case("QUERY") {
        return Err(Error::Parse("PARSE_URL key requires QUERY part".into()));
    }
    Ok(result)
}

fn evaluate_impl(args: &[Value], safe: bool) -> Result<Value> {
    let Some(url) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(part_name) = super::string(&args[1])? else { return Ok(Value::Null); };
    let key = if args.len() == 3 {
        let Some(key) = super::string(&args[2])? else { return Ok(Value::Null); };
        Some(key)
    } else { None };
    match part(url, part_name, key) {
        Ok(Some(value)) => Ok(Value::Text(value)),
        Ok(None) => Ok(Value::Null),
        Err(_) if safe => Ok(Value::Null),
        Err(error) => Err(error),
    }
}

fn parse(args: &[Value]) -> Result<Value> { evaluate_impl(args, false) }
fn try_parse(args: &[Value]) -> Result<Value> { evaluate_impl(args, true) }

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "parse_url", min_args: 2, max_args: 3, result_type: output, call: parse },
    Function { name: "try_parse_url", min_args: 2, max_args: 3, result_type: output, call: try_parse },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_url_parts_and_named_query_values() {
        let url = Value::Text("https://alice:secret@spark.apache.org:443/path?q=1&x=2#result".into());
        let read = |name: &str| parse(&[url.clone(), Value::Text(name.into())]).unwrap();
        assert_eq!(read("HOST"), Value::Text("spark.apache.org".into()));
        assert_eq!(read("PROTOCOL"), Value::Text("https".into()));
        assert_eq!(read("PATH"), Value::Text("/path".into()));
        assert_eq!(read("REF"), Value::Text("result".into()));
        assert_eq!(read("USERINFO"), Value::Text("alice:secret".into()));
        assert_eq!(read("FILE"), Value::Text("/path?q=1&x=2".into()));
        assert_eq!(parse(&[url, Value::Text("QUERY".into()),
            Value::Text("x".into())]).unwrap(), Value::Text("2".into()));
    }

    #[test]
    fn try_parse_suppresses_bad_urls_and_nulls() {
        assert!(parse(&[Value::Text("not a URL".into()), Value::Text("HOST".into())]).is_err());
        assert_eq!(try_parse(&[Value::Text("not a URL".into()), Value::Text("HOST".into())]).unwrap(), Value::Null);
        assert_eq!(try_parse(&[Value::Null, Value::Text("HOST".into())]).unwrap(), Value::Null);
    }
}
