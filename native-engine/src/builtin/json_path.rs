use crate::{Error, Result};

/// Parse bounded JSON documents without a separate SQL backend.
pub(super) fn parse(input: &str) -> Result<serde_json::Value> {
    if input.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("native JSON input exceeds 16 MiB".into()));
    }
    serde_json::from_str(input)
        .map_err(|error| Error::Execution(format!("invalid native JSON input: {error}")))
}

/// Support explicit, bounded $.field[0].nested JSON paths.
pub(super) fn lookup<'a>(mut node: Option<&'a serde_json::Value>, path: &str)
    -> Result<Option<&'a serde_json::Value>>
{
    if path.len() > 4096 || !path.starts_with('$') {
        return Err(Error::Parse("JSON path must start with $ and fit 4096 bytes".into()));
    }
    let mut tail = &path[1..];
    while !tail.is_empty() {
        if let Some(rest) = tail.strip_prefix('.') {
            let end = rest.find(|character: char| character == '.' || character == '[').unwrap_or(rest.len());
            let key = &rest[..end];
            if key.is_empty() || !key.bytes().all(|byte|
                byte.is_ascii_alphanumeric() || byte == b'_') {
                return Err(Error::Parse("unsupported native JSON object path".into()));
            }
            node = node.and_then(|value| value.get(key));
            tail = &rest[end..];
        } else if let Some(rest) = tail.strip_prefix('[') {
            let end = rest.find(']')
                .ok_or_else(|| Error::Parse("JSON array index missing ]".into()))?;
            let digits = &rest[..end];
            if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(Error::Parse("JSON array indices must be unsigned integers".into()));
            }
            let index = digits.parse::<usize>()
                .map_err(|_| Error::Parse("JSON array index overflow".into()))?;
            node = node.and_then(|value| value.get(index));
            tail = &rest[end + 1..];
        } else {
            return Err(Error::Parse("unsupported native JSON path syntax".into()));
        }
    }
    Ok(node)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn paths_walk_nested_values_and_reject_invalid_indices() {
        let doc = parse(r#"{"a":[{"n":7}]}"#).unwrap();
        assert_eq!(lookup(Some(&doc), "$.a[0].n").unwrap(), Some(&serde_json::json!(7)));
        assert_eq!(lookup(Some(&doc), "$.a[2].n").unwrap(), None);
        assert!(lookup(Some(&doc), "$.a[-1]").is_err());
        assert!(lookup(Some(&doc), "a").is_err());
    }
}
