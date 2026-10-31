use crate::{Error, Result};

/// Parse bounded JSON documents without a separate SQL backend.
pub(super) fn parse(input: &str) -> Result<serde_json::Value> {
    if input.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("native JSON input exceeds 16 MiB".into()));
    }
    serde_json::from_str(input)
        .map_err(|error| Error::Execution(format!("invalid native JSON input: {error}")))
}

struct JsonOutput {
    bytes: Vec<u8>,
    limit: usize,
}

impl std::io::Write for JsonOutput {
    fn write(&mut self, chunk: &[u8]) -> std::io::Result<usize> {
        if chunk.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("native JSON output exceeds byte budget"));
        }
        self.bytes.extend_from_slice(chunk);
        Ok(chunk.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

pub(super) fn serialize_bounded(value: &serde_json::Value, limit: usize) -> Result<String> {
    let mut writer = JsonOutput { bytes: Vec::new(), limit };
    serde_json::to_writer(&mut writer, value)
        .map_err(|error| Error::Execution(format!("native JSON serialization failed: {error}")))?;
    Ok(String::from_utf8(writer.bytes).expect("serde_json writes UTF-8"))
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
            let end = rest.bytes().position(|byte| byte == b'.' || byte == b'[').unwrap_or(rest.len());
            let key = &rest[..end];
            if key.is_empty() || !key.bytes().all(|byte|
                byte.is_ascii_alphanumeric() || byte == b'_') {
                return Err(Error::Parse("unsupported native JSON object path".into()));
            }
            node = node.and_then(|value| value.get(key));
            tail = &rest[end..];
        } else if let Some(rest) = tail.strip_prefix('[') {
            if rest.starts_with('\'') || rest.starts_with('"') {
                let quote = char::from(rest.as_bytes()[0]);
                // A quoted key without escapes can be borrowed from the path.
                if let Some(end) = rest[1..].find(quote) {
                    let key = &rest[1..end + 1];
                    if !key.contains('\\') && rest[end + 2..].starts_with(']') {
                        node = node.and_then(|value| value.get(key));
                        tail = &rest[end + 3..];
                        continue;
                    }
                }
                let mut end = quote.len_utf8();
                let mut escaped = false;
                let mut closed = false;
                let mut key = String::new();
                for character in rest[end..].chars() {
                    end += character.len_utf8();
                    if escaped {
                        if character != quote && character != '\\' {
                            return Err(Error::Parse("unsupported JSON path escape".into()));
                        }
                        key.push(character);
                        escaped = false;
                    } else if character == '\\' {
                        escaped = true;
                    } else if character == quote {
                        closed = true;
                        break;
                    } else {
                        key.push(character);
                    }
                }
                if !closed || !rest[end..].starts_with(']') {
                    return Err(Error::Parse("unterminated quoted JSON path key".into()));
                }
                node = node.and_then(|value| value.get(key.as_str()));
                tail = &rest[end + 1..];
                continue;
            }
            let end = rest.find(']')
                .ok_or_else(|| Error::Parse("JSON array index missing ]".into()))?;
            let digits = &rest[..end];
            if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(Error::Parse("JSON array indices must be unsigned integers".into()));
            }
            // Avoid generic decimal parsing for common single-digit JSON arrays.
            let index = if digits.len() == 1 {
                usize::from(digits.as_bytes()[0] - b'0')
            } else {
                digits.parse::<usize>()
                    .map_err(|_| Error::Parse("JSON array index overflow".into()))?
            };
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
        let objects = parse(r#"{"root":{"a.b":7,"é":8,"quote\"key":9}}"#).unwrap();
        assert_eq!(lookup(Some(&objects), "$.root['a.b']").unwrap(), Some(&serde_json::json!(7)));
        assert_eq!(lookup(Some(&objects), r#"$.root["é"]"#).unwrap(), Some(&serde_json::json!(8)));
        assert_eq!(lookup(Some(&objects), r#"$.root["quote\"key"]"#).unwrap(), Some(&serde_json::json!(9)));
        assert_eq!(lookup(Some(&objects), "$.root['missing']").unwrap(), None);
        assert!(lookup(Some(&objects), "$.root['missing]").is_err());
    }
}

#[cfg(test)]
mod ascii_json_path_perf_tests {
    use super::*;
    #[test]
    fn matches_nested_single_and_multidigit_array_paths() {
        let doc = parse(r#"{"rows":[0,1,2,3,4,5,6,7,8,9,10,11,12]}"#).unwrap();
        assert_eq!(lookup(Some(&doc), "$.rows[0]").unwrap(), Some(&serde_json::json!(0)));
        assert_eq!(lookup(Some(&doc), "$.rows[12]").unwrap(), Some(&serde_json::json!(12)));
        assert_eq!(lookup(Some(&doc), "$.rows[200]").unwrap(), None);
        assert!(lookup(Some(&doc), "$.rows[2x]").is_err());
        assert!(lookup(Some(&doc), "$.café").is_err());
    }
}

#[cfg(test)]
mod borrowed_json_quoted_key_tests {
    use super::*;
    #[test]
    fn supports_unicode_simple_keys_and_escaped_fallback() {
        let doc = parse(r#"{"root":{"a.b":1,"é🙂":2,"x'k":3}}"#).unwrap();
        assert_eq!(lookup(Some(&doc), "$.root['a.b']").unwrap(), Some(&serde_json::json!(1)));
        assert_eq!(lookup(Some(&doc), r#"$.root["é🙂"]"#).unwrap(), Some(&serde_json::json!(2)));
        assert_eq!(lookup(Some(&doc), "$.root['missing']").unwrap(), None);
        assert_eq!(lookup(Some(&doc), r#"$.root['x\'k']"#).unwrap(), Some(&serde_json::json!(3)));
    }
}

#[cfg(test)]
mod json_output_budget_tests {
    use super::*;
    #[test]
    fn serializes_exact_json_without_exceeding_byte_budget() {
        let value = serde_json::json!({"hello": [1, 2]});
        let expected = value.to_string();
        assert_eq!(serialize_bounded(&value, expected.len()).unwrap(), expected);
        assert!(matches!(serialize_bounded(&value, expected.len() - 1), Err(Error::Execution(_))));
    }
}
