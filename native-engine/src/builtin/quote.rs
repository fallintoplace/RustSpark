use super::{Function};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() == 1 && types[0] == DataType::Text { Ok(DataType::Text) }
    else { Err(Error::Type("QUOTE has incompatible SQL argument types".into())) }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let escaped = text.bytes().filter(|&byte| byte == b'\'' || byte == b'\\').count();
    let capacity = text.len().checked_add(escaped)
        .and_then(|length| length.checked_add(2))
        .ok_or_else(|| Error::Execution("QUOTE output length overflow".into()))?;
    if capacity > 16 * 1024 * 1024 {
        return Err(Error::Execution("QUOTE output exceeds native 16 MiB budget".into()));
    }
    let mut output = String::with_capacity(capacity);
    output.push('\'');
    for ch in text.chars() {
        if ch == '\'' || ch == '\\' { output.push('\\'); }
        output.push(ch);
    }
    output.push('\'');
    Ok(Value::Text(output))
}


const JSON_QUOTE_LIMIT: usize = 16 * 1024 * 1024;

struct BoundedJsonQuoteWriter {
    bytes: Vec<u8>,
    exceeded: bool,
}

impl std::io::Write for BoundedJsonQuoteWriter {
    fn write(&mut self, chunk: &[u8]) -> std::io::Result<usize> {
        if chunk.len() > JSON_QUOTE_LIMIT.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(std::io::Error::other("JSON_QUOTE output exceeds 16 MiB"));
        }
        self.bytes.extend_from_slice(chunk);
        Ok(chunk.len())
    }

    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

fn json_quote_type(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)
}
fn json_quote(args: &[Value]) -> Result<Value> {
    let Some(input) = super::string(&args[0])? else { return Ok(Value::Null); };
    if input.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("JSON_QUOTE input exceeds 16 MiB".into()));
    }
    let mut writer = BoundedJsonQuoteWriter {
        bytes: Vec::with_capacity(input.len().saturating_add(2).min(JSON_QUOTE_LIMIT)),
        exceeded: false,
    };
    if let Err(error) = serde_json::to_writer(&mut writer, input) {
        if writer.exceeded {
            return Err(Error::Execution("JSON_QUOTE output exceeds 16 MiB".into()));
        }
        return Err(Error::Execution(format!("JSON_QUOTE failed: {error}")));
    }
    Ok(Value::Text(String::from_utf8(writer.bytes)
        .expect("serde_json only writes valid UTF-8")))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "json_quote", min_args: 1, max_args: 1, result_type: json_quote_type, call: json_quote },

    Function { name: "quote", min_args: 1, max_args: 1, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn spark_sql_semantics() {
        assert_eq!(evaluate(&[Value::Text("Don't".into())]).unwrap(), Value::Text("'Don\\'t'".into()));
        assert_eq!(evaluate(&[Value::Text("".into())]).unwrap(), Value::Text("''".into()));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Text("a'b'c".into())]).unwrap(),
            Value::Text("'a\\'b\\'c'".into()));
        assert_eq!(evaluate(&[Value::Text("é🙂".into())]).unwrap(),
            Value::Text("'é🙂'".into()));
    }
}


#[cfg(test)]
mod json_quote_extension_tests {
    use super::*;
    #[test]
    fn escapes_unicode_and_control_characters_as_json() {
        assert!(crate::builtin::supports("json_quote"));
        let input = "Hello\n\"café\"";
        let quoted = json_quote(&[Value::Text(input.into())]).unwrap();
        let Value::Text(encoded) = quoted else { panic!("expected text"); };
        assert_eq!(serde_json::from_str::<String>(&encoded).unwrap(), input);
        assert_eq!(json_quote(&[Value::Text("".into())]).unwrap(), Value::Text("\"\"".into()));
        assert_eq!(json_quote(&[Value::Null]).unwrap(), Value::Null);
        assert!(json_quote(&[Value::Text("a".repeat(16 * 1024 * 1024 + 1))]).is_err());
    }
}

#[cfg(test)]
mod quote_output_limit_tests {
    use super::*;
    #[test]
    fn exact_budget_quote_is_allowed_but_one_extra_byte_is_rejected() {
        let input = Value::Text("x".repeat(16 * 1024 * 1024 - 2));
        let Value::Text(quoted) = evaluate(&[input]).unwrap() else { panic!("expected text"); };
        assert_eq!(quoted.len(), 16 * 1024 * 1024);
        assert!(quoted.starts_with('\''));
        assert!(evaluate(&[Value::Text("x".repeat(16 * 1024 * 1024 - 1))]).is_err());
        assert!(evaluate(&[Value::Text("'".repeat(8 * 1024 * 1024))]).is_err());
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod quote_backslash_tests {
    use super::*;

    #[test]
    fn escapes_existing_backslashes_as_well_as_quotes() {
        assert_eq!(evaluate(&[Value::Text(r"a\b".into())]).unwrap(),
            Value::Text(r"'a\\b'".into()));
        assert_eq!(evaluate(&[Value::Text("a\\'b".into())]).unwrap(),
            Value::Text("'a\\\\\\'b'".into()));
        assert_eq!(evaluate(&[Value::Text("🙂\\z".into())]).unwrap(),
            Value::Text("'🙂\\\\z'".into()));
    }
}

#[cfg(test)]
mod json_quote_bounded_writer_tests {
    use super::*;

    #[test]
    fn writer_preserves_json_string_escaping() {
        for input in ["", "plain", "a\"b\\c", "é🙂", "\u{0000}\n\t\u{001f}"] {
            assert_eq!(
                json_quote(&[Value::Text(input.into())]).unwrap(),
                Value::Text(serde_json::to_string(input).unwrap())
            );
        }
        assert_eq!(json_quote(&[Value::Null]).unwrap(), Value::Null);
    }

    #[test]
    fn rejects_expansion_without_allocating_beyond_output_budget() {
        let text = "\0".repeat(2_800_000);
        let mut writer = BoundedJsonQuoteWriter { bytes: Vec::new(), exceeded: false };
        assert!(serde_json::to_writer(&mut writer, &text).is_err());
        assert!(writer.exceeded);
        assert!(writer.bytes.len() <= JSON_QUOTE_LIMIT);
        assert!(matches!(json_quote(&[Value::Text(text)]), Err(Error::Execution(_))));
    }
}
