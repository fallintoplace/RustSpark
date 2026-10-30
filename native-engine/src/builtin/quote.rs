use super::{Function};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() == 1 && types[0] == DataType::Text { Ok(DataType::Text) }
    else { Err(Error::Type("QUOTE has incompatible SQL argument types".into())) }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let escaped_quotes = text.bytes().filter(|&byte| byte == b'\'').count();
    let capacity = text.len().checked_add(escaped_quotes)
        .and_then(|length| length.checked_add(2))
        .ok_or_else(|| Error::Execution("QUOTE output length overflow".into()))?;
    if capacity > 16 * 1024 * 1024 {
        return Err(Error::Execution("QUOTE output exceeds native 16 MiB budget".into()));
    }
    let mut output = String::with_capacity(capacity);
    output.push('\'');
    for ch in text.chars() {
        if ch == '\'' { output.push('\\'); }
        output.push(ch);
    }
    output.push('\'');
    Ok(Value::Text(output))
}


struct JsonQuoteWriter {
    bytes: Vec<u8>,
    limit: usize,
}

impl std::io::Write for JsonQuoteWriter {
    fn write(&mut self, chunk: &[u8]) -> std::io::Result<usize> {
        if chunk.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("JSON_QUOTE output exceeds byte budget"));
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
    let mut writer = JsonQuoteWriter { bytes: Vec::new(), limit: 16 * 1024 * 1024 };
    serde_json::to_writer(&mut writer, input)
        .map_err(|error| Error::Execution(format!("JSON_QUOTE failed: {error}")))?;
    Ok(Value::Text(String::from_utf8(writer.bytes).expect("serde_json emits UTF-8")))
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
mod json_quote_writer_budget_tests {
    use super::*;
    #[test]
    fn stops_escaped_strings_at_byte_limit() {
        let mut writer = JsonQuoteWriter { bytes: Vec::new(), limit: 8 };
        assert!(serde_json::to_writer(&mut writer, &"\n".repeat(20)).is_err());
        assert!(writer.bytes.len() <= 8);
        assert_eq!(json_quote(&[Value::Text("a\nb".into())]).unwrap(), Value::Text("\"a\\nb\"".into()));
    }
}
