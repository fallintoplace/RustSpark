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
        return Err(Error::Execution("QUOTE output exceeds 16 MiB".into()));
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


fn json_quote_type(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)
}
fn json_quote(args: &[Value]) -> Result<Value> {
    let Some(input) = super::string(&args[0])? else { return Ok(Value::Null); };
    if input.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("JSON_QUOTE input exceeds 16 MiB".into()));
    }
    let encoded = serde_json::to_string(input)
        .map_err(|error| Error::Execution(format!("JSON_QUOTE failed: {error}")))?;
    if encoded.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("JSON_QUOTE output exceeds 16 MiB".into()));
    }
    Ok(Value::Text(encoded))
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
mod quote_output_budget_regressions {
    use super::*;
    #[test]
    fn rejects_oversized_expansion_and_allows_small_unicode() {
        assert!(evaluate(&[Value::Text("'".repeat(8 * 1024 * 1024))]).is_err());
        assert_eq!(evaluate(&[Value::Text("é'".into())]).unwrap(),
            Value::Text("'é\\''".into()));
    }
}
