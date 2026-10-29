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
    let mut output = String::with_capacity(capacity);
    output.push('\'');
    for ch in text.chars() {
        if ch == '\'' { output.push('\\'); }
        output.push(ch);
    }
    output.push('\'');
    Ok(Value::Text(output))
}

pub(super) const FUNCTIONS: &[Function] = &[
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
