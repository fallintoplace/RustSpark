use super::{Function};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() == 1 && types[0] == DataType::Text { Ok(DataType::Text) }
    else { Err(Error::Type("QUOTE has incompatible SQL argument types".into())) }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let extra = text.bytes().filter(|byte| *byte == b'\'').count();
    let mut escaped = String::with_capacity(text.len() + extra + 2);
    escaped.push('\'');
    for character in text.chars() {
        if character == '\'' { escaped.push('\\'); }
        escaped.push(character);
    }
    escaped.push('\'');
    Ok(Value::Text(escaped))
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
    }
}

#[cfg(test)]
mod quote_single_allocation_tests {
    use super::*;
    #[test]
    fn escapes_multiple_quotes_and_unicode() {
        assert_eq!(evaluate(&[Value::Text("é'a'b".into())]).unwrap(), Value::Text("'é\\'a\\'b'".into()));
    }
}
