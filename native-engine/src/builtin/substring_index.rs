use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() == 3 && types[0] == DataType::Text && types[1] == DataType::Text && types[2] == DataType::Integer { Ok(DataType::Text) }
    else { Err(Error::Type("SUBSTRING_INDEX has incompatible SQL argument types".into())) }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(delimiter) = super::string(&args[1])? else { return Ok(Value::Null); };
    let Some(count) = super::integer(&args[2])? else { return Ok(Value::Null); };
    if count == 0 || delimiter.is_empty() { return Ok(Value::Text(String::new())); }
    let Ok(index) = usize::try_from(count.unsigned_abs()) else { return Ok(Value::Text(text.into())); };
    let part = if count > 0 {
        text.match_indices(delimiter).nth(index - 1).map(|(at, _)| &text[..at])
    } else {
        text.rmatch_indices(delimiter).nth(index - 1).map(|(at, _)| &text[at + delimiter.len()..])
    };
    Ok(Value::Text(part.unwrap_or(text).to_string()))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "substring_index", min_args: 3, max_args: 3, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn spark_sql_semantics() {
        let input = Value::Text("www.apache.org".into());
        let dot = Value::Text(".".into());
        assert_eq!(evaluate(&[input.clone(), dot.clone(), Value::Integer(2)]).unwrap(), Value::Text("www.apache".into()));
        assert_eq!(evaluate(&[input.clone(), dot.clone(), Value::Integer(-2)]).unwrap(), Value::Text("apache.org".into()));
        assert_eq!(evaluate(&[input, dot, Value::Integer(0)]).unwrap(), Value::Text("".into()));
        assert_eq!(evaluate(&[Value::Null, Value::Text(".".into()), Value::Integer(1)]).unwrap(), Value::Null);
    }
}
