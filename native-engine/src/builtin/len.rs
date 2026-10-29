use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() == 1 && types[0] == DataType::Text { Ok(DataType::Integer) }
    else { Err(Error::Type("LEN has incompatible SQL argument types".into())) }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let characters = if text.is_ascii() { text.len() } else { text.chars().count() };
    let len = i64::try_from(characters)
        .map_err(|_| Error::Execution("LEN exceeds integer range".into()))?;
    Ok(Value::Integer(len))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "len", min_args: 1, max_args: 1, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn spark_sql_semantics() {
        assert_eq!(evaluate(&[Value::Text("🙂é".into())]).unwrap(), Value::Integer(2));
        assert_eq!(evaluate(&[Value::Text("hello world".into())]).unwrap(),
            Value::Integer(11));
        assert_eq!(evaluate(&[Value::Text("ASCII + é".into())]).unwrap(),
            Value::Integer(9));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}
