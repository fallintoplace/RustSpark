use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() == 1 && types[0] == DataType::Text { Ok(DataType::Integer) }
    else { Err(Error::Type("BIT_LENGTH has incompatible SQL argument types".into())) }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let bits = text.len().checked_mul(8).and_then(|n| i64::try_from(n).ok())
        .ok_or_else(|| Error::Execution("BIT_LENGTH exceeds integer range".into()))?;
    Ok(Value::Integer(bits))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "bit_length", min_args: 1, max_args: 1, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn spark_sql_semantics() {
        assert_eq!(evaluate(&[Value::Text("é".into())]).unwrap(), Value::Integer(16));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}
