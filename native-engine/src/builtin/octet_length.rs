use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() == 1 && types[0] == DataType::Text { Ok(DataType::Integer) }
    else { Err(Error::Type("OCTET_LENGTH has incompatible SQL argument types".into())) }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let len = i64::try_from(text.len())
        .map_err(|_| Error::Execution("OCTET_LENGTH exceeds integer range".into()))?;
    Ok(Value::Integer(len))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "octet_length", min_args: 1, max_args: 1, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn spark_sql_semantics() {
        assert_eq!(evaluate(&[Value::Text("café".into())]).unwrap(), Value::Integer(5));
        assert_eq!(evaluate(&[Value::Text("".into())]).unwrap(), Value::Integer(0));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}
