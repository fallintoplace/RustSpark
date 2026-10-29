use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() == 1 && types[0] == DataType::Integer { Ok(DataType::Text) }
    else { Err(Error::Type("BIN requires an integer input".into())) }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(value) = super::integer(&args[0])? else { return Ok(Value::Null); };
    Ok(Value::Text(format!("{value:b}")))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "bin", min_args: 1, max_args: 1, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn spark_sql_semantics() {
        assert_eq!(evaluate(&[Value::Integer(5)]).unwrap(), Value::Text("101".into()));
        assert_eq!(evaluate(&[Value::Integer(-1)]).unwrap(), Value::Text("1".repeat(64)));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}
