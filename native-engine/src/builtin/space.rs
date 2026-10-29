use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() == 1 && types[0] == DataType::Integer { Ok(DataType::Text) }
    else { Err(Error::Type("SPACE has incompatible SQL argument types".into())) }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(n) = super::integer(&args[0])? else { return Ok(Value::Null); };
    let count = usize::try_from(n.max(0))
        .map_err(|_| Error::Execution("SPACE length exceeds platform range".into()))?;
    if count > 16 * 1024 * 1024 {
        return Err(Error::Execution("SPACE output exceeds native 16 MiB budget".into()));
    }
    Ok(Value::Text(" ".repeat(count)))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "space", min_args: 1, max_args: 1, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn spark_sql_semantics() {
        assert_eq!(evaluate(&[Value::Integer(3)]).unwrap(), Value::Text("   ".into()));
        assert_eq!(evaluate(&[Value::Integer(-3)]).unwrap(), Value::Text("".into()));
        assert!(evaluate(&[Value::Integer(i64::MAX)]).is_err());
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}
