use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() < 2 || types[0] != DataType::Integer {
        return Err(Error::Type("ELT requires integer index and one or more values".into()));
    }
    super::same_type(&types[1..])
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(index) = super::integer(&args[0])? else { return Ok(Value::Null); };
    let Ok(index) = usize::try_from(index) else { return Ok(Value::Null); };
    Ok(index.checked_sub(1).and_then(|n| args.get(n + 1))
        .cloned().unwrap_or(Value::Null))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "elt", min_args: 2, max_args: 128, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn spark_sql_semantics() {
        assert_eq!(evaluate(&[Value::Integer(2), Value::Text("a".into()), Value::Text("b".into())]).unwrap(), Value::Text("b".into()));
        assert_eq!(evaluate(&[Value::Integer(0), Value::Integer(3)]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Integer(5), Value::Integer(3)]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Null, Value::Text("a".into())]).unwrap(), Value::Null);
    }
}
