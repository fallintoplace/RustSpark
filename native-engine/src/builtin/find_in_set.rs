use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() == 2 && types.iter().all(|x| *x == DataType::Text) { Ok(DataType::Integer) }
    else { Err(Error::Type("FIND_IN_SET has incompatible SQL argument types".into())) }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(needle) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(list) = super::string(&args[1])? else { return Ok(Value::Null); };
    if needle.contains(',') { return Ok(Value::Integer(0)); }
    let found = list.split(',').position(|candidate| candidate == needle);
    let position = found.map_or(Ok(0), |i| i.checked_add(1)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or_else(|| Error::Execution("FIND_IN_SET position overflow".into())))?;
    Ok(Value::Integer(position))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "find_in_set", min_args: 2, max_args: 2, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn spark_sql_semantics() {
        assert_eq!(evaluate(&[Value::Text("ab".into()), Value::Text("a,ab,c".into())]).unwrap(), Value::Integer(2));
        assert_eq!(evaluate(&[Value::Text("a,b".into()), Value::Text("a,b".into())]).unwrap(), Value::Integer(0));
        assert_eq!(evaluate(&[Value::Null, Value::Text("a".into())]).unwrap(), Value::Null);
    }
}
