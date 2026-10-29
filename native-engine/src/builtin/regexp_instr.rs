use super::{Function, regex_support};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() == 2 && types.iter().all(|kind| *kind == DataType::Text) { Ok(DataType::Integer) }
    else { Err(Error::Type("REGEXP_INSTR has incompatible SQL argument types".into())) }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(pattern) = super::string(&args[1])? else { return Ok(Value::Null); };
    regex_support::check_input(text)?;
    let compiled = regex_support::compile(pattern)?;
    let Some(found) = compiled.find(text) else { return Ok(Value::Integer(0)); };
    let index = text[..found.start()].chars().count().checked_add(1)
        .and_then(|n| i64::try_from(n).ok())
        .ok_or_else(|| Error::Execution("REGEXP_INSTR offset overflow".into()))?;
    Ok(Value::Integer(index))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "regexp_instr", min_args: 2, max_args: 2, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn spark_sql_semantics() {
        assert_eq!(evaluate(&[Value::Text("café@rust.dev".into()), Value::Text("@[^.]+".into())]).unwrap(), Value::Integer(5));
        assert_eq!(evaluate(&[Value::Text("abc".into()), Value::Text("z".into())]).unwrap(), Value::Integer(0));
        assert_eq!(evaluate(&[Value::Null, Value::Text("x".into())]).unwrap(), Value::Null);
    }
}
