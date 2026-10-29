use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() == 1 && types.iter().all(|x| matches!(x, DataType::Float | DataType::Integer)) { Ok(DataType::Float) }
    else { Err(Error::Type("SEC requires numeric input".into())) }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(value) = super::number(&args[0])? else { return Ok(Value::Null); };
    super::finite(1.0 / value.cos())
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "sec", min_args: 1, max_args: 1, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn spark_sql_semantics() {
        assert_eq!(evaluate(&[Value::Integer(0)]).unwrap(), Value::Float(1.0));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}
