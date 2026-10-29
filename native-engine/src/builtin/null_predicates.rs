use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() == 1 { Ok(DataType::Boolean) }
    else { Err(Error::Type("null check requires one argument".into())) }
}

fn isnull(args: &[Value]) -> Result<Value> { Ok(Value::Boolean(args[0].is_null())) }
fn isnotnull(args: &[Value]) -> Result<Value> { Ok(Value::Boolean(!args[0].is_null())) }

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "isnull", min_args: 1, max_args: 1, result_type: output, call: isnull },
    Function { name: "isnotnull", min_args: 1, max_args: 1, result_type: output, call: isnotnull },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn checks_nulls_without_three_valued_results() {
        assert_eq!(isnull(&[Value::Null]).unwrap(), Value::Boolean(true));
        assert_eq!(isnotnull(&[Value::Null]).unwrap(), Value::Boolean(false));
        assert_eq!(isnull(&[Value::Integer(0)]).unwrap(), Value::Boolean(false));
        assert_eq!(isnotnull(&[Value::Text("".into())]).unwrap(), Value::Boolean(true));
    }
}
