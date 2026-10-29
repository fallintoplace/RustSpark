use super::Function;
use crate::{scalar::Value, schema::DataType, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }
fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    Ok(super::url_decode::decode(text).map_or(Value::Null, Value::Text))
}
pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "try_url_decode", min_args: 1, max_args: 1, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn converts_decode_errors_to_null() {
        assert_eq!(evaluate(&[Value::Text("a+b".into())]).unwrap(), Value::Text("a b".into()));
        assert_eq!(evaluate(&[Value::Text("%GG".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Text("%FF".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}
