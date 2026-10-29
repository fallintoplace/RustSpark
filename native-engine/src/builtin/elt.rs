use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] != DataType::Integer || !types[1..].iter().all(|kind| *kind == DataType::Text) {
        return Err(Error::Type("ELT requires integer index and text alternatives".into()));
    }
    Ok(DataType::Text)
}
fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(index) = super::integer(&args[0])? else { return Ok(Value::Null) };
    let Some(offset) = index.checked_sub(1).and_then(|n| usize::try_from(n).ok()) else {
        return Ok(Value::Null);
    };
    Ok(args.get(offset + 1).cloned().unwrap_or(Value::Null))
}
pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "elt", min_args: 2, max_args: 128, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn picks_one_based_text_arguments() {
        let args = [Value::Integer(2), Value::Text("a".into()), Value::Text("b".into())];
        assert_eq!(evaluate(&args).unwrap(), Value::Text("b".into()));
        assert_eq!(evaluate(&[Value::Integer(0), Value::Text("a".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Integer(5), Value::Text("a".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Null, Value::Text("a".into())]).unwrap(), Value::Null);
        assert!(output(&[DataType::Text, DataType::Text]).is_err());
    }
}
