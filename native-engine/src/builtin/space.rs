use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Integer { Ok(DataType::Text) }
    else { Err(Error::Type("SPACE requires an integer".into())) }
}
fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(count) = super::integer(&args[0])? else { return Ok(Value::Null) };
    let count = usize::try_from(count.max(0))
        .map_err(|_| Error::Execution("SPACE length overflow".into()))?;
    if count > 1_000_000 { return Err(Error::Execution("SPACE exceeds 1M character budget".into())); }
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(count)
        .map_err(|_| Error::Execution("SPACE output allocation failed".into()))?;
    bytes.resize(count, b' ');
    Ok(Value::Text(String::from_utf8(bytes).expect("spaces are UTF-8")))
}
pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "space", min_args: 1, max_args: 1, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pads_and_bounds_spaces() {
        assert_eq!(evaluate(&[Value::Integer(3)]).unwrap(), Value::Text("   ".into()));
        assert_eq!(evaluate(&[Value::Integer(-3)]).unwrap(), Value::Text("".into()));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(evaluate(&[Value::Integer(i64::MAX)]).is_err());
        assert!(output(&[DataType::Text]).is_err());
    }
}

#[cfg(test)]
mod fallible_space_reservation_tests {
    use super::*;
    #[test]
    fn zero_and_budget_boundaries_preserve_exact_width() {
        assert_eq!(evaluate(&[Value::Integer(0)]).unwrap(), Value::Text(String::new()));
        let result = evaluate(&[Value::Integer(1_000_000)]).unwrap();
        let Value::Text(text) = result else { panic!("SPACE must return text"); };
        assert_eq!(text.len(), 1_000_000);
        assert!(text.bytes().all(|byte| byte == b' '));
        assert!(evaluate(&[Value::Integer(1_000_001)]).is_err());
    }
}
