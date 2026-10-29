use super::{same_type, Function};
use crate::{scalar::Value, Result};
use std::cmp::Ordering;

fn evaluate(args: &[Value]) -> Result<Value> {
    if args[0].is_null() { return Ok(Value::Null); }
    if args[1].is_null() { return Ok(args[0].clone()); }
    if args[0].sql_compare(&args[1])? == Some(Ordering::Equal) {
        Ok(Value::Null)
    } else {
        Ok(args[0].clone())
    }
}
pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "nullif", min_args: 2, max_args: 2, result_type: same_type, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn equal_nonnull_values_become_null() {
        assert_eq!(evaluate(&[Value::Integer(5), Value::Integer(5)]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Integer(5), Value::Integer(6)]).unwrap(), Value::Integer(5));
        assert_eq!(evaluate(&[Value::Integer(5), Value::Null]).unwrap(), Value::Integer(5));
    }
}
