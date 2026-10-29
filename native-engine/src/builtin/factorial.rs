use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Integer { Ok(DataType::Integer) }
    else { Err(Error::Type("FACTORIAL requires integer input".into())) }
}
fn factorial(args: &[Value]) -> Result<Value> {
    let Some(value) = super::integer(&args[0])? else { return Ok(Value::Null) };
    if !(0..=20).contains(&value) {
        return Ok(Value::Null);
    }
    let mut product = 1_i64;
    for current in 2..=value {
        product = product.checked_mul(current)
            .ok_or_else(|| Error::Execution("FACTORIAL overflow".into()))?;
    }
    Ok(Value::Integer(product))
}
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "factorial", min_args: 1, max_args: 1, result_type: output, call: factorial },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn factorial_bounds_and_nulls() {
        assert_eq!(factorial(&[Value::Integer(0)]).unwrap(), Value::Integer(1));
        assert_eq!(factorial(&[Value::Integer(5)]).unwrap(), Value::Integer(120));
        assert_eq!(factorial(&[Value::Integer(21)]).unwrap(), Value::Null);
        assert_eq!(factorial(&[Value::Integer(-1)]).unwrap(), Value::Null);
        assert_eq!(factorial(&[Value::Integer(i64::MAX)]).unwrap(), Value::Null);
        assert_eq!(factorial(&[Value::Null]).unwrap(), Value::Null);
    }
}
