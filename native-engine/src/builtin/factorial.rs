use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

const FACTORIALS: [i64; 21] = [
    1, 1, 2, 6, 24, 120, 720,
    5040, 40320, 362880, 3628800, 39916800, 479001600, 6227020800,
    87178291200, 1307674368000, 20922789888000, 355687428096000, 6402373705728000, 121645100408832000, 2432902008176640000,
];

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Integer { Ok(DataType::Integer) }
    else { Err(Error::Type("FACTORIAL requires integer input".into())) }
}
fn factorial(args: &[Value]) -> Result<Value> {
    let Some(value) = super::integer(&args[0])? else { return Ok(Value::Null) };
    if !(0..=20).contains(&value) {
        return Ok(Value::Null);
    }
    Ok(Value::Integer(FACTORIALS[value as usize]))
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

#[cfg(test)]
mod factorial_lookup_tests {
    use super::*;
    #[test]
    fn matches_exact_factorials_across_domain() {
        assert_eq!(factorial(&[Value::Integer(10)]).unwrap(), Value::Integer(3628800));
        assert_eq!(factorial(&[Value::Integer(20)]).unwrap(), Value::Integer(2432902008176640000));
        assert_eq!(factorial(&[Value::Integer(-1)]).unwrap(), Value::Null);
    }
}
