use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() == 1 { Ok(DataType::Boolean) }
    else { Err(Error::Type("null check requires one argument".into())) }
}

fn isnull(args: &[Value]) -> Result<Value> { Ok(Value::Boolean(args[0].is_null())) }
fn isnotnull(args: &[Value]) -> Result<Value> { Ok(Value::Boolean(!args[0].is_null())) }


fn atleastnnonnulls_type(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Integer { Ok(DataType::Boolean) }
    else { Err(Error::Type("ATLEASTNNONNULLS threshold must be integer".into())) }
}
fn atleastnnonnulls(args: &[Value]) -> Result<Value> {
    let Some(minimum) = super::integer(&args[0])? else { return Ok(Value::Null); };
    if minimum <= 0 { return Ok(Value::Boolean(true)); }
    let mut found = 0_i64;
    for value in &args[1..] {
        if !value.is_null() && !matches!(value, Value::Float(n) if n.is_nan()) {
            found += 1;
            if found >= minimum { return Ok(Value::Boolean(true)); }
        }
    }
    Ok(Value::Boolean(false))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "atleastnnonnulls", min_args: 2, max_args: 128, result_type: atleastnnonnulls_type, call: atleastnnonnulls },
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

#[cfg(test)]
mod atleastnnonnulls_extension_tests {
    use super::*;
    #[test]
    fn covers_null_and_boundary_values() {
        assert!(crate::builtin::supports("atleastnnonnulls"));
        assert_eq!(atleastnnonnulls(&[Value::Integer(2), Value::Float(f64::NAN), Value::Integer(1), Value::Text("x".into())]).unwrap(), Value::Boolean(true));
        assert_eq!(atleastnnonnulls(&[Value::Integer(2), Value::Float(f64::NAN), Value::Null, Value::Integer(1)]).unwrap(), Value::Boolean(false));
        assert_eq!(atleastnnonnulls(&[Value::Integer(0), Value::Null]).unwrap(), Value::Boolean(true));
        assert_eq!(atleastnnonnulls(&[Value::Integer(1), Value::Null]).unwrap(), Value::Boolean(false));
    }
}
