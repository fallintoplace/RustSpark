//! Native inverse tangent in degrees over typed SQL numeric values.
use super::{float_type, Function};
use crate::{scalar::Value, Result};

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(x) = super::number(&args[0])? else { return Ok(Value::Null) };
    super::finite(x.atan().to_degrees())
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "atand", min_args: 1, max_args: 1,
    result_type: float_type, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::DataType;

    #[test]
    fn native_function_handles_numeric_values_null_and_types() {
        let Value::Float(result) = evaluate(&[Value::Integer(1)]).unwrap()
            else { panic!("expected float result") };
        assert!((result - 45.0).abs() < 1e-12);
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(float_type(&[DataType::Text]).is_err());
        assert!(crate::builtin::supports("ATAND"));
        
    }
}
