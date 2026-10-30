//! Native numerically stable softplus over typed SQL numeric values.
use super::{float_type, Function};
use crate::{scalar::Value, Result};

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(x) = super::number(&args[0])? else { return Ok(Value::Null) };
    super::finite(if x > 0.0 { x + (-x).exp().ln_1p() } else { x.exp().ln_1p() })
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "softplus", min_args: 1, max_args: 1,
    result_type: float_type, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::DataType;

    #[test]
    fn native_function_handles_numeric_values_null_and_types() {
        let Value::Float(result) = evaluate(&[Value::Float(0.0)]).unwrap()
            else { panic!("expected float result") };
        assert!((result - std::f64::consts::LN_2).abs() < 1e-12);
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(float_type(&[DataType::Text]).is_err());
        assert!(crate::builtin::supports("SOFTPLUS"));
        assert_eq!(evaluate(&[Value::Float(1_000.0)]).unwrap(), Value::Float(1_000.0));
    }
}
