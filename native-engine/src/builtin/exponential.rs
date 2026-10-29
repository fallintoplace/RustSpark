use super::{float_type, Function};
use crate::{scalar::Value, Result};
fn apply(args: &[Value], f: fn(f64) -> f64) -> Result<Value> {
    let Some(value) = super::number(&args[0])? else { return Ok(Value::Null) };
    super::finite(f(value))
}
fn exp(args: &[Value]) -> Result<Value> { apply(args, f64::exp) }
fn expm1(args: &[Value]) -> Result<Value> { apply(args, f64::exp_m1) }

fn sigmoid(args: &[Value]) -> Result<Value> {
    let Some(value) = super::number(&args[0])? else { return Ok(Value::Null); };
    let probability = if value >= 0.0 {
        1.0 / (1.0 + (-value).exp())
    } else {
        let exponential = value.exp();
        exponential / (1.0 + exponential)
    };
    super::finite(probability)
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "sigmoid", min_args: 1, max_args: 1, result_type: float_type, call: sigmoid },

    Function { name: "exp", min_args: 1, max_args: 1, result_type: float_type, call: exp },
    Function { name: "expm1", min_args: 1, max_args: 1, result_type: float_type, call: expm1 },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exponential_math_is_finite() {
        assert_eq!(exp(&[Value::Integer(0)]).unwrap(), Value::Float(1.0));
        assert_eq!(expm1(&[Value::Integer(0)]).unwrap(), Value::Float(0.0));
        assert!(exp(&[Value::Float(1000.0)]).is_err());
    }
}

#[cfg(test)]
mod sigmoid_extension_tests {
    use super::*;
    #[test]
    fn checks_numeric_extremes_and_nulls() {
        assert!(crate::builtin::supports("sigmoid"));
        assert_eq!(sigmoid(&[Value::Integer(0)]).unwrap(), Value::Float(0.5));
        assert_eq!(sigmoid(&[Value::Float(1000.0)]).unwrap(), Value::Float(1.0));
        assert_eq!(sigmoid(&[Value::Float(-1000.0)]).unwrap(), Value::Float(0.0));
        assert_eq!(sigmoid(&[Value::Null]).unwrap(), Value::Null);
    }
}
