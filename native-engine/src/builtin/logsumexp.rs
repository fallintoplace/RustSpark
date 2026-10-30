//! Stable two-term log-sum-exp for native numeric expressions.
use super::{float_type, Function};
use crate::{scalar::Value, Result};

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(a) = super::number(&args[0])? else { return Ok(Value::Null) };
    let Some(b) = super::number(&args[1])? else { return Ok(Value::Null) };
    if !a.is_finite() || !b.is_finite() {
        return super::finite(f64::NAN);
    }
    let maximum = a.max(b);
    super::finite(maximum + ((a - maximum).exp() + (b - maximum).exp()).ln())
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "logsumexp", min_args: 2, max_args: 2,
    result_type: float_type, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::DataType;
    #[test]
    fn large_exponents_remain_finite_and_small_ones_match_math() {
        let Value::Float(result) = evaluate(&[Value::Float(1000.0), Value::Float(1000.0)]).unwrap()
            else { panic!("expected float") };
        assert!((result - (1000.0 + std::f64::consts::LN_2)).abs() < 1e-12);
        let Value::Float(result) = evaluate(&[Value::Integer(0), Value::Integer(0)]).unwrap()
            else { panic!("expected float") };
        assert!((result - std::f64::consts::LN_2).abs() < 1e-12);
        assert_eq!(evaluate(&[Value::Null, Value::Integer(1)]).unwrap(), Value::Null);
        assert!(float_type(&[DataType::Text, DataType::Float]).is_err());
        assert!(crate::builtin::supports("LOGSUMEXP"));
    }
}
