use super::{float_type, Function};
use crate::{scalar::Value, Error, Result};
fn log1p(args: &[Value]) -> Result<Value> {
    let Some(value) = super::number(&args[0])? else { return Ok(Value::Null) };
    if value <= -1.0 {
        return Err(Error::Execution("LOG1P requires argument greater than -1".into()));
    }
    super::finite(value.ln_1p())
}
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "log1p", min_args: 1, max_args: 1, result_type: float_type, call: log1p },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn small_log_values_do_not_underflow() {
        let Value::Float(result) = log1p(&[Value::Float(1e-12)]).unwrap() else { panic!("float"); };
        assert!((result - 1e-12).abs() < 1e-22);
        assert!(log1p(&[Value::Float(-1.0)]).is_err());
        assert_eq!(log1p(&[Value::Null]).unwrap(), Value::Null);
    }
}
