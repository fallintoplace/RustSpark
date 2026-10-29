use super::{float_type, Function};
use crate::{scalar::Value, Error, Result};
fn checked_log(value: &Value, base: Option<f64>) -> Result<Value> {
    let Some(x) = super::number(value)? else { return Ok(Value::Null) };
    if x <= 0.0 { return Err(Error::Execution("logarithm requires positive argument".into())); }
    let result = match base {
        Some(b) if b > 0.0 && b != 1.0 => x.ln() / b.ln(),
        Some(_) => return Err(Error::Execution("logarithm base must be positive and not one".into())),
        None => x.ln(),
    };
    super::finite(result)
}
fn ln(args: &[Value]) -> Result<Value> { checked_log(&args[0], None) }
fn log2(args: &[Value]) -> Result<Value> { checked_log(&args[0], Some(2.0)) }
fn log10(args: &[Value]) -> Result<Value> { checked_log(&args[0], Some(10.0)) }
fn log(args: &[Value]) -> Result<Value> {
    if args.len() == 1 { return ln(args); }
    let Some(base) = super::number(&args[0])? else { return Ok(Value::Null) };
    checked_log(&args[1], Some(base))
}
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "ln", min_args: 1, max_args: 1, result_type: float_type, call: ln },
    Function { name: "log", min_args: 1, max_args: 2, result_type: float_type, call: log },
    Function { name: "log2", min_args: 1, max_args: 1, result_type: float_type, call: log2 },
    Function { name: "log10", min_args: 1, max_args: 1, result_type: float_type, call: log10 },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn logarithms_check_bases_and_negative_values() {
        assert_eq!(log2(&[Value::Integer(8)]).unwrap(), Value::Float(3.0));
        assert!(ln(&[Value::Integer(-1)]).is_err());
        assert!(log(&[Value::Integer(1), Value::Integer(8)]).is_err());
        assert_eq!(log(&[Value::Null, Value::Integer(8)]).unwrap(), Value::Null);
    }
}
