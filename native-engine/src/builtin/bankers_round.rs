use super::{float_type, Function};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    float_type(&types[..1])?;
    if types.len() == 2 && types[1] != DataType::Integer {
        return Err(Error::Type("BROUND scale must be an integer".into()));
    }
    Ok(DataType::Float)
}
fn round_even(args: &[Value]) -> Result<Value> {
    let Some(x) = super::number(&args[0])? else { return Ok(Value::Null) };
    let scale = if args.len() == 2 {
        let Some(scale) = super::integer(&args[1])? else { return Ok(Value::Null) };
        scale
    } else { 0 };
    if !(-15..=15).contains(&scale) {
        return Err(Error::Execution("BROUND scale must be within [-15,15]".into()));
    }
    let factor = 10_f64.powi(scale.unsigned_abs() as i32);
    let result = if scale >= 0 { (x * factor).round_ties_even() / factor }
        else { (x / factor).round_ties_even() * factor };
    super::finite(result)
}
fn rint(args: &[Value]) -> Result<Value> { round_even(args) }
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "bround", min_args: 1, max_args: 2, result_type: output, call: round_even },
    Function { name: "rint", min_args: 1, max_args: 1, result_type: output, call: rint },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ties_to_even_differs_from_round_away_from_zero() {
        assert_eq!(rint(&[Value::Float(2.5)]).unwrap(), Value::Float(2.0));
        assert_eq!(rint(&[Value::Float(3.5)]).unwrap(), Value::Float(4.0));
        assert_eq!(round_even(&[Value::Float(125.0), Value::Integer(-1)]).unwrap(), Value::Float(120.0));
        assert_eq!(round_even(&[Value::Null]).unwrap(), Value::Null);
    }
}
