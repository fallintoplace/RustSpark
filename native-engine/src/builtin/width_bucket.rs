use super::{numeric, Function};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    for &kind in &types[..3] { numeric(kind)?; }
    if types[3] != DataType::Integer {
        return Err(Error::Type("WIDTH_BUCKET count must be integer".into()));
    }
    Ok(DataType::Integer)
}
fn width_bucket(args: &[Value]) -> Result<Value> {
    if args.iter().any(Value::is_null) { return Ok(Value::Null); }
    let x = super::number(&args[0])?.expect("non-NULL");
    let lower = super::number(&args[1])?.expect("non-NULL");
    let upper = super::number(&args[2])?.expect("non-NULL");
    let buckets = super::integer(&args[3])?.expect("non-NULL");
    if !(1..=1_000_000).contains(&buckets) {
        return Err(Error::Execution("WIDTH_BUCKET count must be in 1..=1000000".into()));
    }
    if !x.is_finite() || !lower.is_finite() || !upper.is_finite() || lower >= upper {
        return Err(Error::Execution("WIDTH_BUCKET requires finite increasing bounds".into()));
    }
    let slot = if x < lower { 0 }
        else if x >= upper { buckets + 1 }
        else {
            let ratio = (x - lower) / (upper - lower);
            ((ratio * buckets as f64).floor() as i64 + 1).clamp(1, buckets)
        };
    Ok(Value::Integer(slot))
}
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "width_bucket", min_args: 4, max_args: 4, result_type: output, call: width_bucket },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn histogram_edges_are_stable() {
        let bounds = |x| width_bucket(&[
            Value::Float(x), Value::Integer(0), Value::Integer(100), Value::Integer(10)
        ]).unwrap();
        assert_eq!(bounds(-1.0), Value::Integer(0));
        assert_eq!(bounds(0.0), Value::Integer(1));
        assert_eq!(bounds(45.0), Value::Integer(5));
        assert_eq!(bounds(100.0), Value::Integer(11));
        assert!(width_bucket(&[Value::Integer(2), Value::Integer(10), Value::Integer(0), Value::Integer(10)]).is_err());
    }
}
