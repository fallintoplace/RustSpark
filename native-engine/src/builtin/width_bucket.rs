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
    let buckets = super::integer(&args[3])?.expect("non-NULL");
    if !(1..=1_000_000).contains(&buckets) {
        return Err(Error::Execution("WIDTH_BUCKET count must be in 1..=1000000".into()));
    }
    if let (Value::Integer(x), Value::Integer(lower), Value::Integer(upper)) =
        (&args[0], &args[1], &args[2])
    {
        if lower == upper {
            return Err(Error::Execution("WIDTH_BUCKET bounds must differ".into()));
        }
        let (x, lower, upper) = (i128::from(*x), i128::from(*lower), i128::from(*upper));
        let count = i128::from(buckets);
        let slot = if lower < upper {
            if x < lower { 0 } else if x >= upper { count + 1 }
            else { (x - lower) * count / (upper - lower) + 1 }
        } else if x > lower {
            0
        } else if x <= upper {
            count + 1
        } else {
            (lower - x) * count / (lower - upper) + 1
        };
        return Ok(Value::Integer(slot as i64));
    }
    let x = super::number(&args[0])?.expect("non-NULL");
    let lower = super::number(&args[1])?.expect("non-NULL");
    let upper = super::number(&args[2])?.expect("non-NULL");
    if !x.is_finite() || !lower.is_finite() || !upper.is_finite() || lower == upper {
        return Err(Error::Execution("WIDTH_BUCKET requires finite increasing bounds".into()));
    }
    let descending = lower > upper;
    let slot = if (descending && x > lower) || (!descending && x < lower) { 0 }
        else if (descending && x <= upper) || (!descending && x >= upper) { buckets + 1 }
        else {
            // Halving avoids intermediate overflow for very large finite bounds.
            let span = upper - lower;
            let ratio = if span.is_finite() {
                (x - lower) / span
            } else {
                ((x / 2.0) - (lower / 2.0)) / ((upper / 2.0) - (lower / 2.0))
            };
            ((ratio * buckets as f64).floor() as i64 + 1).clamp(1, buckets)
        };
    Ok(Value::Integer(slot))
}

fn clamp_type(types: &[DataType]) -> Result<DataType> {
    if types.len() == 3
        && types.iter().all(|kind| *kind == types[0])
        && matches!(types[0], DataType::Integer | DataType::Float)
    {
        Ok(types[0])
    } else {
        Err(Error::Type("CLAMP expects three numbers of the same type".into()))
    }
}
fn clamp(args: &[Value]) -> Result<Value> {
    if args.iter().any(Value::is_null) { return Ok(Value::Null); }
    match (&args[0], &args[1], &args[2]) {
        (Value::Integer(value), Value::Integer(lower), Value::Integer(upper)) => {
            if lower > upper { return Err(Error::Execution("CLAMP bounds are reversed".into())); }
            Ok(Value::Integer((*value).clamp(*lower, *upper)))
        }
        (Value::Float(value), Value::Float(lower), Value::Float(upper)) => {
            if !value.is_finite() || !lower.is_finite() || !upper.is_finite() || lower > upper {
                return Err(Error::Execution("CLAMP requires finite increasing bounds".into()));
            }
            Ok(Value::Float((*value).clamp(*lower, *upper)))
        }
        _ => Err(Error::Type("CLAMP expects three numbers of the same type".into())),
    }
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "clamp", min_args: 3, max_args: 3, result_type: clamp_type, call: clamp },

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
        let descending = |x| width_bucket(&[
            Value::Float(x), Value::Float(5.2), Value::Float(0.5), Value::Integer(2)
        ]).unwrap();
        assert_eq!(descending(5.3), Value::Integer(0));
        assert_eq!(descending(5.2), Value::Integer(1));
        assert_eq!(descending(2.0), Value::Integer(2));
        assert_eq!(descending(-0.9), Value::Integer(3));
        assert_eq!(descending(0.5), Value::Integer(3));
        assert!(width_bucket(&[Value::Integer(2), Value::Integer(1), Value::Integer(1), Value::Integer(10)]).is_err());
    }
}

#[cfg(test)]
mod clamp_extension_tests {
    use super::*;
    #[test]
    fn checks_numeric_extremes_and_nulls() {
        assert!(crate::builtin::supports("clamp"));
        assert_eq!(clamp(&[Value::Integer(i64::MAX), Value::Integer(1), Value::Integer(i64::MAX - 1)]).unwrap(), Value::Integer(i64::MAX - 1));
        assert_eq!(clamp(&[Value::Integer(-5), Value::Integer(0), Value::Integer(10)]).unwrap(), Value::Integer(0));
        assert_eq!(clamp(&[Value::Float(1.5), Value::Float(0.0), Value::Float(1.0)]).unwrap(), Value::Float(1.0));
        assert!(clamp(&[Value::Integer(1), Value::Integer(5), Value::Integer(0)]).is_err());
        assert_eq!(clamp(&[Value::Null, Value::Integer(0), Value::Integer(1)]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod exact_bigint_width_bucket_tests {
    use super::*;
    #[test]
    fn distinguishes_adjacent_64_bit_boundaries_without_f64_rounding() {
        let run = |x, low, high| width_bucket(&[
            Value::Integer(x), Value::Integer(low), Value::Integer(high),
            Value::Integer(2),
        ]).unwrap();
        assert_eq!(run(i64::MAX - 2, i64::MAX - 2, i64::MAX), Value::Integer(1));
        assert_eq!(run(i64::MAX - 1, i64::MAX - 2, i64::MAX), Value::Integer(2));
        assert_eq!(run(i64::MAX, i64::MAX - 2, i64::MAX), Value::Integer(3));
        assert_eq!(run(i64::MIN + 1, i64::MIN, i64::MIN + 2), Value::Integer(2));
        assert_eq!(run(i64::MAX - 1, i64::MAX, i64::MAX - 2), Value::Integer(2));
        assert_eq!(run(i64::MAX, i64::MAX, i64::MAX - 2), Value::Integer(1));
    }
}

#[cfg(test)]
mod integer_width_bucket_dispatch_tests {
    use super::*;

    #[test]
    fn exact_large_integers_and_mixed_float_inputs_keep_their_buckets() {
        let run = |x, low, high| width_bucket(&[
            Value::Integer(x), Value::Integer(low), Value::Integer(high), Value::Integer(5),
        ]).unwrap();
        assert_eq!(run(i64::MAX - 1, i64::MAX - 5, i64::MAX), Value::Integer(5));
        assert_eq!(run(i64::MIN + 1, i64::MIN, i64::MIN + 5), Value::Integer(2));
        assert_eq!(run(i64::MIN, i64::MAX, i64::MIN), Value::Integer(6));
        assert_eq!(width_bucket(&[
            Value::Float(2.5), Value::Integer(0), Value::Integer(10), Value::Integer(2),
        ]).unwrap(), Value::Integer(1));
        assert_eq!(width_bucket(&[
            Value::Null, Value::Integer(0), Value::Integer(10), Value::Integer(2),
        ]).unwrap(), Value::Null);
    }
}
