use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("WIDTH_BUCKET", args, 4, 4)?;
    for arg in &args[..3] { numeric_arg(arg, schema)?; }
    if !matches!(args[3], Expr::Literal(Value::Null))
        && args[3].output_type(schema)? != DataType::Integer {
        return Err(Error::Type("WIDTH_BUCKET bucket count must be integer".into()));
    }
    Ok(DataType::Integer)
}
pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    let values = args.iter().map(|arg| arg.evaluate(batch, row))
        .collect::<Result<Vec<_>>>()?;
    if values.iter().any(Value::is_null) { return Ok(Value::Null); }
    let buckets = match values[3] {
        Value::Integer(n) if (1..=1_000_000).contains(&n) => n,
        _ => return Err(Error::Type("WIDTH_BUCKET requires 1..=1000000 buckets".into())),
    };
    // Preserve BIGINT buckets beyond the exact f64 integer range.
    if let (Value::Integer(x), Value::Integer(lower), Value::Integer(upper)) =
        (&values[0], &values[1], &values[2]) {
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
    let value = as_f64(&values[0])?;
    let lower = as_f64(&values[1])?;
    let upper = as_f64(&values[2])?;
    if !value.is_finite() || !lower.is_finite() || !upper.is_finite()
        || lower == upper {
        return Err(Error::Execution("WIDTH_BUCKET requires finite ordered bounds".into()));
    }
    let descending = lower > upper;
    let bucket = if (descending && value > lower) || (!descending && value < lower) {
        0
    } else if (descending && value <= upper) || (!descending && value >= upper) {
        buckets + 1
    } else {
        let span = upper - lower;
        let ratio = if span.is_finite() {
            (value - lower) / span
        } else {
            ((value / 2.0) - (lower / 2.0)) / ((upper / 2.0) - (lower / 2.0))
        };
        (((ratio * buckets as f64).floor() as i64) + 1).clamp(1, buckets)
    };
    Ok(Value::Integer(bucket))
}

#[cfg(test)]
mod descending_bucket_sql_tests {
    use super::*;
    use crate::schema::Field;
    #[test]
    fn handles_descending_limits_and_outside_values() {
        let schema = Schema::new(vec![Field::new("v", DataType::Float, false)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), vec![vec![Value::Float(5.2)], vec![Value::Float(2.0)], vec![Value::Float(0.5)], vec![Value::Float(5.3)]]).unwrap();
        let args = [Expr::Column("v".into()), Expr::Literal(Value::Float(5.2)), Expr::Literal(Value::Float(0.5)), Expr::Literal(Value::Integer(2))];
        assert_eq!(output_type(&args, &schema).unwrap(), DataType::Integer);
        for (row, expected) in [1, 2, 3, 0].into_iter().enumerate() {
            assert_eq!(eval(&args, &batch, row).unwrap(), Value::Integer(expected));
        }
    }
}

#[cfg(test)]
mod exact_integer_bucket_sql_tests {
    use super::*;
    #[test]
    fn distinguishes_adjacent_bigint_bounds_in_sql() {
        let batch = Batch::empty(Schema::new(vec![]).unwrap());
        let run = |x: i64, lower: i64, upper: i64| {
            let args = [
                Expr::Literal(Value::Integer(x)),
                Expr::Literal(Value::Integer(lower)),
                Expr::Literal(Value::Integer(upper)),
                Expr::Literal(Value::Integer(2)),
            ];
            eval(&args, &batch, 0).unwrap()
        };
        assert_eq!(run(i64::MAX - 2, i64::MAX - 2, i64::MAX), Value::Integer(1));
        assert_eq!(run(i64::MAX - 1, i64::MAX - 2, i64::MAX), Value::Integer(2));
        assert_eq!(run(i64::MAX, i64::MAX - 2, i64::MAX), Value::Integer(3));
        assert_eq!(run(i64::MAX - 1, i64::MAX, i64::MAX - 2), Value::Integer(2));
    }
}
