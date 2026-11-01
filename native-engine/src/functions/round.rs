use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("ROUND", args, 1, 2)?;
    numeric_arg(&args[0], schema)?;
    if args.len() == 2 && !matches!(args[1], Expr::Literal(Value::Null))
        && args[1].output_type(schema)? != DataType::Integer {
        return Err(Error::Type("ROUND precision must be an integer".into()));
    }
    Ok(if args[0].output_type(schema)? == DataType::Integer { DataType::Integer } else { DataType::Float })
}

pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    let number = args[0].evaluate(batch, row)?;
    if number.is_null() { return Ok(Value::Null); }
    let precision = if args.len() == 2 {
        match args[1].evaluate(batch, row)? {
            Value::Null => return Ok(Value::Null),
            Value::Integer(n) if (-15..=15).contains(&n) => n as i32,
            _ => return Err(Error::Type("ROUND precision must be within -15..15".into())),
        }
    } else { 0 };
    if let Value::Integer(value) = number {
        if precision >= 0 { return Ok(Value::Integer(value)); }
        let unit = 10_i128.pow((-precision) as u32);
        let value = i128::from(value);
        let magnitude = value.abs();
        let rounded = ((magnitude + unit / 2) / unit) * unit * value.signum();
        return i64::try_from(rounded).map(Value::Integer)
            .map_err(|_| Error::Execution("ROUND integer overflow".into()));
    }
    let Value::Float(number) = number else { return Err(Error::Type("ROUND requires numeric input".into())); };
    let factor = 10f64.powi(precision.abs());
    let result = if precision >= 0 {
        // Scaling large finite values can overflow before rounding.
        if number.abs() > f64::MAX / factor { number }
        else { (number * factor).round() / factor }
    } else {
        // At this magnitude, consecutive f64 values are farther apart than
        // the requested decimal unit. A divide/multiply round trip can
        // introduce an unrelated one-ULP error.
        if number.abs() >= factor * ((1u64 << 53) as f64) {
            number
        } else {
            (number / factor).round() * factor
        }
    };
    if !result.is_finite() {
        return Err(Error::Execution("ROUND non-finite result".into()));
    }
    Ok(Value::Float(result))
}

#[cfg(test)]
mod bigint_round_sql_tests {
    use super::*;
    use crate::schema::Field;
    #[test]
    fn preserves_bigint_outputs_for_sql_calls() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), vec![vec![Value::Integer(9_007_199_254_740_993)], vec![Value::Integer(-125)]]).unwrap();
        let args = [Expr::Column("n".into()), Expr::Literal(Value::Integer(-1))];
        assert_eq!(output_type(&args, &schema).unwrap(), DataType::Integer);
        assert_eq!(eval(&args, &batch, 0).unwrap(), Value::Integer(9_007_199_254_740_990));
        assert_eq!(eval(&args, &batch, 1).unwrap(), Value::Integer(-130));
        assert_eq!(eval(&[Expr::Column("n".into())], &batch, 0).unwrap(), Value::Integer(9_007_199_254_740_993));
    }
}

#[cfg(test)]
mod finite_round_extreme_tests {
    use super::*;
    #[test]
    fn sql_round_handles_extreme_finite_float_values() {
        let batch = Batch::empty(Schema::new(vec![]).unwrap());
        for value in [f64::MAX, -f64::MAX] {
            let args = [
                Expr::Literal(Value::Float(value)),
                Expr::Literal(Value::Integer(15)),
            ];
            assert_eq!(eval(&args, &batch, 0).unwrap(), Value::Float(value));
        }
        let args = [
            Expr::Literal(Value::Float(149.0)),
            Expr::Literal(Value::Integer(-2)),
        ];
        assert_eq!(eval(&args, &batch, 0).unwrap(), Value::Float(100.0));
    }
}

#[cfg(test)]
mod negative_scale_large_float_regressions {
    use super::*;

    #[test]
    fn already_rounded_large_doubles_do_not_change_bits() {
        let batch = Batch::empty(Schema::new(vec![]).unwrap());
        for (value, precision) in [
            (-3.3036582842864976e239, -1),
            (2.4560805462934964e260, -3),
            (6.765823898994833e96, -8),
            (f64::MAX, -15),
            (-f64::MAX, -15),
        ] {
            let args = [Expr::Literal(Value::Float(value)),
                Expr::Literal(Value::Integer(precision))];
            assert_eq!(eval(&args, &batch, 0).unwrap(), Value::Float(value),
                "value={value}, precision={precision}");
        }
        let args = [Expr::Literal(Value::Float(-155.0)),
            Expr::Literal(Value::Integer(-1))];
        assert_eq!(eval(&args, &batch, 0).unwrap(), Value::Float(-160.0));
    }
}
