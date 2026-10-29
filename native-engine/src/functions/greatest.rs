use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("GREATEST", args, 2, 64)?;
    let mut output = None;
    for expression in args {
        if matches!(expression, Expr::Literal(Value::Null)) { continue; }
        let next = expression.output_type(schema)?;
        output = Some(match (output, next) {
            (None, kind) => kind,
            (Some(kind), next) if kind == next => kind,
            (Some(DataType::Integer), DataType::Float)
            | (Some(DataType::Float), DataType::Integer) => DataType::Float,
            _ => return Err(Error::Type("GREATEST values require compatible types".into())),
        });
    }
    Ok(output.unwrap_or(DataType::Text))
}

pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    let target = output_type(args, batch.schema())?;
    let mut greatest: Option<Value> = None;
    for arg in args {
        let candidate = arg.evaluate(batch, row)?;
        if candidate.is_null() { continue; }
        match &mut greatest {
            Some(value) => {
                let comparison = candidate.sql_compare(value)?
                    .ok_or_else(|| Error::Type("GREATEST cannot order NaN".into()))?;
                if comparison == std::cmp::Ordering::Greater {
                    *value = candidate;
                }
            }
            None => greatest = Some(candidate),
        }
    }
    let value = greatest.unwrap_or(Value::Null);
    Ok(match (target, value) {
        (DataType::Float, Value::Integer(n)) => Value::Float(n as f64),
        (_, value) => value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Field;

    #[test]
    fn greatest_supports_mixed_numeric_types_and_null_values() {
        let schema = Schema::new(vec![Field::new("x", DataType::Integer, true)]).unwrap();
        let batch = Batch::from_rows(schema, vec![
            vec![Value::Integer(2)], vec![Value::Integer(8)], vec![Value::Null],
        ]).unwrap();
        let args = vec![Expr::Column("x".into()), Expr::Literal(Value::Float(5.5))];
        assert_eq!(output_type(&args, batch.schema()).unwrap(), DataType::Float);
        assert_eq!(eval(&args, &batch, 0).unwrap(), Value::Float(5.5));
        assert_eq!(eval(&args, &batch, 1).unwrap(), Value::Float(8.0));
        assert_eq!(eval(&args, &batch, 2).unwrap(), Value::Float(5.5));
    }
}
