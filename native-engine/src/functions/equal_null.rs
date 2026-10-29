use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("EQUAL_NULL", args, 2, 2)?;
    let left = args[0].output_type(schema)?;
    let right = args[1].output_type(schema)?;
    let either_null = args.iter().any(|arg| matches!(arg, Expr::Literal(Value::Null)));
    let numeric = matches!(left, DataType::Integer | DataType::Float)
        && matches!(right, DataType::Integer | DataType::Float);
    if left != right && !either_null && !numeric {
        return Err(Error::Type("EQUAL_NULL arguments must have comparable SQL types".into()));
    }
    Ok(DataType::Boolean)
}

pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    let left = args[0].evaluate(batch, row)?;
    let right = args[1].evaluate(batch, row)?;
    let equal = match (&left, &right) {
        (Value::Float(a), Value::Float(b)) if a.is_nan() || b.is_nan() => a.is_nan() && b.is_nan(),
        _ => left.sql_equal(&right)?.unwrap_or(left.is_null() && right.is_null()),
    };
    Ok(Value::Boolean(equal))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{catalog::Catalog, executor, schema::Field, sql};
    #[test]
    fn null_safe_sql_equality_matches_spark() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::from_rows(schema, vec![vec![Value::Null], vec![Value::Integer(2)]]).unwrap()).unwrap();
        let query = sql::parse_sql("SELECT EQUAL_NULL(n, NULL) AS a, EQUAL_NULL(n, n) AS b FROM t").unwrap();
        assert_eq!(executor::execute(&query, &catalog).unwrap().rows().unwrap(),
            vec![vec![Value::Boolean(true), Value::Boolean(true)],
                 vec![Value::Boolean(false), Value::Boolean(true)]]);
        let left = Expr::Literal(Value::Float(f64::NAN));
        let right = Expr::Literal(Value::Float(f64::NAN));
        let data = Batch::from_rows(
            Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap(),
            vec![vec![Value::Null]],
        ).unwrap();
        assert_eq!(eval(&[left, right], &data, 0).unwrap(), Value::Boolean(true));
    }
}
