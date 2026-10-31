use std::cmp::Ordering;
use super::*;

pub(super) fn output_type(name: &str, args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity(name, args, 2, 64)?;
    let mut output = None;
    for expression in args {
        if matches!(expression, Expr::Literal(Value::Null)) { continue; }
        let next = expression.output_type(schema)?;
        output = Some(match (output, next) {
            (None, kind) => kind,
            (Some(kind), next) if kind == next => kind,
            (Some(DataType::Integer), DataType::Float)
            | (Some(DataType::Float), DataType::Integer) => DataType::Float,
            _ => return Err(Error::Type(format!("{name} values require compatible types"))),
        });
    }
    Ok(output.unwrap_or(DataType::Text))
}

pub(super) fn eval(
    name: &str, preferred: Ordering, args: &[Expr], batch: &Batch, row: usize,
) -> Result<Value> {
    let mut best: Option<Value> = None;
    for arg in args {
        let candidate = arg.evaluate(batch, row)?;
        if candidate.is_null() { continue; }
        match &mut best {
            Some(value) => {
                let comparison = candidate.sql_compare(value)?
                    .ok_or_else(|| Error::Type(format!("{name} cannot order NaN")))?;
                if comparison == preferred {
                    *value = candidate;
                }
            }
            None => best = Some(candidate),
        }
    }
    let value = best.unwrap_or(Value::Null);
    // LEAST/GREATEST argument types are validated by the SQL planner.
    // Only an INTEGER winner needs result-type lookup for DOUBLE promotion.
    // Avoid repeated schema scans for text, float and all-NULL rows.
    Ok(match value {
        Value::Integer(n) if output_type(name, args, batch.schema())? == DataType::Float =>
            Value::Float(n as f64),
        value => value,
    })
}

#[cfg(test)]
mod extrema_lazy_numeric_promotion_tests {
    use super::*;

    fn run(name: &str, preferred: Ordering, values: Vec<Value>) -> Value {
        let batch = Batch::new_with_row_count(Schema::new(vec![]).unwrap(), vec![], 1).unwrap();
        let args = values.into_iter().map(Expr::Literal).collect::<Vec<_>>();
        eval(name, preferred, &args, &batch, 0).unwrap()
    }

    #[test]
    fn noninteger_results_skip_promotion_without_losing_sql_ordering() {
        assert_eq!(run("LEAST", Ordering::Less,
            vec![Value::Text("東京".into()), Value::Text("é".into())]),
            Value::Text("é".into()));
        assert_eq!(run("GREATEST", Ordering::Greater,
            vec![Value::Null, Value::Float(0.5), Value::Integer(0)]),
            Value::Float(0.5));
        assert_eq!(run("LEAST", Ordering::Less,
            vec![Value::Integer(1), Value::Float(2.0)]),
            Value::Float(1.0));
        assert_eq!(run("GREATEST", Ordering::Greater,
            vec![Value::Integer(3), Value::Float(2.5)]),
            Value::Float(3.0));
        assert_eq!(run("LEAST", Ordering::Less,
            vec![Value::Null, Value::Null]), Value::Null);
    }
}
