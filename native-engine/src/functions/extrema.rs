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
    let target = output_type(name, args, batch.schema())?;
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
    Ok(match (target, value) {
        (DataType::Float, Value::Integer(n)) => Value::Float(n as f64),
        (_, value) => value,
    })
}
