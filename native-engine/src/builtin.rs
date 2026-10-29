//! Scalar SQL functions implemented by the native expression evaluator.
use crate::{expr::Expr, scalar::Value, schema::{DataType, Schema}, Error, Result};

pub fn return_type(name: &str, args: &[Expr], schema: &Schema) -> Result<DataType> {
    match name.to_ascii_uppercase().as_str() {
        "COALESCE" => {
            if args.is_empty() { return Err(Error::Parse("COALESCE needs at least one argument".into())); }
            let mut kind = None;
            for arg in args {
                if matches!(arg, Expr::Literal(Value::Null)) { continue; }
                let next = arg.output_type(schema)?;
                if kind.is_some_and(|previous| previous != next) {
                    return Err(Error::Type("COALESCE arguments must have the same type".into()));
                }
                kind = Some(next);
            }
            Ok(kind.unwrap_or(DataType::Text))
        }
        "NULLIF" => {
            if args.len() != 2 { return Err(Error::Parse("NULLIF requires two arguments".into())); }
            let first = args[0].output_type(schema)?;
            if first != args[1].output_type(schema)? && !matches!(&args[1], Expr::Literal(Value::Null)) {
                return Err(Error::Type("NULLIF arguments must have the same type".into()));
            }
            Ok(first)
        },
        "ABS" => {
            if args.len() != 1 { return Err(Error::Parse("ABS requires one argument".into())); }
            let kind = args[0].output_type(schema)?;
            if !matches!(kind, DataType::Integer | DataType::Float) { return Err(Error::Type("ABS requires numeric input".into())); }
            Ok(kind)
        },
        _ => Err(Error::Unsupported(format!("unknown native SQL function: {name}"))),
    }
}

pub fn evaluate(name: &str, args: &[Value]) -> Result<Value> {
    match name.to_ascii_uppercase().as_str() {
        "COALESCE" => Ok(args.iter().find(|value| !value.is_null()).cloned().unwrap_or(Value::Null)),
        "NULLIF" => match args {
            [first, second] => Ok(if first.sql_equal(second)? == Some(true) { Value::Null } else { first.clone() }),
            _ => Err(Error::Parse("NULLIF requires two arguments".into())),
        },
        "ABS" => match args {
            [Value::Integer(value)] => value.checked_abs().map(Value::Integer).ok_or_else(|| Error::Execution("ABS integer overflow".into())),
            [Value::Float(value)] => Ok(Value::Float(value.abs())),
            [Value::Null] => Ok(Value::Null),
            _ => Err(Error::Type("ABS requires numeric input".into())),
        },
        _ => Err(Error::Unsupported(format!("unknown native SQL function: {name}"))),
    }
}
