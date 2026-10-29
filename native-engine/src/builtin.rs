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
        "LOWER" => {
            if args.len() != 1 { return Err(Error::Parse("LOWER requires one argument".into())); }
            if args[0].output_type(schema)? != DataType::Text { return Err(Error::Type("LOWER requires text".into())); }
            Ok(DataType::Text)
        },
        "UPPER" => {
            if args.len() != 1 { return Err(Error::Parse("UPPER requires one argument".into())); }
            if args[0].output_type(schema)? != DataType::Text { return Err(Error::Type("UPPER requires text".into())); }
            Ok(DataType::Text)
        },
        "LENGTH" => {
            if args.len() != 1 { return Err(Error::Parse("LENGTH requires one argument".into())); }
            if args[0].output_type(schema)? != DataType::Text { return Err(Error::Type("LENGTH requires text".into())); }
            Ok(DataType::Integer)
        },
        "TRIM" => {
            if args.len() != 1 { return Err(Error::Parse("TRIM requires one argument".into())); }
            if args[0].output_type(schema)? != DataType::Text { return Err(Error::Type("TRIM requires text".into())); }
            Ok(DataType::Text)
        },
        "CONCAT" => {
            if args.len() < 2 { return Err(Error::Parse("CONCAT requires at least two arguments".into())); }
            for arg in args {
                if arg.output_type(schema)? != DataType::Text { return Err(Error::Type("CONCAT requires text".into())); }
            }
            Ok(DataType::Text)
        },
        "SUBSTRING" => {
            if args.len() != 3 { return Err(Error::Parse("SUBSTRING requires text, start, and length".into())); }
            if args[0].output_type(schema)? != DataType::Text || args[1].output_type(schema)? != DataType::Integer || args[2].output_type(schema)? != DataType::Integer {
                return Err(Error::Type("SUBSTRING requires text and integer positions".into()));
            }
            Ok(DataType::Text)
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
        "LOWER" => match args {
            [Value::Text(value)] => Ok(Value::Text(value.to_lowercase())),
            [Value::Null] => Ok(Value::Null),
            _ => Err(Error::Type("LOWER requires text".into())),
        },
        "UPPER" => match args {
            [Value::Text(value)] => Ok(Value::Text(value.to_uppercase())),
            [Value::Null] => Ok(Value::Null),
            _ => Err(Error::Type("UPPER requires text".into())),
        },
        "LENGTH" => match args {
            [Value::Text(value)] => Ok(Value::Integer(i64::try_from(value.chars().count()).map_err(|_| Error::Execution("text length overflow".into()))?)),
            [Value::Null] => Ok(Value::Null),
            _ => Err(Error::Type("LENGTH requires text".into())),
        },
        "TRIM" => match args {
            [Value::Text(value)] => Ok(Value::Text(value.trim().to_string())),
            [Value::Null] => Ok(Value::Null),
            _ => Err(Error::Type("TRIM requires text".into())),
        },
        "CONCAT" => {
            if args.len() < 2 { return Err(Error::Parse("CONCAT requires at least two arguments".into())); }
            let mut text = String::new();
            for value in args {
                match value {
                    Value::Text(part) => text.push_str(part),
                    Value::Null => return Ok(Value::Null),
                    _ => return Err(Error::Type("CONCAT requires text".into())),
                }
            }
            Ok(Value::Text(text))
        },
        "SUBSTRING" => match args {
            [Value::Text(text), Value::Integer(start), Value::Integer(length)] => {
                if *length < 0 { return Err(Error::Execution("SUBSTRING length must be nonnegative".into())); }
                let chars: Vec<char> = text.chars().collect();
                let begin = if *start > 0 {
                    usize::try_from(start - 1).unwrap_or(usize::MAX)
                } else if *start < 0 {
                    chars.len().saturating_sub(usize::try_from(start.unsigned_abs()).unwrap_or(usize::MAX))
                } else { 0 };
                let count = usize::try_from(*length).unwrap_or(usize::MAX);
                Ok(Value::Text(chars.into_iter().skip(begin).take(count).collect()))
            }
            [a, b, c] if a.is_null() || b.is_null() || c.is_null() => Ok(Value::Null),
            _ => Err(Error::Type("SUBSTRING requires text and integer positions".into())),
        },
        _ => Err(Error::Unsupported(format!("unknown native SQL function: {name}"))),
    }
}
