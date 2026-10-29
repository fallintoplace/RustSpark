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
        _ => Err(Error::Unsupported(format!("unknown native SQL function: {name}"))),
    }
}

pub fn evaluate(name: &str, args: &[Value]) -> Result<Value> {
    match name.to_ascii_uppercase().as_str() {
        "COALESCE" => Ok(args.iter().find(|value| !value.is_null()).cloned().unwrap_or(Value::Null)),
        _ => Err(Error::Unsupported(format!("unknown native SQL function: {name}"))),
    }
}
