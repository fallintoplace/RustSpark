//! Scalar SQL functions implemented by the native expression evaluator.
use std::borrow::Cow;
use crate::{expr::Expr, scalar::Value, schema::{DataType, Schema}, Error, Result};

fn normalized_uppercase(name: &str) -> Cow<'_, str> {
    if name.bytes().any(|byte| byte.is_ascii_lowercase()) {
        Cow::Owned(name.to_ascii_uppercase())
    } else {
        Cow::Borrowed(name)
    }
}


pub fn return_type(name: &str, args: &[Expr], schema: &Schema) -> Result<DataType> {
    match normalized_uppercase(name).as_ref() {
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
        "REPLACE" => {
            if !(2..=3).contains(&args.len()) { return Err(Error::Parse("REPLACE requires two or three arguments".into())); }
            for arg in args {
                if arg.output_type(schema)? != DataType::Text { return Err(Error::Type("REPLACE requires text arguments".into())); }
            }
            Ok(DataType::Text)
        },
        "ROUND" => {
            if args.is_empty() || args.len() > 2 { return Err(Error::Parse("ROUND requires one or two arguments".into())); }
            if !matches!(args[0].output_type(schema)?, DataType::Integer | DataType::Float) { return Err(Error::Type("ROUND requires numeric input".into())); }
            if args.len() == 2 && args[1].output_type(schema)? != DataType::Integer { return Err(Error::Type("ROUND precision must be integer".into())); }
            Ok(DataType::Float)
        },
        "POWER" => {
            if args.len() != 2 { return Err(Error::Parse("POWER requires two arguments".into())); }
            for arg in args {
                if !matches!(arg.output_type(schema)?, DataType::Integer | DataType::Float) { return Err(Error::Type("POWER requires numeric inputs".into())); }
            }
            Ok(DataType::Float)
        },
        "SQRT" => {
            if args.len() != 1 { return Err(Error::Parse("SQRT requires one argument".into())); }
            if !matches!(args[0].output_type(schema)?, DataType::Integer | DataType::Float) { return Err(Error::Type("SQRT requires numeric input".into())); }
            Ok(DataType::Float)
        },
        "LN" => {
            if args.len() != 1 { return Err(Error::Parse("LN requires one argument".into())); }
            if !matches!(args[0].output_type(schema)?, DataType::Integer | DataType::Float) { return Err(Error::Type("LN requires numeric input".into())); }
            Ok(DataType::Float)
        },
        "STARTSWITH" => {
            if args.len() != 2 { return Err(Error::Parse("STARTSWITH requires two arguments".into())); }
            for arg in args {
                if arg.output_type(schema)? != DataType::Text { return Err(Error::Type("STARTSWITH requires text".into())); }
            }
            Ok(DataType::Boolean)
        },
        "ENDSWITH" => {
            if args.len() != 2 { return Err(Error::Parse("ENDSWITH requires two arguments".into())); }
            for arg in args {
                if arg.output_type(schema)? != DataType::Text { return Err(Error::Type("ENDSWITH requires text".into())); }
            }
            Ok(DataType::Boolean)
        },
        "CONTAINS" => {
            if args.len() != 2 { return Err(Error::Parse("CONTAINS requires two arguments".into())); }
            for arg in args {
                if arg.output_type(schema)? != DataType::Text { return Err(Error::Type("CONTAINS requires text".into())); }
            }
            Ok(DataType::Boolean)
        },
        "REVERSE" => {
            if args.len() != 1 { return Err(Error::Parse("REVERSE requires one argument".into())); }
            if args[0].output_type(schema)? != DataType::Text { return Err(Error::Type("REVERSE requires text".into())); }
            Ok(DataType::Text)
        },
        "GREATEST" => {
            if args.len() < 2 { return Err(Error::Parse("GREATEST requires at least two arguments".into())); }
            let mut kind = None;
            for arg in args {
                if matches!(arg, Expr::Literal(Value::Null)) { continue; }
                let next = arg.output_type(schema)?;
                if kind.is_some_and(|previous| previous != next) { return Err(Error::Type("GREATEST arguments must share a type".into())); }
                kind = Some(next);
            }
            Ok(kind.unwrap_or(DataType::Text))
        },
        "LEAST" => {
            if args.len() < 2 { return Err(Error::Parse("LEAST requires at least two arguments".into())); }
            let mut kind = None;
            for arg in args {
                if matches!(arg, Expr::Literal(Value::Null)) { continue; }
                let next = arg.output_type(schema)?;
                if kind.is_some_and(|previous| previous != next) { return Err(Error::Type("LEAST arguments must share a type".into())); }
                kind = Some(next);
            }
            Ok(kind.unwrap_or(DataType::Text))
        },
        _ => Err(Error::Unsupported(format!("unknown native SQL function: {name}"))),
    }
}

pub fn evaluate(name: &str, args: &[Value]) -> Result<Value> {
    match normalized_uppercase(name).as_ref() {
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
            [Value::Text(value)] if value.is_ascii() =>
                Ok(Value::Text(value.to_ascii_lowercase())),
            [Value::Text(value)] => Ok(Value::Text(value.to_lowercase())),
            [Value::Null] => Ok(Value::Null),
            _ => Err(Error::Type("LOWER requires text".into())),
        },
        "UPPER" => match args {
            [Value::Text(value)] if value.is_ascii() =>
                Ok(Value::Text(value.to_ascii_uppercase())),
            [Value::Text(value)] => Ok(Value::Text(value.to_uppercase())),
            [Value::Null] => Ok(Value::Null),
            _ => Err(Error::Type("UPPER requires text".into())),
        },
        "LENGTH" => match args {
            [Value::Text(value)] => {
                let count = if value.is_ascii() { value.len() } else { value.chars().count() };
                Ok(Value::Integer(i64::try_from(count)
                    .map_err(|_| Error::Execution("text length overflow".into()))?))
            },
            [Value::Null] => Ok(Value::Null),
            _ => Err(Error::Type("LENGTH requires text".into())),
        },
        "TRIM" => match args {
            [Value::Text(value)] => Ok(Value::Text(value.trim_matches(' ').to_string())),
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
                // Convert character offsets into UTF-8 slice boundaries without
                // allocating a Vec<char> for the entire input string.
                let begin = if *start > 0 {
                    usize::try_from(start - 1).unwrap_or(usize::MAX)
                } else if *start < 0 {
                    text.chars().count().saturating_sub(
                        usize::try_from(start.unsigned_abs()).unwrap_or(usize::MAX))
                } else { 0 };
                let count = usize::try_from(*length).unwrap_or(usize::MAX);
                let start_byte = text.char_indices().nth(begin)
                    .map_or(text.len(), |(offset, _)| offset);
                let end_byte = text[start_byte..].char_indices().nth(count)
                    .map_or(text.len(), |(offset, _)| start_byte + offset);
                Ok(Value::Text(text[start_byte..end_byte].into()))
            }
            [a, b, c] if a.is_null() || b.is_null() || c.is_null() => Ok(Value::Null),
            _ => Err(Error::Type("SUBSTRING requires text and integer positions".into())),
        },
        "REPLACE" => match args {
            [Value::Text(text), Value::Text(pattern)] =>
                Ok(Value::Text(text.replace(pattern.as_str(), ""))),
            [Value::Text(text), Value::Text(pattern), Value::Text(replacement)] =>
                Ok(Value::Text(text.replace(pattern.as_str(), replacement))),
            [a, b] if a.is_null() || b.is_null() => Ok(Value::Null),
            [a, b, c] if a.is_null() || b.is_null() || c.is_null() => Ok(Value::Null),
            _ => Err(Error::Type("REPLACE requires two or three text arguments".into())),
        },
        "ROUND" => {
            if args.is_empty() || args.len() > 2 { return Err(Error::Parse("ROUND requires one or two arguments".into())); }
            let digits = match args.get(1) {
                None => 0,
                Some(Value::Integer(n)) if (-15..=15).contains(n) => *n as i32,
                Some(Value::Null) => return Ok(Value::Null),
                _ => return Err(Error::Execution("ROUND precision must be between -15 and 15".into())),
            };
            let number = match &args[0] {
                Value::Integer(n) => *n as f64,
                Value::Float(n) => *n,
                Value::Null => return Ok(Value::Null),
                _ => return Err(Error::Type("ROUND requires numeric input".into())),
            };
            let scale = 10_f64.powi(digits);
            let value = (number * scale).round() / scale;
            if !value.is_finite() { return Err(Error::Execution("ROUND produced a non-finite value".into())); }
            Ok(Value::Float(value))
        },
        "POWER" => {
            if args.len() != 2 { return Err(Error::Parse("POWER requires two arguments".into())); }
            if args.iter().any(Value::is_null) { return Ok(Value::Null); }
            let numeric = |v: &Value| match v {
                Value::Integer(n) => Ok(*n as f64),
                Value::Float(n) => Ok(*n),
                _ => Err(Error::Type("POWER requires numeric inputs".into())),
            };
            let value = numeric(&args[0])?.powf(numeric(&args[1])?);
            if !value.is_finite() { return Err(Error::Execution("POWER produced a non-finite value".into())); }
            Ok(Value::Float(value))
        },
        "SQRT" => match args {
            [Value::Integer(n)] if *n >= 0 => Ok(Value::Float((*n as f64).sqrt())),
            [Value::Float(n)] if *n >= 0.0 => Ok(Value::Float(n.sqrt())),
            [Value::Null] => Ok(Value::Null),
            [Value::Integer(_) | Value::Float(_)] => Err(Error::Execution("SQRT requires nonnegative input".into())),
            _ => Err(Error::Type("SQRT requires numeric input".into())),
        },
        "LN" => match args {
            [Value::Integer(n)] if *n > 0 => Ok(Value::Float((*n as f64).ln())),
            [Value::Float(n)] if *n > 0.0 => Ok(Value::Float(n.ln())),
            [Value::Null] => Ok(Value::Null),
            [Value::Integer(_) | Value::Float(_)] => Err(Error::Execution("LN requires positive input".into())),
            _ => Err(Error::Type("LN requires numeric input".into())),
        },
        "STARTSWITH" => match args {
            [Value::Text(value), Value::Text(prefix)] => Ok(Value::Boolean(value.starts_with(prefix))),
            [a, b] if a.is_null() || b.is_null() => Ok(Value::Null),
            _ => Err(Error::Type("STARTSWITH requires text".into())),
        },
        "ENDSWITH" => match args {
            [Value::Text(value), Value::Text(suffix)] => Ok(Value::Boolean(value.ends_with(suffix))),
            [a, b] if a.is_null() || b.is_null() => Ok(Value::Null),
            _ => Err(Error::Type("ENDSWITH requires text".into())),
        },
        "CONTAINS" => match args {
            [Value::Text(value), Value::Text(needle)] => Ok(Value::Boolean(value.contains(needle))),
            [a, b] if a.is_null() || b.is_null() => Ok(Value::Null),
            _ => Err(Error::Type("CONTAINS requires text".into())),
        },
        "REVERSE" => match args {
            [Value::Text(value)] => Ok(Value::Text(value.chars().rev().collect())),
            [Value::Null] => Ok(Value::Null),
            _ => Err(Error::Type("REVERSE requires text".into())),
        },
        "GREATEST" => {
            if args.len() < 2 { return Err(Error::Parse("GREATEST requires at least two arguments".into())); }
            let mut best: Option<Value> = None;
            for value in args.iter().filter(|value| !value.is_null()) {
                if let Some(current) = &best {
                    if value.sql_compare(current)?.ok_or_else(|| Error::Type("GREATEST cannot compare NaN".into()))?.is_gt() {
                        best = Some(value.clone());
                    }
                } else { best = Some(value.clone()); }
            }
            Ok(best.unwrap_or(Value::Null))
        },
        "LEAST" => {
            if args.len() < 2 { return Err(Error::Parse("LEAST requires at least two arguments".into())); }
            let mut best: Option<Value> = None;
            for value in args.iter().filter(|value| !value.is_null()) {
                if let Some(current) = &best {
                    if value.sql_compare(current)?.ok_or_else(|| Error::Type("LEAST cannot compare NaN".into()))?.is_lt() {
                        best = Some(value.clone());
                    }
                } else { best = Some(value.clone()); }
            }
            Ok(best.unwrap_or(Value::Null))
        },
        _ => Err(Error::Unsupported(format!("unknown native SQL function: {name}"))),
    }
}

#[cfg(test)]
mod spark_trim_space_tests {
    use super::*;

    #[test]
    fn trims_only_ascii_spaces_by_default() {
        assert_eq!(evaluate("TRIM", &[Value::Text("  abc  ".into())]).unwrap(),
            Value::Text("abc".into()));
        assert_eq!(evaluate("TRIM", &[Value::Text(" \t abc \n ".into())]).unwrap(),
            Value::Text("\t abc \n".into()));
        assert_eq!(evaluate("TRIM", &[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod spark_replace_optional_tests {
    use super::*;

    #[test]
    fn two_arg_replace_removes_occurrences_and_retains_nulls() {
        assert_eq!(evaluate("REPLACE", &[Value::Text("banana".into()), Value::Text("na".into())]).unwrap(),
            Value::Text("ba".into()));
        assert_eq!(evaluate("REPLACE", &[Value::Text("banana".into()), Value::Text("z".into())]).unwrap(),
            Value::Text("banana".into()));
        assert_eq!(evaluate("REPLACE", &[
            Value::Text("banana".into()), Value::Text("na".into()), Value::Text("XYZ".into()),
        ]).unwrap(), Value::Text("baXYZXYZ".into()));
        assert_eq!(evaluate("REPLACE", &[Value::Null, Value::Text("x".into())]).unwrap(), Value::Null);
        assert!(evaluate("REPLACE", &[Value::Text("x".into())]).is_err());
    }
}

#[cfg(test)]
mod ascii_case_conversion_tests {
    use super::*;
    #[test]
    fn lower_upper_preserve_unicode_expansion() {
        assert_eq!(evaluate("LOWER", &[Value::Text("BeRLiN".into())]).unwrap(), Value::Text("berlin".into()));
        assert_eq!(evaluate("UPPER", &[Value::Text("BeRLiN".into())]).unwrap(), Value::Text("BERLIN".into()));
        assert_eq!(evaluate("UPPER", &[Value::Text("straße".into())]).unwrap(), Value::Text("STRASSE".into()));
        assert_eq!(evaluate("LOWER", &[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod ascii_length_tests {
    use super::*;
    #[test]
    fn length_counts_ascii_bytes_and_unicode_characters() {
        assert_eq!(evaluate("LENGTH", &[Value::Text("ABC".into())]).unwrap(), Value::Integer(3));
        assert_eq!(evaluate("LENGTH", &[Value::Text("é🙂".into())]).unwrap(), Value::Integer(2));
        assert_eq!(evaluate("LENGTH", &[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod uppercase_dispatch_tests {
    use super::*;
    #[test]
    fn uppercase_function_names_borrow_without_allocation() {
        assert!(matches!(normalized_uppercase("UPPER"), Cow::Borrowed(_)));
        assert_eq!(normalized_uppercase("upper").as_ref(), "UPPER");
        assert_eq!(normalized_uppercase("UpPeR").as_ref(), "UPPER");
        assert_eq!(evaluate("UPPER", &[Value::Text("abc".into())]).unwrap(), Value::Text("ABC".into()));
    }
}

#[cfg(test)]
mod substring_byte_slicing_tests {
    use super::*;

    #[test]
    fn substring_preserves_unicode_and_negative_offsets() {
        let arg = |start, length| evaluate("SUBSTRING", &[
            Value::Text("aé🙂z".into()), Value::Integer(start), Value::Integer(length)
        ]).unwrap();
        assert_eq!(arg(2, 2), Value::Text("é🙂".into()));
        assert_eq!(arg(-2, 2), Value::Text("🙂z".into()));
        assert_eq!(arg(0, 1), Value::Text("a".into()));
        assert_eq!(arg(9, 3), Value::Text("".into()));
        assert_eq!(arg(i64::MIN, 3), Value::Text("aé🙂".into()));
        assert_eq!(arg(2, 0), Value::Text("".into()));
    }
}
