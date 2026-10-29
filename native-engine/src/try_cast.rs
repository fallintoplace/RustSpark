//! Non-throwing conversions for SQL TRY_CAST. Invalid values produce NULL.
use crate::{scalar::Value, schema::DataType};

pub fn convert(value: Value, target: DataType) -> Value {
    if value.is_null() { return Value::Null; }
    match target {
        DataType::Text => Value::Text(match value {
            Value::Boolean(n) => n.to_string(),
            Value::Integer(n) => n.to_string(),
            Value::Float(n) => n.to_string(),
            Value::Text(n) => n,
            Value::Null => unreachable!(),
        }),
        DataType::Integer => match value {
            Value::Integer(n) => Value::Integer(n),
            Value::Boolean(n) => Value::Integer(i64::from(n)),
            Value::Float(n) if n.is_finite()
                && n >= i64::MIN as f64 && n < i64::MAX as f64 =>
                Value::Integer(n as i64),
            Value::Text(n) => n.trim().parse::<i64>()
                .map(Value::Integer).unwrap_or(Value::Null),
            _ => Value::Null,
        },
        DataType::Float => match value {
            Value::Float(n) if n.is_finite() => Value::Float(n),
            Value::Integer(n) => Value::Float(n as f64),
            Value::Boolean(n) => Value::Float(if n { 1.0 } else { 0.0 }),
            Value::Text(n) => n.trim().parse::<f64>().ok().filter(|n| n.is_finite())
                .map(Value::Float).unwrap_or(Value::Null),
            _ => Value::Null,
        },
        DataType::Boolean => match value {
            Value::Boolean(n) => Value::Boolean(n),
            Value::Integer(n) => Value::Boolean(n != 0),
            Value::Float(n) if n.is_finite() => Value::Boolean(n != 0.0),
            Value::Text(n) => match n.trim().to_ascii_lowercase().as_str() {
                "true" | "1" => Value::Boolean(true),
                "false" | "0" => Value::Boolean(false),
                _ => Value::Null,
            },
            _ => Value::Null,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn invalid_types_and_out_of_range_values_return_null() {
        assert_eq!(convert(Value::Text("abc".into()), DataType::Integer), Value::Null);
        assert_eq!(convert(Value::Float(1e100), DataType::Integer), Value::Null);
        assert_eq!(convert(Value::Text("42".into()), DataType::Integer), Value::Integer(42));
        assert_eq!(convert(Value::Text("FALSE".into()), DataType::Boolean), Value::Boolean(false));
    }
}
