use super::{Function};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() == 1 && types[0] == DataType::Text { Ok(DataType::Boolean) }
    else { Err(Error::Type("LUHN_CHECK has incompatible SQL argument types".into())) }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return Ok(Value::Boolean(false));
    }
    let sum = text.bytes().rev().enumerate().fold(0_u8, |sum, (index, digit)| {
        let mut n = digit - b'0';
        if index % 2 == 1 { n *= 2; if n > 9 { n -= 9; } }
        (sum + n) % 10
    });
    Ok(Value::Boolean(sum % 10 == 0))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "luhn_check", min_args: 1, max_args: 1, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn spark_sql_semantics() {
        assert_eq!(evaluate(&[Value::Text("79927398713".into())]).unwrap(), Value::Boolean(true));
        assert_eq!(evaluate(&[Value::Text("79927398714".into())]).unwrap(), Value::Boolean(false));
        assert_eq!(evaluate(&[Value::Text("7x".into())]).unwrap(), Value::Boolean(false));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Text("0".repeat(1_000_000))]).unwrap(), Value::Boolean(true));
        assert_eq!(evaluate(&[Value::Text(format!("{}1", "0".repeat(1_000_000)))]).unwrap(), Value::Boolean(false));
    }
}
