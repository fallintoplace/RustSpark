use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn soundex_digit(ch: u8) -> u8 {
    match ch.to_ascii_uppercase() {
        b'B' | b'F' | b'P' | b'V' => 1,
        b'C' | b'G' | b'J' | b'K' | b'Q' | b'S' | b'X' | b'Z' => 2,
        b'D' | b'T' => 3, b'L' => 4, b'M' | b'N' => 5, b'R' => 6, _ => 0,
    }
}
fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() == 1 && types[0] == DataType::Text { Ok(DataType::Text) }
    else { Err(Error::Type("SOUNDEX has incompatible SQL argument types".into())) }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some((&first, characters)) = text.as_bytes().split_first() else {
        return Ok(Value::Text(String::new()));
    };
    if !first.is_ascii_alphabetic() { return Ok(Value::Text(text.to_string())); }
    let mut result = String::with_capacity(4);
    result.push(char::from(first.to_ascii_uppercase()));
    let mut last = soundex_digit(first);
    for &ch in characters {
        let code = soundex_digit(ch);
        if code == 0 {
            if !matches!(ch.to_ascii_uppercase(), b'H' | b'W') { last = 0; }
        } else {
            if code != last { result.push(char::from(b'0' + code)); }
            last = code;
        }
        if result.len() >= 4 { break; }
    }
    while result.len() < 4 { result.push('0'); }
    Ok(Value::Text(result))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "soundex", min_args: 1, max_args: 1, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn spark_sql_semantics() {
        assert_eq!(evaluate(&[Value::Text("Robert".into())]).unwrap(), Value::Text("R163".into()));
        assert_eq!(evaluate(&[Value::Text("Rupert".into())]).unwrap(), Value::Text("R163".into()));
        assert_eq!(evaluate(&[Value::Text("Ashcraft".into())]).unwrap(), Value::Text("A261".into()));
        assert_eq!(evaluate(&[Value::Text("Bbbbb".into())]).unwrap(), Value::Text("B000".into()));
        assert_eq!(evaluate(&[Value::Text("AéB".into())]).unwrap(), Value::Text("A100".into()));
        assert_eq!(evaluate(&[Value::Text("🙂X".into())]).unwrap(), Value::Text("🙂X".into()));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}
