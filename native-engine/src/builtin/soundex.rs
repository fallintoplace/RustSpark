use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn soundex_digit(ch: char) -> u8 {
    match ch.to_ascii_uppercase() {
        'B' | 'F' | 'P' | 'V' => 1,
        'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => 2,
        'D' | 'T' => 3, 'L' => 4, 'M' | 'N' => 5, 'R' => 6, _ => 0,
    }
}
fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() == 1 && types[0] == DataType::Text { Ok(DataType::Text) }
    else { Err(Error::Type("SOUNDEX has incompatible SQL argument types".into())) }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let mut characters = text.chars();
    let Some(first) = characters.next() else { return Ok(Value::Text(String::new())); };
    if !first.is_ascii_alphabetic() { return Ok(Value::Text(text.to_string())); }
    let mut result = String::with_capacity(4);
    result.push(first.to_ascii_uppercase());
    let mut last = soundex_digit(first);
    for ch in characters {
        let code = soundex_digit(ch);
        if code == 0 {
            if !matches!(ch.to_ascii_uppercase(), 'H' | 'W') { last = 0; }
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
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}
