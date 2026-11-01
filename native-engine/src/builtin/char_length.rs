use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

pub(super) fn character_count(text: &str) -> usize {
    if text.is_ascii() { text.len() } else { text.chars().count() }
}

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Text {
        Ok(DataType::Integer)
    } else {
        Err(Error::Type("CHAR_LENGTH requires text".into()))
    }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    let characters = character_count(text);
    let length = i64::try_from(characters)
        .map_err(|_| Error::Execution("character length overflow".into()))?;
    Ok(Value::Integer(length))
}


fn word_count_type(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Integer)
}
fn word_count(args: &[Value]) -> Result<Value> {
    let Some(input) = super::string(&args[0])? else { return Ok(Value::Null); };
    if input.is_ascii() {
        let mut count = 0_i64;
        let mut in_word = false;
        for byte in input.bytes() {
            // Unlike Unicode whitespace, Rust's ASCII predicate excludes vertical tab.
            if byte.is_ascii_whitespace() || byte == b'\x0b' { in_word = false; }
            else if !in_word { count += 1; in_word = true; }
        }
        return Ok(Value::Integer(count));
    }
    let count = i64::try_from(input.split_whitespace().count())
        .map_err(|_| Error::Execution("WORD_COUNT overflow".into()))?;
    Ok(Value::Integer(count))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "word_count", min_args: 1, max_args: 1, result_type: word_count_type, call: word_count },
    Function { name: "char_length", min_args: 1, max_args: 1, result_type: output, call: evaluate },
    Function { name: "character_length", min_args: 1, max_args: 1, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_unicode_characters_including_trailing_spaces() {
        assert_eq!(evaluate(&[Value::Text("é🙂 ".into())]).unwrap(), Value::Integer(3));
        assert_eq!(evaluate(&[Value::Text("ASCII strings 123".into())]).unwrap(), Value::Integer(17));
        assert_eq!(evaluate(&[Value::Text("hello é🙂".into())]).unwrap(), Value::Integer(8));
        assert_eq!(evaluate(&[Value::Text(String::new())]).unwrap(), Value::Integer(0));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(output(&[DataType::Integer]).is_err());
    }
}

#[cfg(test)]
mod word_count_extension_tests {
    use super::*;
    #[test]
    fn covers_null_and_boundary_values() {
        assert!(crate::builtin::supports("word_count"));
        assert_eq!(word_count(&[Value::Text(" one\t two \n three ".into())]).unwrap(), Value::Integer(3));
        assert_eq!(word_count(&[Value::Text("".into())]).unwrap(), Value::Integer(0));
        assert_eq!(word_count(&[Value::Text("東京\u{00A0}é".into())]).unwrap(), Value::Integer(2));
        assert_eq!(word_count(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod ascii_word_count_scan_tests {
    use super::*;
    #[test]
    fn whitespace_and_utf8_word_counts_stay_equivalent() {
        let repeated = "one\t two\r\nthree ".repeat(20_000);
        assert_eq!(word_count(&[Value::Text(repeated)]).unwrap(), Value::Integer(60_000));
        assert_eq!(word_count(&[Value::Text(" a\x0bb\x0cc ".into())]).unwrap(), Value::Integer(3));
        assert_eq!(word_count(&[Value::Text("東京\u{00a0}é".into())]).unwrap(), Value::Integer(2));
        assert_eq!(word_count(&[Value::Null]).unwrap(), Value::Null);
    }
}
