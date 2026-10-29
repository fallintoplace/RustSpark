use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Text {
        Ok(DataType::Integer)
    } else {
        Err(Error::Type("CHAR_LENGTH requires text".into()))
    }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    let characters = if text.is_ascii() { text.len() } else { text.chars().count() };
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
    let words = if input.is_ascii() {
        let mut count = 0_usize;
        let mut in_word = false;
        for &byte in input.as_bytes() {
            if byte.is_ascii_whitespace() { in_word = false; }
            else if !in_word { count += 1; in_word = true; }
        }
        count
    } else {
        input.split_whitespace().count()
    };
    let count = i64::try_from(words)
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
mod word_count_ascii_fast_path_tests {
    use super::*;
    #[test]
    fn ascii_word_boundaries_and_unicode_fallback() {
        let input = Value::Text(format!("  {}  end ", "word\t".repeat(1000)));
        assert_eq!(word_count(&[input]).unwrap(), Value::Integer(1001));
        assert_eq!(word_count(&[Value::Text("a\nb\r\tc\u{000c}d".into())]).unwrap(), Value::Integer(4));
        assert_eq!(word_count(&[Value::Text("東京\u{2003}é".into())]).unwrap(), Value::Integer(2));
        assert_eq!(word_count(&[Value::Null]).unwrap(), Value::Null);
    }
}
