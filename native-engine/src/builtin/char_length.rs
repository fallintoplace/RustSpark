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
    // Each Unicode scalar has exactly one non-continuation UTF-8 byte.
    let count = text.bytes().filter(|byte| byte & 0b1100_0000 != 0b1000_0000).count();
    let length = i64::try_from(count)
        .map_err(|_| Error::Execution("character length overflow".into()))?;
    Ok(Value::Integer(length))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "char_length", min_args: 1, max_args: 1, result_type: output, call: evaluate },
    Function { name: "character_length", min_args: 1, max_args: 1, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_unicode_characters_including_trailing_spaces() {
        assert_eq!(evaluate(&[Value::Text("é🙂 ".into())]).unwrap(), Value::Integer(3));
        assert_eq!(evaluate(&[Value::Text(String::new())]).unwrap(), Value::Integer(0));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(output(&[DataType::Integer]).is_err());
    }
}

#[cfg(test)]
mod char_length_utf8_perf_tests {
    use super::*;
    #[test]
    fn counts_multibyte_and_combining_characters() {
        assert_eq!(evaluate(&[Value::Text("e\u{301}🙂中".into())]).unwrap(),
            Value::Integer(4));
        assert_eq!(evaluate(&[Value::Text("a".repeat(100_000))]).unwrap(),
            Value::Integer(100_000));
    }
}
