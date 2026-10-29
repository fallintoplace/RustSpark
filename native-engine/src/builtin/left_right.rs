use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Text && types[1] == DataType::Integer {
        Ok(DataType::Text)
    } else {
        Err(Error::Type("LEFT/RIGHT require text and integer width".into()))
    }
}

fn slice(args: &[Value], from_right: bool) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(width) = super::integer(&args[1])? else { return Ok(Value::Null); };
    // Spark returns an empty string when length is zero or negative.
    let count = usize::try_from(width.max(0)).unwrap_or(usize::MAX);
    let result = if count == 0 {
        ""
    } else if from_right {
        let start = text.char_indices().rev().nth(count - 1)
            .map_or(0, |(byte, _)| byte);
        &text[start..]
    } else {
        let end = text.char_indices().nth(count)
            .map_or(text.len(), |(byte, _)| byte);
        &text[..end]
    };
    Ok(Value::Text(result.to_owned()))
}

fn left(args: &[Value]) -> Result<Value> { slice(args, false) }
fn right(args: &[Value]) -> Result<Value> { slice(args, true) }

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "left", min_args: 2, max_args: 2, result_type: output, call: left },
    Function { name: "right", min_args: 2, max_args: 2, result_type: output, call: right },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn slices_on_unicode_character_boundaries() {
        let input = Value::Text("café".into());
        assert_eq!(left(&[input.clone(), Value::Integer(3)]).unwrap(),
            Value::Text("caf".into()));
        assert_eq!(right(&[input.clone(), Value::Integer(2)]).unwrap(),
            Value::Text("fé".into()));
        assert_eq!(right(&[input.clone(), Value::Integer(0)]).unwrap(),
            Value::Text("".into()));
        assert_eq!(left(&[input.clone(), Value::Integer(-1)]).unwrap(), Value::Text("".into()));
        assert_eq!(right(&[input, Value::Integer(-3)]).unwrap(), Value::Text("".into()));
    }
}

#[cfg(test)]
mod utf8_left_right_perf_tests {
    use super::*;

    #[test]
    fn returns_exact_utf8_boundaries_for_short_and_large_widths() {
        let input = Value::Text("a🙂東京é".into());
        assert_eq!(left(&[input.clone(), Value::Integer(3)]).unwrap(),
            Value::Text("a🙂東".into()));
        assert_eq!(right(&[input.clone(), Value::Integer(2)]).unwrap(),
            Value::Text("京é".into()));
        assert_eq!(right(&[input.clone(), Value::Integer(9999)]).unwrap(), input);
        assert_eq!(left(&[Value::Text("abc".into()), Value::Integer(0)]).unwrap(),
            Value::Text("".into()));
    }
}
