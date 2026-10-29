use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Text && matches!(types[1], DataType::Integer | DataType::Text) {
        Ok(DataType::Text)
    } else {
        Err(Error::Type("LEFT/RIGHT require text and integer or text width".into()))
    }
}

fn slice(args: &[Value], from_right: bool) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let width = match &args[1] {
        Value::Integer(width) => *width,
        Value::Text(text) => match text.trim().parse::<i64>() {
            Ok(width) => width,
            Err(_) => return Ok(Value::Null),
        },
        Value::Null => return Ok(Value::Null),
        _ => return Err(Error::Type("LEFT/RIGHT width must be integer or numeric text".into())),
    };
    // Spark returns an empty string when length is zero or negative.
    let count = usize::try_from(width.max(0)).unwrap_or(usize::MAX);
    let result: String = if from_right {
        let character_count = text.chars().count();
        text.chars().skip(character_count.saturating_sub(count)).collect()
    } else {
        text.chars().take(count).collect()
    };
    Ok(Value::Text(result))
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
mod spark_string_width_tests {
    use super::*;

    #[test]
    fn numeric_string_widths_work_with_unicode_and_invalid_widths_propagate_null() {
        let input = Value::Text("café".into());
        assert_eq!(left(&[input.clone(), Value::Text(" 3 ".into())]).unwrap(),
            Value::Text("caf".into()));
        assert_eq!(right(&[input.clone(), Value::Text("2".into())]).unwrap(),
            Value::Text("fé".into()));
        assert_eq!(right(&[input.clone(), Value::Text("-2".into())]).unwrap(),
            Value::Text(String::new()));
        assert_eq!(left(&[input.clone(), Value::Text("n/a".into())]).unwrap(), Value::Null);
        assert_eq!(left(&[input, Value::Null]).unwrap(), Value::Null);
        assert_eq!(output(&[DataType::Text, DataType::Text]).unwrap(), DataType::Text);
    }
}
