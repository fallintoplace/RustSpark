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
    let count = usize::try_from(width)
        .map_err(|_| Error::Execution("LEFT/RIGHT length must be nonnegative".into()))?;
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
        assert!(left(&[input, Value::Integer(-1)]).is_err());
    }
}
