use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] != DataType::Text || types[1] != DataType::Integer
        || (types.len() == 3 && types[2] != DataType::Integer)
    {
        return Err(Error::Type("SUBSTR requires text and integer positions".into()));
    }
    Ok(DataType::Text)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(position) = super::integer(&args[1])? else { return Ok(Value::Null); };
    let length = if args.len() == 3 {
        let Some(length) = super::integer(&args[2])? else { return Ok(Value::Null); };
        if length < 0 {
            return Err(Error::Execution("SUBSTR length must be nonnegative".into()));
        }
        usize::try_from(length).unwrap_or(usize::MAX)
    } else {
        usize::MAX
    };
    let chars = text.chars().collect::<Vec<_>>();
    let begin = if position > 0 {
        usize::try_from(position - 1).unwrap_or(usize::MAX)
    } else if position < 0 {
        chars.len().saturating_sub(usize::try_from(position.unsigned_abs()).unwrap_or(usize::MAX))
    } else {
        0
    };
    Ok(Value::Text(chars.into_iter().skip(begin).take(length).collect()))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "substr", min_args: 2, max_args: 3, result_type: output, call: evaluate },
    Function { name: "substring", min_args: 2, max_args: 3, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slices_unicode_with_spark_positions_and_optional_length() {
        assert_eq!(evaluate(&[Value::Text("café".into()), Value::Integer(3)]).unwrap(),
            Value::Text("fé".into()));
        assert_eq!(evaluate(&[Value::Text("café".into()), Value::Integer(-2)]).unwrap(),
            Value::Text("fé".into()));
        assert_eq!(evaluate(&[Value::Text("café".into()), Value::Integer(0),
            Value::Integer(2)]).unwrap(), Value::Text("ca".into()));
        assert_eq!(evaluate(&[Value::Text("café".into()), Value::Integer(30)]).unwrap(),
            Value::Text(String::new()));
        assert_eq!(evaluate(&[Value::Text("café".into()), Value::Null]).unwrap(), Value::Null);
        assert!(evaluate(&[Value::Text("a".into()), Value::Integer(1),
            Value::Integer(-1)]).is_err());
    }
}
