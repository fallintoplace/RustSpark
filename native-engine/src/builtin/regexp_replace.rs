use super::{Function, regex_support};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(&types[..3])?;
    if types.len() == 4 && types[3] != DataType::Integer {
        return Err(Error::Type("REGEXP_REPLACE position must be an integer".into()));
    }
    Ok(DataType::Text)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(pattern) = super::string(&args[1])? else { return Ok(Value::Null); };
    let Some(replacement) = super::string(&args[2])? else { return Ok(Value::Null); };
    regex_support::check_input(text)?;
    if replacement.len() > 1024 * 1024 {
        return Err(Error::Execution("regex replacement exceeds 1 MiB".into()));
    }
    let position = if args.len() == 4 {
        let Some(position) = super::integer(&args[3])? else { return Ok(Value::Null) };
        if position < 1 { return Err(Error::Execution("REGEXP_REPLACE position must be positive".into())); }
        usize::try_from(position - 1).unwrap_or(usize::MAX)
    } else { 0 };
    let start = text.char_indices().nth(position).map_or(text.len(), |(index, _)| index);
    if position >= text.chars().count() { return Ok(Value::Text(text.to_owned())); }
    let compiled = regex_support::compile(pattern)?;
    let result = format!("{}{}", &text[..start], compiled.replace_all(&text[start..], replacement));
    if result.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("regex output exceeds native 16 MiB budget".into()));
    }
    Ok(Value::Text(result))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "regexp_replace", min_args: 3, max_args: 4,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn replaces_multiple_matches_and_capture_groups() {
        assert_eq!(evaluate(&[
            Value::Text("id:12 id:34".into()),
            Value::Text("id:([0-9]+)".into()),
            Value::Text("ref-$1".into()),
        ]).unwrap(), Value::Text("ref-12 ref-34".into()));
        assert!(evaluate(&[
            Value::Text("x".into()), Value::Text("(".into()), Value::Text("y".into())
        ]).is_err());
        assert_eq!(evaluate(&[
            Value::Text("é1a2".into()), Value::Text("[0-9]".into()),
            Value::Text("x".into()), Value::Integer(3),
        ]).unwrap(), Value::Text("é1ax".into()));
        assert_eq!(evaluate(&[
            Value::Text("abc123".into()), Value::Text("[0-9]".into()),
            Value::Text("x".into()), Value::Integer(100),
        ]).unwrap(), Value::Text("abc123".into()));
        assert_eq!(evaluate(&[
            Value::Text("abc".into()), Value::Text("a".into()),
            Value::Text("x".into()), Value::Null,
        ]).unwrap(), Value::Null);
        assert!(evaluate(&[
            Value::Text("abc".into()), Value::Text("a".into()),
            Value::Text("x".into()), Value::Integer(0),
        ]).is_err());
        assert!(output(&[DataType::Text, DataType::Text, DataType::Text, DataType::Text]).is_err());
    }
}
