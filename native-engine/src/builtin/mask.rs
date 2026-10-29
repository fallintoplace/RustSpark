use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)
}

fn replacement(args: &[Value], index: usize, default: Option<char>) -> Result<Option<char>> {
    match args.get(index) {
        None => Ok(default),
        Some(Value::Null) => Ok(None),
        Some(Value::Text(text)) => {
            let mut chars = text.chars();
            let first = chars.next().ok_or_else(|| Error::Type("MASK replacement must be one character".into()))?;
            if chars.next().is_some() {
                return Err(Error::Type("MASK replacement must be one character".into()));
            }
            Ok(Some(first))
        }
        _ => Err(Error::Type("MASK replacement must be text or NULL".into())),
    }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let upper = replacement(args, 1, Some('X'))?;
    let lower = replacement(args, 2, Some('x'))?;
    let digit = replacement(args, 3, Some('n'))?;
    let other = replacement(args, 4, None)?;
    let mut output = String::new();
    for character in text.chars() {
        let replacement = if character.is_uppercase() {
            upper
        } else if character.is_lowercase() {
            lower
        } else if character.is_numeric() {
            digit
        } else {
            other
        };
        output.push(replacement.unwrap_or(character));
        if output.len() > 16 * 1024 * 1024 {
            return Err(Error::Execution("MASK output exceeds native 16 MiB budget".into()));
        }
    }
    Ok(Value::Text(output))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "mask", min_args: 1, max_args: 5, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks_sensitive_strings_using_spark_defaults() {
        assert_eq!(evaluate(&[Value::Text("AbCD123-@$#".into())]).unwrap(),
            Value::Text("XxXXnnn-@$#".into()));
        assert_eq!(evaluate(&[Value::Text("AbCD123-@$#".into()),
            Value::Text("Q".into()), Value::Text("q".into()),
            Value::Text("d".into()), Value::Text("o".into())]).unwrap(),
            Value::Text("QqQQdddoooo".into()));
        assert_eq!(evaluate(&[Value::Text("AbCD123".into()), Value::Null]).unwrap(),
            Value::Text("AbCDnnn".into()));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(evaluate(&[Value::Text("a".into()), Value::Text("XY".into())]).is_err());
    }
}
