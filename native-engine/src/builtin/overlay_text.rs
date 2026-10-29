use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] != DataType::Text || types[1] != DataType::Text ||
        types[2] != DataType::Integer ||
        (types.len() == 4 && types[3] != DataType::Integer) {
        return Err(Error::Type("OVERLAY requires text, replacement, start, optional length".into()));
    }
    Ok(DataType::Text)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(replacement) = super::string(&args[1])? else { return Ok(Value::Null); };
    let Some(start) = super::integer(&args[2])? else { return Ok(Value::Null); };
    if start < 1 { return Err(Error::Execution("OVERLAY start must be positive".into())); }
    let count = if args.len() == 4 {
        let Some(count) = super::integer(&args[3])? else { return Ok(Value::Null); };
        usize::try_from(count)
            .map_err(|_| Error::Execution("OVERLAY length must be nonnegative".into()))?
    } else {
        replacement.chars().count()
    };
    if text.len().checked_add(replacement.len()).is_none_or(|n| n > 16 * 1024 * 1024) {
        return Err(Error::Execution("OVERLAY input exceeds native 16 MiB budget".into()));
    }
    let characters = text.chars().collect::<Vec<_>>();
    let index = usize::try_from(start - 1).unwrap_or(usize::MAX).min(characters.len());
    let end = index.saturating_add(count).min(characters.len());
    let mut result = String::new();
    result.extend(characters[..index].iter().copied());
    result.push_str(replacement);
    result.extend(characters[end..].iter().copied());
    Ok(Value::Text(result))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "overlay", min_args: 3, max_args: 4,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn replaces_unicode_segments_by_character_index() {
        assert_eq!(evaluate(&[
            Value::Text("café".into()), Value::Text("X".into()), Value::Integer(4),
        ]).unwrap(), Value::Text("cafX".into()));
        assert_eq!(evaluate(&[
            Value::Text("abcdef".into()), Value::Text("XY".into()),
            Value::Integer(3), Value::Integer(3),
        ]).unwrap(), Value::Text("abXYf".into()));
        assert!(evaluate(&[
            Value::Text("abc".into()), Value::Text("z".into()), Value::Integer(0),
        ]).is_err());
    }
}
