use super::Function;
use crate::{scalar::Value, schema::DataType, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn trim(args: &[Value], left: bool, right: bool) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let characters = if args.len() == 2 {
        let Some(characters) = super::string(&args[1])? else { return Ok(Value::Null); };
        Some(characters)
    } else { None };
    let value = match characters {
        Some(chars) if left && right =>
            text.trim_matches(|ch| chars.contains(ch)),
        Some(chars) if left =>
            text.trim_start_matches(|ch| chars.contains(ch)),
        Some(chars) if right =>
            text.trim_end_matches(|ch| chars.contains(ch)),
        None if left && right => text.trim(),
        None if left => text.trim_start(),
        None if right => text.trim_end(),
        _ => text,
    };
    Ok(Value::Text(value.into()))
}

fn btrim(args: &[Value]) -> Result<Value> { trim(args, true, true) }
fn ltrim(args: &[Value]) -> Result<Value> { trim(args, true, false) }
fn rtrim(args: &[Value]) -> Result<Value> { trim(args, false, true) }

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "btrim", min_args: 1, max_args: 2, result_type: output, call: btrim },
    Function { name: "ltrim", min_args: 1, max_args: 2, result_type: output, call: ltrim },
    Function { name: "rtrim", min_args: 1, max_args: 2, result_type: output, call: rtrim },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn supports_unicode_character_sets_and_whitespace() {
        assert_eq!(btrim(&[Value::Text("ééhelloé".into()),
            Value::Text("é".into())]).unwrap(), Value::Text("hello".into()));
        assert_eq!(ltrim(&[Value::Text("  x ".into())]).unwrap(), Value::Text("x ".into()));
        assert_eq!(rtrim(&[Value::Text("  x ".into())]).unwrap(), Value::Text("  x".into()));
    }
}
