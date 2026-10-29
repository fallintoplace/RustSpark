use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types == [DataType::Text, DataType::Text, DataType::Integer] { Ok(DataType::Text) }
    else { Err(Error::Type("SUBSTRING_INDEX requires text, delimiter, integer count".into())) }
}
fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    let Some(delimiter) = super::string(&args[1])? else { return Ok(Value::Null) };
    let Some(count) = super::integer(&args[2])? else { return Ok(Value::Null) };
    if count == 0 || delimiter.is_empty() { return Ok(Value::Text(String::new())); }
    let index = usize::try_from(count.unsigned_abs() - 1).ok();
    let result = match index {
        None => text,
        Some(index) if count > 0 =>
            text.match_indices(delimiter).nth(index).map_or(text, |(position, _)| &text[..position]),
        Some(index) =>
            text.rmatch_indices(delimiter).nth(index)
                .map_or(text, |(position, _)| &text[position + delimiter.len()..]),
    };
    Ok(Value::Text(result.to_string()))
}
pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "substring_index", min_args: 3, max_args: 3, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn counts_delimiters_from_both_ends() {
        let text = Value::Text("a.é.c".into());
        let delim = Value::Text(".".into());
        assert_eq!(evaluate(&[text.clone(), delim.clone(), Value::Integer(2)]).unwrap(), Value::Text("a.é".into()));
        assert_eq!(evaluate(&[text.clone(), delim.clone(), Value::Integer(-2)]).unwrap(), Value::Text("é.c".into()));
        assert_eq!(evaluate(&[text.clone(), delim.clone(), Value::Integer(0)]).unwrap(), Value::Text("".into()));
        assert_eq!(evaluate(&[text, delim, Value::Integer(i64::MIN)]).unwrap(), Value::Text("a.é.c".into()));
        assert_eq!(evaluate(&[Value::Null, Value::Text(".".into()), Value::Integer(1)]).unwrap(), Value::Null);
    }
}
