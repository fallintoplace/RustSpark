use super::Function;
use crate::{scalar::Value, schema::DataType, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }
fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    let mut result = String::with_capacity(text.len());
    let mut start_of_word = true;
    for ch in text.chars() {
        if ch.is_whitespace() {
            result.push(ch);
            start_of_word = true;
        } else {
            if start_of_word { result.extend(ch.to_uppercase()); }
            else { result.extend(ch.to_lowercase()); }
            start_of_word = false;
        }
    }
    Ok(Value::Text(result))
}
pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "initcap", min_args: 1, max_args: 1, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn title_cases_unicode_words_only_after_whitespace() {
        assert_eq!(evaluate(&[Value::Text("éCOLE   sQL-test".into())]).unwrap(),
            Value::Text("École   Sql-test".into()));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}
