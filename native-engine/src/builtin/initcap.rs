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
            if ch.is_ascii() {
                result.push(if start_of_word { ch.to_ascii_uppercase() }
                    else { ch.to_ascii_lowercase() });
            } else if start_of_word {
                result.extend(ch.to_uppercase());
            } else {
                result.extend(ch.to_lowercase());
            }
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

#[cfg(test)]
mod ascii_initcap_perf_tests {
    use super::*;
    #[test]
    fn ascii_fast_path_and_unicode_expansion_are_unchanged() {
        assert_eq!(evaluate(&[Value::Text("hello WORLD 2026\tRUSTSPARK".into())]).unwrap(),
            Value::Text("Hello World 2026\tRustspark".into()));
        assert_eq!(evaluate(&[Value::Text("straße ßET".into())]).unwrap(),
            Value::Text("Straße SSet".into()));
    }
}
