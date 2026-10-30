use super::{Function, regex_support};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Integer)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(pattern) = super::string(&args[1])? else { return Ok(Value::Null); };
    regex_support::check_input(text)?;
    // An empty Rust regex matches at each UTF-8 character boundary.
    if pattern.is_empty() {
        let count = text.chars().count().checked_add(1)
            .and_then(|n| i64::try_from(n).ok())
            .ok_or_else(|| Error::Execution("REGEXP_COUNT overflow".into()))?;
        return Ok(Value::Integer(count));
    }
    if pattern.len() == 1 && pattern.as_bytes()[0].is_ascii_alphanumeric() {
        let needle = pattern.as_bytes()[0];
        let count = text.bytes().filter(|&byte| byte == needle).count();
        return Ok(Value::Integer(i64::try_from(count)
            .map_err(|_| Error::Execution("REGEXP_COUNT overflow".into()))?));
    }
    let re = regex_support::compile(pattern)?;
    let count = re.find_iter(text).count();
    Ok(Value::Integer(i64::try_from(count)
        .map_err(|_| Error::Execution("REGEXP_COUNT overflow".into()))?))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "regexp_count", min_args: 2, max_args: 2,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn counts_nonoverlapping_unicode_regex_matches() {
        assert_eq!(evaluate(&[Value::Text("a1 b2 c3".into()),
            Value::Text("[0-9]".into())]).unwrap(), Value::Integer(3));
        assert_eq!(evaluate(&[Value::Text("abc".into()),
            Value::Text("[0-9]".into())]).unwrap(), Value::Integer(0));
        assert_eq!(evaluate(&[Value::Text("é🙂".into()),
            Value::Text("".into())]).unwrap(), Value::Integer(3));
        assert_eq!(evaluate(&[Value::Text("".into()),
            Value::Text("".into())]).unwrap(), Value::Integer(1));
        assert_eq!(evaluate(&[Value::Null, Value::Text("a".into())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod literal_ascii_regexp_count_tests {
    use super::*;

    #[test]
    fn counts_literals_without_changing_regex_metacharacters() {
        let run = |text: &str, pattern: &str| evaluate(&[
            Value::Text(text.into()), Value::Text(pattern.into())
        ]).unwrap();
        assert_eq!(run("banana", "a"), Value::Integer(3));
        assert_eq!(run("é1🙂1", "1"), Value::Integer(2));
        assert_eq!(run("Aaa", "a"), Value::Integer(2));
        assert_eq!(run("a.a", "."), Value::Integer(3));
        assert_eq!(run("aaa", "a+"), Value::Integer(1));
        assert_eq!(run("é🙂", ""), Value::Integer(3));
        assert_eq!(evaluate(&[Value::Null, Value::Text("a".into())]).unwrap(),
            Value::Null);
    }
}
