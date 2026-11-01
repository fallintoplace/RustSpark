use super::{Function, regex_support};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] != DataType::Text || types[1] != DataType::Text ||
        (types.len() == 3 && types[2] != DataType::Integer) {
        return Err(Error::Type("REGEXP_EXTRACT expects text, pattern, optional group index".into()));
    }
    Ok(DataType::Text)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(pattern) = super::string(&args[1])? else { return Ok(Value::Null); };
    let group = if args.len() == 3 {
        let Some(index) = super::integer(&args[2])? else { return Ok(Value::Null); };
        usize::try_from(index)
            .map_err(|_| Error::Execution("REGEXP_EXTRACT group must be nonnegative".into()))?
    } else { 1 };
    if group > 100 { return Err(Error::Execution("REGEXP_EXTRACT group budget exceeded".into())); }
    regex_support::check_input(text)?;
    if group == 0 && !pattern.is_empty() && pattern.len() <= 4096 &&
        pattern.chars().all(|ch| ch.is_alphanumeric() || ch == '_')
    {
        return Ok(Value::Text(text.find(pattern).map_or("", |_| pattern).to_owned()));
    }
    let re = regex_support::compile(pattern)?;
    if group >= re.captures_len() {
        return Err(Error::Execution("REGEXP_EXTRACT group index exceeds regex capture count".into()));
    }
    // Group zero can avoid a capture allocation.
    if group == 0 {
        return Ok(Value::Text(re.find(text).map_or("", |matched| matched.as_str()).to_owned()));
    }
    Ok(Value::Text(re.captures(text).and_then(|captures| captures.get(group))
        .map_or("", |value| value.as_str()).to_owned()))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "regexp_extract", min_args: 2, max_args: 3,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn extracts_regex_capture_groups_and_reports_missing_matches() {
        assert_eq!(evaluate(&[
            Value::Text("order-123".into()), Value::Text("order-([0-9]+)".into()),
            Value::Integer(1),
        ]).unwrap(), Value::Text("123".into()));
        assert_eq!(evaluate(&[
            Value::Text("order-123".into()), Value::Text("order-([0-9]+)".into()),
            Value::Integer(0),
        ]).unwrap(), Value::Text("order-123".into()));
        assert_eq!(evaluate(&[
            Value::Text("missing".into()), Value::Text("([0-9]+)".into()),
        ]).unwrap(), Value::Text(String::new()));
        assert!(evaluate(&[
            Value::Text("x".into()), Value::Text("(x)".into()), Value::Integer(-1),
        ]).is_err());
    }
}

#[cfg(test)]
mod spark_extract_regressions {
    use super::*;

    #[test]
    fn returns_empty_strings_for_absent_matches_and_optional_groups() {
        let value = |text: &str, pattern: &str, group: i64| {
            evaluate(&[
                Value::Text(text.into()), Value::Text(pattern.into()), Value::Integer(group),
            ]).unwrap()
        };
        assert_eq!(value("abc", "z+", 0), Value::Text(String::new()));
        assert_eq!(value("abc", "(z)?a", 1), Value::Text(String::new()));
        assert_eq!(value("abc", "a(b)", 1), Value::Text("b".into()));
        assert!(evaluate(&[
            Value::Text("abc".into()), Value::Text("(a)".into()), Value::Integer(2),
        ]).is_err());
        assert_eq!(evaluate(&[Value::Null, Value::Text("a".into())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod spark_default_regexp_extract_group_tests {
    use super::*;
    #[test]
    fn defaults_to_first_group_and_retains_explicit_whole_match() {
        let value = |input: &str, pattern: &str, group: Option<i64>| {
            let mut args = vec![Value::Text(input.into()), Value::Text(pattern.into())];
            if let Some(group) = group { args.push(Value::Integer(group)); }
            evaluate(&args)
        };
        assert_eq!(value("order-123", "order-([0-9]+)", None).unwrap(), Value::Text("123".into()));
        assert_eq!(value("order-123", "order-([0-9]+)", Some(0)).unwrap(), Value::Text("order-123".into()));
        assert_eq!(value("order-123", "order-([0-9]+)", Some(1)).unwrap(), Value::Text("123".into()));
        assert_eq!(value("missing", "([0-9]+)", None).unwrap(), Value::Text(String::new()));
        assert!(value("order-123", "[0-9]+", None).is_err());
    }
}

#[cfg(test)]
mod regexp_extract_group_zero_literal_tests {
    use super::*;
    #[test]
    fn extracts_whole_literal_match_without_changing_capture_semantics() {
        assert_eq!(evaluate(&[Value::Text("éuser_123🙂".into()),
            Value::Text("user_123".into()), Value::Integer(0)]).unwrap(),
            Value::Text("user_123".into()));
        assert_eq!(evaluate(&[Value::Text("hello".into()),
            Value::Text("absent".into()), Value::Integer(0)]).unwrap(),
            Value::Text("".into()));
        assert_eq!(evaluate(&[Value::Text("hello123".into()),
            Value::Text("[0-9]+".into()), Value::Integer(0)]).unwrap(),
            Value::Text("123".into()));
        assert!(evaluate(&[Value::Text("hello".into()),
            Value::Text("hello".into())]).is_err());
        assert_eq!(evaluate(&[Value::Null, Value::Text("hello".into()),
            Value::Integer(0)]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod unicode_literal_extract_regressions {
    use super::*;
    use regex::Regex;

    #[test]
    fn group_zero_literal_matches_unicode_regex_capture() {
        for input in ["", "abc東京def", "ééé", "αβγ αβγ", "🙂café"] {
            for pattern in ["é", "東京", "αβ", "café", "no", "éé"] {
                let expected = Regex::new(pattern).unwrap()
                    .find(input).map_or("", |found| found.as_str()).to_owned();
                let args = [Value::Text(input.into()), Value::Text(pattern.into()), Value::Integer(0)];
                assert_eq!(evaluate(&args).unwrap(), Value::Text(expected),
                    "input={input:?}, pattern={pattern:?}");
            }
        }
    }

    #[test]
    fn capture_groups_still_use_regex_and_retain_utf8() {
        assert_eq!(evaluate(&[
            Value::Text("xééy".into()), Value::Text("(é+)".into()), Value::Integer(1),
        ]).unwrap(), Value::Text("éé".into()));
        assert_eq!(evaluate(&[
            Value::Text("xééy".into()), Value::Text("é".into()), Value::Integer(0),
        ]).unwrap(), Value::Text("é".into()));
    }
}
