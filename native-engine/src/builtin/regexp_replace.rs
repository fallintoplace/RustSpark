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
    let start = if args.len() == 4 {
        let Some(position) = super::integer(&args[3])? else { return Ok(Value::Null); };
        if position < 1 {
            return Err(Error::Execution("REGEXP_REPLACE position must be positive".into()));
        }
        let offset = usize::try_from(position - 1).unwrap_or(usize::MAX);
        text.char_indices().nth(offset).map_or(text.len(), |(index, _)| index)
    } else { 0 };
    let compiled = regex_support::compile(pattern)?;
    const MAX_OUTPUT: usize = 16 * 1024 * 1024;
    let mut result = String::with_capacity(text.len());
    result.push_str(&text[..start]);
    let mut last = start;
    for captures in compiled.captures_iter(&text[start..]) {
        let matched = captures.get(0).expect("regex captures always include group zero");
        let prefix_end = start + matched.start();
        if result.len().checked_add(prefix_end - last).is_none_or(|len| len > MAX_OUTPUT) {
            return Err(Error::Execution("regex output exceeds native 16 MiB budget".into()));
        }
        result.push_str(&text[last..prefix_end]);
        captures.expand(replacement, &mut result);
        if result.len() > MAX_OUTPUT {
            return Err(Error::Execution("regex output exceeds native 16 MiB budget".into()));
        }
        last = start + matched.end();
    }
    if result.len().checked_add(text.len() - last).is_none_or(|len| len > MAX_OUTPUT) {
        return Err(Error::Execution("regex output exceeds native 16 MiB budget".into()));
    }
    result.push_str(&text[last..]);
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
    }
}

#[cfg(test)]
mod spark_position_regressions {
    use super::*;

    #[test]
    fn position_preserves_prefix_and_replaces_only_suffix() {
        let args = |position| [
            Value::Text("ab12cd34".into()), Value::Text("[0-9]+".into()),
            Value::Text("x".into()), Value::Integer(position),
        ];
        assert_eq!(evaluate(&args(5)).unwrap(), Value::Text("ab12cdx".into()));
        assert_eq!(evaluate(&args(1)).unwrap(), Value::Text("abxcdx".into()));
        assert_eq!(evaluate(&args(99)).unwrap(), Value::Text("ab12cd34".into()));
        assert!(evaluate(&args(0)).is_err());
        assert!(output(&[DataType::Text, DataType::Text, DataType::Text, DataType::Text]).is_err());
    }

    #[test]
    fn position_counts_unicode_characters() {
        assert_eq!(evaluate(&[
            Value::Text("éé11".into()), Value::Text("1".into()),
            Value::Text("x".into()), Value::Integer(3),
        ]).unwrap(), Value::Text("ééxx".into()));
    }
}

#[cfg(test)]
mod bounded_capture_expansion_tests {
    use super::*;
    #[test]
    fn retains_capture_and_position_semantics_with_bounded_output() {
        assert_eq!(evaluate(&[
            Value::Text("a1,b2".into()), Value::Text("([ab])([0-9])".into()),
            Value::Text("$2:$1".into()),
        ]).unwrap(), Value::Text("1:a,2:b".into()));
        assert_eq!(evaluate(&[
            Value::Text("ab12".into()), Value::Text("[0-9]".into()),
            Value::Text("x".into()), Value::Integer(3),
        ]).unwrap(), Value::Text("abxx".into()));
        assert!(evaluate(&[
            Value::Text("a".repeat(40)), Value::Text("a".into()),
            Value::Text("x".repeat(1024 * 1024)),
        ]).is_err());
    }
}
