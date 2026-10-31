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
        if offset >= text.len() { text.len() } else {
            text.char_indices().nth(offset).map_or(text.len(), |(index, _)| index)
        }
    } else { 0 };
    let compiled = regex_support::compile(pattern)?;
    let suffix = &text[start..];
    let mut result = String::with_capacity(text.len());
    result.push_str(&text[..start]);
    let mut consumed = 0;
    for captures in compiled.captures_iter(suffix) {
        let matched = captures.get(0).expect("regex captures always contain the whole match");
        let unchanged = &suffix[consumed..matched.start()];
        let mut expanded = String::new();
        captures.expand(replacement, &mut expanded);
        if result.len().checked_add(unchanged.len())
            .and_then(|len| len.checked_add(expanded.len()))
            .is_none_or(|len| len > 16 * 1024 * 1024) {
            return Err(Error::Execution("regex output exceeds native 16 MiB budget".into()));
        }
        result.push_str(unchanged);
        result.push_str(&expanded);
        consumed = matched.end();
    }
    let remaining = &suffix[consumed..];
    if result.len().checked_add(remaining.len())
        .is_none_or(|len| len > 16 * 1024 * 1024) {
        return Err(Error::Execution("regex output exceeds native 16 MiB budget".into()));
    }
    result.push_str(remaining);
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
mod bounded_incremental_regex_replace_tests {
    use super::*;
    #[test]
    fn rejects_expanding_outputs_before_allocating_them_all() {
        let input = Value::Text("a".repeat(1_000));
        let replacement = Value::Text("x".repeat(100_000));
        assert!(evaluate(&[input, Value::Text("a".into()), replacement]).is_err());
    }
    #[test]
    fn preserves_captures_zero_width_matches_and_positions() {
        assert_eq!(evaluate(&[Value::Text("ab12".into()), Value::Text("([a-z]+)([0-9]+)".into()),
            Value::Text("$2/$1".into())]).unwrap(), Value::Text("12/ab".into()));
        assert_eq!(evaluate(&[Value::Text("ab".into()), Value::Text("".into()),
            Value::Text("-".into())]).unwrap(), Value::Text("-a-b-".into()));
        assert_eq!(evaluate(&[Value::Text("abc".into()), Value::Text("z".into()),
            Value::Text("x".into())]).unwrap(), Value::Text("abc".into()));
    }
}

#[cfg(test)]
mod regexp_replace_impossible_position_tests {
    use super::*;
    #[test]
    fn large_unicode_positions_return_input_without_scanning() {
        let input = Value::Text("é🙂a".into());
        assert_eq!(evaluate(&[input.clone(), Value::Text("a".into()),
            Value::Text("x".into()), Value::Integer(i64::MAX)]).unwrap(), input);
        assert!(evaluate(&[Value::Text("abc".into()), Value::Text("(".into()),
            Value::Text("x".into()), Value::Integer(i64::MAX)]).is_err());
    }
}
