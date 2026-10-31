use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(separator) = super::string(&args[0])? else { return Ok(Value::Null); };
    if args.len() == 2 {
        return match super::string(&args[1])? {
            Some(value) if value.len() <= 16 * 1024 * 1024 => Ok(Value::Text(value.to_owned())),
            Some(_) => Err(Error::Execution("CONCAT_WS output exceeds 16 MiB".into())),
            None => Ok(Value::Text(String::new())),
        };
    }
    let mut total = 0usize;
    let mut pieces = 0usize;
    for arg in &args[1..] {
        if let Some(text) = super::string(arg)? {
            total = total.checked_add(text.len())
                .ok_or_else(|| Error::Execution("CONCAT_WS length overflow".into()))?;
            pieces += 1;
        }
    }
    let separators = separator.len().checked_mul(pieces.saturating_sub(1))
        .ok_or_else(|| Error::Execution("CONCAT_WS separator overflow".into()))?;
    let length = total.checked_add(separators)
        .ok_or_else(|| Error::Execution("CONCAT_WS length overflow".into()))?;
    if length > 16 * 1024 * 1024 {
        return Err(Error::Execution("CONCAT_WS output exceeds 16 MiB".into()));
    }

    // The first pass validates the bound; the second writes once into the result.
    let mut output = String::with_capacity(length);
    let mut first = true;
    // The sizing pass already checked every argument's type and NULL state.
    for arg in &args[1..] {
        if let Value::Text(text) = arg {
            if !first { output.push_str(separator); }
            output.push_str(text);
            first = false;
        }
    }
    Ok(Value::Text(output))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "concat_ws", min_args: 1, max_args: 128,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn joins_nonnull_fields_with_separator() {
        assert_eq!(evaluate(&[
            Value::Text("-".into()), Value::Text("Berlin".into()),
            Value::Null, Value::Text("Germany".into()),
        ]).unwrap(), Value::Text("Berlin-Germany".into()));
        assert_eq!(evaluate(&[
            Value::Text("-".into()), Value::Null,
        ]).unwrap(), Value::Text("".into()));
        assert_eq!(evaluate(&[
            Value::Null, Value::Text("x".into()),
        ]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[
            Value::Text("🙂".into()), Value::Null, Value::Text("a".into()),
            Value::Text("".into()), Value::Text("é".into()),
        ]).unwrap(), Value::Text("a🙂🙂é".into()));
        assert!(evaluate(&[
            Value::Text("-".into()), Value::Text("x".repeat(16 * 1024 * 1024)),
            Value::Text("z".into()),
        ]).is_err());
    }
}

#[cfg(test)]
mod separator_only_concat_ws_tests {
    use super::*;
    #[test]
    fn returns_empty_string_for_only_a_separator() {
        assert_eq!(evaluate(&[Value::Text(",".into())]).unwrap(), Value::Text(String::new()));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert_eq!(output(&[DataType::Text]).unwrap(), DataType::Text);
    }
}

#[cfg(test)]
mod single_value_concat_ws_fast_path_tests {
    use super::*;
    #[test]
    fn handles_one_value_nulls_and_separator_escaping() {
        assert_eq!(evaluate(&[Value::Text(",".into()),
            Value::Text("é🙂".into())]).unwrap(), Value::Text("é🙂".into()));
        assert_eq!(evaluate(&[Value::Text("long separator".into()),
            Value::Null]).unwrap(), Value::Text("".into()));
        assert_eq!(evaluate(&[Value::Null, Value::Text("a".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Text("/".into()), Value::Text("a".into()),
            Value::Text("b".into())]).unwrap(), Value::Text("a/b".into()));
        assert!(evaluate(&[Value::Text(".".into()),
            Value::Text("x".repeat(16 * 1024 * 1024 + 1))]).is_err());
    }
}

#[cfg(test)]
mod validated_concat_ws_output_regressions {
    use super::*;

    #[test]
    fn keeps_nullable_unicode_arguments_and_rejects_wrong_types() {
        let mut args = vec![Value::Text("🙂".into())];
        for index in 0..64 {
            args.push(if index % 3 == 0 { Value::Null }
                else { Value::Text(format!("界{index}")) });
        }
        let actual = evaluate(&args).unwrap();
        let expected = args[1..].iter().filter_map(|arg| match arg {
            Value::Text(text) => Some(text.as_str()), _ => None,
        }).collect::<Vec<_>>().join("🙂");
        assert_eq!(actual, Value::Text(expected));
        args[30] = Value::Integer(42);
        assert!(matches!(evaluate(&args), Err(Error::Type(_))));
    }
}
