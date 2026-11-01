use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] != DataType::Integer || !types[1..].iter().all(|kind| *kind == DataType::Text) {
        return Err(Error::Type("ELT requires integer index and text alternatives".into()));
    }
    Ok(DataType::Text)
}
fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(index) = super::integer(&args[0])? else { return Ok(Value::Null) };
    // Index the argv directly, which also avoids offset + 1 overflow on 32-bit.
    let Some(position) = usize::try_from(index).ok().filter(|&n| n > 0) else {
        return Ok(Value::Null);
    };
    match args.get(position) {
        Some(Value::Text(text)) if text.len() > 16 * 1024 * 1024 =>
            Err(Error::Execution("ELT result exceeds native 16 MiB budget".into())),
        Some(value) => Ok(value.clone()),
        None => Ok(Value::Null),
    }
}
pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "elt", min_args: 2, max_args: 128, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn picks_one_based_text_arguments() {
        let args = [Value::Integer(2), Value::Text("a".into()), Value::Text("b".into())];
        assert_eq!(evaluate(&args).unwrap(), Value::Text("b".into()));
        assert_eq!(evaluate(&[Value::Integer(0), Value::Text("a".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Integer(5), Value::Text("a".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Integer(i64::MAX), Value::Text("a".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Integer(i64::from(u32::MAX) + 1), Value::Text("a".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Integer(i64::MIN), Value::Text("a".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Null, Value::Text("a".into())]).unwrap(), Value::Null);
        assert!(output(&[DataType::Text, DataType::Text]).is_err());
    }
}

#[cfg(test)]
mod elt_output_budget_regressions {
    use super::*;
    #[test]
    fn only_the_selected_alternative_needs_to_fit() {
        let long = Value::Text("z".repeat(16 * 1024 * 1024 + 1));
        assert_eq!(evaluate(&[Value::Integer(1), Value::Text("small".into()),
            long.clone()]).unwrap(), Value::Text("small".into()));
        assert!(matches!(evaluate(&[Value::Integer(2), Value::Text("small".into()),
            long]), Err(Error::Execution(_))));
        assert_eq!(evaluate(&[Value::Integer(3), Value::Text("small".into())]).unwrap(),
            Value::Null);
        assert_eq!(evaluate(&[Value::Null, Value::Text("small".into())]).unwrap(),
            Value::Null);
    }
}
