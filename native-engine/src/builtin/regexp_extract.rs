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
    } else { 0 };
    if group > 100 { return Err(Error::Execution("REGEXP_EXTRACT group budget exceeded".into())); }
    regex_support::check_input(text)?;
    let re = regex_support::compile(pattern)?;
    Ok(re.captures(text).and_then(|captures| captures.get(group))
        .map_or(Value::Null, |value| Value::Text(value.as_str().into())))
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
            Value::Text("missing".into()), Value::Text("[0-9]+".into()),
        ]).unwrap(), Value::Null);
        assert!(evaluate(&[
            Value::Text("x".into()), Value::Text("(x)".into()), Value::Integer(-1),
        ]).is_err());
    }
}
