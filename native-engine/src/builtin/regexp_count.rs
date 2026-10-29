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
        assert_eq!(evaluate(&[Value::Null, Value::Text("a".into())]).unwrap(), Value::Null);
    }
}
