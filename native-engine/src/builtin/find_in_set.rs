use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Integer)
}
fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(needle) = super::string(&args[0])? else { return Ok(Value::Null) };
    let Some(haystack) = super::string(&args[1])? else { return Ok(Value::Null) };
    if needle.len() > haystack.len() || needle.contains(',') {
        return Ok(Value::Integer(0));
    }
    let position = haystack.split(',').position(|item| item == needle);
    let position = position.map_or(0, |index| index + 1);
    Ok(Value::Integer(i64::try_from(position)
        .map_err(|_| Error::Execution("FIND_IN_SET result overflow".into()))?))
}
pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "find_in_set", min_args: 2, max_args: 2, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn finds_one_based_csv_items() {
        assert_eq!(evaluate(&[Value::Text("é".into()), Value::Text("a,é,b".into())]).unwrap(), Value::Integer(2));
        assert_eq!(evaluate(&[Value::Text("a,b".into()), Value::Text("a,b".into())]).unwrap(), Value::Integer(0));
        assert_eq!(evaluate(&[Value::Text("x".into()), Value::Text("a,b".into())]).unwrap(), Value::Integer(0));
        assert_eq!(evaluate(&[Value::Text("x".into()), Value::Text("x".into())]).unwrap(), Value::Integer(1));
        assert_eq!(evaluate(&[Value::Text("x".into()), Value::Text("y".into())]).unwrap(), Value::Integer(0));
        assert_eq!(evaluate(&[Value::Text("".into()), Value::Text("".into())]).unwrap(), Value::Integer(1));
        assert_eq!(evaluate(&[Value::Null, Value::Text("x".into())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod overlong_find_in_set_tests {
    use super::*;

    #[test]
    fn rejects_impossible_long_needles_without_changing_real_matches() {
        let long_needle = Value::Text("a".repeat(100_000));
        assert_eq!(evaluate(&[long_needle, Value::Text("a,b".into())]).unwrap(),
            Value::Integer(0));
        assert_eq!(evaluate(&[
            Value::Text("é🙂".into()), Value::Text("a,é🙂,b".into()),
        ]).unwrap(), Value::Integer(2));
        assert_eq!(evaluate(&[
            Value::Text("a,b".into()), Value::Text("a,b".into()),
        ]).unwrap(), Value::Integer(0));
        assert_eq!(evaluate(&[Value::Null, Value::Text("x".into())]).unwrap(),
            Value::Null);
    }
}

#[cfg(test)]
mod find_in_set_single_pass_tests {
    use super::*;

    #[test]
    fn single_and_multiple_fields_preserve_ordinal_positions() {
        let find = |needle: &str, haystack: &str| evaluate(&[
            Value::Text(needle.into()), Value::Text(haystack.into()),
        ]).unwrap();
        assert_eq!(find("a", "a"), Value::Integer(1));
        assert_eq!(find("a", "b"), Value::Integer(0));
        assert_eq!(find("é", "x,é,🙂"), Value::Integer(2));
        assert_eq!(find("", ""), Value::Integer(1));
        assert_eq!(find("x,y", "x,y"), Value::Integer(0));
    }
}
