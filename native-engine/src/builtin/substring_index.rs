use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types == [DataType::Text, DataType::Text, DataType::Integer] { Ok(DataType::Text) }
    else { Err(Error::Type("SUBSTRING_INDEX requires text, delimiter, integer count".into())) }
}
fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    let Some(delimiter) = super::string(&args[1])? else { return Ok(Value::Null) };
    let Some(count) = super::integer(&args[2])? else { return Ok(Value::Null) };
    if count == 0 || delimiter.is_empty() { return Ok(Value::Text(String::new())); }
    if count == 1 {
        return Ok(Value::Text(text.split_once(delimiter).map_or(text, |(head, _)| head).to_owned()));
    }
    if count == -1 {
        return Ok(Value::Text(text.rsplit_once(delimiter).map_or(text, |(_, tail)| tail).to_owned()));
    }
    let index = usize::try_from(count.unsigned_abs() - 1).ok();
    let result = match index {
        None => text,
        Some(index) if count > 0 =>
            text.match_indices(delimiter).nth(index).map_or(text, |(position, _)| &text[..position]),
        Some(index) =>
            text.rmatch_indices(delimiter).nth(index)
                .map_or(text, |(position, _)| &text[position + delimiter.len()..]),
    };
    Ok(Value::Text(result.to_string()))
}

fn split_count_type(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Integer)
}
fn split_count(args: &[Value]) -> Result<Value> {
    let Some(input) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(delimiter) = super::string(&args[1])? else { return Ok(Value::Null); };
    let count = if delimiter.is_empty() {
        if input.is_ascii() { input.len() } else { input.chars().count() }
    } else {
        input.match_indices(delimiter).count().saturating_add(1)
    };
    Ok(Value::Integer(i64::try_from(count)
        .map_err(|_| Error::Execution("SPLIT_COUNT overflow".into()))?))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "split_count", min_args: 2, max_args: 2, result_type: split_count_type, call: split_count },Function {
    name: "substring_index", min_args: 3, max_args: 3, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn counts_delimiters_from_both_ends() {
        let text = Value::Text("a.é.c".into());
        let delim = Value::Text(".".into());
        assert_eq!(evaluate(&[text.clone(), delim.clone(), Value::Integer(2)]).unwrap(), Value::Text("a.é".into()));
        assert_eq!(evaluate(&[text.clone(), delim.clone(), Value::Integer(-2)]).unwrap(), Value::Text("é.c".into()));
        assert_eq!(evaluate(&[text.clone(), delim.clone(), Value::Integer(0)]).unwrap(), Value::Text("".into()));
        assert_eq!(evaluate(&[text.clone(), delim.clone(), Value::Integer(1)]).unwrap(), Value::Text("a".into()));
        assert_eq!(evaluate(&[text.clone(), delim.clone(), Value::Integer(-1)]).unwrap(), Value::Text("c".into()));
        assert_eq!(evaluate(&[Value::Text("aébéé".into()),
            Value::Text("é".into()), Value::Integer(-1)]).unwrap(), Value::Text("".into()));
        assert_eq!(evaluate(&[text, delim, Value::Integer(i64::MIN)]).unwrap(), Value::Text("a.é.c".into()));
        assert_eq!(evaluate(&[Value::Null, Value::Text(".".into()), Value::Integer(1)]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod split_count_extension_tests {
    use super::*;
    #[test]
    fn checks_data_shapes_and_nulls() {
        assert!(crate::builtin::supports("split_count"));
        assert_eq!(split_count(&[Value::Text("a,b,".into()), Value::Text(",".into())]).unwrap(), Value::Integer(3));
        assert_eq!(split_count(&[Value::Text("abc".into()), Value::Text(",".into())]).unwrap(), Value::Integer(1));
        assert_eq!(split_count(&[Value::Text("é🙂".into()), Value::Text("".into())]).unwrap(), Value::Integer(2));
        assert_eq!(split_count(&[Value::Null, Value::Text(",".into())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod ascii_empty_delimiter_split_count_tests {
    use super::*;
    #[test]
    fn empty_separator_counts_ascii_bytes_but_unicode_characters() {
        assert_eq!(split_count(&[Value::Text("z".repeat(200_000)), Value::Text("".into())]).unwrap(), Value::Integer(200_000));
        assert_eq!(split_count(&[Value::Text("é🙂".into()), Value::Text("".into())]).unwrap(), Value::Integer(2));
        assert_eq!(split_count(&[Value::Text("".into()), Value::Text("".into())]).unwrap(), Value::Integer(0));
        assert_eq!(split_count(&[Value::Text("a,b,".into()), Value::Text(",".into())]).unwrap(), Value::Integer(3));
        assert_eq!(split_count(&[Value::Null, Value::Text("".into())]).unwrap(), Value::Null);
    }
}
