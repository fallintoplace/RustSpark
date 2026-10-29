use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn split_type(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Text && types[1] == DataType::Text && types[2] == DataType::Integer {
        Ok(DataType::Text)
    } else { Err(Error::Type("SPLIT_PART expects text, delimiter, and integer position".into())) }
}
fn locate_type(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Text && types[1] == DataType::Text &&
        (types.len() == 2 || types[2] == DataType::Integer) { Ok(DataType::Integer) }
    else { Err(Error::Type("LOCATE expects text, text, optional integer position".into())) }
}
fn split_part(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    let Some(delimiter) = super::string(&args[1])? else { return Ok(Value::Null) };
    let Some(index) = super::integer(&args[2])? else { return Ok(Value::Null) };
    if index == 0 {
        return Err(Error::Execution("SPLIT_PART position cannot be zero".into()));
    }
    if delimiter.is_empty() {
        return Ok(Value::Text(if index == 1 || index == -1 { text } else { "" }.to_string()));
    }
    if index == 1 {
        let result = text.find(delimiter).map_or(text, |position| &text[..position]);
        return Ok(Value::Text(result.to_owned()));
    }
    if index == -1 {
        let result = text.rfind(delimiter).map_or(text, |position| &text[position + delimiter.len()..]);
        return Ok(Value::Text(result.to_owned()));
    }
    let result = if index > 0 {
        usize::try_from(index - 1).ok()
            .and_then(|offset| text.split(delimiter).nth(offset))
    } else {
        usize::try_from(index.unsigned_abs() - 1).ok()
            .and_then(|offset| text.rsplit(delimiter).nth(offset))
    };
    Ok(Value::Text(result.unwrap_or("").to_string()))
}
fn locate_values(needle: &Value, input: &Value, position: Option<&Value>) -> Result<Value> {
    let Some(needle) = super::string(needle)? else { return Ok(Value::Null) };
    let Some(text) = super::string(input)? else { return Ok(Value::Null) };
    let position = if let Some(value) = position {
        let Some(position) = super::integer(value)? else { return Ok(Value::Null) };
        position
    } else { 1 };
    if position < 1 { return Err(Error::Execution("LOCATE position must be positive".into())); }
    let offset = usize::try_from(position - 1)
        .map_err(|_| Error::Execution("LOCATE position overflow".into()))?;
    if text.is_ascii() {
        let Some(found) = text.get(offset..).and_then(|remainder| remainder.find(needle)) else {
            return Ok(Value::Integer(0));
        };
        let ordinal = offset.checked_add(found).and_then(|n| n.checked_add(1))
            .and_then(|n| i64::try_from(n).ok())
            .ok_or_else(|| Error::Execution("LOCATE result overflow".into()))?;
        return Ok(Value::Integer(ordinal));
    }
    let Some(start) = text.char_indices()
        .map(|(index, _)| index)
        .chain(std::iter::once(text.len()))
        .nth(offset) else {
        return Ok(Value::Integer(0));
    };
    let Some(found) = text[start..].find(needle) else { return Ok(Value::Integer(0)); };
    let characters = text[start..start + found].chars().count();
    let position = offset.checked_add(characters).and_then(|n| n.checked_add(1))
        .ok_or_else(|| Error::Execution("LOCATE result overflow".into()))?;
    let position = i64::try_from(position)
        .map_err(|_| Error::Execution("LOCATE result outside signed integer range".into()))?;
    Ok(Value::Integer(position))
}

fn locate(args: &[Value]) -> Result<Value> {
    locate_values(&args[0], &args[1], args.get(2))
}

fn instr(args: &[Value]) -> Result<Value> {
    locate_values(&args[1], &args[0], None)
}
fn strcmp_type(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Integer)
}
fn strcmp(args: &[Value]) -> Result<Value> {
    let Some(left) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(right) = super::string(&args[1])? else { return Ok(Value::Null); };
    let ordering = match left.cmp(right) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    };
    Ok(Value::Integer(ordering))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "strcmp", min_args: 2, max_args: 2, result_type: strcmp_type, call: strcmp },
    Function { name: "split_part", min_args: 3, max_args: 3, result_type: split_type, call: split_part },
    Function { name: "locate", min_args: 2, max_args: 3, result_type: locate_type, call: locate },
    Function { name: "position", min_args: 2, max_args: 3, result_type: locate_type, call: locate },
    Function { name: "instr", min_args: 2, max_args: 2, result_type: locate_type, call: instr },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn split_and_locate_measure_unicode_character_positions() {
        assert_eq!(split_part(&[Value::Text("a,b,c".into()), Value::Text(",".into()), Value::Integer(2)]).unwrap(), Value::Text("b".into()));
        assert_eq!(locate(&[Value::Text("é".into()), Value::Text("café".into())]).unwrap(), Value::Integer(4));
        assert_eq!(instr(&[Value::Text("café".into()), Value::Text("é".into())]).unwrap(), Value::Integer(4));
        assert_eq!(locate(&[Value::Text("z".into()), Value::Text("café".into())]).unwrap(), Value::Integer(0));
        assert!(split_part(&[Value::Text("x".into()), Value::Text(",".into()), Value::Integer(0)]).is_err());
        assert_eq!(split_part(&[Value::Text("a,b,c".into()), Value::Text(",".into()), Value::Integer(-2)]).unwrap(), Value::Text("b".into()));
        assert_eq!(split_part(&[Value::Text("a,b".into()), Value::Text(",".into()), Value::Integer(i64::MIN)]).unwrap(), Value::Text("".into()));
        assert_eq!(split_part(&[Value::Text("abc".into()), Value::Text("".into()), Value::Integer(1)]).unwrap(), Value::Text("abc".into()));
        assert_eq!(split_part(&[Value::Text("abc".into()), Value::Text("".into()), Value::Integer(-1)]).unwrap(), Value::Text("abc".into()));
        assert_eq!(split_part(&[Value::Text("abc".into()), Value::Text("".into()), Value::Integer(2)]).unwrap(), Value::Text("".into()));
        assert!(crate::builtin::supports("position"));
        assert_eq!(locate(&[Value::Text("bar".into()), Value::Text("foobarbar".into()), Value::Integer(5)]).unwrap(), Value::Integer(7));
        assert_eq!(locate(&[Value::Text("bar".into()), Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod instr_borrowed_arguments_perf_tests {
    use super::*;

    #[test]
    fn instr_and_locate_match_for_long_unicode_and_null_values() {
        let content = format!("{}東京", "a".repeat(65_536));
        let needle = Value::Text("東京".into());
        let haystack = Value::Text(content);
        assert_eq!(instr(&[haystack.clone(), needle.clone()]).unwrap(),
            locate(&[needle, haystack]).unwrap());
        assert_eq!(instr(&[Value::Null, Value::Text("x".into())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod split_part_boundary_perf_tests {
    use super::*;
    #[test]
    fn first_last_unicode_fields_and_no_delimiter() {
        let value = Value::Text("α🙂β🙂γ".into());
        let delim = Value::Text("🙂".into());
        assert_eq!(split_part(&[value.clone(), delim.clone(), Value::Integer(1)]).unwrap(), Value::Text("α".into()));
        assert_eq!(split_part(&[value, delim, Value::Integer(-1)]).unwrap(), Value::Text("γ".into()));
        assert_eq!(split_part(&[Value::Text("text".into()), Value::Text("|".into()), Value::Integer(-1)]).unwrap(), Value::Text("text".into()));
    }
}

#[cfg(test)]
mod locate_offset_fast_path_tests {
    use super::*;
    #[test]
    fn empty_needle_at_last_unicode_boundary() {
        let empty = Value::Text(String::new());
        let text = Value::Text("aé".into());
        assert_eq!(locate(&[empty.clone(), text.clone(), Value::Integer(3)]).unwrap(),
            Value::Integer(3));
        assert_eq!(locate(&[empty.clone(), text.clone(), Value::Integer(4)]).unwrap(),
            Value::Integer(0));
        assert_eq!(locate(&[empty, text, Value::Integer(1)]).unwrap(),
            Value::Integer(1));
    }
}

#[cfg(test)]
mod strcmp_extension_tests {
    use super::*;
    #[test]
    fn covers_null_and_boundary_values() {
        assert!(crate::builtin::supports("strcmp"));
        assert_eq!(strcmp(&[Value::Text("a".into()), Value::Text("b".into())]).unwrap(), Value::Integer(-1));
        assert_eq!(strcmp(&[Value::Text("é".into()), Value::Text("é".into())]).unwrap(), Value::Integer(0));
        assert_eq!(strcmp(&[Value::Text("z".into()), Value::Text("a".into())]).unwrap(), Value::Integer(1));
        assert_eq!(strcmp(&[Value::Null, Value::Text("x".into())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod ascii_locate_fast_path_tests {
    use super::*;
    #[test]
    fn ascii_offset_and_empty_needle_boundaries_match_unicode_semantics() {
        let haystack = Value::Text("abcabc".into());
        assert_eq!(locate(&[Value::Text("bc".into()), haystack.clone(), Value::Integer(3)]).unwrap(), Value::Integer(5));
        assert_eq!(locate(&[Value::Text("".into()), haystack.clone(), Value::Integer(7)]).unwrap(), Value::Integer(7));
        assert_eq!(locate(&[Value::Text("".into()), haystack.clone(), Value::Integer(8)]).unwrap(), Value::Integer(0));
        assert_eq!(instr(&[haystack, Value::Text("abc".into())]).unwrap(), Value::Integer(1));
    }
}
