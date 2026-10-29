use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

const MAX_CHARS: usize = 1_000_000;

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] != DataType::Text || types[1] != DataType::Integer ||
        (types.len() == 3 && types[2] != DataType::Text) {
        return Err(Error::Type("LPAD/RPAD expects TEXT, INTEGER, optional TEXT".into()));
    }
    Ok(DataType::Text)
}
fn pad(args: &[Value], left: bool) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    let Some(len) = super::integer(&args[1])? else { return Ok(Value::Null) };
    let target = usize::try_from(len.max(0))
        .map_err(|_| Error::Execution("LPAD/RPAD length overflow".into()))?;
    if target > MAX_CHARS {
        return Err(Error::Execution("LPAD/RPAD length exceeds native output budget".into()));
    }
    let pad = if args.len() == 3 {
        let Some(value) = super::string(&args[2])? else { return Ok(Value::Null) };
        value
    } else { " " };
    let ascii = text.is_ascii();
    let size = if ascii { text.len() } else { text.chars().count() };
    if size >= target {
        if ascii { return Ok(Value::Text(text[..target].to_owned())); }
        return Ok(Value::Text(text.chars().take(target).collect()));
    }
    if pad.is_empty() { return Err(Error::Execution("LPAD/RPAD cannot extend with empty pattern".into())); }
    let pad_chars = pad.chars().collect::<Vec<_>>();
    let mut result = String::with_capacity(
        text.len().saturating_add((target - size).saturating_mul(pad.len().min(4))),
    );
    if !left { result.push_str(text); }
    for i in 0..(target - size) {
        result.push(pad_chars[i % pad_chars.len()]);
    }
    if left { result.push_str(text); }
    Ok(Value::Text(result))
}
fn lpad(args: &[Value]) -> Result<Value> { pad(args, true) }
fn rpad(args: &[Value]) -> Result<Value> { pad(args, false) }
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "lpad", min_args: 2, max_args: 3, result_type: output, call: lpad },
    Function { name: "rpad", min_args: 2, max_args: 3, result_type: output, call: rpad },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pads_unicode_by_scalar_value_not_bytes() {
        assert_eq!(lpad(&[Value::Text("ab".into()), Value::Integer(4), Value::Text("🙂".into())]).unwrap(), Value::Text("🙂🙂ab".into()));
        assert_eq!(rpad(&[Value::Text("a".into()), Value::Integer(3), Value::Text("xy".into())]).unwrap(), Value::Text("axy".into()));
        assert_eq!(lpad(&[Value::Text("界面".into()), Value::Integer(1)]).unwrap(), Value::Text("界".into()));
        assert!(rpad(&[Value::Text("a".into()), Value::Integer(2_000_000)]).is_err());
        assert_eq!(lpad(&[Value::Text("hello".into()), Value::Integer(-1)]).unwrap(), Value::Text("".into()));
        assert_eq!(rpad(&[Value::Text("hello".into()), Value::Integer(i64::MIN)]).unwrap(), Value::Text("".into()));
    }
}

#[cfg(test)]
mod single_buffer_pad_perf_tests {
    use super::*;

    #[test]
    fn preserves_multi_character_padding_on_both_sides() {
        assert_eq!(lpad(&[
            Value::Text("é".into()), Value::Integer(6), Value::Text("ab".into())
        ]).unwrap(), Value::Text("ababaé".into()));
        assert_eq!(rpad(&[
            Value::Text("é".into()), Value::Integer(6), Value::Text("🙂a".into())
        ]).unwrap(), Value::Text("é🙂a🙂a🙂".into()));
    }
}

#[cfg(test)]
mod ascii_padding_slice_perf_tests {
    use super::*;
    #[test]
    fn truncates_ascii_without_changing_unicode_boundaries() {
        assert_eq!(lpad(&[Value::Text("abcdef".into()), Value::Integer(3)]).unwrap(), Value::Text("abc".into()));
        assert_eq!(rpad(&[Value::Text("abcdef".into()), Value::Integer(6)]).unwrap(), Value::Text("abcdef".into()));
        assert_eq!(lpad(&[Value::Text("é🙂x".into()), Value::Integer(2)]).unwrap(), Value::Text("é🙂".into()));
    }
}
