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
    let target = usize::try_from(len)
        .map_err(|_| Error::Execution("LPAD/RPAD length cannot be negative".into()))?;
    if target > MAX_CHARS {
        return Err(Error::Execution("LPAD/RPAD length exceeds native output budget".into()));
    }
    let pad = if args.len() == 3 {
        let Some(value) = super::string(&args[2])? else { return Ok(Value::Null) };
        value
    } else { " " };
    let size = text.chars().count();
    if size >= target { return Ok(Value::Text(text.chars().take(target).collect())); }
    if pad.is_empty() { return Err(Error::Execution("LPAD/RPAD cannot extend with empty pattern".into())); }
    let pad_chars = pad.chars().collect::<Vec<_>>();
    let mut extra = String::new();
    for i in 0..(target - size) {
        extra.push(pad_chars[i % pad_chars.len()]);
    }
    Ok(Value::Text(if left { format!("{extra}{text}") } else { format!("{text}{extra}") }))
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
    }
}
