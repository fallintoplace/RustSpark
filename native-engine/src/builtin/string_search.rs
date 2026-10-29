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
    if delimiter.is_empty() || index <= 0 {
        return Err(Error::Execution("SPLIT_PART needs nonempty delimiter and positive position".into()));
    }
    let index = usize::try_from(index)
        .map_err(|_| Error::Execution("SPLIT_PART position overflow".into()))?;
    Ok(Value::Text(text.split(delimiter).nth(index - 1).unwrap_or("").to_string()))
}
fn locate(args: &[Value]) -> Result<Value> {
    let Some(needle) = super::string(&args[0])? else { return Ok(Value::Null) };
    let Some(text) = super::string(&args[1])? else { return Ok(Value::Null) };
    let position = if args.len() == 3 {
        let Some(position) = super::integer(&args[2])? else { return Ok(Value::Null) };
        position
    } else { 1 };
    if position < 1 { return Err(Error::Execution("LOCATE position must be positive".into())); }
    let offset = usize::try_from(position - 1)
        .map_err(|_| Error::Execution("LOCATE position overflow".into()))?;
    let length = text.chars().count();
    if offset > length { return Ok(Value::Integer(0)); }
    let start = text.char_indices().nth(offset).map_or(text.len(), |(index, _)| index);
    let Some(found) = text[start..].find(needle) else { return Ok(Value::Integer(0)); };
    let characters = text[start..start + found].chars().count();
    let position = offset.checked_add(characters).and_then(|n| n.checked_add(1))
        .ok_or_else(|| Error::Execution("LOCATE result overflow".into()))?;
    let position = i64::try_from(position)
        .map_err(|_| Error::Execution("LOCATE result outside signed integer range".into()))?;
    Ok(Value::Integer(position))
}
fn instr(args: &[Value]) -> Result<Value> {
    locate(&[args[1].clone(), args[0].clone()])
}
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "split_part", min_args: 3, max_args: 3, result_type: split_type, call: split_part },
    Function { name: "locate", min_args: 2, max_args: 3, result_type: locate_type, call: locate },
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
    }
}
