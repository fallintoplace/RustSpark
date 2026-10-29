//! Small Spark SQL compatibility functions that fit the native scalar model.
use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn char_length_type(types: &[DataType]) -> Result<DataType> {
    if types == [DataType::Text] { Ok(DataType::Integer) }
    else { Err(Error::Type("CHAR_LENGTH requires text".into())) }
}

fn char_length(args: &[Value]) -> Result<Value> {
    match super::string(&args[0])? {
        Some(value) => {
            let len = i64::try_from(value.chars().count())
                .map_err(|_| Error::Execution("string is too long".into()))?;
            Ok(Value::Integer(len))
        }
        None => Ok(Value::Null),
    }
}



fn octet_length(args: &[Value]) -> Result<Value> {
    match super::string(&args[0])? {
        Some(value) => Ok(Value::Integer(i64::try_from(value.len())
            .map_err(|_| Error::Execution("string is too long".into()))?)),
        None => Ok(Value::Null),
    }
}

fn bit_length(args: &[Value]) -> Result<Value> {
    match super::string(&args[0])? {
        Some(value) => {
            let bits = value.len().checked_mul(8)
                .and_then(|size| i64::try_from(size).ok())
                .ok_or_else(|| Error::Execution("string is too long".into()))?;
            Ok(Value::Integer(bits))
        }
        None => Ok(Value::Null),
    }
}

fn text_text_type(types: &[DataType]) -> Result<DataType> {
    if types == [DataType::Text] { Ok(DataType::Text) }
    else { Err(Error::Type("function requires text".into())) }
}
fn lcase(args: &[Value]) -> Result<Value> {
    Ok(match super::string(&args[0])? {
        Some(value) => Value::Text(value.to_lowercase()),
        None => Value::Null,
    })
}

fn ucase(args: &[Value]) -> Result<Value> {
    Ok(match super::string(&args[0])? {
        Some(value) => Value::Text(value.to_uppercase()),
        None => Value::Null,
    })
}

fn initcap(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let mut result = String::with_capacity(text.len());
    let mut word_start = true;
    for ch in text.chars() {
        if ch.is_whitespace() {
            word_start = true;
            result.push(ch);
        } else if word_start {
            result.extend(ch.to_uppercase());
            word_start = false;
        } else {
            result.extend(ch.to_lowercase());
        }
    }
    Ok(Value::Text(result))
}

fn space_type(types: &[DataType]) -> Result<DataType> {
    if types == [DataType::Integer] { Ok(DataType::Text) }
    else { Err(Error::Type("SPACE requires an integer".into())) }
}
fn space(args: &[Value]) -> Result<Value> {
    let Some(count) = super::integer(&args[0])? else { return Ok(Value::Null); };
    let length = usize::try_from(count.max(0))
        .map_err(|_| Error::Execution("SPACE length is too large".into()))?;
    if length > 1_000_000 {
        return Err(Error::Execution("SPACE length exceeds native memory guard".into()));
    }
    Ok(Value::Text(" ".repeat(length)))
}

fn quote(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    Ok(Value::Text(format!("'{}'", text.replace('\'', "\\'"))))
}

fn substring_index_type(types: &[DataType]) -> Result<DataType> {
    if types == [DataType::Text, DataType::Text, DataType::Integer] { Ok(DataType::Text) }
    else { Err(Error::Type("SUBSTRING_INDEX requires text, delimiter and integer count".into())) }
}
fn substring_index(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(delimiter) = super::string(&args[1])? else { return Ok(Value::Null); };
    let Some(count) = super::integer(&args[2])? else { return Ok(Value::Null); };
    if delimiter.is_empty() || count == 0 { return Ok(Value::Text(String::new())); }
    if count > 0 {
        let mut next = 0;
        for _ in 0..(count as u64) {
            let Some(found) = text[next..].find(delimiter) else {
                return Ok(Value::Text(text.to_owned()));
            };
            next += found + delimiter.len();
        }
        Ok(Value::Text(text[..next - delimiter.len()].to_owned()))
    } else {
        let mut end = text.len();
        for _ in 0..count.unsigned_abs() {
            let Some(found) = text[..end].rfind(delimiter) else {
                return Ok(Value::Text(text.to_owned()));
            };
            end = found;
        }
        Ok(Value::Text(text[end + delimiter.len()..].to_owned()))
    }
}

fn two_text_integer_type(types: &[DataType]) -> Result<DataType> {
    if types == [DataType::Text, DataType::Text] { Ok(DataType::Integer) }
    else { Err(Error::Type("function requires two text arguments".into())) }
}
fn find_in_set(args: &[Value]) -> Result<Value> {
    let Some(needle) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(haystack) = super::string(&args[1])? else { return Ok(Value::Null); };
    if needle.contains(',') { return Ok(Value::Integer(0)); }
    let position = haystack.split(',').position(|item| item == needle)
        .map(|index| i64::try_from(index + 1))
        .transpose().map_err(|_| Error::Execution("FIND_IN_SET index overflow".into()))?
        .unwrap_or(0);
    Ok(Value::Integer(position))
}

fn position_type(types: &[DataType]) -> Result<DataType> {
    if (types == [DataType::Text, DataType::Text])
        || (types == [DataType::Text, DataType::Text, DataType::Integer]) {
        Ok(DataType::Integer)
    } else { Err(Error::Type("POSITION requires substring, text, optional integer start".into())) }
}
fn position(args: &[Value]) -> Result<Value> {
    let Some(needle) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(text) = super::string(&args[1])? else { return Ok(Value::Null); };
    let start = if args.len() == 3 {
        let Some(start) = super::integer(&args[2])? else { return Ok(Value::Null); };
        start.max(1)
    } else { 1 };
    let chars_to_skip = usize::try_from(start - 1)
        .map_err(|_| Error::Execution("POSITION start overflow".into()))?;
    let Some((byte_start, _)) = text.char_indices().nth(chars_to_skip)
        .or_else(|| (chars_to_skip == text.chars().count()).then_some((text.len(), '\0'))) else {
        return Ok(Value::Integer(0));
    };
    let Some(relative_byte) = text[byte_start..].find(needle) else {
        return Ok(Value::Integer(0));
    };
    let absolute_byte = byte_start + relative_byte;
    let index = i64::try_from(text[..absolute_byte].chars().count())
        .map_err(|_| Error::Execution("POSITION index overflow".into()))?;
    Ok(Value::Integer(index + 1))
}

// PARITY_IMPL

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "char_length", min_args: 1, max_args: 1, result_type: char_length_type, call: char_length },
    Function { name: "character_length", min_args: 1, max_args: 1, result_type: char_length_type, call: char_length },
    Function { name: "len", min_args: 1, max_args: 1, result_type: char_length_type, call: char_length },
    Function { name: "octet_length", min_args: 1, max_args: 1, result_type: char_length_type, call: octet_length },
    Function { name: "bit_length", min_args: 1, max_args: 1, result_type: char_length_type, call: bit_length },
    Function { name: "lcase", min_args: 1, max_args: 1, result_type: text_text_type, call: lcase },
    Function { name: "ucase", min_args: 1, max_args: 1, result_type: text_text_type, call: ucase },
    Function { name: "initcap", min_args: 1, max_args: 1, result_type: text_text_type, call: initcap },
    Function { name: "space", min_args: 1, max_args: 1, result_type: space_type, call: space },
    Function { name: "quote", min_args: 1, max_args: 1, result_type: text_text_type, call: quote },
    Function { name: "substring_index", min_args: 3, max_args: 3, result_type: substring_index_type, call: substring_index },
    Function { name: "find_in_set", min_args: 2, max_args: 2, result_type: two_text_integer_type, call: find_in_set },
    Function { name: "position", min_args: 2, max_args: 3, result_type: position_type, call: position },
    // PARITY_FUNCTIONS
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, schema::{Field, Schema}, sql};

    fn sql_result(expression: &str) -> Result<Value> {
        let schema = Schema::new(vec![
            Field::new("s", DataType::Text, true),
            Field::new("n", DataType::Integer, true),
        ])?;
        let batch = Batch::from_rows(schema, vec![
            vec![Value::Text("café".into()), Value::Integer(5)],
        ])?;
        let mut catalog = Catalog::new();
        catalog.register("t", batch)?;
        let sql_text = format!("SELECT {expression} AS result FROM t");
        let plan = sql::parse_sql(&sql_text)?;
        Ok(executor::execute(&plan, &catalog)?.row(0)?[0].clone())
    }

    #[test]
    fn spark_char_length_counts_unicode_characters() {
        assert_eq!(char_length(&[Value::Text("café".into())]).unwrap(), Value::Integer(4));
        assert_eq!(char_length(&[Value::Null]).unwrap(), Value::Null);
        assert_eq!(sql_result("char_length(s)").unwrap(), Value::Integer(4));
        assert!(sql_result("char_length(n)").is_err());
    }

    #[test]
    fn spark_character_length_alias() {
        assert_eq!(sql_result("character_length(s)").unwrap(), Value::Integer(4));
    }

    #[test]
    fn spark_len_alias_includes_trailing_spaces() {
        assert_eq!(sql_result("len('a  ')").unwrap(), Value::Integer(3));
        assert_eq!(sql_result("len(s)").unwrap(), Value::Integer(4));
    }

    #[test]
    fn spark_octet_length_counts_utf8_bytes() {
        assert_eq!(sql_result("octet_length(s)").unwrap(), Value::Integer(5));
        assert_eq!(octet_length(&[Value::Null]).unwrap(), Value::Null);
    }

    #[test]
    fn spark_bit_length_counts_utf8_bits() {
        assert_eq!(sql_result("bit_length(s)").unwrap(), Value::Integer(40));
        assert_eq!(bit_length(&[Value::Null]).unwrap(), Value::Null);
    }

    #[test]
    fn spark_lcase_lowercases_unicode() {
        assert_eq!(sql_result("lcase('CAFÉ')").unwrap(), Value::Text("café".into()));
        assert_eq!(lcase(&[Value::Null]).unwrap(), Value::Null);
    }

    #[test]
    fn spark_ucase_uppercases_unicode() {
        assert_eq!(sql_result("ucase(s)").unwrap(), Value::Text("CAFÉ".into()));
        assert_eq!(ucase(&[Value::Null]).unwrap(), Value::Null);
    }

    #[test]
    fn spark_initcap_preserves_whitespace_and_capitalizes_words() {
        assert_eq!(sql_result("initcap('sPARK   sql')").unwrap(), Value::Text("Spark   Sql".into()));
        assert_eq!(initcap(&[Value::Null]).unwrap(), Value::Null);
    }

    #[test]
    fn spark_space_generates_bounded_spaces() {
        assert_eq!(sql_result("space(3)").unwrap(), Value::Text("   ".into()));
        assert_eq!(space(&[Value::Integer(-2)]).unwrap(), Value::Text(String::new()));
        assert_eq!(space(&[Value::Null]).unwrap(), Value::Null);
        assert!(space(&[Value::Integer(1_000_001)]).is_err());
    }

    #[test]
    fn spark_quote_escapes_single_quotes() {
        assert_eq!(sql_result("quote(s)").unwrap(), Value::Text("'café'".into()));
        assert_eq!(quote(&[Value::Text("don't".into())]).unwrap(), Value::Text("'don\\'t'".into()));
        assert_eq!(quote(&[Value::Null]).unwrap(), Value::Null);
    }

    #[test]
    fn spark_substring_index_counts_both_directions() {
        assert_eq!(sql_result("substring_index('www.apache.org', '.', 2)").unwrap(),
            Value::Text("www.apache".into()));
        assert_eq!(sql_result("substring_index('www.apache.org', '.', -1)").unwrap(),
            Value::Text("org".into()));
        assert_eq!(sql_result("substring_index('a.b', '.', 99)").unwrap(),
            Value::Text("a.b".into()));
        assert_eq!(substring_index(&[Value::Null, Value::Text(".".into()), Value::Integer(1)]).unwrap(),
            Value::Null);
    }

    #[test]
    fn spark_find_in_set_uses_one_based_items() {
        assert_eq!(sql_result("find_in_set('ab', 'abc,b,ab,c,def')").unwrap(), Value::Integer(3));
        assert_eq!(sql_result("find_in_set('missing', 'abc,b')").unwrap(), Value::Integer(0));
        assert_eq!(sql_result("find_in_set('a,b', 'a,b')").unwrap(), Value::Integer(0));
    }

    #[test]
    fn spark_position_finds_unicode_character_indices() {
        assert_eq!(sql_result("position('fé', s)").unwrap(), Value::Integer(3));
        assert_eq!(sql_result("position('a', 'banana', 4)").unwrap(), Value::Integer(4));
        assert_eq!(sql_result("position('x', s)").unwrap(), Value::Integer(0));
        assert_eq!(position(&[Value::Null, Value::Text("x".into())]).unwrap(), Value::Null);
    }

    // PARITY_TESTS
}
