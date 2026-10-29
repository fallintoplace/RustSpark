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

fn substr_type(types: &[DataType]) -> Result<DataType> {
    if types == [DataType::Text, DataType::Integer]
        || types == [DataType::Text, DataType::Integer, DataType::Integer] {
        Ok(DataType::Text)
    } else {
        Err(Error::Type("SUBSTR requires text, integer start, optional integer length".into()))
    }
}
fn substr(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(position) = super::integer(&args[1])? else { return Ok(Value::Null); };
    let count = if args.len() == 3 {
        let Some(length) = super::integer(&args[2])? else { return Ok(Value::Null); };
        usize::try_from(length).map_err(|_| Error::Execution("SUBSTR length must be nonnegative".into()))?
    } else { usize::MAX };
    let total_chars = text.chars().count();
    let start = if position < 0 {
        let back = usize::try_from(position.unsigned_abs()).unwrap_or(usize::MAX);
        total_chars.saturating_sub(back)
    } else {
        usize::try_from(position.saturating_sub(1)).unwrap_or(usize::MAX)
    };
    Ok(Value::Text(text.chars().skip(start).take(count).collect()))
}

fn luhn_type(types: &[DataType]) -> Result<DataType> {
    if types == [DataType::Text] { Ok(DataType::Boolean) }
    else { Err(Error::Type("LUHN_CHECK requires text digits".into())) }
}
fn luhn_check(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Ok(Value::Boolean(false));
    }
    let sum: u64 = text.bytes().rev().enumerate().map(|(index, byte)| {
        let digit = u64::from(byte - b'0');
        if index % 2 == 1 { let doubled = digit * 2; doubled / 10 + doubled % 10 } else { digit }
    }).sum();
    Ok(Value::Boolean(sum % 10 == 0))
}

fn soundex_digit(ch: char) -> char {
    match ch.to_ascii_uppercase() {
        'B' | 'F' | 'P' | 'V' => '1',
        'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => '2',
        'D' | 'T' => '3',
        'L' => '4',
        'M' | 'N' => '5',
        'R' => '6',
        _ => '0',
    }
}
fn soundex(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let mut letters = text.chars().filter(|ch| ch.is_ascii_alphabetic());
    let Some(first) = letters.next() else { return Ok(Value::Text(text.to_owned())); };
    let mut result = String::from(first.to_ascii_uppercase());
    let mut previous = soundex_digit(first);
    for letter in letters {
        let code = soundex_digit(letter);
        if code != '0' && code != previous { result.push(code); }
        if !matches!(letter.to_ascii_uppercase(), 'H' | 'W') { previous = code; }
        if result.len() == 4 { break; }
    }
    while result.len() < 4 { result.push('0'); }
    Ok(Value::Text(result))
}

fn elt_type(types: &[DataType]) -> Result<DataType> {
    if types.len() < 2 || types[0] != DataType::Integer {
        return Err(Error::Type("ELT requires an integer index and values".into()));
    }
    let selected = types[1];
    if types[1..].iter().any(|kind| *kind != selected) {
        return Err(Error::Type("ELT values must have one native type".into()));
    }
    Ok(selected)
}
fn elt(args: &[Value]) -> Result<Value> {
    let Some(index) = super::integer(&args[0])? else { return Ok(Value::Null); };
    let Some(zero_based) = index.checked_sub(1).and_then(|value| usize::try_from(value).ok()) else {
        return Ok(Value::Null);
    };
    Ok(args.get(zero_based + 1).cloned().unwrap_or(Value::Null))
}

fn number_float_type(types: &[DataType]) -> Result<DataType> {
    if types.len() == 1 && matches!(types[0], DataType::Integer | DataType::Float) {
        Ok(DataType::Float)
    } else { Err(Error::Type("function requires one numeric argument".into())) }
}
fn signum(args: &[Value]) -> Result<Value> {
    let Some(value) = super::number(&args[0])? else { return Ok(Value::Null); };
    let sign = if value > 0.0 { 1.0 } else if value < 0.0 { -1.0 } else { 0.0 };
    Ok(Value::Float(sign))
}

fn cot(args: &[Value]) -> Result<Value> {
    let Some(value) = super::number(&args[0])? else { return Ok(Value::Null); };
    super::finite(1.0 / value.tan())
}

fn sec(args: &[Value]) -> Result<Value> {
    let Some(value) = super::number(&args[0])? else { return Ok(Value::Null); };
    super::finite(1.0 / value.cos())
}

fn csc(args: &[Value]) -> Result<Value> {
    let Some(value) = super::number(&args[0])? else { return Ok(Value::Null); };
    super::finite(1.0 / value.sin())
}

fn bin_type(types: &[DataType]) -> Result<DataType> {
    if types.len() == 1 && matches!(types[0], DataType::Integer | DataType::Float) {
        Ok(DataType::Text)
    } else { Err(Error::Type("BIN requires numeric input".into())) }
}
fn bin(args: &[Value]) -> Result<Value> {
    let number = match &args[0] {
        Value::Integer(value) => *value,
        Value::Float(value) if value.is_finite() => *value as i64,
        Value::Null => return Ok(Value::Null),
        _ => return Err(Error::Type("BIN requires a finite number".into())),
    };
    Ok(Value::Text(format!("{number:b}")))
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
    Function { name: "substr", min_args: 2, max_args: 3, result_type: substr_type, call: substr },
    Function { name: "luhn_check", min_args: 1, max_args: 1, result_type: luhn_type, call: luhn_check },
    Function { name: "soundex", min_args: 1, max_args: 1, result_type: text_text_type, call: soundex },
    Function { name: "elt", min_args: 2, max_args: 128, result_type: elt_type, call: elt },
    Function { name: "signum", min_args: 1, max_args: 1, result_type: number_float_type, call: signum },
    Function { name: "cot", min_args: 1, max_args: 1, result_type: number_float_type, call: cot },
    Function { name: "sec", min_args: 1, max_args: 1, result_type: number_float_type, call: sec },
    Function { name: "csc", min_args: 1, max_args: 1, result_type: number_float_type, call: csc },
    Function { name: "bin", min_args: 1, max_args: 1, result_type: bin_type, call: bin },
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

    #[test]
    fn spark_substr_supports_optional_length_and_negative_start() {
        assert_eq!(sql_result("substr('Spark SQL', -3)").unwrap(), Value::Text("SQL".into()));
        assert_eq!(sql_result("substr(s, 2, 2)").unwrap(), Value::Text("af".into()));
        assert_eq!(sql_result("substr(s, 0, 2)").unwrap(), Value::Text("ca".into()));
        assert!(sql_result("substr(s, 1, -1)").is_err());
    }

    #[test]
    fn spark_luhn_check_validates_digits() {
        assert_eq!(sql_result("luhn_check('79927398713')").unwrap(), Value::Boolean(true));
        assert_eq!(sql_result("luhn_check('79927398714')").unwrap(), Value::Boolean(false));
        assert_eq!(sql_result("luhn_check('not digits')").unwrap(), Value::Boolean(false));
        assert_eq!(luhn_check(&[Value::Null]).unwrap(), Value::Null);
    }

    #[test]
    fn spark_soundex_encodes_english_names() {
        assert_eq!(sql_result("soundex('Miller')").unwrap(), Value::Text("M460".into()));
        assert_eq!(sql_result("soundex('Robert')").unwrap(), Value::Text("R163".into()));
        assert_eq!(soundex(&[Value::Null]).unwrap(), Value::Null);
    }

    #[test]
    fn spark_elt_selects_one_based_argument() {
        assert_eq!(sql_result("elt(2, 'scala', 'java')").unwrap(), Value::Text("java".into()));
        assert_eq!(sql_result("elt(9, 'scala', 'java')").unwrap(), Value::Null);
        assert_eq!(elt(&[Value::Null, Value::Text("x".into())]).unwrap(), Value::Null);
    }

    #[test]
    fn spark_signum_returns_float_sign() {
        assert_eq!(sql_result("signum(-3)").unwrap(), Value::Float(-1.0));
        assert_eq!(sql_result("signum(0)").unwrap(), Value::Float(0.0));
        assert_eq!(sql_result("signum(3)").unwrap(), Value::Float(1.0));
        assert_eq!(signum(&[Value::Null]).unwrap(), Value::Null);
    }

    #[test]
    fn spark_cot_matches_reciprocal_tangent() {
        let Value::Float(actual) = sql_result("cot(1)").unwrap() else { panic!("expected float") };
        assert!((actual - 0.6420926159343306).abs() < 1e-12);
        assert_eq!(cot(&[Value::Null]).unwrap(), Value::Null);
    }

    #[test]
    fn spark_sec_matches_reciprocal_cosine() {
        assert_eq!(sql_result("sec(0)").unwrap(), Value::Float(1.0));
        assert_eq!(sec(&[Value::Null]).unwrap(), Value::Null);
    }

    #[test]
    fn spark_csc_matches_reciprocal_sine() {
        let Value::Float(actual) = sql_result("csc(1)").unwrap() else { panic!("expected float") };
        assert!((actual - 1.1883951057781212).abs() < 1e-12);
        assert_eq!(csc(&[Value::Null]).unwrap(), Value::Null);
    }

    #[test]
    fn spark_bin_uses_twos_complement_for_negative_numbers() {
        assert_eq!(sql_result("bin(13)").unwrap(), Value::Text("1101".into()));
        assert_eq!(sql_result("bin(-13)").unwrap(),
            Value::Text("1111111111111111111111111111111111111111111111111111111111110011".into()));
        assert_eq!(sql_result("bin(13.3)").unwrap(), Value::Text("1101".into()));
    }

    // PARITY_TESTS
}
