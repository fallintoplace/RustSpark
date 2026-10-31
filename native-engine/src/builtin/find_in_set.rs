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
    let position = if needle.is_empty() {
        // Empty fields are significant, including leading and trailing ones.
        haystack.split(',').position(str::is_empty).map_or(0, |index| index + 1)
    } else {
        // Search for the full needle rather than comparing every short CSV
        // field. Only delimiter-bounded matches are valid set members.
        haystack.match_indices(needle).find_map(|(start, _)| {
            let end = start + needle.len();
            let bytes = haystack.as_bytes();
            ((start == 0 || bytes[start - 1] == b',')
                && (end == bytes.len() || bytes[end] == b','))
                .then_some(start)
        }).map_or(0, |start| {
            haystack.as_bytes()[..start].iter().filter(|&&byte| byte == b',').count() + 1
        })
    };
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

#[cfg(test)]
mod delimiter_bounded_search_tests {
    use super::*;

    #[test]
    fn ignores_substrings_and_preserves_empty_fields() {
        let check = |needle: &str, text: &str, expected| {
            assert_eq!(evaluate(&[Value::Text(needle.into()), Value::Text(text.into())])
                .unwrap(), Value::Integer(expected), "needle={needle:?} text={text:?}");
        };
        check("ab", "xabc,abab,ab", 3);
        check("ab", "abx,ab", 2);
        check("é🙂", "pré🙂,é🙂,é🙂z", 2);
        check("", ",x,", 1);
        check("", "x,,", 2);
        check("", "x,", 2);
        check("a", "aa,ba,a", 3);
    }

    #[test]
    fn agrees_with_split_reference_for_small_delimited_inputs() {
        let alphabet = [b'a', b'b', b','];
        for length in 0..=5 {
            for mut encoded in 0..3usize.pow(length) {
                let mut bytes = vec![b'a'; length as usize];
                for byte in &mut bytes {
                    *byte = alphabet[encoded % 3];
                    encoded /= 3;
                }
                let text = String::from_utf8(bytes).unwrap();
                for needle in ["", "a", "b", "ab", "aa", "ba", "a,b", "é"] {
                    let expected = if needle.contains(',') { 0 } else {
                        text.split(',').position(|field| field == needle)
                            .map_or(0, |index| index + 1)
                    };
                    assert_eq!(evaluate(&[
                        Value::Text(needle.into()), Value::Text(text.clone()),
                    ]).unwrap(), Value::Integer(expected as i64),
                        "needle={needle:?} text={text:?}");
                }
            }
        }
    }
}
