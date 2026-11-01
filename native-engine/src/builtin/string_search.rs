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
fn bounded_split_field(value: &str) -> Result<Value> {
    if value.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("SPLIT_PART result exceeds native 16 MiB budget".into()));
    }
    Ok(Value::Text(value.to_owned()))
}

fn split_part(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    let Some(delimiter) = super::string(&args[1])? else { return Ok(Value::Null) };
    let Some(index) = super::integer(&args[2])? else { return Ok(Value::Null) };
    if index == 0 {
        return Err(Error::Execution("SPLIT_PART position cannot be zero".into()));
    }
    if delimiter.is_empty() {
        return bounded_split_field(if index == 1 || index == -1 { text } else { "" });
    }
    if index == 1 {
        let result = text.find(delimiter).map_or(text, |position| &text[..position]);
        return bounded_split_field(result);
    }
    if index == -1 {
        // Reverse substring searches choose different matches for overlapping
        // delimiters. SQL positions refer to the same forward split fields.
        return bounded_split_field(text.split(delimiter).last().unwrap_or(""));
    }
    // Nonoverlapping delimiters can produce at most one additional field
    // per delimiter-width span. Byte lengths give a safe UTF-8 upper bound.
    let possible_fields = text.len() / delimiter.len();
    if index.unsigned_abs() > u64::try_from(possible_fields).unwrap_or(u64::MAX).saturating_add(1) {
        return Ok(Value::Text(String::new()));
    }
    if index < -1 {
        let from_end = usize::try_from(index.unsigned_abs()).unwrap_or(usize::MAX);
        if delimiter.chars().count() == 1 {
            // A single Unicode scalar never overlaps a second occurrence.
            // Reverse splitting can select the requested tail in one scan.
            return bounded_split_field(text.rsplit(delimiter)
                .nth(from_end - 1).unwrap_or(""));
        }
        if from_end <= 16 {
            // Multicharacter delimiters can overlap. Keep forward split
            // semantics with a bounded stack ring instead of two scans.
            let mut recent: [Option<&str>; 16] = [None; 16];
            let mut seen = 0usize;
            for field in text.split(delimiter) {
                recent[seen % from_end] = Some(field);
                seen += 1;
            }
            let selected = if seen >= from_end { recent[seen % from_end] }
                else { None };
            return bounded_split_field(selected.unwrap_or(""));
        }
    }
    let result = if index > 0 {
        usize::try_from(index - 1).ok()
            .and_then(|offset| text.split(delimiter).nth(offset))
    } else {
        usize::try_from(index.unsigned_abs()).ok()
            .and_then(|position| text.split(delimiter).count().checked_sub(position))
            .and_then(|offset| text.split(delimiter).nth(offset))
    };
    bounded_split_field(result.unwrap_or(""))
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
    let ascii = text.is_ascii();
    // A UTF-8 non-ASCII needle cannot occur in an ASCII-only source.
    if ascii && !needle.is_ascii() {
        return Ok(Value::Integer(0));
    }
    let start = if ascii {
        if offset > text.len() { return Ok(Value::Integer(0)); }
        offset
    } else {
        let Some(start) = text.char_indices()
            .map(|(index, _)| index)
            .chain(std::iter::once(text.len()))
            .nth(offset) else {
            return Ok(Value::Integer(0));
        };
        start
    };
    let Some(found) = text[start..].find(needle) else { return Ok(Value::Integer(0)); };
    let characters = if ascii { found } else {
        super::char_length::character_count(&text[start..start + found])
    };
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
        assert_eq!(split_part(&[Value::Text("a,b".into()), Value::Text(",".into()), Value::Integer(i64::MAX)]).unwrap(), Value::Text("".into()));
        assert_eq!(split_part(&[Value::Text("a,b".into()), Value::Text(",".into()), Value::Integer(3)]).unwrap(), Value::Text("".into()));
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
mod locate_ascii_offset_tests {
    use super::*;
    #[test]
    fn large_offsets_are_one_based_and_unicode_stays_character_based() {
        let input = format!("{}xyz", "a".repeat(20_000));
        assert_eq!(locate(&[Value::Text("xyz".into()), Value::Text(input),
            Value::Integer(19_999)]).unwrap(), Value::Integer(20_001));
        assert_eq!(locate(&[Value::Text("x".into()), Value::Text("ééx".into()),
            Value::Integer(2)]).unwrap(), Value::Integer(3));
    }
}

#[cfg(test)]
mod locate_ascii_match_offset_regressions {
    use super::*;

    #[test]
    fn reports_one_based_offsets_on_large_ascii_and_mixed_unicode() {
        let ascii = format!("{}needle{}", "a".repeat(100_000), "z".repeat(100_000));
        assert_eq!(locate(&[Value::Text("needle".into()), Value::Text(ascii.clone())]).unwrap(),
            Value::Integer(100_001));
        assert_eq!(locate(&[Value::Text("needle".into()), Value::Text(ascii),
            Value::Integer(99_999)]).unwrap(), Value::Integer(100_001));
        assert_eq!(locate(&[Value::Text("x".into()), Value::Text("é🙂xy".into()),
            Value::Integer(2)]).unwrap(), Value::Integer(3));
        assert_eq!(instr(&[Value::Text("🙂ab".into()), Value::Text("b".into())]).unwrap(),
            Value::Integer(3));
        assert_eq!(locate(&[Value::Text("absent".into()), Value::Text("abc".into())]).unwrap(),
            Value::Integer(0));
    }
}

#[cfg(test)]
mod delimiter_width_split_part_regressions {
    use super::*;

    #[test]
    fn rejects_impossible_positive_and_negative_fields_without_changing_splits() {
        let run = |text: &str, delimiter: &str, index: i64| {
            split_part(&[
                Value::Text(text.into()), Value::Text(delimiter.into()),
                Value::Integer(index),
            ]).unwrap()
        };
        for (text, delimiter) in [
            ("aa::bb::cc", "::"), ("🙂é🙂é🙂", "🙂"),
            ("abc", "abc"), ("", "::"), ("foofoofoo", "foo"),
        ] {
            let fields = text.split(delimiter).collect::<Vec<_>>();
            for (i, value) in fields.iter().enumerate() {
                assert_eq!(run(text, delimiter, i as i64 + 1), Value::Text((*value).into()));
                assert_eq!(run(text, delimiter, -(i as i64) - 1),
                    Value::Text(fields[fields.len() - 1 - i].into()));
            }
            assert_eq!(run(text, delimiter, 1_000_000), Value::Text(String::new()));
            assert_eq!(run(text, delimiter, i64::MIN), Value::Text(String::new()));
        }
        assert_eq!(run("a|b", "", 1), Value::Text("a|b".into()));
    }
}

#[cfg(test)]
mod impossible_unicode_locate_tests {
    use super::*;

    #[test]
    fn unicode_needles_never_match_ascii_sources() {
        let haystack = Value::Text("plain-ASCII".repeat(30_000));
        for needle in ["é", "🙂", "aé", "́"] {
            assert_eq!(locate(&[Value::Text(needle.into()), haystack.clone()]).unwrap(),
                Value::Integer(0));
            assert_eq!(instr(&[haystack.clone(), Value::Text(needle.into())]).unwrap(),
                Value::Integer(0));
            assert_eq!(locate(&[
                Value::Text(needle.into()), haystack.clone(), Value::Integer(50_000),
            ]).unwrap(), Value::Integer(0));
        }
        assert_eq!(locate(&[
            Value::Text("ASCII".into()), Value::Text("plain-ASCII".into()),
        ]).unwrap(), Value::Integer(7));
        assert!(locate(&[
            Value::Text("é".into()), haystack, Value::Integer(0),
        ]).is_err());
    }
}

#[cfg(test)]
mod overlapping_delimiter_split_part_tests {
    use super::*;

    #[test]
    fn negative_positions_use_the_same_left_to_right_fields() {
        for (text, delimiter, expected) in [
            ("ababa", "aba", "ba"),
            ("aaaaa", "aaa", "aa"),
            ("ééé", "éé", "é"),
        ] {
            let args = |position| [
                Value::Text(text.into()), Value::Text(delimiter.into()),
                Value::Integer(position),
            ];
            assert_eq!(split_part(&args(2)).unwrap(), Value::Text(expected.into()));
            assert_eq!(split_part(&args(-1)).unwrap(), Value::Text(expected.into()));
            assert_eq!(split_part(&args(-2)).unwrap(), Value::Text(String::new()));
            assert_eq!(split_part(&args(-3)).unwrap(), Value::Text(String::new()));
        }
    }
}

#[cfg(test)]
mod single_scan_negative_split_part_regressions {
    use super::*;

    fn result(text: &str, delimiter: &str, index: i64) -> Value {
        split_part(&[
            Value::Text(text.into()), Value::Text(delimiter.into()), Value::Integer(index),
        ]).unwrap()
    }

    #[test]
    fn single_unicode_delimiters_match_forward_split_for_all_positions() {
        for text in ["", "a,b,c,", ",a,,b,", "é🙂東京🙂🙂x", "no match"] {
            for delimiter in [",", "🙂", "é"] {
                let pieces = text.split(delimiter).collect::<Vec<_>>();
                for from_end in 2..=pieces.len() + 2 {
                    let expected = pieces.len().checked_sub(from_end)
                        .and_then(|index| pieces.get(index)).copied().unwrap_or("");
                    assert_eq!(result(text, delimiter, -(from_end as i64)),
                        Value::Text(expected.into()),
                        "text={text:?}, delimiter={delimiter:?}, from_end={from_end}");
                }
            }
        }
    }

    #[test]
    fn overlapping_delimiters_use_forward_nonoverlapping_fields() {
        for text in ["aaaaa", "aaaa", "abababab", "ababa", "éééé"] {
            for delimiter in ["aa", "aba", "éé"] {
                let pieces = text.split(delimiter).collect::<Vec<_>>();
                for from_end in 2..=pieces.len() + 2 {
                    let expected = pieces.len().checked_sub(from_end)
                        .and_then(|index| pieces.get(index)).copied().unwrap_or("");
                    assert_eq!(result(text, delimiter, -(from_end as i64)),
                        Value::Text(expected.into()));
                }
            }
        }
    }

    #[test]
    fn large_negative_indices_preserve_cheap_out_of_range_behavior() {
        let large = "a,".repeat(30_000);
        assert_eq!(result(&large, ",", -500),
            Value::Text("a".into()));
        assert_eq!(result("a,b", ",", i64::MIN), Value::Text(String::new()));
        assert_eq!(result("abc", "", -1), Value::Text("abc".into()));
    }
}

#[cfg(test)]
mod split_part_result_limit_regressions {
    use super::*;
    #[test]
    fn selects_small_fields_from_large_text_without_rejecting_input() {
        let source = format!("first,{}", "x".repeat(16 * 1024 * 1024 + 1));
        let args = |field| [Value::Text(source.clone()),
            Value::Text(",".into()), Value::Integer(field)];
        assert_eq!(split_part(&args(1)).unwrap(), Value::Text("first".into()));
        assert!(matches!(split_part(&args(2)), Err(Error::Execution(_))));
        assert!(matches!(split_part(&args(-1)), Err(Error::Execution(_))));
        assert_eq!(split_part(&args(3)).unwrap(), Value::Text(String::new()));
        assert_eq!(split_part(&[Value::Null, Value::Text(",".into()),
            Value::Integer(1)]).unwrap(), Value::Null);
    }
}
