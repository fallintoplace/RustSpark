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
    let mut owned = String::new();
    owned.try_reserve_exact(value.len())
        .map_err(|_| Error::Execution("SPLIT_PART output allocation failed".into()))?;
    owned.push_str(value);
    Ok(Value::Text(owned))
}

fn trailing_split_field<'a>(
    text: &'a str, delimiter: &str, recent: &mut [Option<&'a str>],
) -> Option<&'a str> {
    let from_end = recent.len();
    debug_assert!(from_end > 0);
    let mut seen = 0usize;
    for field in text.split(delimiter) {
        recent[seen % from_end] = Some(field);
        seen += 1;
    }
    if seen >= from_end { recent[seen % from_end] } else { None }
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
            let mut recent: [Option<&str>; 16] = [None; 16];
            return bounded_split_field(trailing_split_field(
                text, delimiter, &mut recent[..from_end],
            ).unwrap_or(""));
        }
        if from_end <= 256 {
            let mut recent = Vec::new();
            recent.try_reserve_exact(from_end)
                .map_err(|_| Error::Execution("SPLIT_PART ring allocation failed".into()))?;
            recent.resize(from_end, None);
            return bounded_split_field(trailing_split_field(
                text, delimiter, &mut recent,
            ).unwrap_or(""));
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

fn instr_type(types: &[DataType]) -> Result<DataType> {
    if !(2..=4).contains(&types.len())
        || types[0] != DataType::Text
        || types[1] != DataType::Text
        || types[2..].iter().any(|kind| *kind != DataType::Integer)
    {
        return Err(Error::Type(
            "INSTR requires text, substring, optional integer start and occurrence".into()
        ));
    }
    Ok(DataType::Integer)
}

fn instr(args: &[Value]) -> Result<Value> {
    // Keep the original two-argument path unchanged.
    if args.len() == 2 {
        return locate_values(&args[1], &args[0], None);
    }
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(needle) = super::string(&args[1])? else { return Ok(Value::Null); };
    let Some(start) = super::integer(&args[2])? else { return Ok(Value::Null); };
    let mut occurrence = if let Some(arg) = args.get(3) {
        let Some(count) = super::integer(arg)? else { return Ok(Value::Null); };
        count
    } else {
        1
    };
    if occurrence <= 0 {
        return Err(Error::Execution("INSTR occurrence must be positive".into()));
    }
    // Spark treats the empty needle as matching position one, for either
    // search direction. It checks this before the zero-start sentinel.
    if needle.is_empty() { return Ok(Value::Integer(1)); }
    if start == 0 { return Ok(Value::Integer(0)); }

    let match_position = if start > 0 {
        let first = usize::try_from(start - 1).unwrap_or(usize::MAX);
        text.char_indices().enumerate().find_map(|(index, (byte, _))| {
            if index >= first && text[byte..].starts_with(needle) {
                occurrence -= 1;
                (occurrence == 0).then_some(index)
            } else {
                None
            }
        })
    } else {
        let from_end = usize::try_from(start.unsigned_abs()).unwrap_or(usize::MAX);
        let char_count = text.chars().count();
        // Step back by one Unicode scalar after every candidate, so
        // overlapping substrings count as separate occurrences.
        text.char_indices().rev().enumerate().find_map(|(reverse_index, (byte, _))| {
            if reverse_index + 1 >= from_end && text[byte..].starts_with(needle) {
                occurrence -= 1;
                (occurrence == 0).then_some(char_count - reverse_index - 1)
            } else {
                None
            }
        })
    };
    let Some(position) = match_position else { return Ok(Value::Integer(0)); };
    let one_based = position.checked_add(1).and_then(|n| i64::try_from(n).ok())
        .ok_or_else(|| Error::Execution("INSTR position overflow".into()))?;
    Ok(Value::Integer(one_based))
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
    Function { name: "instr", min_args: 2, max_args: 4, result_type: instr_type, call: instr },
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

#[cfg(test)]
mod medium_tail_split_part_ring_tests {
    use super::*;

    #[test]
    fn negative_offsets_match_forward_split_for_overlapping_and_unicode_delimiters() {
        for (source, delimiter) in [
            ("abaX".repeat(350), "aba"),
            ("🙂a".repeat(350), "🙂a"),
            ("aaaaa".repeat(180), "aa"),
        ] {
            let fields = source.split(delimiter).collect::<Vec<_>>();
            for from_end in [17_usize, 32, 64, 128, 256, 257] {
                let expected = fields.len().checked_sub(from_end)
                    .and_then(|index| fields.get(index)).copied().unwrap_or("");
                let result = split_part(&[
                    Value::Text(source.clone()),
                    Value::Text(delimiter.into()),
                    Value::Integer(-(from_end as i64)),
                ]).unwrap();
                assert_eq!(result, Value::Text(expected.into()),
                    "delimiter={delimiter:?} from_end={from_end}");
            }
        }
    }
}

#[cfg(test)]
mod spark_instr_occurrence_tests {
    use super::*;
    use crate::{catalog::Catalog, executor, sql};

    fn check(text: &str, needle: &str, start: i64, occurrence: i64) -> Result<Value> {
        instr(&[Value::Text(text.into()), Value::Text(needle.into()),
            Value::Integer(start), Value::Integer(occurrence)])
    }

    #[test]
    fn finds_overlapping_occurrences_in_both_directions() {
        assert_eq!(check("abcabc", "b", 1, 2).unwrap(), Value::Integer(5));
        assert_eq!(check("abcabc", "b", -1, 2).unwrap(), Value::Integer(2));
        assert_eq!(check("aaaa", "aa", 1, 3).unwrap(), Value::Integer(3));
        assert_eq!(check("aaaa", "aa", -1, 2).unwrap(), Value::Integer(2));
        assert_eq!(check("aaaa", "aa", -2, 3).unwrap(), Value::Integer(1));
        assert_eq!(check("aaaa", "aa", -1, 4).unwrap(), Value::Integer(0));
        assert_eq!(check("你好世界你好", "你好", -2, 2).unwrap(),
            Value::Integer(1));
        assert_eq!(check("abcde", "cd", -3, 1).unwrap(), Value::Integer(0));
        assert_eq!(check("abcde", "cd", -4, 1).unwrap(), Value::Integer(3));
    }

    #[test]
    fn validates_start_occurrence_and_null_arguments() {
        assert_eq!(check("abc", "b", 0, 1).unwrap(), Value::Integer(0));
        assert_eq!(check("abc", "", 0, 1).unwrap(), Value::Integer(1));
        assert_eq!(check("abc", "b", 99, 1).unwrap(), Value::Integer(0));
        assert_eq!(check("abc", "b", -99, 1).unwrap(), Value::Integer(0));
        assert!(check("abc", "b", 1, 0).is_err());
        assert!(check("abc", "b", 1, -1).is_err());
        assert!(instr_type(&[DataType::Text, DataType::Text, DataType::Integer,
            DataType::Integer]).is_ok());
        assert!(instr_type(&[DataType::Text, DataType::Text, DataType::Float]).is_err());
        assert_eq!(instr(&[Value::Null, Value::Text("b".into()),
            Value::Integer(1)]).unwrap(), Value::Null);
        assert_eq!(instr(&[Value::Text("abc".into()), Value::Text("b".into()),
            Value::Integer(1), Value::Null]).unwrap(), Value::Null);
    }

    #[test]
    fn executes_four_argument_calls_from_native_sql() {
        let plan = sql::parse_sql(
            "SELECT INSTR('abcabc', 'b', 1, 2), INSTR('aaaa', 'aa', -1, 2)"
        ).unwrap();
        let rows = executor::execute(&plan, &Catalog::new()).unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Integer(5), Value::Integer(2)]]);
    }
}

#[cfg(test)]
mod fallible_split_part_result_and_reverse_ring_tests {
    use super::*;

    fn field(text: &str, delimiter: &str, position: i64) -> Result<Value> {
        split_part(&[Value::Text(text.into()), Value::Text(delimiter.into()),
            Value::Integer(position)])
    }

    #[test]
    fn negative_positions_keep_forward_split_semantics_for_overlaps() {
        assert_eq!(field("aaaaa", "aa", -1).unwrap(), Value::Text("a".into()));
        assert_eq!(field("aaaaa", "aa", -2).unwrap(), Value::Text("".into()));
        assert_eq!(field("🙂::東京::é", "::", -2).unwrap(),
            Value::Text("東京".into()));
        assert_eq!(field("🙂::東京::é", "::", -3).unwrap(),
            Value::Text("🙂".into()));
    }

    #[test]
    fn ring_and_two_pass_fallback_match_the_forward_reference() {
        let input = (0..300).map(|index| format!("field{index}"))
            .collect::<Vec<_>>().join("::");
        for index in [2_i64, 16, 17, 255, 256, 257, 300] {
            let expected = input.split("::").nth(300 - index as usize).unwrap();
            assert_eq!(field(&input, "::", -index).unwrap(),
                Value::Text(expected.into()));
        }
    }

    #[test]
    fn copies_large_utf8_fields_and_checks_result_limit() {
        let large = "🙂é東京".repeat(32_768);
        let input = format!("{large}::tail");
        assert_eq!(field(&input, "::", 1).unwrap(), Value::Text(large));
        let oversize = "a".repeat(16 * 1024 * 1024 + 1);
        assert!(field(&oversize, ",", 1).is_err());
        assert_eq!(split_part(&[Value::Null, Value::Text(",".into()),
            Value::Integer(1)]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod shared_split_tail_buffer_tests {
    use super::*;

    #[test]
    fn negative_indices_preserve_forward_split_fields() {
        for (text, delimiter) in [
            ("a".repeat(610) + "x", "aa"),
            ("ab".repeat(310) + "x", "ab"),
        ] {
            let fields: Vec<_> = text.split(delimiter).collect();
            for from_end in [2_usize, 15, 16, 17, 255, 256, 257, 301] {
                let expected = fields.iter().rev().nth(from_end - 1)
                    .copied().unwrap_or("");
                let actual = split_part(&[
                    Value::Text(text.clone()), Value::Text(delimiter.into()),
                    Value::Integer(-(from_end as i64)),
                ]).unwrap();
                assert_eq!(actual, Value::Text(expected.into()),
                    "delimiter={delimiter:?} from_end={from_end}");
            }
        }
    }
}
