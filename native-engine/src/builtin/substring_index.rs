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
    // Matches do not overlap, so each one consumes at least delimiter.len() bytes.
    // Skip scanning if the requested occurrence cannot exist.
    let max_matches = text.len() / delimiter.len();
    if count.unsigned_abs() > u64::try_from(max_matches).unwrap_or(u64::MAX) {
        return Ok(Value::Text(text.to_owned()));
    }
    if count == 1 {
        return Ok(Value::Text(text.split_once(delimiter).map_or(text, |(head, _)| head).to_owned()));
    }
    if count == -1 {
        // Reversing a non-overlapping search changes the matches for
        // self-overlapping delimiters (e.g. "aaaaa" split on "aa").
        // SUBSTRING_INDEX must agree with the forward split fields.
        let tail = text.match_indices(delimiter).last()
            .map_or(text, |(position, _)| &text[position + delimiter.len()..]);
        return Ok(Value::Text(tail.to_owned()));
    }
    if (-16..=-2).contains(&count) {
        // Track only the requested trailing delimiter positions. Forward
        // match_indices preserves the nonoverlapping split contract even
        // when the delimiter can overlap with itself.
        let from_end = (-count) as usize;
        let mut recent = [0_usize; 16];
        let mut seen = 0usize;
        for (position, _) in text.match_indices(delimiter) {
            recent[seen % from_end] = position;
            seen += 1;
        }
        let suffix = if seen < from_end {
            text
        } else {
            &text[recent[seen % from_end] + delimiter.len()..]
        };
        return Ok(Value::Text(suffix.to_owned()));
    }
    let index = usize::try_from(count.unsigned_abs() - 1).ok();
    let result = match index {
        None => text,
        Some(index) if count > 0 =>
            text.match_indices(delimiter).nth(index).map_or(text, |(position, _)| &text[..position]),
        Some(index) => {
            // Count forward non-overlapping matches, then locate the selected
            // one with a second forward scan. This is O(n), O(1) memory and
            // preserves SQL split semantics even with overlapping patterns.
            let matches = text.match_indices(delimiter).count();
            if index >= matches { text } else {
                let selected = matches - index - 1;
                text.match_indices(delimiter).nth(selected)
                    .map_or(text, |(position, _)| &text[position + delimiter.len()..])
            }
        },
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
        super::char_length::character_count(input)
    } else if delimiter.len() == 1 && delimiter.is_ascii() {
        let needle = delimiter.as_bytes()[0];
        input.as_bytes().iter().filter(|&&byte| byte == needle)
            .count().saturating_add(1)
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
mod huge_substring_index_count_tests {
    use super::*;
    #[test]
    fn impossible_counts_return_unmodified_text() {
        for count in [i64::MAX, i64::MIN] {
            assert_eq!(evaluate(&[Value::Text("a🙂b🙂c".into()), Value::Text("🙂".into()), Value::Integer(count)]).unwrap(), Value::Text("a🙂b🙂c".into()));
        }
        assert_eq!(evaluate(&[Value::Text("a..b".into()), Value::Text(".".into()), Value::Integer(2)]).unwrap(), Value::Text("a.".into()));
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

#[cfg(test)]
mod single_byte_split_count_fast_path_tests {
    use super::*;
    #[test]
    fn counts_ascii_separator_bytes_in_unicode_text() {
        let f = |text: &str, separator: &str| split_count(&[
            Value::Text(text.into()), Value::Text(separator.into()),
        ]).unwrap();
        assert_eq!(f("a,b,c,", ","), Value::Integer(4));
        assert_eq!(f("é,🙂,東京", ","), Value::Integer(3));
        assert_eq!(f("é🙂é", "é"), Value::Integer(3));
        assert_eq!(f("hello", ","), Value::Integer(1));
    }
}

#[cfg(test)]
mod impossible_multibyte_delimiter_tests {
    use super::*;

    #[test]
    fn skips_counts_above_the_nonoverlapping_occurrence_bound() {
        let delimiter = "é".repeat(10_000);
        let source = format!("{delimiter}x{delimiter}");
        for count in [3, -3, i64::MIN, i64::MAX] {
            assert_eq!(
                evaluate(&[
                    Value::Text(source.clone()),
                    Value::Text(delimiter.clone()),
                    Value::Integer(count),
                ]).unwrap(),
                Value::Text(source.clone())
            );
        }
        assert_eq!(
            evaluate(&[
                Value::Text(source.clone()),
                Value::Text(delimiter.clone()),
                Value::Integer(2),
            ]).unwrap(),
            Value::Text(format!("{delimiter}x"))
        );
        assert_eq!(
            evaluate(&[
                Value::Text(source),
                Value::Text(delimiter),
                Value::Integer(-1),
            ]).unwrap(),
            Value::Text(String::new())
        );
    }
}

#[cfg(test)]
mod overlapping_substring_index_regressions {
    use super::*;

    fn run(input: &str, delimiter: &str, count: i64) -> String {
        let Value::Text(result) = evaluate(&[
            Value::Text(input.into()), Value::Text(delimiter.into()), Value::Integer(count),
        ]).unwrap() else { panic!("expected TEXT") };
        result
    }

    #[test]
    fn negative_indices_follow_left_to_right_nonoverlapping_matches() {
        for (input, delimiter) in [
            ("aaaaa", "aa"), ("abababa", "aba"), ("aaaaaa", "aaa"),
            ("ééééé", "éé"), ("abcabcab", "abcab"), ("banana", "ana"),
            ("a,a,a", ","), ("no separators", "xx"),
        ] {
            let fields = input.split(delimiter).collect::<Vec<_>>();
            for count in -8_i64..=8 {
                let expected = if count == 0 { String::new() }
                    else if count > 0 {
                        fields.iter().take(count as usize).copied().collect::<Vec<_>>()
                            .join(delimiter)
                    } else {
                        let requested = count.unsigned_abs() as usize;
                        fields.iter().skip(fields.len().saturating_sub(requested))
                            .copied().collect::<Vec<_>>().join(delimiter)
                    };
                assert_eq!(run(input, delimiter, count), expected,
                    "input={input:?}, delimiter={delimiter:?}, count={count}");
            }
        }
        assert_eq!(run("aaaaa", "aa", -1), "a");
        assert_eq!(run("aaaaa", "aa", -2), "aaa");
        assert_eq!(run("aaaaa", "aa", -3), "aaaaa");
    }
}


#[cfg(test)]
mod bounded_negative_index_ring_tests {
    use super::*;

    #[test]
    fn negative_offsets_match_forward_split_for_unicode_and_overlaps() {
        for (text, delimiter) in [
            ("", "aa"), ("aaaaa", "aa"), ("ababababa", "aba"),
            ("ééééé", "éé"), ("a,b,c,d,e,f,g", ","),
            ("東京🙂東京🙂end", "🙂"), ("none", "::"),
        ] {
            let fields = text.split(delimiter).collect::<Vec<_>>();
            for count in -20_i64..=-1 {
                let wanted = count.unsigned_abs() as usize;
                let expected = if wanted > fields.len() { text.to_string() }
                    else { fields[fields.len() - wanted..].join(delimiter) };
                assert_eq!(evaluate(&[
                    Value::Text(text.into()), Value::Text(delimiter.into()),
                    Value::Integer(count),
                ]).unwrap(), Value::Text(expected),
                    "text={text:?}, delimiter={delimiter:?}, count={count}");
            }
        }
    }

    #[test]
    fn large_nonoverlapping_stream_keeps_requested_suffix() {
        let value = "a|".repeat(100_000);
        for wanted in [2, 3, 8, 16, 17] {
            let expected = value.split('|')
                .collect::<Vec<_>>();
            let suffix = expected[expected.len() - wanted..].join("|");
            assert_eq!(evaluate(&[
                Value::Text(value.clone()), Value::Text("|".into()),
                Value::Integer(-(wanted as i64)),
            ]).unwrap(), Value::Text(suffix));
        }
        assert_eq!(evaluate(&[
            Value::Text(value.clone()), Value::Text("|".into()),
            Value::Integer(i64::MIN),
        ]).unwrap(), Value::Text(value));
    }
}
