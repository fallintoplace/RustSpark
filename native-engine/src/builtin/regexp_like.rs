use super::{Function, regex_support};
use crate::{scalar::Value, schema::DataType, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Boolean)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(pattern) = super::string(&args[1])? else { return Ok(Value::Null); };
    regex_support::check_input(text)?;
    if pattern.is_empty() { return Ok(Value::Boolean(true)); }
    if pattern == ".*" { return Ok(Value::Boolean(true)); }
    if pattern == "." {
        // A non-newline UTF-8 byte implies a non-newline Unicode scalar.
        return Ok(Value::Boolean(text.bytes().any(|byte| byte != b'\n')));
    }
    // A leading ^ followed only by literal word characters is a prefix
    // test. Patterns with $, escapes, groups or modifiers use the regex
    // compiler so their full semantics remain intact.
    if let Some(prefix) = pattern.strip_prefix('^') {
        if prefix.len() <= 4095 && prefix.chars().all(|ch|
            ch.is_alphanumeric() || ch == '_') {
            return Ok(Value::Boolean(text.starts_with(prefix)));
        }
    }
    // Plain ASCII identifiers have no regex metacharacters.
    if pattern.len() <= 4096 && pattern.chars().all(|ch|
        ch.is_alphanumeric() || ch == '_') {
        return Ok(Value::Boolean(text.contains(pattern)));
    }
    Ok(Value::Boolean(regex_support::compile(pattern)?.is_match(text)))
}


fn regexp_escape_type(types: &[DataType]) -> Result<DataType> { super::text_type(types) }
fn regexp_escape(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    if text.len() > 4096 {
        return Err(crate::Error::Execution("REGEXP_ESCAPE input exceeds pattern budget".into()));
    }
    let escaped = regex::escape(text);
    if escaped.len() > 4096 {
        return Err(crate::Error::Execution("REGEXP_ESCAPE output exceeds pattern budget".into()));
    }
    Ok(Value::Text(escaped))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "regexp_escape", min_args: 1, max_args: 1, result_type: regexp_escape_type, call: regexp_escape },Function {
    name: "regexp_like", min_args: 2, max_args: 2,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, schema::{Field, Schema}, sql};
    #[test]
    fn matches_unicode_strings_through_native_sql() {
        let mut catalog = Catalog::new();
        let schema = Schema::new(vec![Field::new("name", DataType::Text, true)]).unwrap();
        catalog.register("t", Batch::from_rows(schema, vec![
            vec![Value::Text("Berlin".into())], vec![Value::Text("Łódź".into())],
        ]).unwrap()).unwrap();
        let rows = executor::execute(&sql::parse_sql(
            "SELECT REGEXP_LIKE(name, '^B.*') AS match_name FROM t"
        ).unwrap(), &catalog).unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Boolean(true)], vec![Value::Boolean(false)]]);
    }
}

#[cfg(test)]
mod regexp_escape_extension_tests {
    use super::*;
    #[test]
    fn preserves_unicode_and_null_semantics() {
        assert!(crate::builtin::supports("regexp_escape"));
        let escaped = regexp_escape(&[Value::Text("a+b?".into())]).unwrap();
        let Value::Text(pattern) = escaped else { panic!("expected text"); };
        assert!(regex_support::compile(&pattern).unwrap().is_match("a+b?"));
        assert!(!regex_support::compile(&pattern).unwrap().is_match("abbb"));
        assert_eq!(regexp_escape(&[Value::Null]).unwrap(), Value::Null);
        assert!(regexp_escape(&[Value::Text("(".repeat(4097))]).is_err());
    }
}

#[cfg(test)]
mod empty_regex_like_perf_tests {
    use super::*;
    #[test]
    fn empty_pattern_matches_every_nonnull_input() {
        for text in ["", "hello", "é🙂"] {
            assert_eq!(evaluate(&[Value::Text(text.into()), Value::Text("".into())])
                .unwrap(), Value::Boolean(true));
        }
        assert_eq!(evaluate(&[Value::Null, Value::Text("".into())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod literal_regex_like_tests {
    use super::*;

    #[test]
    fn literal_fast_path_matches_regex_on_unicode_and_missing_values() {
        for text in ["foo_bar", "éfoo_bar", "bar", ""] {
            for pattern in ["foo", "_", "FOO", ""] {
                let values = [Value::Text(text.into()), Value::Text(pattern.into())];
                assert_eq!(evaluate(&values).unwrap(),
                    Value::Boolean(regex_support::compile(pattern).unwrap().is_match(text)));
            }
        }
    }
}

#[cfg(test)]
mod dot_pattern_like_tests {
    use super::*;

    #[test]
    fn single_dot_matches_non_newline_scalars_like_regex() {
        for text in ["", "\n", "\n\n", "🙂", "\né", "\r\n", "a\nb"] {
            let expected = regex_support::compile(".").unwrap().is_match(text);
            assert_eq!(
                evaluate(&[Value::Text(text.into()), Value::Text(".".into())]).unwrap(),
                Value::Boolean(expected)
            );
        }
        assert_eq!(evaluate(&[Value::Null, Value::Text(".".into())]).unwrap(),
            Value::Null);
    }
}

#[cfg(test)]
mod anchored_literal_regex_fast_path_tests {
    use super::*;

    #[test]
    fn anchored_prefixes_agree_with_regex_engine() {
        for text in ["", "foo", "foobar", "xfoo", "\nfoo", "foo\nbar", "éfoo", "foo🙂"] {
            for pattern in ["^", "^foo", "^foo_bar", "^FOO", "^1", "^f.o", "^foo$", "(?m)^foo"] {
                let actual = evaluate(&[Value::Text(text.into()), Value::Text(pattern.into())])
                    .unwrap();
                let expected = Value::Boolean(regex_support::compile(pattern)
                    .unwrap().is_match(text));
                assert_eq!(actual, expected, "pattern={pattern:?} text={text:?}");
            }
        }
    }
}

#[cfg(test)]
mod dot_star_regexp_like_regressions {
    use super::*;

    #[test]
    fn always_matches_a_bounded_nonnull_input_like_the_regex_engine() {
        let regex = regex_support::compile(".*").unwrap();
        for text in ["", "\n", "\n\n", "é🙂", "a\nb", "東京", "\r\n"] {
            assert_eq!(evaluate(&[
                Value::Text(text.into()), Value::Text(".*".into()),
            ]).unwrap(), Value::Boolean(regex.is_match(text)));
        }
        assert_eq!(evaluate(&[Value::Null, Value::Text(".*".into())]).unwrap(),
            Value::Null);
        assert!(evaluate(&[
            Value::Text("x".repeat(16 * 1024 * 1024 + 1)),
            Value::Text(".*".into()),
        ]).is_err());
    }
}


#[cfg(test)]
mod unicode_literal_predicate_tests {
    use super::*;

    #[test]
    fn unicode_literals_and_anchors_agree_with_regex_engine() {
        for text in ["", "café", "東京東京", "αβγ", "é🙂東京", "FOOé", "\n東京"] {
            for pattern in ["é", "東京", "αβ", "café", "^é", "^東京", "^αβ", "é+", "é$"] {
                let expected = regex_support::compile(pattern).unwrap().is_match(text);
                assert_eq!(evaluate(&[
                    Value::Text(text.into()), Value::Text(pattern.into()),
                ]).unwrap(), Value::Boolean(expected),
                    "text={text:?}, pattern={pattern:?}");
            }
        }
        assert_eq!(evaluate(&[Value::Null, Value::Text("é".into())]).unwrap(),
            Value::Null);
    }
}
)) {
        if exact.len() <= 4094 && exact.chars().all(|ch| ch.is_alphanumeric() || ch == '_') {
            return Ok(Value::Boolean(text == exact));
        }
    }
    // Anchored identifier literals only need a prefix check.
    if let Some(prefix) = pattern.strip_prefix('^') {
        if prefix.len() <= 4095 && prefix.chars().all(|ch|
            ch.is_alphanumeric() || ch == '_') {
            return Ok(Value::Boolean(text.starts_with(prefix)));
        }
    }
    // Plain ASCII identifiers have no regex metacharacters.
    if pattern.len() <= 4096 && pattern.chars().all(|ch|
        ch.is_alphanumeric() || ch == '_') {
        return Ok(Value::Boolean(text.contains(pattern)));
    }
    Ok(Value::Boolean(regex_support::compile(pattern)?.is_match(text)))
}


fn regexp_escape_type(types: &[DataType]) -> Result<DataType> { super::text_type(types) }
fn regexp_escape(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    if text.len() > 4096 {
        return Err(crate::Error::Execution("REGEXP_ESCAPE input exceeds pattern budget".into()));
    }
    let escaped = regex::escape(text);
    if escaped.len() > 4096 {
        return Err(crate::Error::Execution("REGEXP_ESCAPE output exceeds pattern budget".into()));
    }
    Ok(Value::Text(escaped))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "regexp_escape", min_args: 1, max_args: 1, result_type: regexp_escape_type, call: regexp_escape },Function {
    name: "regexp_like", min_args: 2, max_args: 2,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, schema::{Field, Schema}, sql};
    #[test]
    fn matches_unicode_strings_through_native_sql() {
        let mut catalog = Catalog::new();
        let schema = Schema::new(vec![Field::new("name", DataType::Text, true)]).unwrap();
        catalog.register("t", Batch::from_rows(schema, vec![
            vec![Value::Text("Berlin".into())], vec![Value::Text("Łódź".into())],
        ]).unwrap()).unwrap();
        let rows = executor::execute(&sql::parse_sql(
            "SELECT REGEXP_LIKE(name, '^B.*') AS match_name FROM t"
        ).unwrap(), &catalog).unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Boolean(true)], vec![Value::Boolean(false)]]);
    }
}

#[cfg(test)]
mod regexp_escape_extension_tests {
    use super::*;
    #[test]
    fn preserves_unicode_and_null_semantics() {
        assert!(crate::builtin::supports("regexp_escape"));
        let escaped = regexp_escape(&[Value::Text("a+b?".into())]).unwrap();
        let Value::Text(pattern) = escaped else { panic!("expected text"); };
        assert!(regex_support::compile(&pattern).unwrap().is_match("a+b?"));
        assert!(!regex_support::compile(&pattern).unwrap().is_match("abbb"));
        assert_eq!(regexp_escape(&[Value::Null]).unwrap(), Value::Null);
        assert!(regexp_escape(&[Value::Text("(".repeat(4097))]).is_err());
    }
}

#[cfg(test)]
mod empty_regex_like_perf_tests {
    use super::*;
    #[test]
    fn empty_pattern_matches_every_nonnull_input() {
        for text in ["", "hello", "é🙂"] {
            assert_eq!(evaluate(&[Value::Text(text.into()), Value::Text("".into())])
                .unwrap(), Value::Boolean(true));
        }
        assert_eq!(evaluate(&[Value::Null, Value::Text("".into())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod literal_regex_like_tests {
    use super::*;

    #[test]
    fn literal_fast_path_matches_regex_on_unicode_and_missing_values() {
        for text in ["foo_bar", "éfoo_bar", "bar", ""] {
            for pattern in ["foo", "_", "FOO", ""] {
                let values = [Value::Text(text.into()), Value::Text(pattern.into())];
                assert_eq!(evaluate(&values).unwrap(),
                    Value::Boolean(regex_support::compile(pattern).unwrap().is_match(text)));
            }
        }
    }
}

#[cfg(test)]
mod dot_pattern_like_tests {
    use super::*;

    #[test]
    fn single_dot_matches_non_newline_scalars_like_regex() {
        for text in ["", "\n", "\n\n", "🙂", "\né", "\r\n", "a\nb"] {
            let expected = regex_support::compile(".").unwrap().is_match(text);
            assert_eq!(
                evaluate(&[Value::Text(text.into()), Value::Text(".".into())]).unwrap(),
                Value::Boolean(expected)
            );
        }
        assert_eq!(evaluate(&[Value::Null, Value::Text(".".into())]).unwrap(),
            Value::Null);
    }
}

#[cfg(test)]
mod anchored_literal_regex_fast_path_tests {
    use super::*;

    #[test]
    fn anchored_prefixes_agree_with_regex_engine() {
        for text in ["", "foo", "foobar", "xfoo", "\nfoo", "foo\nbar", "éfoo", "foo🙂"] {
            for pattern in ["^", "^foo", "^foo_bar", "^FOO", "^1", "^f.o", "^foo$", "(?m)^foo"] {
                let actual = evaluate(&[Value::Text(text.into()), Value::Text(pattern.into())])
                    .unwrap();
                let expected = Value::Boolean(regex_support::compile(pattern)
                    .unwrap().is_match(text));
                assert_eq!(actual, expected, "pattern={pattern:?} text={text:?}");
            }
        }
    }
}

#[cfg(test)]
mod dot_star_regexp_like_regressions {
    use super::*;

    #[test]
    fn always_matches_a_bounded_nonnull_input_like_the_regex_engine() {
        let regex = regex_support::compile(".*").unwrap();
        for text in ["", "\n", "\n\n", "é🙂", "a\nb", "東京", "\r\n"] {
            assert_eq!(evaluate(&[
                Value::Text(text.into()), Value::Text(".*".into()),
            ]).unwrap(), Value::Boolean(regex.is_match(text)));
        }
        assert_eq!(evaluate(&[Value::Null, Value::Text(".*".into())]).unwrap(),
            Value::Null);
        assert!(evaluate(&[
            Value::Text("x".repeat(16 * 1024 * 1024 + 1)),
            Value::Text(".*".into()),
        ]).is_err());
    }
}


#[cfg(test)]
mod unicode_literal_predicate_tests {
    use super::*;

    #[test]
    fn unicode_literals_and_anchors_agree_with_regex_engine() {
        for text in ["", "café", "東京東京", "αβγ", "é🙂東京", "FOOé", "\n東京"] {
            for pattern in ["é", "東京", "αβ", "café", "^é", "^東京", "^αβ", "é+", "é$"] {
                let expected = regex_support::compile(pattern).unwrap().is_match(text);
                assert_eq!(evaluate(&[
                    Value::Text(text.into()), Value::Text(pattern.into()),
                ]).unwrap(), Value::Boolean(expected),
                    "text={text:?}, pattern={pattern:?}");
            }
        }
        assert_eq!(evaluate(&[Value::Null, Value::Text("é".into())]).unwrap(),
            Value::Null);
    }
}

#[cfg(test)]
mod exact_anchored_literal_regressions {
    use super::*;
    #[test]
    fn exact_identifier_matches_regex_for_unicode_and_line_endings() {
        for text in ["", "foo", "foobar", "foo\n", "\nfoo", "é_9", "é_9\n", "東京", "a\n東京"] {
            for pattern in ["^$", "^foo$", "^é_9$", "^東京$", "^Foo$", "^foo.$", "(?m)^foo$"] {
                let actual = evaluate(&[Value::Text(text.into()), Value::Text(pattern.into())]).unwrap();
                let expected = Value::Boolean(regex_support::compile(pattern).unwrap().is_match(text));
                assert_eq!(actual, expected, "text={text:?} pattern={pattern:?}");
            }
        }
        assert_eq!(evaluate(&[Value::Null, Value::Text("^foo$".into())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod recovered_regex_prefix_parser_regressions {
    use super::*;

    #[test]
    fn anchored_prefix_and_exact_patterns_agree_with_the_regex_engine() {
        for text in [
            "", "foo", "foobar", "foo\n", "xfoo", "café", "café🙂",
            "\nfoo", "FOO", "abc_123", "abc_123\n",
        ] {
            for pattern in [
                "^", "^foo", "^foo$", "^foo.*", "foo$", "^café",
                "^abc_123", "^abc_123$", "(?m)^foo", "^foo\\z",
            ] {
                let actual = evaluate(&[
                    Value::Text(text.into()), Value::Text(pattern.into()),
                ]).unwrap();
                let reference = Value::Boolean(regex_support::compile(pattern).unwrap()
                    .is_match(text));
                assert_eq!(actual, reference, "pattern={pattern:?}, text={text:?}");
            }
        }
        assert_eq!(evaluate(&[Value::Null, Value::Text("^foo".into())]).unwrap(),
            Value::Null);
        assert!(evaluate(&[Value::Text("hello".into()), Value::Text("(".into())]).is_err());
    }
}
