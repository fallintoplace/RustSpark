use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

const MAX_OUTPUT: usize = 16 * 1024 * 1024;

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)
}

fn replacement(args: &[Value], index: usize, default: Option<char>) -> Result<Option<char>> {
    let Some(value) = args.get(index) else { return Ok(default); };
    let Some(text) = super::string(value)? else { return Ok(None); };
    let mut characters = text.chars();
    let character = characters.next().ok_or_else(||
        Error::Type("MASK replacement must be a single character".into()))?;
    if characters.next().is_some() {
        return Err(Error::Type("MASK replacement must be a single character".into()));
    }
    Ok(Some(character))
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(input) = super::string(&args[0])? else { return Ok(Value::Null); };
    if input.len() > MAX_OUTPUT {
        return Err(Error::Execution("MASK input exceeds 16 MiB budget".into()));
    }
    let uppercase = replacement(args, 1, Some('X'))?;
    let lowercase = replacement(args, 2, Some('x'))?;
    let digits = replacement(args, 3, Some('n'))?;
    let others = replacement(args, 4, None)?;
    // ASCII replacements preserve the exact output length. Rewrite one
    // buffer without checking UTF-8 growth on every character.
    if input.is_ascii() && [uppercase, lowercase, digits, others].into_iter()
        .flatten().all(|replacement| replacement.is_ascii()) {
        let mut output = input.as_bytes().to_vec();
        for byte in &mut output {
            let original = char::from(*byte);
            let selected = match *byte {
                b'A'..=b'Z' => uppercase.unwrap_or(original),
                b'a'..=b'z' => lowercase.unwrap_or(original),
                b'0'..=b'9' => digits.unwrap_or(original),
                _ => others.unwrap_or(original),
            };
            *byte = selected as u8;
        }
        return Ok(Value::Text(String::from_utf8(output)
            .expect("ASCII replacements preserve UTF-8")));
    }
    let mut result = String::with_capacity(input.len());
    if input.is_ascii() {
        for &byte in input.as_bytes() {
            let original = char::from(byte);
            let selected = match byte {
                b'A'..=b'Z' => uppercase.unwrap_or(original),
                b'a'..=b'z' => lowercase.unwrap_or(original),
                b'0'..=b'9' => digits.unwrap_or(original),
                _ => others.unwrap_or(original),
            };
            if selected.len_utf8() > MAX_OUTPUT.saturating_sub(result.len()) {
                return Err(Error::Execution("MASK output exceeds 16 MiB budget".into()));
            }
            result.push(selected);
        }
        return Ok(Value::Text(result));
    }
    for character in input.chars() {
        let substitution = if character.is_uppercase() {
            uppercase
        } else if character.is_lowercase() {
            lowercase
        } else if character.is_numeric() {
            digits
        } else {
            others
        };
        let selected = substitution.unwrap_or(character);
        if selected.len_utf8() > MAX_OUTPUT.saturating_sub(result.len()) {
            return Err(Error::Execution("MASK output exceeds 16 MiB budget".into()));
        }
        result.push(selected);
    }
    Ok(Value::Text(result))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "mask", min_args: 1, max_args: 5, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, schema::{Field, Schema}, sql};

    fn text(input: &str) -> Value { Value::Text(input.into()) }

    #[test]
    fn masks_spark_character_classes_and_custom_replacements() {
        assert_eq!(evaluate(&[text("abcd-EFGH-8765-4321")]).unwrap(),
            text("xxxx-XXXX-nnnn-nnnn"));
        assert_eq!(evaluate(&[text("AbCD123-@$#"), text("Q"), text("q"), text("d"), text("o")]).unwrap(),
            text("QqQQdddoooo"));
        assert_eq!(evaluate(&[text("AbCD123-@$#"), Value::Null, text("q"), text("d"), text("o")]).unwrap(),
            text("AqCDdddoooo"));
        assert_eq!(evaluate(&[text("AbCD123-@$#"), Value::Null, Value::Null,
            Value::Null, Value::Null]).unwrap(), text("AbCD123-@$#"));
        assert_eq!(evaluate(&[text("Éé٣🙂")]).unwrap(), text("Xxn🙂"));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(evaluate(&[text("a"), text("")]).is_err());
        assert!(evaluate(&[text("a"), text("XX")]).is_err());
        assert!(output(&[DataType::Integer]).is_err());
    }

    #[test]
    fn sql_can_mask_nullable_columns_without_touching_other_characters() {
        let schema = Schema::new(vec![Field::new("value", DataType::Text, true)]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::from_rows(schema, vec![
            vec![text("Aa1!")], vec![Value::Null],
        ]).unwrap()).unwrap();
        let plan = sql::parse_sql(
            "SELECT MASK(value, NULL, 'q', 'd', 'o') AS masked FROM t"
        ).unwrap();
        assert_eq!(executor::execute(&plan, &catalog).unwrap().rows().unwrap(),
            vec![vec![text("Aqdo")], vec![Value::Null]]);
    }

    #[test]
    fn output_budget_applies_to_utf8_growth_from_replacement() {
        let huge = text(&"a".repeat(MAX_OUTPUT / 4 + 1));
        assert!(evaluate(&[huge, text("🙂")]).is_ok());
        let huge = text(&"A".repeat(MAX_OUTPUT / 4 + 1));
        assert!(evaluate(&[huge, text("🙂")]).is_err());
    }
}

#[cfg(test)]
mod ascii_mask_classifier_regressions {
    use super::*;

    #[test]
    fn classifies_ascii_and_handles_unicode_replacement_growth() {
        let input = Value::Text("Aa1-+? ".repeat(3000));
        assert_eq!(evaluate(&[input]).unwrap(),
            Value::Text("Xxn-+? ".repeat(3000)));
        assert_eq!(evaluate(&[
            Value::Text("Aa1!".into()), Value::Text("🙂".into()),
            Value::Text("é".into()), Value::Null, Value::Text("界".into()),
        ]).unwrap(), Value::Text("🙂é1界".into()));
        assert!(matches!(evaluate(&[
            Value::Text("A".repeat(MAX_OUTPUT / 4 + 1)),
            Value::Text("🙂".into()),
        ]), Err(Error::Execution(_))));
        assert_eq!(evaluate(&[Value::Text("é🙂".into())]).unwrap(),
            Value::Text("x🙂".into()));
    }
}

#[cfg(test)]
mod ascii_in_place_mask_regression_tests {
    use super::*;

    #[test]
    fn ascii_replacements_preserve_size_and_classification() {
        let source = "Aa9-*? ".repeat(2000);
        let expected = "Xxn-*? ".repeat(2000);
        assert_eq!(evaluate(&[Value::Text(source)]).unwrap(), Value::Text(expected));
        assert_eq!(evaluate(&[
            Value::Text("Az9-_".into()),
            Value::Text("Q".into()), Value::Text("k".into()),
            Value::Text("7".into()), Value::Text("?".into()),
        ]).unwrap(), Value::Text("Qk7??".into()));
        assert_eq!(evaluate(&[
            Value::Text("Az9".into()), Value::Null, Value::Null, Value::Null,
        ]).unwrap(), Value::Text("Az9".into()));
    }

    #[test]
    fn unicode_replacements_use_the_bounded_fallback() {
        assert_eq!(evaluate(&[
            Value::Text("Aa1!".into()),
            Value::Text("🙂".into()), Value::Text("é".into()),
            Value::Text("界".into()), Value::Null,
        ]).unwrap(), Value::Text("🙂é界!".into()));
    }
}
