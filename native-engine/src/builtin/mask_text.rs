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
    let mut result = String::with_capacity(input.len());
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
