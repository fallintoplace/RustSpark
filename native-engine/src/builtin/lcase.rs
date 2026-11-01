use super::Function;
use crate::{scalar::Value, schema::DataType, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Text)
}

fn lowercase_bounded(text: &str, limit: usize) -> Result<String> {
    super::case_conversion::convert_bounded(
        text, limit, "LCASE", str::to_ascii_lowercase, char::to_lowercase,
    )
}
fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    Ok(Value::Text(lowercase_bounded(text, 16 * 1024 * 1024)?))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "lcase", min_args: 1, max_args: 1, result_type: output, call: evaluate },
    Function { name: "lower", min_args: 1, max_args: 1, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn supports_unicode_and_null() {
        assert_eq!(evaluate(&[Value::Text("ÉCOLE".into())]).unwrap(), Value::Text("école".into()));
        assert_eq!(evaluate(&[Value::Text("HELLO WORLD 123".into())]).unwrap(),
            Value::Text("hello world 123".into()));
        assert_eq!(evaluate(&[Value::Text("İstanbul".into())]).unwrap(),
            Value::Text("i\u{307}stanbul".into()));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(output(&[DataType::Integer]).is_err());
    }
}

#[cfg(test)]
mod bounded_lowercase_tests {
    use super::*;
    #[test]
    fn unicode_expansions_cannot_exceed_output_budget() {
        assert_eq!(lowercase_bounded("İ", 3).unwrap(), "i\u{307}");
        assert!(lowercase_bounded("İ", 2).is_err());
        assert_eq!(lowercase_bounded("ABC", 3).unwrap(), "abc");
        assert!(lowercase_bounded("ABCD", 3).is_err());
    }
}

#[cfg(test)]
mod lower_alias_bound_tests {
    use super::*;
    use crate::{batch::Batch, expr::Expr, schema::Schema};

    #[test]
    fn lower_alias_uses_the_same_bounded_unicode_implementation() {
        let batch = Batch::new_with_row_count(Schema::new(vec![]).unwrap(), vec![], 1).unwrap();
        assert_eq!(crate::functions::evaluate("LOWER", &[
            Expr::Literal(Value::Text("İSTANBUL".into())),
        ], &batch, 0).unwrap(), Value::Text("i\u{307}stanbul".into()));
        assert!(crate::functions::evaluate("LOWER", &[
            Expr::Literal(Value::Text("A".repeat(16 * 1024 * 1024 + 1))),
        ], &batch, 0).is_err());
    }
}
