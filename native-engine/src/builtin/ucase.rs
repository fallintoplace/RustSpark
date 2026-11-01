use super::Function;
use crate::{scalar::Value, schema::DataType, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Text)
}

fn uppercase_bounded(text: &str, limit: usize) -> Result<String> {
    super::case_conversion::convert_bounded(
        text, limit, "UCASE", str::to_ascii_uppercase, char::to_uppercase,
    )
}
fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    Ok(Value::Text(uppercase_bounded(text, 16 * 1024 * 1024)?))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "ucase", min_args: 1, max_args: 1, result_type: output, call: evaluate },
    Function { name: "upper", min_args: 1, max_args: 1, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn supports_unicode_and_null() {
        assert_eq!(evaluate(&[Value::Text("café".into())]).unwrap(), Value::Text("CAFÉ".into()));
        assert_eq!(evaluate(&[Value::Text("hello world 123".into())]).unwrap(),
            Value::Text("HELLO WORLD 123".into()));
        assert_eq!(evaluate(&[Value::Text("straße".into())]).unwrap(),
            Value::Text("STRASSE".into()));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(output(&[DataType::Integer]).is_err());
    }
}

#[cfg(test)]
mod bounded_uppercase_tests {
    use super::*;
    #[test]
    fn case_expansion_keeps_unicode_and_limits() {
        assert_eq!(uppercase_bounded("straße", 16).unwrap(), "STRASSE");
        assert!(uppercase_bounded("straße", 5).is_err());
        assert_eq!(uppercase_bounded("abc", 3).unwrap(), "ABC");
        assert!(uppercase_bounded("abcd", 3).is_err());
    }
}

#[cfg(test)]
mod upper_alias_bound_tests {
    use super::*;
    use crate::{batch::Batch, expr::Expr, schema::Schema};

    #[test]
    fn upper_alias_reuses_bounded_case_expansion() {
        let batch = Batch::new_with_row_count(Schema::new(vec![]).unwrap(), vec![], 1).unwrap();
        assert_eq!(crate::functions::evaluate("UPPER", &[
            Expr::Literal(Value::Text("straße".into())),
        ], &batch, 0).unwrap(), Value::Text("STRASSE".into()));
        assert!(crate::functions::evaluate("UPPER", &[
            Expr::Literal(Value::Text("B".repeat(16 * 1024 * 1024 + 1))),
        ], &batch, 0).is_err());
    }
}
