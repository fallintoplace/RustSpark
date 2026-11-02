//! SQL assertions for validation queries and error-propagation checks.
use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn assert_output(types: &[DataType]) -> Result<DataType> {
    // NULL literals use the native Text placeholder type.
    if !matches!(types[0], DataType::Boolean | DataType::Text)
        || (types.len() == 2 && types[1] != DataType::Text)
    {
        return Err(Error::Type(
            "ASSERT_TRUE requires boolean condition and optional text message".into()
        ));
    }
    Ok(DataType::Text)
}

fn raise_output(types: &[DataType]) -> Result<DataType> {
    if types[0] != DataType::Text {
        return Err(Error::Type("RAISE_ERROR requires a text message".into()));
    }
    Ok(DataType::Text)
}

fn bounded_message(value: &Value) -> Result<&str> {
    match value {
        Value::Text(message) if message.len() <= 16 * 1024 * 1024 => Ok(message),
        Value::Text(_) => Err(Error::Execution("SQL assertion message exceeds 16 MiB".into())),
        Value::Null => Ok("SQL assertion failed"),
        _ => Err(Error::Type("SQL assertion message must be text".into())),
    }
}

fn assert_true(args: &[Value]) -> Result<Value> {
    match args[0] {
        Value::Boolean(true) => Ok(Value::Null),
        Value::Boolean(false) | Value::Null => {
            let message = match args.get(1) {
                Some(value) => bounded_message(value)?,
                None => "SQL assertion failed",
            };
            Err(Error::Execution(format!("ASSERT_TRUE: {message}")))
        }
        _ => Err(Error::Type("ASSERT_TRUE condition must be boolean".into())),
    }
}

fn raise_error(args: &[Value]) -> Result<Value> {
    let message = bounded_message(&args[0])?;
    Err(Error::Execution(format!("RAISE_ERROR: {message}")))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function {
        name: "assert_true", min_args: 1, max_args: 2,
        result_type: assert_output, call: assert_true,
    },
    Function {
        name: "raise_error", min_args: 1, max_args: 1,
        result_type: raise_output, call: raise_error,
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{catalog::Catalog, executor, sql};

    #[test]
    fn true_assertion_returns_null_and_false_raises_message() {
        assert_eq!(assert_true(&[Value::Boolean(true)]).unwrap(), Value::Null);
        assert!(assert_true(&[Value::Boolean(false)]).is_err());
        assert!(assert_true(&[Value::Null, Value::Text("missing".into())])
            .unwrap_err().to_string().contains("missing"));
        assert!(assert_true(&[Value::Text("true".into())]).is_err());
        assert!(assert_output(&[DataType::Integer]).is_err());
    }

    #[test]
    fn assertions_execute_end_to_end_in_native_sql() {
        let catalog = Catalog::new();
        let rows = executor::execute(
            &sql::parse_sql("SELECT assert_true(TRUE) AS ok").unwrap(),
            &catalog,
        ).unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Null]]);
        for (query, marker) in [
            ("SELECT assert_true(FALSE, 'bad row')", "bad row"),
            ("SELECT raise_error('stop pipeline')", "stop pipeline"),
        ] {
            let error = executor::execute(&sql::parse_sql(query).unwrap(), &catalog)
                .unwrap_err();
            assert!(error.to_string().contains(marker));
        }
    }
}
