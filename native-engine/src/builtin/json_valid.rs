use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Boolean)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    if text.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("JSON_VALID document exceeds 16 MiB".into()));
    }
    // The result only needs syntax validity, not an allocated JSON value tree.
    Ok(Value::Boolean(
        serde_json::from_str::<serde::de::IgnoredAny>(text).is_ok()
    ))
}


fn json_pretty_type(types: &[DataType]) -> Result<DataType> { super::text_type(types) }
fn json_pretty(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let doc = json_path::parse(text)?;
    let formatted = serde_json::to_string_pretty(&doc)
        .map_err(|error| Error::Execution(format!("JSON_PRETTY failed: {error}")))?;
    if formatted.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("JSON_PRETTY output exceeds 16 MiB".into()));
    }
    Ok(Value::Text(formatted))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "json_pretty", min_args: 1, max_args: 1, result_type: json_pretty_type, call: json_pretty },Function {
    name: "json_valid", min_args: 1, max_args: 1,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, schema::{Field, Schema}, sql};
    #[test]
    fn json_valid_parses_native_sql_document_columns() {
        let mut catalog = Catalog::new();
        let schema = Schema::new(vec![Field::new("doc", DataType::Text, true)]).unwrap();
        catalog.register("t", Batch::from_rows(schema, vec![
            vec![Value::Text(r#"{"n":1}"#.into())],
            vec![Value::Text("{invalid}".into())],
            vec![Value::Null],
        ]).unwrap()).unwrap();
        let plan = sql::parse_sql("SELECT JSON_VALID(doc) AS valid FROM t").unwrap();
        let rows = executor::execute(&plan, &catalog).unwrap().rows().unwrap();
        assert_eq!(rows, vec![
            vec![Value::Boolean(true)], vec![Value::Boolean(false)], vec![Value::Null],
        ]);
        assert_eq!(evaluate(&[Value::Text(r#"{"a":[true,null,{"x":"🙂"}]}"#.into())]).unwrap(), Value::Boolean(true));
        assert_eq!(evaluate(&[Value::Text("[] trailing".into())]).unwrap(), Value::Boolean(false));
        assert_eq!(evaluate(&[Value::Text("{".into())]).unwrap(), Value::Boolean(false));
        assert_eq!(evaluate(&[Value::Text("[]".into())]).unwrap(), Value::Boolean(true));
    }
}

#[cfg(test)]
mod json_pretty_extension_tests {
    use super::*;
    #[test]
    fn checks_data_shapes_and_nulls() {
        assert!(crate::builtin::supports("json_pretty"));
        let output = json_pretty(&[Value::Text(r#"{"a":[1,2]}"#.into())]).unwrap();
        let Value::Text(formatted) = output else { panic!("expected text"); };
        assert!(formatted.contains("\n"));
        assert_eq!(serde_json::from_str::<serde_json::Value>(&formatted).unwrap(), serde_json::json!({"a":[1,2]}));
        assert_eq!(json_pretty(&[Value::Null]).unwrap(), Value::Null);
        assert!(json_pretty(&[Value::Text("{broken".into())]).is_err());
    }
}
