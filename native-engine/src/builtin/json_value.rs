use super::{Function, json_path};
use crate::{scalar::Value, schema::DataType, Result, Error};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(path) = super::string(&args[1])? else { return Ok(Value::Null); };
    let document = json_path::parse(text)?;
    let Some(value) = json_path::lookup(Some(&document), path)? else { return Ok(Value::Null); };
    match value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::String(value) => Ok(Value::Text(value.clone())),
        serde_json::Value::Bool(value) => Ok(Value::Text(value.to_string())),
        serde_json::Value::Number(value) => Ok(Value::Text(value.to_string())),
        _ => Err(Error::Type("JSON_VALUE requires a scalar JSON node".into())),
    }
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "json_value", min_args: 2, max_args: 2,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{catalog::Catalog, executor, batch::Batch, schema::{Field, Schema}, sql};
    #[test]
    fn extracts_nested_scalar_from_native_sql() {
        let mut catalog = Catalog::new();
        let schema = Schema::new(vec![Field::new("doc", DataType::Text, false)]).unwrap();
        let data = Batch::from_rows(schema, vec![
            vec![Value::Text(r#"{"customer":{"id":"abc"}}"#.into())],
        ]).unwrap();
        catalog.register("t", data).unwrap();
        let rows = executor::execute(&sql::parse_sql(
            "SELECT JSON_VALUE(doc, '$.customer.id') AS customer FROM t"
        ).unwrap(), &catalog).unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Text("abc".into())]]);
        assert!(evaluate(&[
            Value::Text(r#"{"x":[1]}"#.into()), Value::Text("$.x".into())
        ]).is_err());
    }
}
