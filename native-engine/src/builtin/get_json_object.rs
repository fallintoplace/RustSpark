use super::{Function, json_extract};
use crate::{scalar::Value, schema::DataType, Error, Result};

// Spark GET_JSON_OBJECT returns unquoted JSON strings, serialized objects
// and arrays, and NULL for JSON null, missing paths or malformed input.
fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn evaluate(args: &[Value]) -> Result<Value> {
    match json_extract::evaluate(args) {
        Ok(Value::Text(json)) if json == "null" => Ok(Value::Null),
        Ok(Value::Text(json)) if json.starts_with('"') => {
            let decoded: String = serde_json::from_str(&json)
                .map_err(|error| Error::Execution(format!(
                    "GET_JSON_OBJECT decoded string: {error}"
                )))?;
            Ok(Value::Text(decoded))
        }
        Ok(result) => Ok(result),
        // A malformed document is an absent value in Spark's JSON extractor.
        // Do not suppress allocation, size-budget or other execution errors.
        Err(Error::Execution(message))
            if message.starts_with("invalid native JSON input:") => Ok(Value::Null),
        Err(error) => Err(error),
    }
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "get_json_object", min_args: 2, max_args: 2,
        result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{catalog::Catalog, executor, sql};

    fn extract(json: &str, path: &str) -> Value {
        evaluate(&[Value::Text(json.into()), Value::Text(path.into())]).unwrap()
    }

    #[test]
    fn matches_spark_scalar_container_missing_and_null_semantics() {
        let json = r#"{"s":"café🙂","n":42,"nil":null,"array":[1,null,3],"obj":{"a":1}}"#;
        assert_eq!(extract(json, "$.s"), Value::Text("café🙂".into()));
        assert_eq!(extract(json, "$.n"), Value::Text("42".into()));
        assert_eq!(extract(json, "$.nil"), Value::Null);
        assert_eq!(extract(json, "$.missing"), Value::Null);
        assert_eq!(extract(json, "$.array"), Value::Text("[1,null,3]".into()));
        assert_eq!(extract(json, "$.obj"), Value::Text(r#"{"a":1}"#.into()));
    }

    #[test]
    fn validates_complete_document_and_preserves_resource_limits() {
        assert_eq!(extract(r#"{"a":"x"} garbage"#, "$.a"), Value::Null);
        assert_eq!(extract("{bad", "$.a"), Value::Null);
        assert_eq!(extract(r#"{"a":"x","a":"last"}"#, "$.a"),
            Value::Text("last".into()));
        assert_eq!(extract(r#"{"a":"line\\nnext"}"#, "$.a"),
            Value::Text("line\\nnext".into()));
        assert_eq!(evaluate(&[Value::Null, Value::Text("$.a".into())]).unwrap(), Value::Null);
        assert!(matches!(evaluate(&[
            Value::Text("x".repeat(16 * 1024 * 1024 + 1)),
            Value::Text("$".into()),
        ]), Err(Error::Execution(_))));
    }

    #[test]
    fn executes_sql_with_quoted_json_string_and_array_paths() {
        let plan = sql::parse_sql(
            r#"SELECT GET_JSON_OBJECT('{"a":["é",null]}', '$.a[0]'),
                      GET_JSON_OBJECT('{"a":[1,2]}', '$.a')"#
        ).unwrap();
        let values = executor::execute(&plan, &Catalog::new()).unwrap().rows().unwrap();
        assert_eq!(values, vec![vec![
            Value::Text("é".into()), Value::Text("[1,2]".into()),
        ]]);
    }
}
