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


fn json_merge_patch_type(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn apply_merge_patch(target: &mut serde_json::Value, patch: &serde_json::Value) {
    match patch {
        serde_json::Value::Object(updates) => {
            if !target.is_object() { *target = serde_json::Value::Object(serde_json::Map::new()); }
            let Some(fields) = target.as_object_mut() else { return; };
            for (key, value) in updates {
                if value.is_null() {
                    fields.remove(key);
                } else {
                    apply_merge_patch(fields.entry(key.clone()).or_insert(serde_json::Value::Null), value);
                }
            }
        }
        _ => *target = patch.clone(),
    }
}
fn json_merge_patch(args: &[Value]) -> Result<Value> {
    let Some(document) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(patch) = super::string(&args[1])? else { return Ok(Value::Null); };
    let mut document = json_path::parse(document)?;
    let patch = json_path::parse(patch)?;
    apply_merge_patch(&mut document, &patch);
    let result = document.to_string();
    if result.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("JSON_MERGE_PATCH output exceeds 16 MiB".into()));
    }
    Ok(Value::Text(result))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "json_merge_patch", min_args: 2, max_args: 2, result_type: json_merge_patch_type, call: json_merge_patch },Function {
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

#[cfg(test)]
mod json_merge_patch_extension_tests {
    use super::*;
    #[test]
    fn handles_special_values_and_nulls() {
        assert!(crate::builtin::supports("json_merge_patch"));
        let actual = json_merge_patch(&[
            Value::Text(r#"{"a":1,"b":{"c":2,"d":3}}"#.into()),
            Value::Text(r#"{"a":null,"b":{"c":5},"e":9}"#.into()),
        ]).unwrap();
        let Value::Text(result) = actual else { panic!("expected text"); };
        assert_eq!(serde_json::from_str::<serde_json::Value>(&result).unwrap(),
            serde_json::json!({"b":{"c":5,"d":3},"e":9}));
        assert_eq!(json_merge_patch(&[Value::Text("[]".into()), Value::Text(r#"{"x":1}"#.into())]).unwrap(), Value::Text(r#"{"x":1}"#.into()));
        assert_eq!(json_merge_patch(&[Value::Null, Value::Text("{}".into())]).unwrap(), Value::Null);
    }
}
