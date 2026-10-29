use super::{Function, json_path};
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
    Ok(Value::Boolean(json_path::parse(text).is_ok()))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
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
    }
}
