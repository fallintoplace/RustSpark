use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    match &args[0] {
        Value::Text(message) if message.len() <= 4096 => {
            Err(Error::Execution(message.clone()))
        }
        Value::Text(_) => Err(Error::Execution("RAISE_ERROR message exceeds 4096 bytes".into())),
        Value::Null => Err(Error::Execution("RAISE_ERROR called with NULL message".into())),
        _ => Err(Error::Type("RAISE_ERROR requires text".into())),
    }
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "raise_error", min_args: 1, max_args: 1, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, schema::{Field, Schema}, sql};

    #[test]
    fn error_works_inside_spark_case_branches() {
        let mut catalog = Catalog::new();
        let schema = Schema::new(vec![Field::new("x", DataType::Integer, false)]).unwrap();
        catalog.register("t", Batch::from_rows(schema,
            vec![vec![Value::Integer(1)]]).unwrap()).unwrap();
        let okay = sql::parse_sql("SELECT CASE WHEN x = 1 THEN 'ok' ELSE RAISE_ERROR('bad') END FROM t").unwrap();
        assert_eq!(executor::execute(&okay, &catalog).unwrap().rows().unwrap(),
            vec![vec![Value::Text("ok".into())]]);
        let broken = sql::parse_sql("SELECT RAISE_ERROR('bad row') FROM t").unwrap();
        assert!(executor::execute(&broken, &catalog).unwrap_err().to_string().contains("bad row"));
        assert!(evaluate(&[Value::Text("x".repeat(4097))]).is_err());
    }
}
