use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] != DataType::Boolean
        || (types.len() == 2 && types[1] != DataType::Text)
    {
        return Err(Error::Type("ASSERT_TRUE requires boolean and optional text message".into()));
    }
    Ok(DataType::Text)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    match &args[0] {
        Value::Boolean(true) => Ok(Value::Null),
        Value::Boolean(false) | Value::Null => {
            let message = match args.get(1) {
                Some(Value::Text(text)) => {
                    if text.len() > 4096 {
                        return Err(Error::Execution("ASSERT_TRUE message exceeds 4096 bytes".into()));
                    }
                    text.as_str()
                }
                Some(Value::Null) | None => "ASSERT_TRUE failed",
                _ => return Err(Error::Type("ASSERT_TRUE message must be text".into())),
            };
            Err(Error::Execution(message.to_string()))
        }
        _ => Err(Error::Type("ASSERT_TRUE condition must be boolean".into())),
    }
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "assert_true", min_args: 1, max_args: 2,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, schema::{Field, Schema}, sql};

    fn run(query: &str) -> Result<Vec<Vec<Value>>> {
        let mut catalog = Catalog::new();
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        catalog.register("t", Batch::from_rows(schema,
            vec![vec![Value::Integer(1)], vec![Value::Null]]).unwrap()).unwrap();
        executor::execute(&sql::parse_sql(query)?, &catalog)?.rows()
    }

    #[test]
    fn asserts_conditions_through_native_sql() {
        assert_eq!(run("SELECT ASSERT_TRUE(n IS NULL OR n > 0, 'bad number') FROM t").unwrap(),
            vec![vec![Value::Null], vec![Value::Null]]);
        assert!(run("SELECT ASSERT_TRUE(n > 1, 'bad number') FROM t").unwrap_err()
            .to_string().contains("bad number"));
        assert!(run("SELECT ASSERT_TRUE(n > 0) FROM t").is_err());
        assert!(output(&[DataType::Integer]).is_err());
    }
}
