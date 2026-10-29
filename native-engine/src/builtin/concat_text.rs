use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn concat(args: &[Value]) -> Result<Value> {
    let mut length = 0_usize;
    for value in args {
        let Some(text) = super::string(value)? else { return Ok(Value::Null); };
        length = length.checked_add(text.len())
            .ok_or_else(|| Error::Execution("CONCAT output length overflow".into()))?;
        if length > 16 * 1024 * 1024 {
            return Err(Error::Execution("CONCAT output exceeds 16 MiB".into()));
        }
    }
    // Type and size were checked above; avoid intermediate strings or joins.
    let mut output = String::with_capacity(length);
    for value in args {
        if let Value::Text(text) = value {
            output.push_str(text);
        }
    }
    Ok(Value::Text(output))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "concat", min_args: 1, max_args: 128,
    result_type: output, call: concat,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, schema::{Field, Schema}, sql};

    #[test]
    fn joins_unicode_text_and_propagates_nulls() {
        assert_eq!(concat(&[Value::Text("café".into()), Value::Text("🙂".into())]).unwrap(),
            Value::Text("café🙂".into()));
        assert_eq!(concat(&[Value::Text("".into())]).unwrap(), Value::Text("".into()));
        assert_eq!(concat(&[Value::Text("x".into()), Value::Null]).unwrap(), Value::Null);
        assert!(concat(&[Value::Text("x".repeat(16 * 1024 * 1024)),
            Value::Text("y".into())]).is_err());
        assert!(output(&[DataType::Text, DataType::Integer]).is_err());
    }

    #[test]
    fn executes_concat_through_native_sql() {
        let schema = Schema::new(vec![Field::new("city", DataType::Text, true)]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::from_rows(schema,
            vec![vec![Value::Text("Berlin".into())], vec![Value::Null]]).unwrap()).unwrap();
        let plan = sql::parse_sql("SELECT CONCAT(city, '-DE') AS value FROM t").unwrap();
        let rows = executor::execute(&plan, &catalog).unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Text("Berlin-DE".into())], vec![Value::Null]]);
    }
}
