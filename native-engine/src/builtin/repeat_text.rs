use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Text && types[1] == DataType::Integer {
        Ok(DataType::Text)
    } else {
        Err(Error::Type("REPEAT requires text and integer count".into()))
    }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(times) = super::integer(&args[1])? else { return Ok(Value::Null); };
    let count = usize::try_from(times)
        .map_err(|_| Error::Execution("REPEAT count must be nonnegative".into()))?;
    if count > 1_000_000 {
        return Err(Error::Execution("REPEAT exceeds repetition limit".into()));
    }
    let bytes = text.len().checked_mul(count)
        .ok_or_else(|| Error::Execution("REPEAT length overflow".into()))?;
    if bytes > 16 * 1024 * 1024 {
        return Err(Error::Execution("REPEAT result exceeds 16 MiB".into()));
    }
    Ok(Value::Text(text.repeat(count)))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "repeat", min_args: 2, max_args: 2,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, schema::{Field, Schema}, sql};
    #[test]
    fn repeats_unicode_text_through_native_sql() {
        let schema = Schema::new(vec![Field::new("name", DataType::Text, true)]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::from_rows(schema, vec![
            vec![Value::Text("é".into())], vec![Value::Null],
        ]).unwrap()).unwrap();
        let rows = executor::execute(&sql::parse_sql("SELECT REPEAT(name, 3) AS n FROM t").unwrap(), &catalog)
            .unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Text("ééé".into())], vec![Value::Null]]);
        assert!(evaluate(&[Value::Text("x".into()), Value::Integer(-1)]).is_err());
        assert!(evaluate(&[Value::Text("x".into()), Value::Integer(16_777_217)]).is_err());
    }
}
