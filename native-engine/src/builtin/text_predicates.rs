use super::Function;
use crate::{scalar::Value, schema::DataType, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Boolean)
}

fn inputs(args: &[Value]) -> Result<Option<(&str, &str)>> {
    let Some(text) = super::string(&args[0])? else { return Ok(None); };
    let Some(pattern) = super::string(&args[1])? else { return Ok(None); };
    Ok(Some((text, pattern)))
}

fn startswith(args: &[Value]) -> Result<Value> {
    Ok(inputs(args)?.map_or(Value::Null, |(text, prefix)| Value::Boolean(text.starts_with(prefix))))
}

fn endswith(args: &[Value]) -> Result<Value> {
    Ok(inputs(args)?.map_or(Value::Null, |(text, suffix)| Value::Boolean(text.ends_with(suffix))))
}

fn contains(args: &[Value]) -> Result<Value> {
    Ok(inputs(args)?.map_or(Value::Null, |(text, needle)| Value::Boolean(text.contains(needle))))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "startswith", min_args: 2, max_args: 2, result_type: output, call: startswith },
    Function { name: "endswith", min_args: 2, max_args: 2, result_type: output, call: endswith },
    Function { name: "contains", min_args: 2, max_args: 2, result_type: output, call: contains },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, schema::{Field, Schema}, sql};

    #[test]
    fn checks_unicode_boundaries_and_nulls() {
        let text = Value::Text("école🙂".into());
        assert_eq!(startswith(&[text.clone(), Value::Text("éc".into())]).unwrap(), Value::Boolean(true));
        assert_eq!(endswith(&[text.clone(), Value::Text("🙂".into())]).unwrap(), Value::Boolean(true));
        assert_eq!(contains(&[text.clone(), Value::Text("ole".into())]).unwrap(), Value::Boolean(true));
        assert_eq!(contains(&[text.clone(), Value::Text("Berlin".into())]).unwrap(), Value::Boolean(false));
        assert_eq!(contains(&[text.clone(), Value::Text("".into())]).unwrap(), Value::Boolean(true));
        assert_eq!(startswith(&[Value::Null, Value::Text("".into())]).unwrap(), Value::Null);
        assert_eq!(endswith(&[text, Value::Null]).unwrap(), Value::Null);
        assert!(output(&[DataType::Integer, DataType::Text]).is_err());
    }

    #[test]
    fn evaluates_all_three_predicates_in_native_sql() {
        let schema = Schema::new(vec![Field::new("word", DataType::Text, true)]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::from_rows(schema,
            vec![vec![Value::Text("école🙂".into())]]).unwrap()).unwrap();
        let sql = "SELECT STARTSWITH(word, 'éc') AS a, ENDSWITH(word, '🙂') AS b, CONTAINS(word, 'ole') AS c FROM t";
        let rows = executor::execute(&sql::parse_sql(sql).unwrap(), &catalog)
            .unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Boolean(true), Value::Boolean(true), Value::Boolean(true)]]);
    }
}
