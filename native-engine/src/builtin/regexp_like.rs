use super::{Function, regex_support};
use crate::{scalar::Value, schema::DataType, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Boolean)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(pattern) = super::string(&args[1])? else { return Ok(Value::Null); };
    regex_support::check_input(text)?;
    Ok(Value::Boolean(regex_support::compile(pattern)?.is_match(text)))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "regexp_like", min_args: 2, max_args: 2,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, schema::{Field, Schema}, sql};
    #[test]
    fn matches_unicode_strings_through_native_sql() {
        let mut catalog = Catalog::new();
        let schema = Schema::new(vec![Field::new("name", DataType::Text, true)]).unwrap();
        catalog.register("t", Batch::from_rows(schema, vec![
            vec![Value::Text("Berlin".into())], vec![Value::Text("Łódź".into())],
        ]).unwrap()).unwrap();
        let rows = executor::execute(&sql::parse_sql(
            "SELECT REGEXP_LIKE(name, '^B.*') AS match_name FROM t"
        ).unwrap(), &catalog).unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Boolean(true)], vec![Value::Boolean(false)]]);
    }
}
