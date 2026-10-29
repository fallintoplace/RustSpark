use super::{same_type, Function};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] != DataType::Boolean {
        return Err(Error::Type("IF condition must be boolean".into()));
    }
    same_type(&types[1..])
}
fn evaluate(args: &[Value]) -> Result<Value> {
    let condition = args[0].as_boolean()?;
    Ok(args[if condition == Some(true) { 1 } else { 2 }].clone())
}
pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "if", min_args: 3, max_args: 3, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, schema::{Field, Schema}, sql};
    #[test]
    fn conditionals_short_circuit_across_native_sql() {
        let mut catalog = Catalog::new();
        let batch = Batch::from_rows(Schema::new(vec![
            Field::new("condition", DataType::Boolean, true)
        ]).unwrap(), vec![vec![Value::Boolean(true)], vec![Value::Null]]).unwrap();
        catalog.register("t", batch).unwrap();
        let query = "SELECT IF(condition, 5, 9223372036854775807 + 1) AS n FROM t";
        let plan = sql::parse_sql(query).unwrap();
        let mut rows = executor::execute(&plan, &catalog).unwrap_err();
        assert!(rows.to_string().contains("overflow"));
        let query = "SELECT IF(condition, 5, 7) AS n FROM t";
        let rows = executor::execute(&sql::parse_sql(query).unwrap(), &catalog)
            .unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Integer(5)], vec![Value::Integer(7)]]);
    }
}
