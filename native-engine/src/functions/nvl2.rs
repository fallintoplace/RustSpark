use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("NVL2", args, 3, 3)?;
    unified_type(&args[1..], schema)
}

pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    let first = args[0].evaluate(batch, row)?;
    args[if first.is_null() { 2 } else { 1 }].evaluate(batch, row)
}

#[cfg(test)]
mod tests {
    use crate::{batch::Batch, catalog::Catalog, executor, scalar::Value, schema::{DataType, Field, Schema}, sql};
    #[test]
    fn nvl2_is_null_aware_and_evaluates_only_selected_arm() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::from_rows(schema, vec![vec![Value::Null], vec![Value::Integer(9)]]).unwrap()).unwrap();
        let query = sql::parse_sql("SELECT NVL2(n, 5, 4) AS value FROM t").unwrap();
        assert_eq!(executor::execute(&query, &catalog).unwrap().rows().unwrap(),
            vec![vec![Value::Integer(4)], vec![Value::Integer(5)]]);
        let lazy = sql::parse_sql("SELECT NVL2(NULL, 9223372036854775807 + 1, 4) AS value FROM t").unwrap();
        assert_eq!(executor::execute(&lazy, &catalog).unwrap().row(0).unwrap(), vec![Value::Integer(4)]);
    }
}
