use std::sync::Arc;
use arrow_array::{ArrayRef, Decimal128Array, RecordBatch};
use arrow_schema::{DataType as ArrowType, Field as ArrowField, Schema as ArrowSchema};
use rustspark_native::{
    arrow_bridge, catalog::Catalog, executor::execute, scalar::Value,
    schema::DataType, sql::parse_sql,
};

fn input(values: Vec<Option<i128>>) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("l_extendedprice", ArrowType::Decimal128(15, 2), true),
    ]));
    let array: ArrayRef = Arc::new(
        Decimal128Array::from(values).with_precision_and_scale(15, 2).unwrap()
    );
    RecordBatch::try_new(schema, vec![array]).unwrap()
}

#[test]
fn tpch_parquet_decimal_prices_support_numeric_sql() {
    let native = arrow_bridge::from_arrow(&input(vec![
        Some(12_345), Some(500), None,
    ])).unwrap();
    assert_eq!(native.schema().fields()[0].data_type, DataType::Float);
    assert_eq!(native.rows().unwrap(), vec![
        vec![Value::Float(123.45)],
        vec![Value::Float(5.0)],
        vec![Value::Null],
    ]);
    let mut catalog = Catalog::new();
    catalog.register("lineitem", native).unwrap();
    let result = execute(&parse_sql(
        "SELECT SUM(l_extendedprice) AS revenue FROM lineitem"
    ).unwrap(), &catalog).unwrap().rows().unwrap();
    let Value::Float(revenue) = result[0][0] else { panic!("expected float"); };
    assert!((revenue - 128.45).abs() < 1e-10);
}

#[test]
fn decimal_coefficients_beyond_exact_float_integer_range_are_rejected() {
    assert!(arrow_bridge::from_arrow(&input(vec![
        Some((1_i128 << 53) + 1),
    ])).is_err());
}
