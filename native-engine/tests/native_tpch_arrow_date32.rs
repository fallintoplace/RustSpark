use std::sync::Arc;
use arrow_array::{ArrayRef, Date32Array, RecordBatch};
use arrow_schema::{DataType as ArrowType, Field as ArrowField, Schema as ArrowSchema};
use rustspark_native::{
    arrow_bridge, catalog::Catalog, executor::execute, scalar::Value,
    schema::DataType, sql::parse_sql,
};

#[test]
fn tpch_date32_parquet_columns_decode_to_canonical_iso_text() {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("l_shipdate", ArrowType::Date32, true),
    ]));
    let dates: ArrayRef = Arc::new(Date32Array::from(vec![
        Some(-1), Some(0), Some(10_957), None,
    ]));
    let arrow = RecordBatch::try_new(schema, vec![dates]).unwrap();
    let batch = arrow_bridge::from_arrow(&arrow).unwrap();
    assert_eq!(batch.schema().fields()[0].data_type, DataType::Text);
    assert_eq!(batch.rows().unwrap(), vec![
        vec![Value::Text("1969-12-31".into())],
        vec![Value::Text("1970-01-01".into())],
        vec![Value::Text("2000-01-01".into())],
        vec![Value::Null],
    ]);

    let mut catalog = Catalog::new();
    catalog.register("lineitem", batch).unwrap();
    let result = execute(&parse_sql(
        "SELECT l_shipdate FROM lineitem WHERE l_shipdate >= '2000-01-01'"
    ).unwrap(), &catalog).unwrap();
    assert_eq!(result.rows().unwrap(),
        vec![vec![Value::Text("2000-01-01".into())]]);
}

#[test]
fn date32_outside_supported_text_year_range_is_rejected() {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("shipdate", ArrowType::Date32, false),
    ]));
    let dates: ArrayRef = Arc::new(Date32Array::from(vec![i32::MAX]));
    let arrow = RecordBatch::try_new(schema, vec![dates]).unwrap();
    assert!(arrow_bridge::from_arrow(&arrow).is_err());
}
