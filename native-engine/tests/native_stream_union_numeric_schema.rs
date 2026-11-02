use rustspark_native::{
    batch::Batch, plan::Plan,
    scale::{cancel::QueryControl, file_catalog::FileCatalog,
        stream_execute::{execute, output_schema, StreamOptions}},
    scalar::Value, schema::{DataType, Field, Schema}, Result,
};

#[test]
fn numeric_union_all_uses_positional_widening_and_left_names() {
    let left = Batch::from_rows(Schema::new(vec![
        Field::new("amount", DataType::Integer, false),
    ]).unwrap(), vec![vec![Value::Integer(7)]]).unwrap();
    let right = Batch::from_rows(Schema::new(vec![
        Field::new("price", DataType::Float, true),
    ]).unwrap(), vec![vec![Value::Float(2.5)]]).unwrap();
    let plan = Plan::UnionAll {
        left: Box::new(Plan::Values { batch: left }),
        right: Box::new(Plan::Values { batch: right }),
    };
    let options = StreamOptions::default();
    let schema = output_schema(&plan, &FileCatalog::new(), &options).unwrap();
    assert_eq!(schema.field(0).unwrap().name, "amount");
    assert_eq!(schema.field(0).unwrap().data_type, DataType::Float);
    assert!(schema.field(0).unwrap().nullable);
    let batches = execute(&plan, &FileCatalog::new(), &options, QueryControl::default())
        .unwrap().collect::<Result<Vec<_>>>().unwrap();
    let rows = batches.iter().flat_map(|batch| {
        assert_eq!(batch.schema(), &schema);
        batch.rows().unwrap()
    }).collect::<Vec<_>>();
    assert_eq!(rows, vec![vec![Value::Float(7.0)], vec![Value::Float(2.5)]]);
}
