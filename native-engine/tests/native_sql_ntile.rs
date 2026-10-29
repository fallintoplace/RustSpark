use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog()->Catalog {
    let mut c=Catalog::new();
    let data=(1..=7).map(|id| vec![
        Value::Integer(id),Value::Text("A".into()),
    ]).chain((8..=9).map(|id|vec![
        Value::Integer(id),Value::Text("B".into()),
    ])).collect();
    c.register("t",Batch::from_rows(Schema::new(vec![
        Field::new("id",DataType::Integer,false),
        Field::new("dept",DataType::Text,false),
    ]).unwrap(),data).unwrap()).unwrap();
    c
}
fn rows(q:&str)->Vec<Vec<Value>>{
    execute(&parse_sql(q).unwrap(),&catalog()).unwrap().rows().unwrap()
}
#[test]
fn ntile_spreads_remainder_across_first_buckets(){
    let got=rows("SELECT id, NTILE(3) OVER (PARTITION BY dept ORDER BY id) AS bucket FROM t ORDER BY id");
    let expected=[1,1,1,2,2,3,3,1,2];
    assert_eq!(got,expected.iter().enumerate().map(|(i,b)|vec![
        Value::Integer((i+1) as i64),Value::Integer(*b),
    ]).collect::<Vec<_>>());
}
#[test]
fn ntile_more_buckets_than_rows_uses_consecutive_bucket_numbers(){
    let got=rows("SELECT id, NTILE(10) OVER (PARTITION BY dept ORDER BY id) AS bucket FROM t ORDER BY id");
    for (index,row) in got.iter().enumerate(){
        let expected=if index<7 {index+1} else {index-6};
        assert_eq!(row[1],Value::Integer(expected as i64));
    }
}
#[test]
fn ntile_one_bucket_does_not_split_any_partition(){
    let got=rows("SELECT NTILE(1) OVER (PARTITION BY dept ORDER BY id) FROM t");
    assert!(got.iter().all(|row|row==&vec![Value::Integer(1)]));
}
#[test]
fn ntile_with_descending_order_reverses_buckets(){
    let got=rows("SELECT id, NTILE(3) OVER (PARTITION BY dept ORDER BY id DESC) AS bucket FROM t ORDER BY id");
    assert_eq!(got[0][1],Value::Integer(3));
    assert_eq!(got[6][1],Value::Integer(1));
}
#[test]
fn unsupported_bucket_counts_and_missing_orders_fail(){
    for sql in [
        "SELECT NTILE(0) OVER (ORDER BY id) FROM t",
        "SELECT NTILE(1000001) OVER (ORDER BY id) FROM t",
        "SELECT NTILE(3) OVER () FROM t",
        "SELECT NTILE(-1) OVER (ORDER BY id) FROM t",
    ] { assert!(parse_sql(sql).is_err(),"{sql}"); }
}
