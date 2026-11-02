use std::io::{BufReader, Cursor};
use rustspark_native::{
    jsonl_stream::JsonlScan, scalar::Value,
    schema::{DataType, Field, Schema},
};

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("ID", DataType::Integer, false),
        Field::new("score", DataType::Float, true),
        Field::new("label", DataType::Text, true),
    ]).unwrap()
}

#[test]
fn streams_typed_json_rows_across_small_io_buffers() {
    let json = "{\"id\":1,\"score\":1}\n\n{\"ID\":2,\"score\":2.5,\"label\":\"🐈\"}\r\n\
        {\"id\":3,\"score\":null}\n{\"id\":4}\n{\"id\":5,\"score\":9}\n";
    let reader = BufReader::with_capacity(3, Cursor::new(json.as_bytes()));
    let mut scan = JsonlScan::new(reader, schema(), 2, 4096).unwrap();
    let first = scan.next().unwrap().unwrap();
    assert_eq!(first.num_rows(), 2);
    assert_eq!(first.value(0, 1).unwrap(), &Value::Float(1.0));
    assert_eq!(first.value(1, 2).unwrap(), &Value::Text("🐈".into()));
    let second = scan.next().unwrap().unwrap();
    assert_eq!(second.num_rows(), 2);
    assert_eq!(second.value(1, 1).unwrap(), &Value::Null);
    assert_eq!(scan.next().unwrap().unwrap().num_rows(), 1);
    assert!(scan.next().is_none());
    assert!(scan.bytes_read() <= json.len());
}

#[test]
fn malformed_or_budget_exceeded_inputs_fail_once_without_exposing_partial_batch() {
    for input in [
        "{\"id\":1}\n{\"id\":2,\"ID\":3}\n",
        "{\"id\":1}\n{\"id\":2,\"unknown\":3}\n",
        "{\"id\":1}\n{\"id\":2,\"score\":{}}\n",
        "{\"id\":1}\n{\"id\":2,\"id\":3}\n",
        "{\"id\":1}\n{\"id\":2,\"score\":\"bad\"}\n",
        "{\"id\":1}\n{\"id\":2,\"score\":9223372036854775808}\n",
    ] {
        let mut scan = JsonlScan::new(Cursor::new(input.as_bytes()), schema(), 4, 4096).unwrap();
        assert!(scan.next().unwrap().is_err(), "{input}");
        assert!(scan.next().is_none(), "{input}");
    }
    let mut truncated = JsonlScan::new(Cursor::new(b"{\"id\":1}\n{\"id\":2}\n"),
        schema(), 2, 10).unwrap();
    assert!(truncated.next().unwrap().is_err());
    assert!(truncated.next().is_none());
}

#[test]
fn streaming_limits_rows_columns_and_nonnullable_fields() {
    let input = Cursor::new(b"{\"score\":1}\n");
    let mut missing = JsonlScan::new(input, schema(), 1, 1024).unwrap();
    assert!(missing.next().unwrap().is_err());
    assert!(JsonlScan::new(Cursor::new(b"".as_slice()), schema(), 0, 10).is_err());
    assert!(JsonlScan::new(Cursor::new(b"".as_slice()), schema(), 20_000, 10).is_err());
    assert_eq!(JsonlScan::new(Cursor::new(b"".as_slice()), schema(), 8, 0)
        .unwrap().count(), 0);
}


#[test]
fn batch_byte_quota_preserves_pending_rows_without_unbounded_growth() {
    let json = "{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n";
    let mut scan = JsonlScan::with_batch_byte_budget(
        Cursor::new(json.as_bytes()), schema(), 8, 4096, 120
    ).unwrap();
    for id in 1..=3 {
        let batch = scan.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.value(0, 0).unwrap(), &Value::Integer(id));
    }
    assert!(scan.next().is_none());
    assert_eq!(scan.bytes_read(), json.len());
    let mut too_small = JsonlScan::with_batch_byte_budget(
        Cursor::new(json.as_bytes()), schema(), 8, 4096, 1
    ).unwrap();
    assert!(too_small.next().unwrap().is_err());
    assert!(too_small.next().is_none());
    assert!(JsonlScan::with_batch_byte_budget(
        Cursor::new(json.as_bytes()), schema(), 8, 4096, 0
    ).is_err());
}
