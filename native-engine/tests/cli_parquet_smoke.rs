use std::{fs, process::Command, time::{SystemTime, UNIX_EPOCH}};
use parquet::arrow::ArrowWriter;
use rustspark_native::{
    arrow_bridge, batch::Batch, scalar::Value,
    schema::{DataType, Field, Schema},
};

#[test]
fn native_cli_reads_parquet_and_aggregates_without_datafusion() {
    let unique = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let path = std::env::temp_dir().join(format!(
        "rustspark-native-cli-parquet-{}-{unique}.parquet", std::process::id()
    ));
    let schema = Schema::new(vec![
        Field::new("city", DataType::Text, false),
        Field::new("amount", DataType::Integer, false),
    ]).unwrap();
    let data = Batch::from_rows(schema, vec![
        vec![Value::Text("Berlin".into()), Value::Integer(2)],
        vec![Value::Text("Berlin".into()), Value::Integer(3)],
        vec![Value::Text("Paris".into()), Value::Integer(1)],
    ]).unwrap();
    let arrow = arrow_bridge::to_arrow(&data).unwrap();
    let mut writer = ArrowWriter::try_new(fs::File::create(&path).unwrap(), arrow.schema(), None).unwrap();
    writer.write(&arrow).unwrap();
    writer.close().unwrap();

    let binary = env!("CARGO_BIN_EXE_rustspark-native");
    let output = Command::new(binary).args([
        "--parquet", path.to_str().unwrap(), "--table", "sales",
        "--sql", "SELECT city, SUM(amount) AS total FROM sales GROUP BY city ORDER BY city",
    ]).output().unwrap();
    assert!(output.status.success(), "native CLI: {}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "city,total\nBerlin,5\nParis,1\n");

    let rejected = Command::new(binary).args([
        "--parquet", path.to_str().unwrap(), "--sql", "SELECT * FROM data", "--stream",
    ]).output().unwrap();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("CSV"));
    fs::remove_file(path).unwrap();
}
