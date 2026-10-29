use rustspark_native::{
    scale::{file_catalog::FileCatalog, stream_execute::{self, StreamOptions},
        cancel::QueryControl},
    scalar::Value, schema::{DataType, Field, Schema}, sql::parse_sql, Result,
};
use std::{fs, path::PathBuf, sync::atomic::{AtomicUsize, Ordering}};

static NEXT: AtomicUsize = AtomicUsize::new(1);
struct Fixture { directory: PathBuf, catalog: FileCatalog }
impl Fixture {
    fn new(left_data: &str, right_data: &str) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "rustspark-stream-outer-{}-{}", std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        let left = directory.join("a.csv");
        let right = directory.join("b.csv");
        fs::write(&left, left_data).unwrap();
        fs::write(&right, right_data).unwrap();
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, true)]).unwrap();
        let mut catalog = FileCatalog::new();
        catalog.register_csv("a", left, schema.clone(), 1024).unwrap();
        catalog.register_csv("b", right, schema, 1024).unwrap();
        Self { directory, catalog }
    }
    fn run(&self, query: &str) -> Vec<Vec<Value>> {
        let plan = parse_sql(query).unwrap();
        let options = StreamOptions {
            batch_rows: 1, scratch_directory: self.directory.clone(),
            ..StreamOptions::default()
        };
        stream_execute::execute(&plan, &self.catalog, &options, QueryControl::default())
            .unwrap().collect::<Result<Vec<_>>>().unwrap().iter()
            .flat_map(|batch| batch.rows().unwrap()).collect()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) { fs::remove_dir_all(&self.directory).unwrap(); }
}

#[test]
fn streaming_right_join_emits_each_unmatched_build_row_once() {
    let fixture = Fixture::new("id\n1\n1\n3\n", "id\n1\n2\n2\n4\n");
    let rows = fixture.run("SELECT * FROM a RIGHT OUTER JOIN b ON id = id");
    assert_eq!(rows.iter().filter(|row| *row ==
        &vec![Value::Integer(1), Value::Integer(1)]).count(), 2);
    for id in [2,4] {
        let n = if id == 2 { 2 } else { 1 };
        assert_eq!(rows.iter().filter(|row| *row ==
            &vec![Value::Null, Value::Integer(id)]).count(), n);
    }
    assert_eq!(rows.len(), 5);
}

#[test]
fn streaming_full_join_preserves_unmatched_probe_and_build_rows() {
    let fixture = Fixture::new("id\n1\n3\n", "id\n1\n2\n4\n");
    let rows = fixture.run("SELECT * FROM a FULL OUTER JOIN b ON id = id");
    assert_eq!(rows.len(), 4);
    for expected in [
        vec![Value::Integer(1), Value::Integer(1)],
        vec![Value::Integer(3), Value::Null],
        vec![Value::Null, Value::Integer(2)],
        vec![Value::Null, Value::Integer(4)],
    ] {
        assert!(rows.contains(&expected), "{expected:?}");
    }
}

#[test]
fn output_schema_widens_nullable_fields_for_outer_joins() {
    let fixture = Fixture::new("id\n1\n", "id\n2\n");
    let plan = parse_sql("SELECT * FROM a RIGHT JOIN b ON id = id").unwrap();
    let schema = stream_execute::output_schema(&plan, &fixture.catalog, &StreamOptions::default()).unwrap();
    assert!(schema.fields()[0].nullable);
    assert!(schema.fields()[1].nullable);
}

#[test]
fn large_probe_with_tiny_batches_has_no_duplicate_unmatched_right_rows() {
    let left_rows = (1..=100).map(|i| i.to_string()).collect::<Vec<_>>().join("\n");
    let fixture = Fixture::new(&format!("id\n{left_rows}\n"), "id\n0\n100\n101\n");
    let rows = fixture.run("SELECT * FROM a RIGHT JOIN b ON id = id");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows.iter().filter(|row| row[1] == Value::Integer(101)).count(), 1);
}
