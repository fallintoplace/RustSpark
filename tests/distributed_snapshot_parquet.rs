use std::{
    fs::{self, File},
    sync::Arc,
};

use datafusion::{
    arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    parquet::arrow::ArrowWriter,
};
use rustspark::{
    RustSparkSession,
    distributed_storage::{ReplicaStore, StorageNode},
};

#[tokio::test]
async fn replicated_snapshot_is_queryable_as_parquet() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let node = root.path().join("node");
    fs::create_dir(&node)?;
    let fixture = root.path().join("source.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    )?;
    let mut writer = ArrowWriter::try_new(File::create(&fixture)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    let store = ReplicaStore::new(vec![StorageNode::new("a", node)?], 1)?;
    store.put("part-1.parquet", &fs::read(fixture)?)?;
    store.create_snapshot("sales", 1, vec!["part-1.parquet".into()])?;

    let session = RustSparkSession::new();
    session
        .register_snapshot_parquet("sales", &store, "sales", 1, &root.path().join("shared"))
        .await?;
    let batches = session
        .sql("SELECT SUM(n) AS total FROM sales")
        .await?
        .collect()
        .await?;
    let totals = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(totals.value(0), 6);
    Ok(())
}
