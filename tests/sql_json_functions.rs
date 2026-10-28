use datafusion::arrow::array::{BooleanArray, Int64Array, StringArray};
use rustspark::RustSparkSession;

#[tokio::test]
async fn get_json_object_reads_nested_paths_and_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let rows = RustSparkSession::new()
        .sql(
            r#"SELECT GET_JSON_OBJECT(doc, '$.a[1].name') AS value
               FROM (VALUES
                   (0, '{"a":[{"name":"first"},{"name":"second"}]}'),
                   (1, '{"a":[]}'),
                   (2, '{invalid}'),
                   (3, CAST(NULL AS VARCHAR)))
               AS records(idx, doc)
               ORDER BY idx"#,
        )
        .await?
        .collect()
        .await?;
    let mut values = Vec::new();
    for batch in rows {
        let data = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        values.extend(data.iter().map(|value| value.map(str::to_owned)));
    }
    assert_eq!(values, vec![Some("second".into()), None, None, None]);
    Ok(())
}

#[tokio::test]
async fn json_valid_and_array_length_handle_arrays_and_invalid_input() -> Result<(), Box<dyn std::error::Error>> {
    let rows = RustSparkSession::new()
        .sql(
            r#"SELECT JSON_VALID(doc) AS valid, JSON_ARRAY_LENGTH(doc) AS length
               FROM (VALUES (0, '[1,2,3]'), (1, '[]'),
                   (2, '{"a":1}'), (3, '{bad}'), (4, CAST(NULL AS VARCHAR)))
               AS records(idx, doc)
               ORDER BY idx"#,
        )
        .await?
        .collect()
        .await?;
    let mut valid = Vec::new();
    let mut lengths = Vec::new();
    for batch in rows {
        let booleans = batch.column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
        let integers = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        valid.extend(booleans.iter());
        lengths.extend(integers.iter());
    }
    assert_eq!(valid, vec![Some(true), Some(true), Some(true), Some(false), None]);
    assert_eq!(lengths, vec![Some(3), Some(0), None, None, None]);
    Ok(())
}

#[tokio::test]
async fn json_functions_support_scalar_arguments() -> Result<(), Box<dyn std::error::Error>> {
    let rows = RustSparkSession::new()
        .sql(
            r#"SELECT GET_JSON_OBJECT('{"value":"ok"}', '$.value'),
                      JSON_VALID('{"x":1}'), JSON_ARRAY_LENGTH('[1,2]')"#,
        )
        .await?
        .collect()
        .await?;
    let values = rows[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
    let valid = rows[0].column(1).as_any().downcast_ref::<BooleanArray>().unwrap();
    let length = rows[0].column(2).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.value(0), "ok");
    assert!(valid.value(0));
    assert_eq!(length.value(0), 2);
    Ok(())
}
