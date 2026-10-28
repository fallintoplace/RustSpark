use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, Int64Array, StringArray, StringBuilder},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// The substring before the Nth separator, or after the Nth from the right.
fn evaluate<'a>(text: &'a str, delimiter: &str, count: i64) -> &'a str {
    if count == 0 || delimiter.is_empty() {
        return "";
    }
    if count > 0 {
        text.match_indices(delimiter)
            .nth((count - 1) as usize)
            .map_or(text, |(offset, _)| &text[..offset])
    } else {
        text.rmatch_indices(delimiter)
            .nth(count.unsigned_abs().saturating_sub(1) as usize)
            .map_or(text, |(offset, _)| &text[offset + delimiter.len()..])
    }
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "substring_index",
        vec![DataType::Utf8, DataType::Utf8, DataType::Int64],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let text = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("substring_index expects Utf8".into()))?;
            let delimiter = arrays[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("substring_index expects Utf8".into()))?;
            let count = arrays[2]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("substring_index expects Int64 count".into()))?;
            let mut result = StringBuilder::new();
            for row in 0..text.len() {
                if text.is_null(row) || delimiter.is_null(row) || count.is_null(row) {
                    result.append_null();
                } else {
                    result.append_value(evaluate(
                        text.value(row),
                        delimiter.value(row),
                        count.value(row),
                    ));
                }
            }
            let result = result.finish();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    result.iter().next().flatten().map(str::to_owned),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::evaluate;
    use crate::RustSparkSession;
    use datafusion::arrow::array::StringArray;

    #[test]
    fn spark_reference_cases() {
        assert_eq!(evaluate("www.apache.org", ".", 2), "www.apache");
        assert_eq!(evaluate("www.apache.org", ".", -1), "org");
        assert_eq!(evaluate("www.apache.org", ".", -2), "apache.org");
        assert_eq!(evaluate("a,b", ",", 5), "a,b");
        assert_eq!(evaluate("a,b", ",", -5), "a,b");
        assert_eq!(evaluate("a,b", ",", 0), "");
        assert_eq!(evaluate("a,b", "", 2), "");
        assert_eq!(evaluate("café-東京-z", "-", -2), "東京-z");
    }

    #[tokio::test]
    async fn sql_supports_scalar_and_nullable_columns() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql("SELECT SUBSTRING_INDEX(text, '.', n) FROM (VALUES (0, 'www.apache.org', 2), (1, 'a.b.c', -1), (2, CAST(NULL AS VARCHAR), 1)) AS t(idx, text, n) ORDER BY idx")
            .await?
            .collect()
            .await?;
        let values = rows[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some("www.apache"), Some("c"), None]);
        let rows = RustSparkSession::new()
            .sql("SELECT SUBSTRING_INDEX('a.b.c', '.', -2)")
            .await?
            .collect()
            .await?;
        let values = rows[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(values.value(0), "b.c");
        Ok(())
    }
}
