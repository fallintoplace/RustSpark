use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, Int64Array, StringArray, StringBuilder},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark returns an empty string for nonpositive LEFT or RIGHT lengths.
fn evaluate(text: &str, count: i64) -> String {
    if count <= 0 {
        return String::new();
    }
    let limit = usize::try_from(count).unwrap_or(usize::MAX);
    let char_count = text.chars().count();
    let skip = char_count.saturating_sub(limit);
    let start = text
        .char_indices()
        .nth(skip)
        .map_or(text.len(), |(byte, _)| byte);
    text[start..].to_owned()
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "right",
        vec![DataType::Utf8, DataType::Int64],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let text = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal(
                    "right expects Utf8".into(),
                ))?;
            let count = arrays[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal(
                    "right expects Int64".into(),
                ))?;
            let mut result = StringBuilder::new();
            for row in 0..text.len() {
                if text.is_null(row) || count.is_null(row) {
                    result.append_null();
                } else {
                    result.append_value(evaluate(
                        text.value(row),
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
        assert_eq!(evaluate("Spark SQL", -3), "");
        assert_eq!(evaluate("Spark SQL", 0), "");
        assert_eq!(evaluate("Spark SQL", 3), "SQL");
        assert_eq!(evaluate("café", 100), "café");
        assert_eq!(evaluate("café", 1), "é");
    }

    #[tokio::test]
    async fn sql_preserves_spark_nonpositive_semantics()
        -> datafusion::error::Result<()>
    {
        let rows = RustSparkSession::new()
            .sql("SELECT RIGHT('Spark SQL', -2)")
            .await?
            .collect()
            .await?;
        let column = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(column.value(0), "");
        Ok(())
    }
}
