use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Int64Array, StringArray},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark FIND_IN_SET uses one-based positions in a comma-delimited list.
fn evaluate(needle: &str, list: &str) -> i64 {
    if needle.contains(',') {
        return 0;
    }
    list.split(',')
        .position(|entry| entry == needle)
        .map_or(0, |index| index as i64 + 1)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "find_in_set",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let needle = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("find_in_set expects Utf8".into()))?;
            let list = arrays[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("find_in_set expects Utf8".into()))?;
            let result: Int64Array = needle
                .iter()
                .zip(list.iter())
                .map(|(needle, list)| needle.zip(list).map(|(needle, list)| evaluate(needle, list)))
                .collect();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                    result.iter().next().flatten(),
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
    use datafusion::arrow::array::Int64Array;

    #[test]
    fn spark_reference_cases() {
        assert_eq!(evaluate("ab", "abc,b,ab,c,def"), 3);
        assert_eq!(evaluate("absent", "a,b"), 0);
        assert_eq!(evaluate("a,b", "a,b"), 0);
        assert_eq!(evaluate("", ",a"), 1);
        assert_eq!(evaluate("a", ""), 0);
    }

    #[tokio::test]
    async fn sql_handles_scalar_columns_and_nulls() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql("SELECT FIND_IN_SET(needle, words) FROM (VALUES (0, 'ab', 'a,ab,c'), (1, 'missing', 'a,b'), (2, 'a,b', 'a,b'), (3, CAST(NULL AS VARCHAR), 'a,b')) AS t(idx, needle, words) ORDER BY idx")
            .await?
            .collect()
            .await?;
        let result = rows[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result.iter().collect::<Vec<_>>(), vec![Some(2), Some(0), Some(0), None]);
        let scalar = RustSparkSession::new()
            .sql("SELECT FIND_IN_SET('b', 'a,b,c')")
            .await?
            .collect()
            .await?;
        let result = scalar[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result.value(0), 2);
        Ok(())
    }
}
