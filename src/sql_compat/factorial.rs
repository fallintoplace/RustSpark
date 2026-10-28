use std::sync::Arc;

use datafusion::{
    arrow::{array::Int64Array, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

const FACTORIALS: [i64; 21] = {
    let mut values = [1; 21];
    let mut index = 1;
    while index < values.len() {
        values[index] = values[index - 1] * index as i64;
        index += 1;
    }
    values
};

fn factorial(value: i64) -> Option<i64> {
    usize::try_from(value)
        .ok()
        .and_then(|index| FACTORIALS.get(index).copied())
}

/// Spark's FACTORIAL returns NULL outside the inclusive range 0..=20.
pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "factorial",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Int64(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                    value.and_then(factorial),
                )));
            }
            let [ColumnarValue::Array(array)] = args else {
                return Err(DataFusionError::Internal(
                    "factorial expects a Int64 array".into(),
                ));
            };
            let values = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("factorial expects Int64".into()))?;
            let result: Int64Array = values
                .iter()
                .map(|value| value.and_then(factorial))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::factorial;

    #[test]
    fn factorial_table_preserves_spark_boundaries() {
        assert_eq!(factorial(0), Some(1));
        assert_eq!(factorial(1), Some(1));
        assert_eq!(factorial(5), Some(120));
        assert_eq!(factorial(20), Some(2_432_902_008_176_640_000));
        assert_eq!(factorial(-1), None);
        assert_eq!(factorial(21), None);
    }
}

#[cfg(test)]
mod array_fast_path_tests {
    use crate::RustSparkSession;
    use datafusion::arrow::array::Int64Array;

    #[tokio::test]
    async fn handles_nullable_column_batches() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql(
                "SELECT FACTORIAL(v) FROM (VALUES (0, CAST(5 AS BIGINT)), (1, CAST(NULL AS BIGINT)), (2, CAST(21 AS BIGINT)), (3, CAST(0 AS BIGINT))) AS t(idx, v) ORDER BY idx",
            )
            .await?
            .collect()
            .await?;
        let mut actual = Vec::new();
        for batch in rows {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            actual.extend(values.iter());
        }
        assert_eq!(actual, vec![Some(120), None, None, Some(1)]);
        Ok(())
    }
}
