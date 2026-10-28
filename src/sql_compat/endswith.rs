use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{BooleanArray, StringArray},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "endswith",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(|args| {
            // A constant input string does not need an Arrow broadcast array.
            if let [
                ColumnarValue::Scalar(ScalarValue::Utf8(left)),
                ColumnarValue::Array(right),
            ] = args
            {
                let right = right
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| DataFusionError::Internal("endswith expects Utf8".into()))?;
                let result: BooleanArray = right
                    .iter()
                    .map(|pattern| {
                        left.as_deref()
                            .zip(pattern)
                            .map(|(value, pattern)| value.ends_with(pattern))
                    })
                    .collect();
                return Ok(ColumnarValue::Array(Arc::new(result)));
            }
            // Reuse the scalar right argument without making a repeated Arrow array.
            if let [
                ColumnarValue::Array(left),
                ColumnarValue::Scalar(ScalarValue::Utf8(rhs)),
            ] = args
            {
                let left = left
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| DataFusionError::Internal("endswith expects Utf8".into()))?;
                let result: BooleanArray = left
                    .iter()
                    .map(|value| {
                        value
                            .zip(rhs.as_deref())
                            .map(|(value, suffix)| value.ends_with(suffix))
                    })
                    .collect();
                return Ok(ColumnarValue::Array(Arc::new(result)));
            }
            let scalar = args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("endswith expects Utf8".into()))?;
            let suffix = arrays[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("endswith expects Utf8".into()))?;
            let result: BooleanArray = input
                .iter()
                .zip(suffix.iter())
                .map(|(value, suffix)| value.zip(suffix).map(|(s, p)| s.ends_with(p)))
                .collect();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(
                    result.iter().next().flatten(),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }),
    )
}

#[cfg(test)]
mod constant_left_regression {
    use crate::RustSparkSession;
    use datafusion::arrow::array::BooleanArray;

    #[tokio::test]
    async fn handles_constant_input_with_nullable_patterns() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql("SELECT ENDSWITH('prefix-value', p) FROM (VALUES (0, 'value'), (1, 'prefix'), (2, CAST(NULL AS VARCHAR)), (3, '')) AS t(idx, p) ORDER BY idx")
            .await?.collect().await?;
        let mut actual = Vec::new();
        for batch in rows {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            actual.extend(column.iter());
        }
        assert_eq!(actual, vec![Some(true), Some(false), None, Some(true)]);
        Ok(())
    }
}
