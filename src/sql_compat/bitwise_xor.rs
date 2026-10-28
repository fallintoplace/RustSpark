use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, Int64Array},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark BITWISE_XOR performs a signed 64-bit bitwise exclusive OR.
fn evaluate(left: i64, right: i64) -> Option<i64> {
    Some(left ^ right)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "bitwise_xor",
        vec![DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [
                ColumnarValue::Scalar(ScalarValue::Int64(left)),
                ColumnarValue::Scalar(ScalarValue::Int64(right)),
            ] = args
            {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                    left.zip(*right).and_then(|(left, right)| evaluate(left, right)),
                )));
            }
            if let [
                ColumnarValue::Scalar(ScalarValue::Int64(left)),
                ColumnarValue::Array(right),
            ] = args
            {
                let right = right
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFusionError::Internal("bitwise_xor expects Int64".into()))?;
                let result: Int64Array = right
                    .iter()
                    .map(|right| left.zip(right).and_then(|(left, right)| evaluate(left, right)))
                    .collect();
                return Ok(ColumnarValue::Array(Arc::new(result)));
            }
            if let [
                ColumnarValue::Array(left),
                ColumnarValue::Scalar(ScalarValue::Int64(right)),
            ] = args
            {
                let left = left
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFusionError::Internal("bitwise_xor expects Int64".into()))?;
                let result: Int64Array = left
                    .iter()
                    .map(|left| left.zip(*right).and_then(|(left, right)| evaluate(left, right)))
                    .collect();
                return Ok(ColumnarValue::Array(Arc::new(result)));
            }
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input0 = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("bitwise_xor expects Int64".into()))?;
            let input1 = arrays[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("bitwise_xor expects Int64".into()))?;
            let result: Int64Array = (0..input0.len())
                .map(|row| {
                    if input0.is_null(row) || input1.is_null(row) {
                        None
                    } else {
                        evaluate(input0.value(row), input1.value(row))
                    }
                })
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
    fn reference_vectors() {
        assert_eq!(evaluate(12, 10), Some(6));
        assert_eq!(evaluate(42, 42), Some(0));
    }

    #[tokio::test]
    async fn sql_function_is_registered() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT BITWISE_XOR(CAST(12 AS BIGINT), CAST(10 AS BIGINT))")
            .await?
            .collect()
            .await?;
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 6);
        Ok(())
    }
    #[tokio::test]
    async fn scalar_column_batches_preserve_nulls() -> datafusion::error::Result<()> {
        let session = RustSparkSession::new();
        let batches = session
            .sql("SELECT BITWISE_XOR(CAST(12 AS BIGINT), v) FROM (VALUES (0, CAST(10 AS BIGINT)), (1, CAST(NULL AS BIGINT))) AS t(id, v) ORDER BY id")
            .await?
            .collect()
            .await?;
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(6), None]);
        Ok(())
    }

}
