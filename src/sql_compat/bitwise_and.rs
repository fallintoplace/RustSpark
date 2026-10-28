use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, Int64Array},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark BITWISE_AND performs a signed 64-bit bitwise conjunction.
fn evaluate(left: i64, right: i64) -> Option<i64> {
    Some(left & right)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "bitwise_and",
        vec![DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input0 = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("bitwise_and expects Int64".into()))?;
            let input1 = arrays[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("bitwise_and expects Int64".into()))?;
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
        assert_eq!(evaluate(0b1100, 0b1010), Some(0b1000));
        assert_eq!(evaluate(-1, 42), Some(42));
    }

    #[tokio::test]
    async fn sql_function_is_registered() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT BITWISE_AND(CAST(12 AS BIGINT), CAST(10 AS BIGINT))")
            .await?
            .collect()
            .await?;
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 8);
        Ok(())
    }
}
