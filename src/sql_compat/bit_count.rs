use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, Int32Array, Int64Array},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Count the set bits of an Int64 using two's-complement representation.
/// Spark BIT_COUNT returns a 32-bit integer even when the argument is BIGINT.
pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "bit_count",
        vec![DataType::Int64],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Int64(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int32(
                    value.map(|value| i32::try_from(value.count_ones()).unwrap()),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("bit_count expects Int64".into()))?;
            // Retain the null-free integer kernel for large input batches.
            let result: Int32Array = if input.null_count() == 0 {
                Int32Array::from_iter_values(
                    input
                        .values()
                        .iter()
                        .map(|value| i32::try_from(value.count_ones()).unwrap()),
                )
            } else {
                input
                    .iter()
                    .map(|value| value.map(|value| i32::try_from(value.count_ones()).unwrap()))
                    .collect()
            };
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod no_nulls_regression {
    use crate::RustSparkSession;
    use datafusion::arrow::array::Int32Array;

    #[tokio::test]
    async fn counts_set_bits_on_non_nullable_int64_batches() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql("SELECT BIT_COUNT(v) FROM (VALUES (0, CAST(1 AS BIGINT)), (1, CAST(-1 AS BIGINT)), (2, CAST(0 AS BIGINT))) AS t(idx, v) ORDER BY idx")
            .await?.collect().await?;
        let mut actual = Vec::new();
        for batch in rows {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            actual.extend(column.iter());
        }
        assert_eq!(actual, vec![Some(1), Some(64), Some(0)]);
        Ok(())
    }
}
