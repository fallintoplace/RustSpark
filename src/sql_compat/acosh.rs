use std::sync::Arc;

use datafusion::{
    arrow::{array::Float64Array, compute::kernels::arity::unary, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark SQL ACOSH, including NULL and floating-point edge cases.
pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "acosh",
        vec![DataType::Float64],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Float64(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Float64(
                    value.map(|x| x.acosh()),
                )));
            }
            let [ColumnarValue::Array(array)] = args else {
                return Err(DataFusionError::Internal("acosh expects a Float64 array".into()));
            };
            let values = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("acosh expects Float64".into()))?;
            let result: Float64Array = unary(values, |x| x.acosh());
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod array_fast_path_tests {
    use crate::RustSparkSession;
    use datafusion::arrow::array::Float64Array;

    #[tokio::test]
    async fn preserves_array_values_and_nulls() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql("SELECT ACOSH(v) FROM (VALUES (0, CAST(1.5 AS DOUBLE)), (1, CAST(NULL AS DOUBLE)), (2, CAST(2.0 AS DOUBLE))) AS t(idx, v) ORDER BY idx")
            .await?.collect().await?;
        let mut actual = Vec::new();
        for batch in rows {
            let values = batch.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
            actual.extend(values.iter());
        }
        assert_eq!(actual, vec![Some(1.5_f64.acosh()), None, Some(2.0_f64.acosh())]);
        Ok(())
    }
}
