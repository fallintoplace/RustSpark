use std::sync::Arc;

use datafusion::{
    arrow::{array::Float64Array, compute::kernels::arity::unary, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark SQL EXPM1 with Float64 coercion and NULL propagation.
pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "expm1",
        vec![DataType::Float64],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Float64(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Float64(
                    value.map(|x| x.exp_m1()),
                )));
            }
            let [ColumnarValue::Array(array)] = args else {
                return Err(DataFusionError::Internal("expm1 expects a Float64 array".into()));
            };
            let values = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("expm1 expects Float64".into()))?;
            let result: Float64Array = unary(values, |x| x.exp_m1());
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
            .sql(
                "SELECT EXPM1(v) FROM (VALUES (0, CAST(0.25 AS DOUBLE)), (1, CAST(NULL AS DOUBLE)), (2, CAST(0.5 AS DOUBLE))) AS t(idx, v) ORDER BY idx",
            )
            .await?
            .collect()
            .await?;
        let mut actual = Vec::new();
        for batch in rows {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            actual.extend(values.iter());
        }
        assert_eq!(actual, vec![Some(0.25_f64.exp_m1()), None, Some(0.5_f64.exp_m1())]);
        Ok(())
    }
}
