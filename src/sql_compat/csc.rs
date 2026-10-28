use std::sync::Arc;

use datafusion::{
    arrow::{array::Float64Array, compute::kernels::arity::unary, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Provide cosecant for analytical SQL expressions.
pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "csc",
        vec![DataType::Float64],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Float64(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Float64(
                    value.map(|x| 1.0 / x.sin()),
                )));
            }
            let [ColumnarValue::Array(array)] = args else {
                return Err(DataFusionError::Internal(
                    "csc expects a Float64 array".into(),
                ));
            };
            let input = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("csc expects Float64".into()))?;
            let result: Float64Array = unary(input, |x| 1.0 / x.sin());
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
                "SELECT CSC(v) FROM (VALUES (0, CAST(0.25 AS DOUBLE)), (1, CAST(NULL AS DOUBLE)), (2, CAST(0.5 AS DOUBLE))) AS t(idx, v) ORDER BY idx",
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
        assert_eq!(
            actual,
            vec![Some(1.0 / 0.25_f64.sin()), None, Some(1.0 / 0.5_f64.sin())]
        );
        Ok(())
    }
}
