use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, Float64Array},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark LOG1P computes the logarithm of one plus its floating point argument.
fn evaluate(value: f64) -> Option<f64> {
    Some(value.ln_1p())
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "log1p",
        vec![DataType::Float64],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Float64(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Float64(
                    value.and_then(evaluate),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input0 = arrays[0]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("log1p expects Float64".into()))?;
            let result: Float64Array = (0..input0.len())
                .map(|row| {
                    if input0.is_null(row) {
                        None
                    } else {
                        evaluate(input0.value(row))
                    }
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::evaluate;
    use crate::RustSparkSession;
    use datafusion::arrow::array::Float64Array;

    #[test]
    fn reference_vectors() {
        assert_eq!(evaluate(0.0), Some(0.0));
        assert!((evaluate(1e-10).unwrap() - 1e-10).abs() < 1e-18);
    }

    #[tokio::test]
    async fn sql_function_is_registered() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT LOG1P(CAST(0 AS DOUBLE))")
            .await?
            .collect()
            .await?;
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(values.value(0), 0.0);
        Ok(())
    }
    #[test]
    fn preserves_logarithm_domain_and_precision() {
        assert_eq!(evaluate(-1.0), Some(f64::NEG_INFINITY));
        assert!(evaluate(-2.0).unwrap().is_nan());
        assert_eq!(evaluate(0.0), Some(0.0));
    }

}
