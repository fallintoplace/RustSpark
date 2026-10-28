use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Float64Array},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark SQL NANVL compatibility.
fn evaluate(x0: f64, x1: f64) -> Option<f64> {
    Some(if x0.is_nan() { x1 } else { x0 })
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "nanvl",
        vec![DataType::Float64, DataType::Float64],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input0 = arrays[0]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("nanvl expects Float64".into())
                })?;
            let input1 = arrays[1]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("nanvl expects Float64".into())
                })?;
            let result: Float64Array = (0..input0.len())
                .map(|row| {
                    if input0.is_null(row) || input1.is_null(row) {
                        None
                    } else {
                        evaluate(input0.value(row), input1.value(row))
                    }
                })
                .collect();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(
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
    use datafusion::arrow::array::Float64Array;

    #[test]
    fn spark_reference_cases() {
        assert_eq!(evaluate(f64::NAN, 7.0), Some(7.0));
        assert_eq!(evaluate(2.0, 7.0), Some(2.0));
    }

    #[tokio::test]
    async fn sql_function_is_registered() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql("SELECT NANVL(CAST(2.0 AS DOUBLE), CAST(7.0 AS DOUBLE))")
            .await?
            .collect()
            .await?;
        let values = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(values.value(0), 2.0);
        Ok(())
    }
}
