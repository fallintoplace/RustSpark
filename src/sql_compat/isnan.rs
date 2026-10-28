use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{BooleanArray, Float64Array},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark SQL ISNAN compatibility.
fn evaluate(x0: f64) -> Option<bool> {
    Some(x0.is_nan())
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "isnan",
        vec![DataType::Float64],
        DataType::Boolean,
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
                    DataFusionError::Internal("isnan expects Float64".into())
                })?;
            let result: BooleanArray = (0..input0.len())
                .map(|row| {
                    if input0.is_null(row) {
                        Some(false)
                    } else {
                        evaluate(input0.value(row))
                    }
                })
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
mod tests {
    use super::evaluate;
    use crate::RustSparkSession;
    use datafusion::arrow::array::BooleanArray;

    #[test]
    fn spark_reference_cases() {
        assert_eq!(evaluate(f64::NAN), Some(true));
        assert_eq!(evaluate(0.0), Some(false));
        assert_eq!(evaluate(f64::INFINITY), Some(false));
    }

    #[tokio::test]
    async fn sql_function_is_registered() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql("SELECT ISNAN(CAST(0.0 AS DOUBLE))")
            .await?
            .collect()
            .await?;
        let values = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(values.value(0), false);
        Ok(())
    }
}
