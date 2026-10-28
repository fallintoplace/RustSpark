use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, Float64Array},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark RINT rounds floating point values using ties-to-even semantics.
fn evaluate(value: f64) -> Option<f64> {
    Some(value.round_ties_even())
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "rint",
        vec![DataType::Float64],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input0 = arrays[0]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("rint expects Float64".into()))?;
            let result: Float64Array = (0..input0.len())
                .map(|row| {
                    if input0.is_null(row) {
                        None
                    } else {
                        evaluate(input0.value(row))
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
    fn reference_vectors() {
        assert_eq!(evaluate(2.5), Some(2.0));
        assert_eq!(evaluate(3.5), Some(4.0));
        assert_eq!(evaluate(-2.5), Some(-2.0));
    }

    #[tokio::test]
    async fn sql_function_is_registered() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT RINT(CAST(2.5 AS DOUBLE))")
            .await?
            .collect()
            .await?;
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(values.value(0), 2.0);
        Ok(())
    }
}
