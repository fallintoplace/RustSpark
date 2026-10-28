use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, Int64Array, StringArray},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark SQL INSTR compatibility.
fn evaluate(x0: &str, x1: &str) -> Option<i64> {
    Some(x0.find(x1).map_or(0, |offset| {
        x0[..offset].chars().count() as i64 + 1
    }))
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "instr",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input0 = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("instr expects Utf8".into())
                })?;
            let input1 = arrays[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("instr expects Utf8".into())
                })?;
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
    fn spark_reference_cases() {
        assert_eq!(evaluate("café", "fé"), Some(3));
        assert_eq!(evaluate("spark", "no"), Some(0));
        assert_eq!(evaluate("spark", ""), Some(1));
    }

    #[tokio::test]
    async fn sql_function_is_registered() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql("SELECT INSTR('café', 'fé')")
            .await?
            .collect()
            .await?;
        let values = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 3);
        Ok(())
    }
}
