use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Int64Array, StringArray},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark SQL ASCII compatibility.
fn evaluate(x0: &str) -> Option<i64> {
    Some(x0.as_bytes().first().map_or(0, |byte| i64::from(*byte)))
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "ascii",
        vec![DataType::Utf8],
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
                    DataFusionError::Internal("ascii expects Utf8".into())
                })?;
            let result: Int64Array = (0..input0.len())
                .map(|row| {
                    if input0.is_null(row) {
                        None
                    } else {
                        evaluate(input0.value(row))
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
        assert_eq!(evaluate("A"), Some(65));
        assert_eq!(evaluate("é"), Some(195));
        assert_eq!(evaluate(""), Some(0));
    }

    #[tokio::test]
    async fn sql_function_is_registered() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql("SELECT ASCII('A')")
            .await?
            .collect()
            .await?;
        let values = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 65);
        Ok(())
    }
}
