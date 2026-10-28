use std::sync::Arc;

use datafusion::{
    arrow::{
        array::Int64Array,
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark POSITIVE leaves a signed 64-bit integer unchanged.
fn evaluate(value: i64) -> Option<i64> {
    Some(value)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "positive",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| match args {
            [ColumnarValue::Scalar(ScalarValue::Int64(value))] => {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(value.and_then(evaluate))))
            }
            [ColumnarValue::Array(array)] => {
                if array.as_any().downcast_ref::<Int64Array>().is_none() {
                    return Err(DataFusionError::Internal("positive expects Int64".into()));
                }
                Ok(ColumnarValue::Array(Arc::clone(array)))
            }
            _ => Err(DataFusionError::Internal("positive expects Int64".into())),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::evaluate;
    use crate::RustSparkSession;
    use datafusion::arrow::array::Int64Array;

    #[test]
    fn reference_vectors() {
        assert_eq!(evaluate(12), Some(12));
        assert_eq!(evaluate(-5), Some(-5));
    }

    #[tokio::test]
    async fn sql_function_is_registered() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT POSITIVE(CAST(12 AS BIGINT))")
            .await?
            .collect()
            .await?;
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 12);
        Ok(())
    }
    #[tokio::test]
    async fn identity_keeps_nulls_and_signed_limits() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql("SELECT POSITIVE(v) FROM (VALUES (0, CAST(NULL AS BIGINT)), (1, CAST(-9223372036854775807 AS BIGINT)), (2, CAST(0 AS BIGINT))) AS t(id, v) ORDER BY id")
            .await?
            .collect()
            .await?;
        let values = rows[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), vec![None, Some(-9223372036854775807), Some(0)]);
        Ok(())
    }

}
