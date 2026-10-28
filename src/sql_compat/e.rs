use std::sync::Arc;

use datafusion::{
    arrow::datatypes::DataType,
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Euler's number for Spark SQL E().
pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "e",
        vec![],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(|args| {
            if !args.is_empty() {
                return Err(DataFusionError::Internal("e expects no arguments".into()));
            }
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(
                std::f64::consts::E,
            ))))
        }),
    )
}

#[cfg(test)]
mod tests {
    use crate::RustSparkSession;
    use datafusion::arrow::array::Float64Array;

    #[tokio::test]
    async fn e_is_available_as_sql_constant() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT E()")
            .await?
            .collect()
            .await?;
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(values.value(0), std::f64::consts::E);
        Ok(())
    }
}
