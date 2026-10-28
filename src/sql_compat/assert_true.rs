use std::sync::Arc;

use datafusion::{
    arrow::{array::{Array, BooleanArray, new_null_array}, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "assert_true",
        vec![DataType::Boolean],
        DataType::Null,
        Volatility::Volatile,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Boolean(value))] = args {
                if *value == Some(true) {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Null));
                }
                return Err(DataFusionError::Execution("ASSERT_TRUE condition failed".into()));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| DataFusionError::Internal("assert_true expects Boolean".into()))?;
            for value in input.iter() {
                if value != Some(true) {
                    return Err(DataFusionError::Execution("ASSERT_TRUE condition failed".into()));
                }
            }
            Ok(ColumnarValue::Array(new_null_array(&DataType::Null, input.len())))
        }),
    )
}

#[cfg(test)]
mod tests {
    use crate::RustSparkSession;

    #[tokio::test]
    async fn true_returns_null_and_false_raises() -> datafusion::error::Result<()> {
        let session = RustSparkSession::new();
        let batches = session.sql("SELECT ASSERT_TRUE(true)").await?.collect().await?;
        assert_eq!(batches[0].num_rows(), 1);
        assert!(session.sql("SELECT ASSERT_TRUE(false)").await?.collect().await.is_err());
        Ok(())
    }
}
