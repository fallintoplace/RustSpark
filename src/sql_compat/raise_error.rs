use std::sync::Arc;

use datafusion::{
    arrow::datatypes::DataType,
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "raise_error",
        vec![DataType::Utf8],
        DataType::Null,
        Volatility::Volatile,
        Arc::new(|args| {
            let message = match args {
                [ColumnarValue::Scalar(ScalarValue::Utf8(message))] => {
                    message.as_deref().unwrap_or("NULL")
                }
                _ => "user-raised SQL exception",
            };
            Err(DataFusionError::Execution(format!("RAISE_ERROR: {message}")))
        }),
    )
}

#[cfg(test)]
mod tests {
    use crate::RustSparkSession;

    #[tokio::test]
    async fn error_function_fails_sql_query() {
        let session = RustSparkSession::new();
        let result = session.sql("SELECT RAISE_ERROR('bad input')").await;
        if let Ok(frame) = result {
            assert!(frame.collect().await.is_err());
        }
    }
}
