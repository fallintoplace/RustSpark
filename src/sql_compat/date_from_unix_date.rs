use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Date32Array, Int64Array},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn evaluate(days: i64) -> Option<i32> {
    i32::try_from(days).ok()
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "date_from_unix_date",
        vec![DataType::Int64],
        DataType::Date32,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Int64(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Date32(
                    value.and_then(evaluate),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("Expected Int64".into()))?;
            let result: Date32Array = input
                .iter()
                .map(|value| value.and_then(evaluate))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::evaluate;
    use crate::RustSparkSession;
    use datafusion::arrow::array::BooleanArray;

    #[test]
    fn reference_vector() {
        assert_eq!(evaluate(-1), Some(-1));
    }

    #[tokio::test]
    async fn sql_scalar_and_null() -> datafusion::error::Result<()> {
        let session = RustSparkSession::new();
        let rows = session
            .sql("SELECT DATE_FROM_UNIX_DATE(1) = DATE '1970-01-02'")
            .await?
            .collect()
            .await?;
        let values = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(true)]);
        let rows = session
            .sql("SELECT DATE_FROM_UNIX_DATE(CAST(NULL AS BIGINT)) IS NULL")
            .await?
            .collect()
            .await?;
        let values = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(true)]);
        Ok(())
    }
}
