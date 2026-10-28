use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Date32Array, Int32Array},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn value(days: i32) -> Option<i32> {
    Some(days)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "unix_date",
        vec![DataType::Date32],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Date32(days))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int32(days.and_then(value))));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input0 = arrays[0]
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("unix_date expects Date32".into()))?;
            let result: Int32Array = input0.iter().map(|a| a.and_then(value)).collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::value;
    use crate::RustSparkSession;
    use datafusion::arrow::array::Int32Array;

    #[test]
    fn matches_spark_rules() {
        assert_eq!(value(0), Some(0));
        assert_eq!(value(-1), Some(-1));
    }

    #[tokio::test]
    async fn accessible_in_sql() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT unix_date(DATE '1969-12-31')")
            .await?
            .collect()
            .await?;
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.value(0), -1);
        Ok(())
    }
}
