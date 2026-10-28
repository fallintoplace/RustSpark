use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Date32Array, Int32Array},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn value(end: i32, start: i32) -> Option<i32> {
    end.checked_sub(start)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "datediff",
        vec![DataType::Date32, DataType::Date32],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Date32(end)),
                ColumnarValue::Scalar(ScalarValue::Date32(start))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int32(end.as_ref().and_then(|a| start.as_ref().and_then(|b| value(*a, *b))))));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input0 = arrays[0]
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("datediff expects Date32".into()))?;
            let input1 = arrays[1]
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("datediff expects Date32".into()))?;
            let result: Int32Array = input0.iter().zip(input1.iter()).map(|(a, b)| match (a, b) { (Some(a), Some(b)) => value(a, b), _ => None }).collect();
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
        assert_eq!(value(4, 0), Some(4));
        assert_eq!(value(-3, 2), Some(-5));
        assert_eq!(value(i32::MAX, -1), None);
    }

    #[tokio::test]
    async fn accessible_in_sql() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT datediff(DATE '1970-01-05', DATE '1970-01-01')")
            .await?
            .collect()
            .await?;
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.value(0), 4);
        Ok(())
    }
}
