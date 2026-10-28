use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Date32Array, Int32Array},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark DAYOFWEEK: Sunday is 1, Saturday is 7.
fn evaluate(days: i32) -> i32 {
    ((i64::from(days) + 4).rem_euclid(7) + 1) as i32
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "dayofweek",
        vec![DataType::Date32],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Date32(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int32(
                    value.map(evaluate),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("DAYOFWEEK expects DATE".into()))?;
            let result: Int32Array = input.iter().map(|value| value.map(evaluate)).collect();
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
    fn epoch_and_negative_days() {
        assert_eq!(evaluate(0), 5);
        assert_eq!(evaluate(-1), 4);
        assert_eq!(evaluate(3), 1);
    }

    #[tokio::test]
    async fn sql_scalar_and_null_input() -> datafusion::error::Result<()> {
        let session = RustSparkSession::new();
        let scalar = session
            .sql("SELECT DAYOFWEEK(DATE '2024-02-29') = 5")
            .await?
            .collect()
            .await?;
        let value = scalar[0].column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(value.iter().collect::<Vec<_>>(), vec![Some(true)]);

        let nulls = session
            .sql("SELECT DAYOFWEEK(CAST(NULL AS DATE)) IS NULL")
            .await?
            .collect()
            .await?;
        let value = nulls[0].column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(value.iter().collect::<Vec<_>>(), vec![Some(true)]);
        Ok(())
    }
}
