use std::sync::Arc;

use datafusion::{
    arrow::{array::Int64Array, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "nullifzero",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Int64(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                    value.filter(|number| *number != 0),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("nullifzero expects Int64".into()))?;
            let result: Int64Array = input
                .iter()
                .map(|value| value.filter(|number| *number != 0))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use crate::RustSparkSession;
    use datafusion::arrow::array::Int64Array;

    #[tokio::test]
    async fn preserves_zero_and_null_behavior() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT NULLIFZERO(v) FROM (VALUES (0, CAST(0 AS BIGINT)), (1, CAST(-5 AS BIGINT)), (2, CAST(NULL AS BIGINT))) AS t(idx, v) ORDER BY idx")
            .await?
            .collect()
            .await?;
        let mut actual = Vec::new();
        for batch in batches {
            let values = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            actual.extend(values.iter());
        }
        assert_eq!(actual, vec![None, Some(-5), None]);
        Ok(())
    }
}
