use std::{fmt::Write as _, sync::Arc};

use datafusion::{
    arrow::{
        array::{Array, Int64Array, StringBuilder},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark HEX formats a signed 64-bit integer as uppercase hexadecimal.
fn evaluate(value: i64) -> Option<String> {
    Some(format!("{value:X}"))
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "hex",
        vec![DataType::Int64],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Int64(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    value.and_then(evaluate),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input0 = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("hex expects Int64".into()))?;
            let mut result =
                StringBuilder::with_capacity(input0.len(), input0.len().saturating_mul(16));
            for row in 0..input0.len() {
                if input0.is_null(row) {
                    result.append_null();
                } else {
                    write!(&mut result, "{:X}", input0.value(row))
                        .expect("Arrow string writes cannot fail");
                    result.append_value("");
                }
            }
            let result = result.finish();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::evaluate;
    use crate::RustSparkSession;
    use datafusion::arrow::array::StringArray;

    #[test]
    fn reference_vectors() {
        assert_eq!(evaluate(255), Some("FF".into()));
        assert_eq!(evaluate(-1), Some("FFFFFFFFFFFFFFFF".into()));
    }

    #[tokio::test]
    async fn sql_function_is_registered() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT HEX(CAST(255 AS BIGINT))")
            .await?
            .collect()
            .await?;
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "FF");
        Ok(())
    }
}
