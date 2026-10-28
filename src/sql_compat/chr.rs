use std::{fmt::Write as _, sync::Arc};

use datafusion::{
    arrow::{
        array::{Array, Int64Array, StringBuilder},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark CHR converts an integer modulo 256 into a single character.
fn evaluate(value: i64) -> Option<String> {
    char::from_u32(value.rem_euclid(256) as u32).map(|character| character.to_string())
}

pub(crate) fn udf() -> ScalarUDF {
    udf_named("chr")
}

pub(crate) fn char_udf() -> ScalarUDF {
    udf_named("char")
}

fn udf_named(name: &str) -> ScalarUDF {
    create_udf(
        name,
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
                .ok_or_else(|| DataFusionError::Internal("chr expects Utf8".into()))?;
            let mut result =
                StringBuilder::with_capacity(input0.len(), input0.len().saturating_mul(16));
            for row in 0..input0.len() {
                if input0.is_null(row) {
                    result.append_null();
                } else {
                    let codepoint = input0.value(row).rem_euclid(256) as u32;
                    let character =
                        char::from_u32(codepoint).expect("code point between 0 and 255 is valid");
                    result
                        .write_char(character)
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
        assert_eq!(evaluate(65), Some("A".into()));
        assert_eq!(evaluate(321), Some("A".into()));
    }

    #[tokio::test]
    async fn sql_function_is_registered() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT CHR(CAST(65 AS BIGINT))")
            .await?
            .collect()
            .await?;
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "A");
        Ok(())
    }
}
