use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, BinaryArray, StringBuilder},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn evaluate(value: &[u8]) -> datafusion::error::Result<Option<String>> {
    let text = std::str::from_utf8(value)
        .map_err(|_| DataFusionError::Execution("validate_utf8: invalid UTF-8".into()))?;
    Ok(Some(text.to_owned()))
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "validate_utf8",
        vec![DataType::Binary],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| DataFusionError::Internal("validate_utf8 expects Binary".into()))?;

            let mut output = StringBuilder::new();
            for row in 0..input.len() {
                let value = if input.is_null(row) {
                    None
                } else {
                    evaluate(input.value(row))?
                };
                match value {
                    Some(value) => output.append_value(value),
                    None => output.append_null(),
                }
            }
            let output = output.finish();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    (!output.is_null(0)).then(|| output.value(0).to_owned()),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(output)))
            }
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::evaluate;

    #[test]
    fn handles_valid_invalid_and_empty_bytes() {
        assert_eq!(evaluate(b"Spark").unwrap().as_deref(), Some("Spark"));
        assert_eq!(evaluate(b"").unwrap().as_deref(), Some(""));
        assert!(evaluate(&[0xFF]).is_err());
    }
}
