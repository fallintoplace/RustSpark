use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, BinaryArray, StringArray, StringBuilder},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn evaluate(input0: &[u8], input1: &str) -> datafusion::error::Result<Option<String>> {
    match input1.to_ascii_lowercase().as_str() {
        "utf8" | "utf-8" => Ok(Some(String::from_utf8_lossy(input0).into_owned())),
        _ => Err(DataFusionError::Execution(
            "DECODE currently supports UTF-8 only".into(),
        )),
    }
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "decode",
        vec![DataType::Binary, DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let arg0 = arrays[0]
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| DataFusionError::Internal("decode expects Binary".into()))?;
            let arg1 = arrays[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("decode expects Utf8".into()))?;
            let mut result = StringBuilder::new();
            for row in 0..arg0.len() {
                let value = if arg0.is_null(row) || arg1.is_null(row) {
                    None
                } else {
                    evaluate(arg0.value(row), arg1.value(row))?
                };
                match value {
                    Some(value) => result.append_value(value),
                    None => result.append_null(),
                }
            }
            let result = result.finish();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    (!result.is_null(0)).then(|| result.value(0).to_owned()),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::evaluate;

    #[test]
    fn covers_valid_invalid_and_empty_values() {
        assert_eq!(evaluate(b"hello", "UTF-8").unwrap().as_deref(), Some("hello"));
        assert_eq!(evaluate(&[0xFF], "utf8").unwrap().as_deref(), Some("�"));
        assert!(evaluate(b"hello", "invalid").is_err());
    }
}
