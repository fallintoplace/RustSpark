use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, BinaryBuilder, StringArray},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn evaluate(input0: &str, input1: &str) -> datafusion::error::Result<Option<Vec<u8>>> {
    match input1.to_ascii_lowercase().as_str() {
        "utf8" | "utf-8" => Ok(Some(input0.as_bytes().to_vec())),
        _ => Err(DataFusionError::Execution(
            "ENCODE currently supports UTF-8 only".into(),
        )),
    }
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "encode",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Binary,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let arg0 = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("encode expects Utf8".into()))?;
            let arg1 = arrays[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("encode expects Utf8".into()))?;
            let mut result = BinaryBuilder::new();
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
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(
                    (!result.is_null(0)).then(|| result.value(0).to_vec()),
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
        assert_eq!(evaluate("é", "UTF-8").unwrap(), Some(vec![0xC3, 0xA9]));
        assert_eq!(evaluate("", "utf8").unwrap(), Some(Vec::new()));
        assert!(evaluate("abc", "invalid").is_err());
    }
}
