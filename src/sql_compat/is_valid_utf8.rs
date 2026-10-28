use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, BinaryArray, BooleanArray},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn evaluate(value: &[u8]) -> datafusion::error::Result<Option<bool>> {
    Ok(Some(std::str::from_utf8(value).is_ok()))
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "is_valid_utf8",
        vec![DataType::Binary],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| DataFusionError::Internal("is_valid_utf8 expects Binary".into()))?;

            let mut values = Vec::with_capacity(input.len());
            for row in 0..input.len() {
                values.push(if input.is_null(row) {
                    None
                } else {
                    evaluate(input.value(row))?
                });
            }
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(
                    values.into_iter().next().flatten(),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(values))))
            }
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::evaluate;

    #[test]
    fn handles_valid_invalid_and_empty_bytes() {
        assert_eq!(evaluate(b"Spark").unwrap(), Some(true));
        assert_eq!(evaluate(b"").unwrap(), Some(true));
        assert_eq!(evaluate(&[0xFF]).unwrap(), Some(false));
    }
}
