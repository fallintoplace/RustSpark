use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{BooleanArray, StringArray},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Luhn digit-checksum validation for text identifiers.
fn valid_luhn(input: &str) -> bool {
    if input.is_empty() || !input.bytes().all(|byte| byte.is_ascii_digit()) {
        return false;
    }
    let mut sum = 0_u32;
    for (index, byte) in input.bytes().rev().enumerate() {
        let mut digit = u32::from(byte - b'0');
        if index % 2 == 1 {
            digit *= 2;
            if digit > 9 {
                digit -= 9;
            }
        }
        sum = (sum + digit) % 10;
    }
    sum == 0
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "luhn_check",
        vec![DataType::Utf8],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("luhn_check expects Utf8".into()))?;
            let result: Vec<Option<bool>> = input
                .iter()
                .map(|value| value.map(valid_luhn))
                .collect();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(
                    result.into_iter().next().flatten(),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(result))))
            }
        }),
    )
}
