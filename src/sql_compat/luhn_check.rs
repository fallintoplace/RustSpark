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
    let mut sum = 0_u64;
    let mut has_digits = false;
    for (index, byte) in input.bytes().rev().enumerate() {
        if !byte.is_ascii_digit() {
            return false;
        }
        has_digits = true;
        let mut digit = u64::from(byte - b'0');
        if index % 2 == 1 {
            digit *= 2;
            if digit > 9 {
                digit -= 9;
            }
        }
        sum += digit;
    }
    has_digits && sum % 10 == 0
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "luhn_check",
        vec![DataType::Utf8],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Utf8(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(
                    value.as_deref().map(valid_luhn),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("luhn_check expects Utf8".into()))?;
            let result: BooleanArray = input.iter().map(|value| value.map(valid_luhn)).collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::valid_luhn;

    #[test]
    fn validates_digits_and_rejects_invalid_input() {
        assert!(valid_luhn("79927398713"));
        assert!(!valid_luhn("79927398714"));
        assert!(valid_luhn("0"));
        assert!(!valid_luhn(""));
        assert!(!valid_luhn("7992 7398713"));
        assert!(!valid_luhn("79927398713x"));
        assert!(!valid_luhn("９９"));
    }
}
