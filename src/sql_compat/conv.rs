use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, Int64Array, StringArray, StringBuilder},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn format_radix(mut value: u64, base: u32) -> String {
    const DIGITS: &[u8; 36] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut output = Vec::new();
    loop {
        output.push(DIGITS[(value % u64::from(base)) as usize] as char);
        value /= u64::from(base);
        if value == 0 {
            break;
        }
    }
    output.reverse();
    output.into_iter().collect()
}

/// Convert an integer string between radices 2 to 36.
/// A negative destination base produces signed output.
fn evaluate(text: &str, from_base: i64, to_base: i64) -> Option<String> {
    if !(2..=36).contains(&from_base) || !(2..=36).contains(&to_base.unsigned_abs()) {
        return None;
    }
    let text = text.trim();
    let negative = text.starts_with('-');
    let digits = text
        .strip_prefix('-')
        .or_else(|| text.strip_prefix('+'))
        .unwrap_or(text);
    let parsed = u64::from_str_radix(digits, from_base as u32).ok()?;
    let value = if negative {
        parsed.wrapping_neg()
    } else {
        parsed
    };
    let target = to_base.unsigned_abs() as u32;
    if to_base < 0 && (value as i64) < 0 {
        Some(format!(
            "-{}",
            format_radix((value as i64).unsigned_abs(), target)
        ))
    } else {
        Some(format_radix(value, target))
    }
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "conv",
        vec![DataType::Utf8, DataType::Int64, DataType::Int64],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let text = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("conv expects Utf8".into()))?;
            let from = arrays[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("conv expects Int64".into()))?;
            let to = arrays[2]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("conv expects Int64".into()))?;

            let mut result = StringBuilder::with_capacity(text.len(), text.value_data().len());
            for row in 0..text.len() {
                let value = if text.is_null(row) || from.is_null(row) || to.is_null(row) {
                    None
                } else {
                    evaluate(text.value(row), from.value(row), to.value(row))
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
    use crate::RustSparkSession;
    use datafusion::arrow::array::StringArray;

    #[test]
    fn supports_unsigned_signed_and_invalid_inputs() {
        assert_eq!(evaluate("FF", 16, 10), Some("255".into()));
        assert_eq!(evaluate("101", 2, 16), Some("5".into()));
        assert_eq!(evaluate("-1", 10, 16), Some("FFFFFFFFFFFFFFFF".into()));
        assert_eq!(evaluate("-1", 10, -16), Some("-1".into()));
        assert_eq!(evaluate("xyz", 10, 16), None);
        assert_eq!(evaluate("20", 1, 10), None);
    }

    #[tokio::test]
    async fn executes_conv_sql() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT CONV('FF', CAST(16 AS BIGINT), CAST(10 AS BIGINT))")
            .await?
            .collect()
            .await?;
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "255");
        Ok(())
    }
}
