use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, Float64Array, Int64Array, StringBuilder},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark FORMAT_NUMBER groups thousands in fixed-precision numeric text.
fn evaluate(number: f64, digits: i64) -> Option<String> {
    if !(0..=18).contains(&digits) {
        return None;
    }
    if !number.is_finite() {
        return Some(number.to_string());
    }
    let precision = digits as usize;
    let raw = format!("{number:.precision$}");
    let unsigned = raw.strip_prefix('-').unwrap_or(&raw);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let mut output = String::with_capacity(raw.len() + whole.len() / 3);
    if raw.starts_with('-') {
        output.push('-');
    }
    for (index, character) in whole.chars().enumerate() {
        if index > 0 && (whole.len() - index) % 3 == 0 {
            output.push(',');
        }
        output.push(character);
    }
    if !fraction.is_empty() {
        output.push('.');
        output.push_str(fraction);
    }
    Some(output)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "format_number",
        vec![DataType::Float64, DataType::Int64],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input0 = arrays[0]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("format_number expects Float64".into()))?;
            let input1 = arrays[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("format_number expects Int64".into()))?;
            let mut result =
                StringBuilder::with_capacity(input0.len(), input0.len().saturating_mul(16));
            for row in 0..input0.len() {
                let value = if input0.is_null(row) || input1.is_null(row) {
                    None
                } else {
                    evaluate(input0.value(row), input1.value(row))
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
    fn reference_vectors() {
        assert_eq!(evaluate(12345.67, 2), Some("12,345.67".into()));
        assert_eq!(evaluate(-12345.0, 0), Some("-12,345".into()));
        assert_eq!(evaluate(1.0, -1), None);
    }

    #[tokio::test]
    async fn sql_function_is_registered() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT FORMAT_NUMBER(CAST(12345.67 AS DOUBLE), CAST(2 AS BIGINT))")
            .await?
            .collect()
            .await?;
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "12,345.67");
        Ok(())
    }
}
