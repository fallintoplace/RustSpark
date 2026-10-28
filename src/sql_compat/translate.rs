use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, StringArray, StringBuilder},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Use Spark Unicode code-point translation and deletion, including literal NUL replacements.
fn evaluate(input: &str, matching: &str, replacement: &str) -> String {
    use std::collections::HashMap;

    if matching.is_empty() {
        return input.to_owned();
    }
    let mut dictionary = HashMap::new();
    let mut replacements = replacement.chars();
    for original in matching.chars() {
        let mapped = replacements.next();
        dictionary.entry(original).or_insert(mapped);
    }
    let mut output = String::with_capacity(input.len());
    for character in input.chars() {
        match dictionary.get(&character) {
            Some(Some(mapped)) => output.push(*mapped),
            Some(None) => {}
            None => output.push(character),
        }
    }
    output
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "translate",
        vec![DataType::Utf8, DataType::Utf8, DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal(
                    "translate expects Utf8".into(),
                ))?;
            let matching = arrays[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal(
                    "translate expects Utf8".into(),
                ))?;
            let replacement = arrays[2]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal(
                    "translate expects Utf8".into(),
                ))?;
            let mut output = StringBuilder::new();
            for row in 0..input.len() {
                let value = if input.is_null(row) || matching.is_null(row) || replacement.is_null(row) {
                    None
                } else {
                    Some(evaluate(input.value(row), matching.value(row), replacement.value(row)))
                };
                match value {
                    Some(value) => output.append_value(value),
                    None => output.append_null(),
                }
            }
            let output = output.finish();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    output.iter().next().flatten().map(str::to_owned),
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
    use crate::RustSparkSession;
    use datafusion::arrow::array::StringArray;

    #[test]
    fn spark_reference_cases() {
        assert_eq!(evaluate("abcde", "acd", "15"), "1b5e");
        assert_eq!(evaluate("AaBbCc", "abc", "1"), "A1BC");
        assert_eq!(evaluate("spark", "sa", "12"), "1p2rk");
        assert_eq!(evaluate("spark", "ss", "12"), "1park");
        assert_eq!(evaluate("café", "é", "e"), "cafe");
    }

    #[tokio::test]
    async fn sql_uses_spark_semantics() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql("SELECT TRANSLATE('AaBbCc', 'abc', '123')")
            .await?
            .collect()
            .await?;
        let column = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(column.value(0), "A1B2C3");
        Ok(())
    }
}
