use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, StringArray, StringBuilder},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Match Spark whitespace-only word boundaries instead of DataFusion punctuation boundaries.
fn evaluate(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut at_word_start = true;
    for character in input.chars() {
        if character.is_whitespace() {
            output.push(character);
            at_word_start = true;
        } else if at_word_start {
            output.extend(character.to_uppercase());
            at_word_start = false;
        } else {
            output.extend(character.to_lowercase());
        }
    }
    output
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "initcap",
        vec![DataType::Utf8],
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
                    "initcap expects Utf8".into(),
                ))?;
            let mut output = StringBuilder::new();
            for row in 0..input.len() {
                let value = if input.is_null(row) {
                    None
                } else {
                    Some(evaluate(input.value(row)))
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
        assert_eq!(evaluate("sPark sql"), "Spark Sql");
        assert_eq!(evaluate("hello_world"), "Hello_world");
        assert_eq!(evaluate("robert rose-smith"), "Robert Rose-smith");
        assert_eq!(evaluate("foo.bar/baz"), "Foo.bar/baz");
    }

    #[tokio::test]
    async fn sql_uses_spark_semantics() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql("SELECT INITCAP('hello_world')")
            .await?
            .collect()
            .await?;
        let column = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(column.value(0), "Hello_world");
        Ok(())
    }
}
