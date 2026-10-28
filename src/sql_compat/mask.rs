use std::{fmt::Write as _, sync::Arc};

use datafusion::{
    arrow::{
        array::{Array, StringArray, StringBuilder},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Default Spark MASK characters: X for uppercase, x for lowercase, n for digits.
fn mask_character(character: char) -> char {
    if character.is_uppercase() {
        'X'
    } else if character.is_lowercase() {
        'x'
    } else if character.is_numeric() {
        'n'
    } else {
        character
    }
}

fn mask_default(input: &str) -> String {
    input.chars().map(mask_character).collect()
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "mask",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Utf8(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    value.as_deref().map(mask_default),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("mask expects Utf8".into()))?;
            let mut result = StringBuilder::with_capacity(input.len(), input.value_data().len());
            for value in input.iter() {
                match value {
                    Some(value) => {
                        for character in value.chars() {
                            result
                                .write_char(mask_character(character))
                                .expect("writing into Arrow string buffer cannot fail");
                        }
                        result.append_value("");
                    }
                    None => result.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(result.finish())))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::mask_default;
    use crate::RustSparkSession;
    use datafusion::arrow::array::StringArray;

    #[test]
    fn masks_categories_but_retains_punctuation() {
        assert_eq!(mask_default("Ab3-é!"), "Xxn-x!");
        assert_eq!(mask_default(""), "");
    }

    #[tokio::test]
    async fn handles_nullable_column_values() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql(concat!(
                "SELECT MASK(value) FROM ",
                "(VALUES (0, 'Ab3'), (1, CAST(NULL AS VARCHAR)), (2, 'a-b')) ",
                "AS t(idx, value) ORDER BY idx"
            ))
            .await?
            .collect()
            .await?;
        let mut actual = Vec::new();
        for batch in rows {
            let values = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            actual.extend(values.iter().map(|value| value.map(str::to_owned)));
        }
        assert_eq!(actual, vec![Some("Xxn".into()), None, Some("x-x".into())]);
        Ok(())
    }
}
