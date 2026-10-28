use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, StringArray, StringBuilder},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// ASCII Soundex phonetic codes for approximate matching.
fn soundex_bytes(input: &str) -> Option<[u8; 4]> {
    fn digit(letter: u8) -> u8 {
        match letter {
            b'B' | b'F' | b'P' | b'V' => b'1',
            b'C' | b'G' | b'J' | b'K' | b'Q' | b'S' | b'X' | b'Z' => b'2',
            b'D' | b'T' => b'3',
            b'L' => b'4',
            b'M' | b'N' => b'5',
            b'R' => b'6',
            _ => b'0',
        }
    }
    let mut letters = input.bytes();
    let first = letters.next()?;
    if !first.is_ascii_alphabetic() {
        return None;
    }
    let first = first.to_ascii_uppercase();
    let mut result = [b'0'; 4];
    result[0] = first;
    let mut length = 1;
    let mut previous = digit(first);
    for letter in letters {
        // Spark treats nonletters as phonetic separators, not invisible bytes.
        if !letter.is_ascii_alphabetic() {
            previous = b'0';
            continue;
        }
        let letter = letter.to_ascii_uppercase();
        let code = digit(letter);
        if code == b'0' {
            if letter != b'H' && letter != b'W' {
                previous = b'0';
            }
            continue;
        }
        if code != previous {
            result[length] = code;
            length += 1;
            if length == 4 {
                break;
            }
        }
        previous = code;
    }
    Some(result)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "soundex",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Utf8(value))] = args {
                let encoded = value.as_deref().map(|name| {
                    soundex_bytes(name)
                        .map(|digits| std::str::from_utf8(&digits).unwrap().to_owned())
                        .unwrap_or_default()
                });
                return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(encoded)));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("soundex expects Utf8".into()))?;
            let mut result =
                StringBuilder::with_capacity(input.len(), input.len().saturating_mul(4));
            for value in input.iter() {
                match value {
                    None => result.append_null(),
                    Some(name) => match soundex_bytes(name) {
                        Some(digits) => result.append_value(std::str::from_utf8(&digits).unwrap()),
                        None => result.append_value(""),
                    },
                }
            }
            Ok(ColumnarValue::Array(Arc::new(result.finish())))
        }),
    )
}

#[cfg(test)]
mod internal_separator_regressions {
    use super::soundex_bytes;
    use crate::RustSparkSession;
    use datafusion::arrow::array::StringArray;

    #[test]
    fn separators_reset_duplicate_suppression() {
        assert_eq!(soundex_bytes("PP"), Some(*b"P000"));
        assert_eq!(soundex_bytes("P-P"), Some(*b"P100"));
        assert_eq!(soundex_bytes("P P"), Some(*b"P100"));
        assert_eq!(soundex_bytes("PH P"), Some(*b"P100"));
        assert_eq!(soundex_bytes("PWP"), Some(*b"P000"));
    }

    #[tokio::test]
    async fn sql_handles_punctuation_between_equal_consonants()
        -> datafusion::error::Result<()>
    {
        let rows = RustSparkSession::new()
            .sql("SELECT SOUNDEX('P-P'), SOUNDEX('PP')")
            .await?
            .collect()
            .await?;
        let first = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let second = rows[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(first.value(0), "P100");
        assert_eq!(second.value(0), "P000");
        Ok(())
    }
}
