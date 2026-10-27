use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{StringArray},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// ASCII Soundex phonetic codes for approximate matching.
fn soundex(input: &str) -> String {
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
    let mut letters = input.bytes().filter(u8::is_ascii_alphabetic);
    let Some(first) = letters.next() else {
        return String::new();
    };
    let first = first.to_ascii_uppercase();
    let mut result = String::with_capacity(4);
    result.push(char::from(first));
    let mut previous = digit(first);
    for letter in letters {
        let letter = letter.to_ascii_uppercase();
        let code = digit(letter);
        if code == b'0' {
            if letter != b'H' && letter != b'W' {
                previous = b'0';
            }
            continue;
        }
        if code != previous {
            result.push(char::from(code));
            if result.len() == 4 {
                break;
            }
        }
        previous = code;
    }
    while result.len() < 4 {
        result.push('0');
    }
    result
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "soundex",
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
                .ok_or_else(|| DataFusionError::Internal("soundex expects Utf8".into()))?;
            let result: Vec<Option<String>> = input
                .iter()
                .map(|value| value.map(soundex))
                .collect();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    result.into_iter().next().flatten(),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(StringArray::from(result))))
            }
        }),
    )
}
