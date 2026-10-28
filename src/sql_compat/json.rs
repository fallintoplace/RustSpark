use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{BooleanArray, Int64Array, StringArray, StringBuilder},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

enum PathSegment<'a> {
    Field(&'a str),
    Index(usize),
}

/// A deliberately small JSON path subset: $, $.field, $[index], and $['quoted-key'].
fn parse_json_path(path: &str) -> Option<Vec<PathSegment<'_>>> {
    let mut rest = path.strip_prefix('$')?;
    let mut segments = Vec::new();
    while !rest.is_empty() {
        if let Some(after_dot) = rest.strip_prefix('.') {
            let end = after_dot
                .find(|character: char| character == '.' || character == '[')
                .unwrap_or(after_dot.len());
            let name = &after_dot[..end];
            if name.is_empty() || name == "*" {
                return None;
            }
            segments.push(PathSegment::Field(name));
            rest = &after_dot[end..];
        } else if let Some(after_bracket) = rest.strip_prefix('[') {
            if let Some(quote @ ('\'' | '"')) = after_bracket.chars().next() {
                let contents = &after_bracket[quote.len_utf8()..];
                let end = contents.find(quote)?;
                let name = &contents[..end];
                if name.is_empty() {
                    return None;
                }
                rest = contents[end + quote.len_utf8()..].strip_prefix(']')?;
                segments.push(PathSegment::Field(name));
            } else {
                let end = after_bracket.find(']')?;
                let index = after_bracket[..end].parse::<usize>().ok()?;
                segments.push(PathSegment::Index(index));
                rest = &after_bracket[end + 1..];
            }
        } else {
            return None;
        }
    }
    Some(segments)
}

fn extract_json(input: &str, path: &[PathSegment<'_>]) -> Option<String> {
    let root: serde_json::Value = serde_json::from_str(input).ok()?;
    let mut value = &root;
    for segment in path {
        value = match segment {
            PathSegment::Field(name) => value.as_object()?.get(*name)?,
            PathSegment::Index(index) => value.as_array()?.get(*index)?,
        };
    }
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::String(string) => Some(string.clone()),
        other => serde_json::to_string(other).ok(),
    }
}

fn query_json(input: &str, path: &str) -> Option<String> {
    let segments = parse_json_path(path)?;
    extract_json(input, &segments)
}

pub(crate) fn get_json_object_udf() -> ScalarUDF {
    create_udf(
        "get_json_object",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            let all_scalars = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("get_json_object expects UTF-8 JSON".into()))?;
            let paths = arrays[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("get_json_object expects UTF-8 path".into()))?;
            let mut result = StringBuilder::new();
            for (input, path) in input.iter().zip(paths.iter()) {
                match input.zip(path).and_then(|(input, path)| query_json(input, path)) {
                    Some(value) => result.append_value(value),
                    None => result.append_null(),
                }
            }
            let result = result.finish();
            if all_scalars {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    result.iter().next().flatten().map(str::to_owned),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }),
    )
}

pub(crate) fn json_valid_udf() -> ScalarUDF {
    create_udf(
        "json_valid",
        vec![DataType::Utf8],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(|args| {
            match args {
                [ColumnarValue::Scalar(ScalarValue::Utf8(value))] => {
                    Ok(ColumnarValue::Scalar(ScalarValue::Boolean(
                        value.as_deref().map(|text| serde_json::from_str::<serde_json::Value>(text).is_ok()),
                    )))
                }
                [ColumnarValue::Array(array)] => {
                    let input = array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| DataFusionError::Internal("json_valid expects UTF-8".into()))?;
                    let result: BooleanArray = input
                        .iter()
                        .map(|text| text.map(|text| serde_json::from_str::<serde_json::Value>(text).is_ok()))
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(result)))
                }
                _ => Err(DataFusionError::Internal("json_valid expects UTF-8".into())),
            }
        }),
    )
}

pub(crate) fn json_array_length_udf() -> ScalarUDF {
    create_udf(
        "json_array_length",
        vec![DataType::Utf8],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            fn length(input: &str) -> Option<i64> {
                let value: serde_json::Value = serde_json::from_str(input).ok()?;
                let array = value.as_array()?;
                i64::try_from(array.len()).ok()
            }
            match args {
                [ColumnarValue::Scalar(ScalarValue::Utf8(value))] => {
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                        value.as_deref().and_then(length),
                    )))
                }
                [ColumnarValue::Array(array)] => {
                    let input = array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| DataFusionError::Internal("json_array_length expects UTF-8".into()))?;
                    let result: Int64Array = input.iter().map(|value| value.and_then(length)).collect();
                    Ok(ColumnarValue::Array(Arc::new(result)))
                }
                _ => Err(DataFusionError::Internal("json_array_length expects UTF-8".into())),
            }
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::query_json;

    #[test]
    fn supports_fields_indices_and_quoted_keys() {
        let doc = r#"{"outer":{"items":[{"x":1},{"x":"two"}]},"hyphen-key":true}"#;
        assert_eq!(query_json(doc, "$.outer.items[1].x"), Some("two".into()));
        assert_eq!(query_json(doc, "$['hyphen-key']"), Some("true".into()));
        assert_eq!(query_json(doc, "$.outer.items[0]"), Some(r#"{"x":1}"#.into()));
        assert_eq!(query_json(doc, "$.outer.items[9]"), None);
    }

    #[test]
    fn rejects_malformed_json_and_unsupported_paths() {
        for path in ["x", "$.missing", "$.outer[*]", "$[not-an-index]", "$."] {
            assert!(query_json(r#"{"outer":[1,2]}"#, path).is_none(), "{path}");
        }
        assert!(query_json("{bad json}", "$").is_none());
        assert!(query_json(r#"{"null":null}"#, "$.null").is_none());
        assert_eq!(query_json(r#"{"x":1}"#, "$"), Some(r#"{"x":1}"#.into()));
    }
}
