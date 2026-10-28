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
            if let [
                ColumnarValue::Scalar(ScalarValue::Utf8(input)),
                ColumnarValue::Scalar(ScalarValue::Utf8(path)),
            ] = args
            {
                let value = input
                    .as_deref()
                    .zip(path.as_deref())
                    .and_then(|(input, path)| query_json(input, path));
                return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(value)));
            }
            // A literal JSON path is shared by the entire input batch.
            // Parse it once rather than once for every JSON document.
            if let [
                ColumnarValue::Array(column),
                ColumnarValue::Scalar(ScalarValue::Utf8(path)),
            ] = args
            {
                let input = column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal("get_json_object expects UTF-8 JSON".into())
                    })?;
                let parsed_path = path.as_deref().and_then(parse_json_path);
                let mut result = StringBuilder::new();
                for value in input.iter() {
                    let extracted = value.and_then(|value| {
                        parsed_path
                            .as_deref()
                            .and_then(|path| extract_json(value, path))
                    });
                    match extracted {
                        Some(value) => result.append_value(value),
                        None => result.append_null(),
                    }
                }
                return Ok(ColumnarValue::Array(Arc::new(result.finish())));
            }
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

// Only validate JSON syntax; building a Value tree allocates for every
// nested object, array and string, which JSON_VALID never needs.
fn is_valid_json(input: &str) -> bool {
    serde_json::from_str::<serde::de::IgnoredAny>(input).is_ok()
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
                        value.as_deref().map(|text| is_valid_json(text)),
                    )))
                }
                [ColumnarValue::Array(array)] => {
                    let input = array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| DataFusionError::Internal("json_valid expects UTF-8".into()))?;
                    let result: BooleanArray = input
                        .iter()
                        .map(|text| text.map(|text| is_valid_json(text)))
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(result)))
                }
                _ => Err(DataFusionError::Internal("json_valid expects UTF-8".into())),
            }
        }),
    )
}

// IgnoredAny consumes nested JSON without allocating per element or building a Value tree.
fn json_array_length(input: &str) -> Option<i64> {
    let elements: Vec<serde::de::IgnoredAny> = serde_json::from_str(input).ok()?;
    i64::try_from(elements.len()).ok()
}

pub(crate) fn json_array_length_udf() -> ScalarUDF {
    create_udf(
        "json_array_length",
        vec![DataType::Utf8],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            match args {
                [ColumnarValue::Scalar(ScalarValue::Utf8(value))] => {
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                        value.as_deref().and_then(json_array_length),
                    )))
                }
                [ColumnarValue::Array(array)] => {
                    let input = array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| DataFusionError::Internal("json_array_length expects UTF-8".into()))?;
                    let result: Int64Array = input.iter().map(|value| value.and_then(json_array_length)).collect();
                    Ok(ColumnarValue::Array(Arc::new(result)))
                }
                _ => Err(DataFusionError::Internal("json_array_length expects UTF-8".into())),
            }
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::{is_valid_json, json_array_length, query_json};

    #[test]
    fn json_array_length_streams_nested_values_without_parsing_a_value_tree() {
        assert_eq!(json_array_length(r#"[{"nested":[1,2]},null,true,42]"#), Some(4));
        assert_eq!(json_array_length("[]"), Some(0));
        assert_eq!(json_array_length("{}"), None);
        assert_eq!(json_array_length("null"), None);
        assert_eq!(json_array_length("[1,]"), None);
    }

    #[tokio::test]
    async fn constant_path_extracts_from_each_row_with_nulls() -> datafusion::error::Result<()> {
        use crate::RustSparkSession;
        use datafusion::arrow::array::StringArray;

        let batches = RustSparkSession::new()
            .sql(
                r#"SELECT GET_JSON_OBJECT(payload, '$.n')
                   FROM (VALUES (0, '{"n":"first"}'), (1, '{"n":"second"}'),
                                (2, '{"other":1}'), (3, CAST(NULL AS VARCHAR)),
                                (4, 'bad json'))
                        AS t(idx, payload) ORDER BY idx"#,
            )
            .await?
            .collect()
            .await?;
        let mut actual = Vec::new();
        for batch in batches {
            let column = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            actual.extend(column.iter().map(|value| value.map(str::to_owned)));
        }
        assert_eq!(
            actual,
            vec![Some("first".into()), Some("second".into()), None, None, None]
        );
        Ok(())
    }

    #[test]
    fn validates_without_materializing_nested_values() {
        for valid in [
            r#"{"nested":[{"x":"é"},true,null,17]}"#,
            r#" [1, 2, {"item": "value"}] "#,
            "null",
            r#""a string""#,
            "0.5",
        ] {
            assert!(is_valid_json(valid), "{valid}");
        }
        for invalid in [
            "",
            "{bad json}",
            r#"{"nested":[1,2}"#,
            "{} extra",
            r#""unterminated"#,
        ] {
            assert!(!is_valid_json(invalid), "{invalid}");
        }
    }

    #[tokio::test]
    async fn json_valid_preserves_sql_nulls_and_validation() -> datafusion::error::Result<()> {
        use datafusion::arrow::array::BooleanArray;

        let batches = crate::RustSparkSession::new()
            .sql(
                r#"SELECT JSON_VALID(payload)
                   FROM (VALUES (0, '{"nested":[1, {"ok":true}]}'),
                                (1, '{"nested":[1}'),
                                (2, CAST(NULL AS VARCHAR)),
                                (3, ' 0 '), (4, '"hello"'),
                                (5, '{} trailing')) AS t(idx, payload)
                   ORDER BY idx"#,
            )
            .await?
            .collect()
            .await?;
        let mut actual = Vec::new();
        for batch in batches {
            let column = batch.column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
            actual.extend(column.iter());
        }
        assert_eq!(
            actual,
            [Some(true), Some(false), None, Some(true), Some(true), Some(false)]
        );
        Ok(())
    }

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
