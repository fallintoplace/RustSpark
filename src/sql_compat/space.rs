use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Int64Array, StringArray},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "space",
        vec![DataType::Int64],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("space expects Int64".into()))?;
            let mut result: Vec<Option<String>> = Vec::with_capacity(input.len());
            for value in input.iter() {
                let padded = match value {
                    None => None,
                    Some(n) if n <= 0 => Some(String::new()),
                    Some(n) => {
                        let count = usize::try_from(n).map_err(|_| {
                            DataFusionError::Execution("SPACE length overflows usize".into())
                        })?;
                        if count > 1_048_576 {
                            return Err(DataFusionError::Execution(
                                "SPACE length exceeds the 1048576-character limit".into(),
                            ));
                        }
                        Some(" ".repeat(count))
                    }
                };
                result.push(padded);
            }
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
