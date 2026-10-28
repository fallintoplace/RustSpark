use std::{fmt::Write as _, sync::Arc};

use datafusion::{
    arrow::{
        array::{Int64Array, StringBuilder},
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
            // Use one contiguous Arrow buffer instead of allocating a String per row.
            const CHUNK: &str = "                                                                ";
            let mut result = StringBuilder::with_capacity(input.len(), input.len().saturating_mul(16));
            for value in input.iter() {
                match value {
                    None => result.append_null(),
                    Some(n) if n <= 0 => result.append_value(""),
                    Some(n) => {
                        let count = usize::try_from(n).map_err(|_| {
                            DataFusionError::Execution("SPACE length overflows usize".into())
                        })?;
                        if count > 1_048_576 {
                            return Err(DataFusionError::Execution(
                                "SPACE length exceeds the 1048576-character limit".into(),
                            ));
                        }
                        for _ in 0..count / CHUNK.len() {
                            result.write_str(CHUNK).expect("writing to an Arrow string buffer cannot fail");
                        }
                        result.write_str(&CHUNK[..count % CHUNK.len()])
                            .expect("writing to an Arrow string buffer cannot fail");
                        result.append_value("");
                    }
                }
            }
            let result = result.finish();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    result.iter().next().flatten().map(str::to_owned),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }),
    )
}
