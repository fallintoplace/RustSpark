use std::{fmt::Write as _, sync::Arc};

use datafusion::{
    arrow::{
        array::{Int64Array, StringBuilder},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn scalar_spaces(value: i64) -> datafusion::error::Result<String> {
    if value <= 0 {
        return Ok(String::new());
    }
    let count = usize::try_from(value)
        .map_err(|_| DataFusionError::Execution("SPACE length overflows usize".into()))?;
    if count > 1_048_576 {
        return Err(DataFusionError::Execution(
            "SPACE length exceeds the 1048576-character limit".into(),
        ));
    }
    Ok(" ".repeat(count))
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "space",
        vec![DataType::Int64],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Int64(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    value.map(scalar_spaces).transpose()?,
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("space expects Int64".into()))?;
            const CHUNK: &str = "                                                                ";
            let mut result =
                StringBuilder::with_capacity(input.len(), input.len().saturating_mul(16));
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
                            result
                                .write_str(CHUNK)
                                .expect("writing to an Arrow string buffer cannot fail");
                        }
                        result
                            .write_str(&CHUNK[..count % CHUNK.len()])
                            .expect("writing to an Arrow string buffer cannot fail");
                        result.append_value("");
                    }
                }
            }
            Ok(ColumnarValue::Array(Arc::new(result.finish())))
        }),
    )
}
