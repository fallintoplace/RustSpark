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
        "oct",
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
                .ok_or_else(|| DataFusionError::Internal("oct expects Int64".into()))?;
            if scalar {
                return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    input.iter().next().flatten().map(|number| format!("{:o}", number as u64)),
                )));
            }
            // Write formatted digits directly into the Arrow string buffer.
            let mut result = StringBuilder::with_capacity(input.len(), input.len().saturating_mul(16));
            for value in input.iter() {
                if let Some(number) = value {
                    write!(&mut result, "{:o}", number as u64)
                        .expect("formatting into an Arrow string buffer cannot fail");
                    result.append_value("");
                } else {
                    result.append_null();
                }
            }
            Ok(ColumnarValue::Array(Arc::new(result.finish())))
        }),
    )
}
