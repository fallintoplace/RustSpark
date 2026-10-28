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
        "bin",
        vec![DataType::Int64],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Int64(value))] = args {
                let formatted = value.map(|number| {
                    if number < 0 {
                        format!("{:b}", number as u64)
                    } else {
                        format!("{number:b}")
                    }
                });
                return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(formatted)));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("bin expects Int64".into()))?;
            let mut result =
                StringBuilder::with_capacity(input.len(), input.len().saturating_mul(16));
            for value in input.iter() {
                if let Some(number) = value {
                    let written = if number < 0 {
                        write!(&mut result, "{:b}", number as u64)
                    } else {
                        write!(&mut result, "{number:b}")
                    };
                    written.expect("formatting into an Arrow string buffer cannot fail");
                    result.append_value("");
                } else {
                    result.append_null();
                }
            }
            Ok(ColumnarValue::Array(Arc::new(result.finish())))
        }),
    )
}
