use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, Int32Array, StringArray},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn evaluate(value0: &str) -> datafusion::error::Result<Option<i32>> {
    Ok(i32::try_from(value0.chars().count()).ok())
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "len",
        vec![DataType::Utf8],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input0 = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("len expects Utf8".into()))?;

            let mut values = Vec::with_capacity(input0.len());
            for row in 0..input0.len() {
                let value = if input0.is_null(row) {
                    None
                } else {
                    evaluate(input0.value(row))?
                };
                values.push(value);
            }
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(
                    values.into_iter().next().flatten(),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(Int32Array::from(values))))
            }
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::evaluate;

    #[test]
    fn matches_reference_vectors() {
        assert_eq!(evaluate("Spark SQL ").unwrap(), Some(10));
        assert_eq!(evaluate("é名").unwrap(), Some(2));
        assert_eq!(evaluate("").unwrap(), Some(0));
    }
}
