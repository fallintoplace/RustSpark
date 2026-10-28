use std::sync::Arc;

use datafusion::{
    arrow::{array::Int64Array, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

const FACTORIALS: [i64; 21] = {
    let mut values = [1; 21];
    let mut index = 1;
    while index < values.len() {
        values[index] = values[index - 1] * index as i64;
        index += 1;
    }
    values
};

fn factorial(value: i64) -> Option<i64> {
    usize::try_from(value)
        .ok()
        .and_then(|index| FACTORIALS.get(index).copied())
}

/// Spark's FACTORIAL returns NULL outside the inclusive range 0..=20.
pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "factorial",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Int64(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                    value.and_then(factorial),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let values = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("factorial expects Int64".into()))?;
            let result: Int64Array = values.iter().map(|value| value.and_then(factorial)).collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::factorial;

    #[test]
    fn factorial_table_preserves_spark_boundaries() {
        assert_eq!(factorial(0), Some(1));
        assert_eq!(factorial(1), Some(1));
        assert_eq!(factorial(5), Some(120));
        assert_eq!(factorial(20), Some(2_432_902_008_176_640_000));
        assert_eq!(factorial(-1), None);
        assert_eq!(factorial(21), None);
    }
}
