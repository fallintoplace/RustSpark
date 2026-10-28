use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Date32Array, Int32Array},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn calendar(days: i64) -> (i32, i32, i32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    if month <= 2 {
        year += 1;
    }
    (year as i32, month as i32, day as i32)
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - if month <= 2 { 1 } else { 0 };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let shifted_month = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * shifted_month + 2) / 5 + day - 1;
    let doe = 365 * yoe + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Spark SQL WEEKOFYEAR for DATE values.
fn evaluate(days: i32) -> i32 {
    let iso_day = (i64::from(days) + 3).rem_euclid(7);
    let thursday = i64::from(days) + 3 - iso_day;
    let (year, _, _) = calendar(thursday);
    let jan4 = days_from_civil(i64::from(year), 1, 4);
    let week1_monday = jan4 - (jan4 + 3).rem_euclid(7);
    ((i64::from(days) - week1_monday).div_euclid(7) + 1) as i32
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "weekofyear",
        vec![DataType::Date32],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Date32(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int32(
                    value.map(evaluate),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("WEEKOFYEAR expects DATE".into())
                })?;
            let result: Int32Array =
                input.iter().map(|value| value.map(evaluate)).collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::evaluate;
    use crate::RustSparkSession;
    use datafusion::arrow::array::BooleanArray;

    #[test]
    fn epoch_and_boundary_reference() {
        assert_eq!(evaluate(0), 1);
        assert_eq!(evaluate(18_628), 53);
    }

    #[tokio::test]
    async fn scalar_and_null_sql() -> datafusion::error::Result<()> {
        let session = RustSparkSession::new();
        let rows = session
            .sql("SELECT WEEKOFYEAR(DATE '2024-02-29') = 9")
            .await?
            .collect()
            .await?;
        let values = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(true)]);

        let rows = session
            .sql("SELECT WEEKOFYEAR(CAST(NULL AS DATE)) IS NULL")
            .await?
            .collect()
            .await?;
        let values = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(true)]);
        Ok(())
    }
}
