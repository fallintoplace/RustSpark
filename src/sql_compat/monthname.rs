use std::sync::Arc;

use chrono::Datelike;
use datafusion::{
    arrow::{array::{Date32Array, StringArray}, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

use crate::sql_compat_spark_dates;

/// Spark 4 abbreviated month names are English and independent of machine locale.
fn calculate(days: i32) -> Option<&'static str> {
    let date = sql_compat_spark_dates::date_from_days(days)?;
    Some(match date.month() {
        1 => "Jan",
        2 => "Feb",
        3 => "Mar",
        4 => "Apr",
        5 => "May",
        6 => "Jun",
        7 => "Jul",
        8 => "Aug",
        9 => "Sep",
        10 => "Oct",
        11 => "Nov",
        12 => "Dec",
        _ => return None,
    })
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "monthname",
        vec![DataType::Date32],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Date32(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    (*value).and_then(calculate).map(str::to_owned),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0].as_any().downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("monthname expects Date32".into()))?;
            let result: StringArray = input.iter().map(|v| v.and_then(calculate)).collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use crate::RustSparkSession;
    use datafusion::arrow::array::StringArray;

    #[tokio::test]
    async fn uses_english_abbreviations_and_preserves_nulls() -> datafusion::error::Result<()> {
        let query = "SELECT MONTHNAME(d) FROM (VALUES (0, DATE '2024-02-29'), (1, DATE '2024-12-01'), (2, CAST(NULL AS DATE))) AS t(id,d) ORDER BY id";
        let batches = RustSparkSession::new().sql(query).await?.collect().await?;
        let actual: Vec<_> = batches.iter().flat_map(|batch| {
            let values = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            values.iter().map(|v| v.map(str::to_owned)).collect::<Vec<_>>()
        }).collect();
        assert_eq!(actual, vec![Some("Feb".to_owned()), Some("Dec".to_owned()), None]);
        Ok(())
    }
}
