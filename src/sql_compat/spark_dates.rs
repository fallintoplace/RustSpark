//! Calendar helpers shared by the Spark-compatible date SQL functions.
//! Arrow Date32 is a signed number of UTC days from 1970-01-01.

use chrono::{Datelike, Duration, NaiveDate};

fn epoch() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("the Unix epoch is valid")
}

pub(crate) fn date_from_days(days: i32) -> Option<NaiveDate> {
    epoch().checked_add_signed(Duration::days(i64::from(days)))
}

pub(crate) fn days_from_date(date: NaiveDate) -> Option<i32> {
    i32::try_from(date.signed_duration_since(epoch()).num_days()).ok()
}

pub(crate) fn last_day_of_month(date: NaiveDate) -> Option<NaiveDate> {
    let (year, month) = if date.month() == 12 {
        (date.year().checked_add(1)?, 1)
    } else {
        (date.year(), date.month() + 1)
    };
    NaiveDate::from_ymd_opt(year, month, 1)?.pred_opt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn day_conversion_handles_epoch_leap_days_and_negative_values() {
        assert_eq!(date_from_days(0), NaiveDate::from_ymd_opt(1970, 1, 1));
        assert_eq!(date_from_days(-1), NaiveDate::from_ymd_opt(1969, 12, 31));
        let leap_day = NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();
        assert_eq!(date_from_days(days_from_date(leap_day).unwrap()), Some(leap_day));
        assert_eq!(last_day_of_month(leap_day), Some(leap_day));
        assert_eq!(
            last_day_of_month(NaiveDate::from_ymd_opt(2023, 2, 1).unwrap()),
            NaiveDate::from_ymd_opt(2023, 2, 28)
        );
    }
}
