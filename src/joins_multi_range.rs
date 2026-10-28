//! Join a fact table against multiple interval tables in one lazy query.

use std::{collections::HashSet, fmt::Write as _};

use datafusion::{
    error::{DataFusionError, Result},
    logical_expr::JoinType,
    prelude::DataFrame,
};

use crate::{RangeJoinBounds, RustSparkSession, quote_catalog_table, quote_join_identifier};

/// An interval table joined against a value in the original fact table.
///
/// Each step uses the same fact alias, not the result of the previous interval
/// step, so predicates always compare the fact value to this interval's bounds.
#[derive(Clone, Debug)]
pub struct RangeTableJoin<'a> {
    pub table: &'a str,
    pub alias: &'a str,
    pub join_type: JoinType,
    pub value_column: &'a str,
    pub lower_column: &'a str,
    pub upper_column: &'a str,
    pub bounds: RangeJoinBounds,
}

fn range_comparators(bounds: RangeJoinBounds) -> (&'static str, &'static str) {
    match bounds {
        RangeJoinBounds::Open => (">", "<"),
        RangeJoinBounds::Closed => (">=", "<="),
        RangeJoinBounds::LeftClosed => (">=", "<"),
        RangeJoinBounds::RightClosed => (">", "<="),
    }
}

impl RustSparkSession {
    /// Join facts to several interval tables using independently chosen bounds.
    ///
    /// Each step is INNER or LEFT. NULL values and endpoints do not match.
    /// LEFT joins preserve unmatched fact rows, while overlapping intervals
    /// may yield multiple rows per fact. No interval table is materialized.
    pub async fn join_many_range_tables(
        &self,
        fact_table: &str,
        fact_alias: &str,
        ranges: &[RangeTableJoin<'_>],
    ) -> Result<DataFrame> {
        if ranges.is_empty() {
            return Err(DataFusionError::Plan(
                "join_many_range_tables requires at least one interval table".into(),
            ));
        }
        if fact_alias.trim().is_empty() {
            return Err(DataFusionError::Plan(
                "Range join fact alias cannot be blank".into(),
            ));
        }
        let mut seen = HashSet::with_capacity(ranges.len() + 1);
        seen.insert(fact_alias.to_ascii_lowercase());
        let fact = quote_join_identifier(fact_alias);
        let mut query = format!(
            "SELECT * FROM {} AS {fact}",
            quote_catalog_table(fact_table, "Invalid range join fact table name")?
        );
        query.reserve(ranges.len().saturating_mul(128));

        for range in ranges {
            if range.alias.trim().is_empty() || !seen.insert(range.alias.to_ascii_lowercase()) {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate or blank interval join alias: {}", range.alias
                )));
            }
            if range.value_column.trim().is_empty()
                || range.lower_column.trim().is_empty()
                || range.upper_column.trim().is_empty()
            {
                return Err(DataFusionError::Plan(
                    "Range join columns cannot be blank".into(),
                ));
            }
            let operator = match range.join_type {
                JoinType::Inner => "INNER",
                JoinType::Left => "LEFT",
                _ => return Err(DataFusionError::Plan(
                    "join_many_range_tables supports inner and left joins only".into(),
                )),
            };
            let (lower_cmp, upper_cmp) = range_comparators(range.bounds);
            let interval_table = quote_catalog_table(
                range.table, "Invalid range join interval table name"
            )?;
            let alias = quote_join_identifier(range.alias);
            write!(
                &mut query,
                " {operator} JOIN {interval_table} AS {alias} ON {fact}.{} {lower_cmp} {alias}.{} AND {fact}.{} {upper_cmp} {alias}.{}",
                quote_join_identifier(range.value_column),
                quote_join_identifier(range.lower_column),
                quote_join_identifier(range.value_column),
                quote_join_identifier(range.upper_column)
            ).expect("writing to a String cannot fail");
        }
        self.sql(&query).await
    }
}

#[cfg(test)]
mod tests {
    use super::{RangeJoinBounds, range_comparators};

    #[test]
    fn interval_comparators_preserve_open_and_closed_endpoints() {
        assert_eq!(range_comparators(RangeJoinBounds::Open), (">", "<"));
        assert_eq!(range_comparators(RangeJoinBounds::Closed), (">=", "<="));
        assert_eq!(range_comparators(RangeJoinBounds::LeftClosed), (">=", "<"));
        assert_eq!(range_comparators(RangeJoinBounds::RightClosed), (">", "<="));
    }
}
