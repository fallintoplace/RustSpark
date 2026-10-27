//! Correlated multi-table existence filters that preserve source row counts.

use std::fmt::Write as _;

use datafusion::{
    error::{DataFusionError, Result},
    prelude::DataFrame,
};

use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};

/// Whether a source row must have (or must not have) a matching table row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExistenceMode {
    Exists,
    NotExists,
}

/// A source-to-table equality predicate used by an EXISTS or NOT EXISTS filter.
///
/// Keys are (source_column, matching_table_column) pairs. Multiple keys
/// must all match. SQL NULLs do not equal other NULLs.
#[derive(Clone, Debug)]
pub struct ExistenceFilter<'a> {
    pub table: &'a str,
    pub mode: ExistenceMode,
    pub keys: &'a [(&'a str, &'a str)],
}

impl RustSparkSession {
    /// Filter a registered table with any number of EXISTS/NOT EXISTS checks.
    ///
    /// Multiple checks are combined with AND. The source row appears at most
    /// once even when lookup tables contain duplicate matching keys, unlike
    /// an ordinary inner join. No input rows are collected during planning.
    pub async fn filter_table_by_existence(
        &self,
        source: &str,
        filters: &[ExistenceFilter<'_>],
    ) -> Result<DataFrame> {
        if filters.is_empty() {
            return Err(DataFusionError::Plan(
                "filter_table_by_existence requires at least one filter".into(),
            ));
        }

        let source_alias = "\"__rustspark_source\"";
        let mut query = format!(
            "SELECT {source_alias}.* FROM {} AS {source_alias} WHERE ",
            quote_catalog_table(source, "Invalid source table name")?
        );
        for (index, filter) in filters.iter().enumerate() {
            if filter.keys.is_empty() {
                return Err(DataFusionError::Plan(
                    "Every existence filter requires non-empty join keys".into(),
                ));
            }
            if index != 0 {
                query.push_str(" AND ");
            }
            let operator = match filter.mode {
                ExistenceMode::Exists => "EXISTS",
                ExistenceMode::NotExists => "NOT EXISTS",
            };
            let match_alias = quote_join_identifier(&format!("__rustspark_match_{index}"));
            let lookup = quote_catalog_table(filter.table, "Invalid existence filter table name")?;
            write!(
                &mut query, "{operator} (SELECT 1 FROM {lookup} AS {match_alias} WHERE "
            ).expect("writing to a String cannot fail");

            for (key_index, (source_key, match_key)) in filter.keys.iter().enumerate() {
                if source_key.trim().is_empty() || match_key.trim().is_empty() {
                    return Err(DataFusionError::Plan(
                        "Existence filter key names cannot be blank".into(),
                    ));
                }
                if key_index != 0 {
                    query.push_str(" AND ");
                }
                write!(
                    &mut query,
                    "{source_alias}.{} = {match_alias}.{}",
                    quote_join_identifier(source_key),
                    quote_join_identifier(match_key)
                ).expect("writing to a String cannot fail");
            }
            query.push(')');
        }
        self.sql(&query).await
    }
}
