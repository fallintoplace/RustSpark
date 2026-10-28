//! Lazy star-schema joins with predictable dimension output names.

use std::{collections::HashSet, fmt::Write as _};

use datafusion::{
    error::{DataFusionError, Result},
    logical_expr::JoinType,
    prelude::DataFrame,
};

use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};

/// A dimension joined to a common fact table.
///
/// Keys are pairs of fact-table and dimension-table column names.
/// Selected dimension columns appear as alias__column in the result.
/// Join types are restricted to INNER and LEFT to preserve fact semantics.
#[derive(Debug, Clone)]
pub struct StarDimension<'a> {
    pub table: &'a str,
    pub alias: &'a str,
    pub join_type: JoinType,
    pub keys: &'a [(&'a str, &'a str)],
    pub columns: &'a [&'a str],
}

fn star_join_operator(kind: JoinType) -> Result<&'static str> {
    match kind {
        JoinType::Left => Ok("LEFT"),
        JoinType::Inner => Ok("INNER"),
        _ => Err(DataFusionError::Plan(
            "join_star_tables supports inner and left joins only".into(),
        )),
    }
}

impl RustSparkSession {
    /// Join one fact table to multiple dimensions without loading input rows.
    ///
    /// The fact columns are projected once. Selected dimension columns are
    /// prefixed with their aliases to avoid ambiguous join output columns.
    /// LEFT joins preserve unmatched facts; INNER joins filter unmatched facts.
    /// Duplicate dimension keys retain ordinary SQL join multiplicity.
    pub async fn join_star_tables(
        &self,
        fact_table: &str,
        dimensions: &[StarDimension<'_>],
    ) -> Result<DataFrame> {
        if dimensions.is_empty() {
            return Err(DataFusionError::Plan(
                "join_star_tables requires at least one dimension".into(),
            ));
        }
        let fact = "\"__rustspark_fact\"";
        let fact_frame = self.table(fact_table).await?;
        let fact_columns: HashSet<&str> = fact_frame.schema().fields()
            .iter().map(|f| f.name().as_str()).collect();
        let mut output_names: HashSet<String> = fact_columns.iter()
            .map(|name| name.to_ascii_lowercase()).collect();
        let mut seen_aliases: HashSet<String> = HashSet::from([
            "__rustspark_fact".to_string()
        ]);

        let mut query = format!(
            " FROM {} AS {fact}",
            quote_catalog_table(fact_table, "Invalid fact table name")?
        );
        let mut projection = format!("{fact}.*");

        for dimension in dimensions {
            if dimension.alias.trim().is_empty() {
                return Err(DataFusionError::Plan(
                    "Dimension alias cannot be blank".into()
                ));
            }
            if !seen_aliases.insert(dimension.alias.to_ascii_lowercase()) {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate star join alias: {}", dimension.alias
                )));
            }
            let operator = star_join_operator(dimension.join_type)?;
            if dimension.keys.is_empty() {
                return Err(DataFusionError::Plan(
                    "Every star join dimension needs at least one equality key".into()
                ));
            }
            if dimension.columns.is_empty() {
                return Err(DataFusionError::Plan(
                    "Every star join dimension needs at least one projected column".into()
                ));
            }
            let dimension_frame = self.table(dimension.table).await?;
            let dimension_columns: HashSet<&str> = dimension_frame.schema().fields()
                .iter().map(|f| f.name().as_str()).collect();
            let right_alias = quote_join_identifier(dimension.alias);
            let right_table = quote_catalog_table(
                dimension.table, "Invalid star dimension table name"
            )?;

            write!(&mut query, " {operator} JOIN {right_table} AS {right_alias} ON ")
                .expect("writing to a String cannot fail");
            for (index, (fact_key, dimension_key)) in dimension.keys.iter().enumerate() {
                if !fact_columns.contains(*fact_key) || !dimension_columns.contains(*dimension_key) {
                    return Err(DataFusionError::Plan(format!(
                        "Missing star join key: {fact_key} or {dimension_key}"
                    )));
                }
                if index > 0 {
                    query.push_str(" AND ");
                }
                write!(
                    &mut query, "{fact}.{} = {right_alias}.{}",
                    quote_join_identifier(fact_key), quote_join_identifier(dimension_key)
                ).expect("writing to a String cannot fail");
            }
            for column in dimension.columns {
                if !dimension_columns.contains(*column) {
                    return Err(DataFusionError::Plan(format!(
                        "Missing projected dimension column: {column}"
                    )));
                }
                let output_name = format!("{}__{column}", dimension.alias);
                if !output_names.insert(output_name.to_ascii_lowercase()) {
                    return Err(DataFusionError::Plan(format!(
                        "Duplicate star join output name: {output_name}"
                    )));
                }
                write!(
                    &mut projection, ", {right_alias}.{} AS {}",
                    quote_join_identifier(column), quote_join_identifier(&output_name)
                ).expect("writing to a String cannot fail");
            }
        }
        self.sql(&format!("SELECT {projection}{query}")).await
    }
}

#[cfg(test)]
mod tests {
    use super::{JoinType, star_join_operator};

    #[test]
    fn rejects_unsupported_star_join_kinds() {
        assert_eq!(star_join_operator(JoinType::Inner).unwrap(), "INNER");
        assert_eq!(star_join_operator(JoinType::Left).unwrap(), "LEFT");
        assert!(star_join_operator(JoinType::Full).is_err());
    }
}
