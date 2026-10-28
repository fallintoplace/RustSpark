//! Lazy multi-way equality joins for registered tables.

use std::{borrow::Cow, collections::HashSet, fmt::Write as _};

use datafusion::{
    error::{DataFusionError, Result},
    logical_expr::JoinType,
    prelude::DataFrame,
};

use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};

/// One table added to a multi-way join.
///
/// Each key tuple names an earlier alias, its key column, and a key column
/// on this step's table. Different steps may reference different earlier
/// aliases, including the original table. Repeat a table with a fresh alias
/// for self joins.
#[derive(Debug, Clone)]
pub struct MultiJoinStep<'a> {
    pub table: &'a str,
    pub alias: &'a str,
    pub join_type: JoinType,
    pub keys: &'a [(&'a str, &'a str, &'a str)],
}

fn join_operator(kind: JoinType) -> Result<&'static str> {
    match kind {
        JoinType::Inner => Ok("INNER"),
        JoinType::Left => Ok("LEFT"),
        JoinType::Right => Ok("RIGHT"),
        JoinType::Full => Ok("FULL OUTER"),
        _ => Err(DataFusionError::Plan(
            "join_many_tables supports inner and outer joins only".into(),
        )),
    }
}

fn append_join_step_predicates(
    query: &mut String,
    known_aliases: &HashSet<String>,
    right_table_alias: &str,
    keys: &[(&str, &str, &str)],
) -> Result<()> {
    for (index, (left_alias, left_key, right_key)) in keys.iter().enumerate() {
        let alias: Cow<'_, str> = if left_alias.bytes().any(|byte| byte.is_ascii_uppercase()) {
            Cow::Owned(left_alias.to_ascii_lowercase())
        } else {
            Cow::Borrowed(left_alias)
        };
        if !known_aliases.contains(alias.as_ref()) {
            return Err(DataFusionError::Plan(format!(
                "Join references unknown or future alias: {left_alias}"
            )));
        }
        if left_key.trim().is_empty() || right_key.trim().is_empty() {
            return Err(DataFusionError::Plan(
                "Join key column names cannot be blank".into(),
            ));
        }
        if index != 0 {
            query.push_str(" AND ");
        }
        write!(
            query,
            "{}.{} = {right_table_alias}.{}",
            quote_join_identifier(left_alias),
            quote_join_identifier(left_key),
            quote_join_identifier(right_key)
        )
        .expect("writing to a String cannot fail");
    }
    Ok(())
}

impl RustSparkSession {
    /// Build a lazy equality join across two or more registered tables.
    ///
    /// All table names, aliases and column names are quoted. Each ON clause
    /// refers to a previously introduced alias, so star and snowflake joins
    /// can use different source tables for each edge. Standard SQL NULL
    /// equality rules apply and duplicate matches are preserved.
    pub async fn join_many_tables(
        &self,
        base_table: &str,
        base_alias: &str,
        steps: &[MultiJoinStep<'_>],
    ) -> Result<DataFrame> {
        if steps.is_empty() {
            return Err(DataFusionError::Plan(
                "join_many_tables requires at least one join step".into(),
            ));
        }
        if base_alias.trim().is_empty() {
            return Err(DataFusionError::Plan("Base join alias cannot be blank".into()));
        }
        let mut aliases = HashSet::with_capacity(steps.len() + 1);
        aliases.insert(base_alias.to_ascii_lowercase());
        let mut query = format!(
            "SELECT * FROM {} AS {}",
            quote_catalog_table(base_table, "Invalid base join table name")?,
            quote_join_identifier(base_alias)
        );

        for step in steps {
            if step.alias.trim().is_empty() {
                return Err(DataFusionError::Plan("Join alias cannot be blank".into()));
            }
            let alias_key = step.alias.to_ascii_lowercase();
            if aliases.contains(&alias_key) {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate join alias: {}", step.alias
                )));
            }
            if step.keys.is_empty() {
                return Err(DataFusionError::Plan(
                    "Each multi-way join requires at least one equality key".into(),
                ));
            }
            let operator = join_operator(step.join_type)?;
            let table = quote_catalog_table(step.table, "Invalid joined table name")?;
            let right_alias = quote_join_identifier(step.alias);
            write!(&mut query, " {operator} JOIN {table} AS {right_alias} ON ")
                .expect("writing to a String cannot fail");

            append_join_step_predicates(&mut query, &aliases, &right_alias, step.keys)?;
            aliases.insert(alias_key);
        }
        self.sql(&query).await
    }
}
