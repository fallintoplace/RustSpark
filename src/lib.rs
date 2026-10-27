use std::sync::Arc;

use datafusion::{
    arrow::record_batch::RecordBatch,
    error::{DataFusionError, Result},
    execution::runtime_env::RuntimeEnv,
    prelude::{DataFrame, SessionConfig, SessionContext},
};

mod export;
mod joins;
mod set_operations;
mod sources;

pub use export::SqlExportFormat;

fn quote_join_identifier(name: &str) -> String {
    let mut quoted = String::with_capacity(name.len() + 2);
    quoted.push('"');
    for character in name.chars() {
        if character == '"' {
            quoted.push('"');
        }
        quoted.push(character);
    }
    quoted.push('"');
    quoted
}

#[cfg(test)]
mod join_identifier_tests {
    use super::quote_join_identifier;

    #[test]
    fn quotes_names_without_intermediate_strings() {
        assert_eq!(quote_join_identifier("col"), "\"col\"");
        assert_eq!(quote_join_identifier("a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_join_identifier("é名\""), "\"é名\"\"\"");
        assert_eq!(quote_join_identifier(""), "\"\"");
    }
}

fn quote_catalog_table(path: &str, invalid_name_message: &str) -> Result<String> {
    let mut quoted = String::with_capacity(path.len() + 8);
    for (index, part) in path.split('.').enumerate() {
        if part.is_empty() || part.trim() != part {
            return Err(DataFusionError::Plan(invalid_name_message.into()));
        }
        if index != 0 {
            quoted.push('.');
        }
        quoted.push('"');
        for character in part.chars() {
            if character == '"' {
                quoted.push('"');
            }
            quoted.push(character);
        }
        quoted.push('"');
    }
    Ok(quoted)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NullDropMode {
    Any,
    All,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RangeJoinBounds {
    Open,
    Closed,
    LeftClosed,
    RightClosed,
}

pub struct RustSparkSession {
    context: SessionContext,
}

impl RustSparkSession {
    pub fn new() -> Self {
        Self::with_config(SessionConfig::new())
    }

    pub fn with_config(config: SessionConfig) -> Self {
        Self {
            context: SessionContext::new_with_config(config),
        }
    }

    pub fn from_context(context: SessionContext) -> Self {
        Self { context }
    }

    pub fn with_config_and_runtime(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self {
        Self {
            context: SessionContext::new_with_config_rt(config, runtime),
        }
    }

    pub async fn table_provider(
        &self,
        name: &str,
    ) -> Result<Arc<dyn datafusion::datasource::TableProvider>> {
        self.context.table_provider(name).await
    }

    pub fn register_table_provider(
        &self,
        name: &str,
        provider: Arc<dyn datafusion::datasource::TableProvider>,
    ) -> Result<()> {
        self.context.register_table(name, provider)?;
        Ok(())
    }

    pub fn list_catalogs(&self) -> Vec<String> {
        let mut catalogs = self.context.catalog_names();
        catalogs.sort();
        catalogs
    }

    fn sorted_catalog_entries(&self, include_tables: bool) -> Vec<String> {
        let mut entries = Vec::new();
        for catalog_name in self.context.catalog_names() {
            if let Some(catalog) = self.context.catalog(&catalog_name) {
                for schema_name in catalog.schema_names() {
                    if include_tables {
                        if let Some(schema) = catalog.schema(&schema_name) {
                            for table_name in schema.table_names() {
                                entries.push(format!("{catalog_name}.{schema_name}.{table_name}"));
                            }
                        }
                    } else {
                        entries.push(format!("{catalog_name}.{schema_name}"));
                    }
                }
            }
        }
        entries.sort();
        entries
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.sorted_catalog_entries(true)
    }

    pub fn list_schemas(&self) -> Vec<String> {
        self.sorted_catalog_entries(false)
    }

    pub fn list_tables_in_schema(&self, catalog: &str, schema: &str) -> Result<Vec<String>> {
        let catalog_provider = self
            .context
            .catalog(catalog)
            .ok_or_else(|| DataFusionError::Plan(format!("Unknown catalog: {catalog}")))?;
        let schema_provider = catalog_provider
            .schema(schema)
            .ok_or_else(|| DataFusionError::Plan(format!("Unknown schema: {catalog}.{schema}")))?;
        let mut tables = schema_provider.table_names();
        tables.sort();
        Ok(tables)
    }

    pub fn list_schemas_in_catalog(&self, catalog_name: &str) -> Result<Vec<String>> {
        let catalog = self
            .context
            .catalog(catalog_name)
            .ok_or_else(|| DataFusionError::Plan(format!("Unknown catalog: {catalog_name}")))?;
        let mut schemas = catalog.schema_names();
        schemas.sort();
        Ok(schemas)
    }

    pub fn read_batches(
        &self,
        batches: impl IntoIterator<Item = RecordBatch>,
    ) -> Result<DataFrame> {
        self.context.read_batches(batches)
    }

    pub async fn sql_stream(
        &self,
        query: &str,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        self.sql(query).await?.execute_stream().await
    }

    pub async fn sql_stream_with_params(
        &self,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        self.sql_with_params(query, params)
            .await?
            .execute_stream()
            .await
    }

    pub fn register_batches(
        &self,
        table: &str,
        batches: impl IntoIterator<Item = RecordBatch>,
    ) -> Result<()> {
        self.register_dataframe(table, self.read_batches(batches)?)
    }

    pub async fn table_stream(
        &self,
        table: &str,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        self.table(table).await?.execute_stream().await
    }

    async fn register_view_dataframe(
        &self,
        name: &str,
        dataframe: DataFrame,
        materialized: bool,
    ) -> Result<()> {
        let dataframe = if materialized {
            dataframe.cache().await?
        } else {
            dataframe
        };
        self.register_dataframe(name, dataframe)
    }

    pub async fn register_sql_view(&self, name: &str, query: &str) -> Result<()> {
        self.register_view_dataframe(name, self.sql_read_only(query).await?, false)
            .await
    }

    pub async fn register_materialized_sql_view(&self, name: &str, query: &str) -> Result<()> {
        self.register_view_dataframe(name, self.sql_read_only(query).await?, true)
            .await
    }

    pub async fn register_sql_view_with_params(
        &self,
        name: &str,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<()> {
        self.register_view_dataframe(
            name,
            self.sql_read_only_with_params(query, params).await?,
            false,
        )
        .await
    }

    pub async fn register_materialized_sql_view_with_params(
        &self,
        name: &str,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<()> {
        self.register_view_dataframe(
            name,
            self.sql_read_only_with_params(query, params).await?,
            true,
        )
        .await
    }

    pub async fn sql_snapshot(&self, query: &str) -> Result<DataFrame> {
        self.sql_read_only(query).await?.cache().await
    }

    pub async fn sql_snapshot_with_params(
        &self,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<DataFrame> {
        self.sql_read_only_with_params(query, params)
            .await?
            .cache()
            .await
    }

    pub fn register_dataframe(&self, table: &str, dataframe: DataFrame) -> Result<()> {
        if dataframe.task_ctx().session_id() != self.context.session_id() {
            return Err(DataFusionError::Plan(
                "DataFrame belongs to a different RustSpark session".into(),
            ));
        }

        self.context.register_table(table, dataframe.into_view())?;
        Ok(())
    }

    fn describe_columns(
        dataframe: &DataFrame,
    ) -> Vec<(String, datafusion::arrow::datatypes::DataType, bool)> {
        dataframe
            .schema()
            .fields()
            .iter()
            .map(|field| {
                (
                    field.name().to_owned(),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect()
    }

    pub async fn describe_table(
        &self,
        name: &str,
    ) -> Result<Vec<(String, datafusion::arrow::datatypes::DataType, bool)>> {
        let dataframe = self.table(name).await?;
        Ok(Self::describe_columns(&dataframe))
    }

    pub async fn column_names(&self, table: &str) -> Result<Vec<String>> {
        let dataframe = self.table(table).await?;
        Ok(dataframe
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_owned())
            .collect())
    }

    pub async fn column_extrema(&self, table: &str, column: &str) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[column])?;
        let source = quote_catalog_table(table, "column_extrema requires a table path")?;
        let key = quote_join_identifier(column);
        self.sql_read_only(&format!(
            "SELECT MIN({key}) AS min_value, MAX({key}) AS max_value FROM {source}"
        ))
        .await
    }

    pub async fn table_column_count(&self, table: &str) -> Result<usize> {
        Ok(self.table(table).await?.schema().fields().len())
    }

    pub async fn null_counts(&self, table: &str, columns: &[&str]) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        let selected: Vec<&str> = if columns.is_empty() {
            dataframe
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect()
        } else {
            columns.to_vec()
        };
        if selected.is_empty() {
            return Err(DataFusionError::Plan(
                "null_counts requires at least one column".into(),
            ));
        }
        Self::validate_selected_columns(&dataframe, &selected)?;

        let expressions = selected
            .iter()
            .map(|name| {
                let quoted = quote_join_identifier(name);
                format!("COUNT(*) - COUNT({quoted}) AS {quoted}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        let source = quote_catalog_table(table, "null_counts requires a table name")?;
        self.sql(&format!("SELECT {expressions} FROM {source}"))
            .await
    }

    pub async fn describe_sql(
        &self,
        query: &str,
    ) -> Result<Vec<(String, datafusion::arrow::datatypes::DataType, bool)>> {
        let dataframe = self.sql_read_only(query).await?;
        Ok(Self::describe_columns(&dataframe))
    }

    pub async fn describe_sql_with_params(
        &self,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<Vec<(String, datafusion::arrow::datatypes::DataType, bool)>> {
        let dataframe = self.sql_read_only_with_params(query, params).await?;
        Ok(Self::describe_columns(&dataframe))
    }

    fn validate_named_columns(
        dataframe: &DataFrame,
        columns: &[&str],
        operation: &str,
    ) -> Result<()> {
        use std::collections::HashSet;

        let fields = dataframe.schema().fields();
        let indexed = (columns.len() > 8).then(|| {
            fields
                .iter()
                .map(|field| field.name().as_str())
                .collect::<HashSet<_>>()
        });
        let mut seen = HashSet::new();
        for &name in columns {
            if !seen.insert(name.to_ascii_lowercase()) {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate {operation} column: {name}"
                )));
            }
            let present = match &indexed {
                Some(indexed) => indexed.contains(name),
                None => fields.iter().any(|field| field.name().as_str() == name),
            };
            if !present {
                return Err(DataFusionError::Plan(format!(
                    "Unknown {operation} column: {name}"
                )));
            }
        }
        Ok(())
    }

    fn validate_selected_columns(dataframe: &DataFrame, columns: &[&str]) -> Result<()> {
        Self::validate_named_columns(dataframe, columns, "selected")
    }

    pub async fn select_columns(&self, table: &str, columns: &[&str]) -> Result<DataFrame> {
        if columns.is_empty() {
            return Err(DataFusionError::Plan(
                "select_columns requires at least one column".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, columns)?;
        dataframe.select_columns(columns)
    }

    pub async fn select_columns_except(&self, table: &str, excluded: &[&str]) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        if excluded.is_empty() {
            return Ok(dataframe);
        }
        Self::validate_selected_columns(&dataframe, excluded)?;
        let remaining: Vec<String> = dataframe
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .filter(|name| !excluded.contains(name))
            .map(str::to_owned)
            .collect();
        if remaining.is_empty() {
            return Err(DataFusionError::Plan(
                "select_columns_except would remove every column".into(),
            ));
        }
        let names: Vec<&str> = remaining.iter().map(String::as_str).collect();
        dataframe.select_columns(&names)
    }

    pub async fn distinct_column_values(&self, table: &str, column: &str) -> Result<DataFrame> {
        self.select_columns(table, &[column]).await?.distinct()
    }

    pub async fn distinct_columns(&self, table: &str, columns: &[&str]) -> Result<DataFrame> {
        if columns.is_empty() {
            return Err(DataFusionError::Plan(
                "distinct_columns requires at least one column".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, columns)?;
        dataframe.select_columns(columns)?.distinct()
    }

    pub async fn select_expressions(
        &self,
        table: &str,
        expressions: &[datafusion::logical_expr::Expr],
    ) -> Result<DataFrame> {
        if expressions.is_empty() {
            return Err(DataFusionError::Plan(
                "select_expressions requires at least one expression".into(),
            ));
        }
        self.table(table).await?.select(expressions.to_vec())
    }

    fn validate_dropna_columns(dataframe: &DataFrame, selected: &[&str]) -> Result<()> {
        Self::validate_named_columns(dataframe, selected, "dropna")
    }

    pub async fn drop_null_rows(
        &self,
        table: &str,
        columns: &[&str],
        mode: NullDropMode,
    ) -> Result<DataFrame> {
        use datafusion::{common::Column, logical_expr::Expr};

        let dataframe = self.table(table).await?;
        let selected: Vec<&str> = if columns.is_empty() {
            dataframe
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect()
        } else {
            columns.to_vec()
        };
        if selected.is_empty() {
            return Ok(dataframe);
        }

        Self::validate_dropna_columns(&dataframe, &selected)?;

        let mut checks = selected
            .iter()
            .map(|name| Expr::Column(Column::from_name(*name)).is_not_null());
        let first = checks.next().expect("non-empty selected columns");
        let predicate = checks.fold(first, |acc, check| match mode {
            NullDropMode::Any => acc.and(check),
            NullDropMode::All => acc.or(check),
        });
        dataframe.filter(predicate)
    }

    pub async fn count_null_rows(
        &self,
        table: &str,
        columns: &[&str],
        mode: NullDropMode,
    ) -> Result<usize> {
        self.filter_null_rows(table, columns, mode)
            .await?
            .count()
            .await
    }

    pub async fn filter_null_rows(
        &self,
        table: &str,
        columns: &[&str],
        mode: NullDropMode,
    ) -> Result<DataFrame> {
        use datafusion::{common::Column, logical_expr::Expr};

        let dataframe = self.table(table).await?;
        let selected: Vec<&str> = if columns.is_empty() {
            dataframe
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect()
        } else {
            columns.to_vec()
        };
        if selected.is_empty() {
            return Ok(dataframe);
        }
        Self::validate_dropna_columns(&dataframe, &selected)?;

        let mut checks = selected
            .iter()
            .map(|name| Expr::Column(Column::from_name(*name)).is_null());
        let first = checks.next().expect("non-empty selected columns");
        let predicate = checks.fold(first, |acc, check| match mode {
            NullDropMode::Any => acc.or(check),
            NullDropMode::All => acc.and(check),
        });
        dataframe.filter(predicate)
    }

    pub async fn filter_rows_all(
        &self,
        table: &str,
        predicates: &[datafusion::logical_expr::Expr],
    ) -> Result<DataFrame> {
        let mut predicates = predicates.iter().cloned();
        let first = predicates
            .next()
            .ok_or_else(|| DataFusionError::Plan("filter_rows_all requires a predicate".into()))?;
        self.table(table)
            .await?
            .filter(predicates.fold(first, |combined, predicate| combined.and(predicate)))
    }

    pub async fn filter_rows_any(
        &self,
        table: &str,
        predicates: &[datafusion::logical_expr::Expr],
    ) -> Result<DataFrame> {
        let mut predicates = predicates.iter().cloned();
        let first = predicates
            .next()
            .ok_or_else(|| DataFusionError::Plan("filter_rows_any requires a predicate".into()))?;
        self.table(table)
            .await?
            .filter(predicates.fold(first, |combined, predicate| combined.or(predicate)))
    }

    pub async fn filter_rows(
        &self,
        table: &str,
        predicate: datafusion::logical_expr::Expr,
    ) -> Result<DataFrame> {
        self.table(table).await?.filter(predicate)
    }

    pub async fn filter_rows_equal_value(
        &self,
        table: &str,
        column: &str,
        value: datafusion::common::ScalarValue,
    ) -> Result<DataFrame> {
        use datafusion::{common::Column, logical_expr::Expr, prelude::lit};

        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[column])?;
        let key = Expr::Column(Column::from_name(column));
        let predicate = if value.is_null() {
            key.is_null()
        } else {
            key.eq(lit(value))
        };
        dataframe.filter(predicate)
    }

    pub async fn filter_rows_between(
        &self,
        table: &str,
        column: &str,
        lower: datafusion::common::ScalarValue,
        upper: datafusion::common::ScalarValue,
    ) -> Result<DataFrame> {
        use datafusion::{common::Column, logical_expr::Expr, prelude::lit};

        if lower.is_null() || upper.is_null() {
            return Err(DataFusionError::Plan(
                "filter_rows_between requires non-NULL bounds".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[column])?;
        let key = Expr::Column(Column::from_name(column));
        dataframe.filter(key.clone().gt_eq(lit(lower)).and(key.lt_eq(lit(upper))))
    }

    pub async fn filter_rows_not_equal_value(
        &self,
        table: &str,
        column: &str,
        value: datafusion::common::ScalarValue,
    ) -> Result<DataFrame> {
        use datafusion::{common::Column, logical_expr::Expr, prelude::lit};

        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[column])?;
        let key = Expr::Column(Column::from_name(column));
        let predicate = if value.is_null() {
            key.is_not_null()
        } else {
            key.not_eq(lit(value))
        };
        dataframe.filter(predicate)
    }

    pub async fn filter_rows_in_values(
        &self,
        table: &str,
        column: &str,
        values: &[datafusion::common::ScalarValue],
    ) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[column])?;
        if values.is_empty() {
            return dataframe.filter(datafusion::prelude::lit(false));
        }
        use datafusion::{common::Column, logical_expr::Expr};
        let list = values
            .iter()
            .cloned()
            .map(datafusion::prelude::lit)
            .collect();
        dataframe.filter(Expr::Column(Column::from_name(column)).in_list(list, false))
    }

    pub async fn filter_rows_not_in_values(
        &self,
        table: &str,
        column: &str,
        values: &[datafusion::common::ScalarValue],
    ) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[column])?;
        if values.is_empty() {
            return Ok(dataframe);
        }
        use datafusion::{common::Column, logical_expr::Expr};
        let list = values
            .iter()
            .cloned()
            .map(datafusion::prelude::lit)
            .collect();
        dataframe.filter(Expr::Column(Column::from_name(column)).in_list(list, true))
    }

    pub fn register_udf(&self, udf: datafusion::logical_expr::ScalarUDF) {
        self.context.register_udf(udf);
    }

    pub async fn filter_rows_like(
        &self,
        table: &str,
        column: &str,
        pattern: &str,
    ) -> Result<DataFrame> {
        use datafusion::{common::Column, logical_expr::Expr, prelude::lit};

        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[column])?;
        dataframe.filter(Expr::Column(Column::from_name(column)).like(lit(pattern)))
    }

    pub async fn filter_rows_ilike(
        &self,
        table: &str,
        column: &str,
        pattern: &str,
    ) -> Result<DataFrame> {
        use datafusion::{common::Column, logical_expr::Expr, prelude::lit};

        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[column])?;
        dataframe.filter(Expr::Column(Column::from_name(column)).ilike(lit(pattern)))
    }

    pub fn register_udaf(&self, udf: datafusion::logical_expr::AggregateUDF) {
        self.context.register_udaf(udf);
    }

    pub fn register_udwf(&self, udf: datafusion::logical_expr::WindowUDF) {
        self.context.register_udwf(udf);
    }

    pub async fn filter_rows_outside_range(
        &self,
        table: &str,
        column: &str,
        lower: datafusion::common::ScalarValue,
        upper: datafusion::common::ScalarValue,
    ) -> Result<DataFrame> {
        use datafusion::{common::Column, logical_expr::Expr, prelude::lit};

        if lower.is_null() || upper.is_null() {
            return Err(DataFusionError::Plan(
                "filter_rows_outside_range requires non-NULL bounds".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[column])?;
        let key = Expr::Column(Column::from_name(column));
        dataframe.filter(key.clone().lt(lit(lower)).or(key.gt(lit(upper))))
    }

    pub fn deregister_table(&self, name: &str) -> Result<bool> {
        Ok(self.context.deregister_table(name)?.is_some())
    }

    pub async fn count_filtered_rows(
        &self,
        table: &str,
        predicate: datafusion::logical_expr::Expr,
    ) -> Result<usize> {
        self.filter_rows(table, predicate).await?.count().await
    }

    pub fn table_exists(&self, name: &str) -> Result<bool> {
        self.context.table_exist(name)
    }

    pub async fn table_row_count(&self, table: &str) -> Result<usize> {
        self.table(table).await?.count().await
    }

    pub async fn rename_table(&self, source: &str, destination: &str) -> Result<()> {
        if source == destination || self.context.table_exist(destination)? {
            return Err(DataFusionError::Plan(format!(
                "Destination table already exists: {destination}"
            )));
        }
        let provider = self.context.table_provider(source).await?;
        self.context.register_table(destination, provider)?;
        let result = match self.context.deregister_table(source) {
            Ok(Some(_)) => Ok(()),
            Ok(None) => Err(DataFusionError::Plan(format!(
                "Source table disappeared while renaming: {source}"
            ))),
            Err(error) => Err(error),
        };
        if result.is_err() {
            let _ = self.context.deregister_table(destination);
        }
        result
    }

    fn renamed_column_projection(
        dataframe: &DataFrame,
        renames: &[(&str, &str)],
    ) -> Result<Vec<datafusion::logical_expr::Expr>> {
        use datafusion::{common::Column, logical_expr::Expr};
        use std::collections::{HashMap, HashSet};

        let mut mappings = HashMap::new();
        for &(source, target) in renames {
            if source.trim().is_empty() || target.trim().is_empty() {
                return Err(DataFusionError::Plan(
                    "Column rename requires non-empty names".into(),
                ));
            }
            if mappings.insert(source, target).is_some() {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate column rename source: {source}"
                )));
            }
        }
        let available: HashSet<&str> = dataframe
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        for source in mappings.keys() {
            if !available.contains(*source) {
                return Err(DataFusionError::Plan(format!(
                    "Unknown column rename source: {source}"
                )));
            }
        }

        let mut names = HashSet::new();
        let mut projection = Vec::new();
        for field in dataframe.schema().fields() {
            let source = field.name().as_str();
            let target = mappings.get(source).copied().unwrap_or(source);
            if !names.insert(target.to_ascii_lowercase()) {
                return Err(DataFusionError::Plan(format!(
                    "Column rename produces duplicate name: {target}"
                )));
            }
            let column = Expr::Column(Column::from_name(source));
            projection.push(if target == source {
                column
            } else {
                column.alias(target)
            });
        }
        Ok(projection)
    }

    pub async fn rename_columns(&self, table: &str, renames: &[(&str, &str)]) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        if renames.is_empty() {
            return Ok(dataframe);
        }
        let projection = Self::renamed_column_projection(&dataframe, renames)?;
        dataframe.select(projection)
    }

    pub async fn drop_columns(&self, table: &str, columns: &[&str]) -> Result<DataFrame> {
        use datafusion::{common::Column, logical_expr::Expr};
        use std::collections::HashSet;

        let dataframe = self.table(table).await?;
        if columns.is_empty() {
            return Ok(dataframe);
        }
        let removed: HashSet<&str> = columns.iter().copied().collect();
        let remaining: Vec<_> = dataframe
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .filter(|name| !removed.contains(name))
            .map(|name| Expr::Column(Column::from_name(name)))
            .collect();
        if remaining.is_empty() {
            return Err(DataFusionError::Plan(
                "drop_columns would remove every column".into(),
            ));
        }
        dataframe.select(remaining)
    }

    pub async fn cache_table(&self, name: &str) -> Result<()> {
        let cached = self.table(name).await?.cache().await?;
        let previous = self.context.deregister_table(name)?;
        let result = self.register_dataframe(name, cached);
        if result.is_err()
            && let Some(previous) = previous
        {
            self.context.register_table(name, previous)?;
        }
        result
    }

    pub async fn fill_nulls(
        &self,
        table: &str,
        replacements: &[(&str, datafusion::common::ScalarValue)],
    ) -> Result<DataFrame> {
        use std::collections::HashSet;

        let mut dataframe = self.table(table).await?;
        let columns: HashSet<_> = dataframe
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_owned())
            .collect();
        let mut seen = HashSet::new();
        let mut grouped = Vec::<(&datafusion::common::ScalarValue, Vec<&str>)>::new();
        let mut group_indices: std::collections::HashMap<&datafusion::common::ScalarValue, usize> =
            std::collections::HashMap::new();
        for (name, value) in replacements {
            if !seen.insert(name.to_ascii_lowercase()) {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate fillna column: {name}"
                )));
            }
            if !columns.contains(*name) {
                return Err(DataFusionError::Plan(format!(
                    "Unknown fillna column: {name}"
                )));
            }
            if value.is_null() {
                return Err(DataFusionError::Plan(format!(
                    "fillna replacement cannot be NULL: {name}"
                )));
            }
            if let Some(&index) = group_indices.get(value) {
                grouped[index].1.push(*name);
            } else {
                group_indices.insert(value, grouped.len());
                grouped.push((value, vec![*name]));
            }
        }
        for (value, names) in grouped {
            dataframe = dataframe.fill_null(value, &names)?;
        }
        Ok(dataframe)
    }

    fn computed_column_projection(
        dataframe: &DataFrame,
        columns: &[(&str, datafusion::logical_expr::Expr)],
    ) -> Result<Vec<datafusion::logical_expr::Expr>> {
        use datafusion::{common::Column, logical_expr::Expr};
        use std::collections::{HashMap, HashSet};

        let mut updates = HashMap::new();
        let mut seen = HashSet::new();
        for (name, expr) in columns {
            if name.trim().is_empty() || name.trim() != *name {
                return Err(DataFusionError::Plan(
                    "with_columns requires non-empty, unpadded names".into(),
                ));
            }
            if !seen.insert(name.to_ascii_lowercase()) {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate withColumns name: {name}"
                )));
            }
            updates.insert(*name, (*expr).clone());
        }

        let mut projection = Vec::new();
        for field in dataframe.schema().fields() {
            let name = field.name().as_str();
            let expression = match updates.remove(name) {
                Some(expr) => expr.alias(name),
                None => Expr::Column(Column::from_name(name)),
            };
            projection.push(expression);
        }
        for (name, _) in columns {
            if let Some(expr) = updates.remove(*name) {
                projection.push(expr.alias(*name));
            }
        }
        Ok(projection)
    }

    pub async fn drop_null_rows_with_threshold(
        &self,
        table: &str,
        columns: &[&str],
        required: usize,
    ) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        let selected: Vec<&str> = if columns.is_empty() {
            dataframe
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect()
        } else {
            columns.to_vec()
        };
        Self::validate_dropna_columns(&dataframe, &selected)?;
        if required > selected.len() {
            return Err(DataFusionError::Plan(
                "drop_null_rows_with_threshold exceeds selected column count".into(),
            ));
        }
        if required == 0 {
            return Ok(dataframe);
        }
        let non_nulls = selected
            .iter()
            .map(|name| {
                format!(
                    "(CASE WHEN {} IS NOT NULL THEN 1 ELSE 0 END)",
                    quote_join_identifier(name)
                )
            })
            .collect::<Vec<_>>()
            .join(" + ");
        let source =
            quote_catalog_table(table, "drop_null_rows_with_threshold requires a table path")?;
        self.sql_read_only(&format!(
            "SELECT * FROM {source} WHERE ({non_nulls}) >= {required}"
        ))
        .await
    }

    pub async fn with_columns(
        &self,
        table: &str,
        columns: &[(&str, datafusion::logical_expr::Expr)],
    ) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        if columns.is_empty() {
            return Ok(dataframe);
        }

        let projection = Self::computed_column_projection(&dataframe, columns)?;
        dataframe.select(projection)
    }

    pub async fn with_row_numbers(
        &self,
        table: &str,
        output_column: &str,
        order: &[(&str, bool)],
    ) -> Result<DataFrame> {
        if output_column.is_empty() || output_column.trim() != output_column {
            return Err(DataFusionError::Plan(
                "with_row_numbers requires an unpadded output column name".into(),
            ));
        }
        if order.is_empty() {
            return Err(DataFusionError::Plan(
                "with_row_numbers requires at least one sort key".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        if dataframe
            .schema()
            .fields()
            .iter()
            .any(|field| field.name().eq_ignore_ascii_case(output_column))
        {
            return Err(DataFusionError::Plan(format!(
                "Row number column already exists: {output_column}"
            )));
        }
        Self::sort_dataframe(dataframe, order)?;
        let ordering = order
            .iter()
            .map(|(name, ascending)| {
                let direction = if *ascending { "ASC" } else { "DESC" };
                let nulls = if *ascending { "FIRST" } else { "LAST" };
                format!("{} {direction} NULLS {nulls}", quote_join_identifier(name))
            })
            .collect::<Vec<_>>()
            .join(", ");
        let source =
            quote_catalog_table(table, "with_row_numbers requires a non-empty table path")?;
        let alias = quote_join_identifier(output_column);
        self.sql_read_only(&format!(
            "SELECT *, ROW_NUMBER() OVER (ORDER BY {ordering}) AS {alias} FROM {source}"
        ))
        .await
    }

    pub async fn group_rows(
        &self,
        table: &str,
        groups: &[datafusion::logical_expr::Expr],
        aggregates: &[datafusion::logical_expr::Expr],
    ) -> Result<DataFrame> {
        if aggregates.is_empty() {
            return Err(DataFusionError::Plan(
                "group_rows requires at least one aggregate".into(),
            ));
        }
        self.table(table)
            .await?
            .aggregate(groups.to_vec(), aggregates.to_vec())
    }

    pub async fn register_table_alias(&self, alias: &str, source: &str) -> Result<()> {
        let dataframe = self.table(source).await?;
        self.register_dataframe(alias, dataframe)
    }

    pub async fn table(&self, name: &str) -> Result<DataFrame> {
        self.context.table(name).await
    }

    pub async fn sample_rows(&self, table: &str, fraction: f64) -> Result<DataFrame> {
        if !fraction.is_finite() || !(0.0..=1.0).contains(&fraction) {
            return Err(DataFusionError::Plan(
                "sample_rows fraction must be finite and between zero and one".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        if fraction == 1.0 {
            return Ok(dataframe);
        }
        if fraction == 0.0 {
            return dataframe.limit(0, Some(0));
        }
        let source = quote_catalog_table(table, "sample_rows requires a table path")?;
        self.sql_read_only(&format!(
            "SELECT * FROM {source} WHERE random() < {fraction}"
        ))
        .await
    }

    pub async fn table_first_row(
        &self,
        table: &str,
    ) -> Result<Option<Vec<datafusion::common::ScalarValue>>> {
        Self::extract_first_row(self.table(table).await?).await
    }

    pub async fn slice_rows(&self, table: &str, offset: usize, length: usize) -> Result<DataFrame> {
        offset.checked_add(length).ok_or_else(|| {
            DataFusionError::Plan("slice_rows offset plus length exceeds usize".into())
        })?;
        self.table(table).await?.limit(offset, Some(length))
    }

    pub async fn table_page(&self, table: &str, offset: usize, limit: usize) -> Result<DataFrame> {
        Self::paginate(self.table(table), offset, limit).await
    }

    async fn union_registered_tables(&self, names: &[&str], by_name: bool) -> Result<DataFrame> {
        if names.is_empty() {
            let operation = if by_name {
                "union_tables_by_name"
            } else {
                "union_tables"
            };
            return Err(DataFusionError::Plan(format!(
                "{operation} requires at least one table"
            )));
        }
        use futures::{StreamExt, TryStreamExt, stream};
        let frames: Vec<DataFrame> = stream::iter(names.iter().map(|name| self.table(name)))
            .buffered(16)
            .try_collect()
            .await?;
        let mut frames = frames.into_iter();
        let mut dataframe = frames.next().expect("non-empty table list");
        for frame in frames {
            dataframe = if by_name {
                dataframe.union_by_name(frame)?
            } else {
                dataframe.union(frame)?
            };
        }
        Ok(dataframe)
    }

    pub async fn union_tables(&self, names: &[&str]) -> Result<DataFrame> {
        self.union_registered_tables(names, false).await
    }

    pub async fn union_tables_distinct(&self, names: &[&str]) -> Result<DataFrame> {
        self.union_tables(names).await?.distinct()
    }

    pub async fn union_tables_by_name(&self, names: &[&str]) -> Result<DataFrame> {
        self.union_registered_tables(names, true).await
    }

    pub async fn union_tables_distinct_by_name(&self, names: &[&str]) -> Result<DataFrame> {
        self.union_tables_by_name(names).await?.distinct()
    }

    fn validate_dedup_subset(dataframe: &DataFrame, subset: &[&str]) -> Result<()> {
        Self::validate_named_columns(dataframe, subset, "dropDuplicates")
    }

    pub async fn drop_duplicates(&self, table: &str, subset: &[&str]) -> Result<DataFrame> {
        use datafusion::{common::Column, logical_expr::Expr};

        let dataframe = self.table(table).await?;
        if subset.is_empty() {
            return dataframe.distinct();
        }

        Self::validate_dedup_subset(&dataframe, subset)?;

        let keys = subset
            .iter()
            .map(|name| Expr::Column(Column::from_name(*name)))
            .collect();
        let projection = dataframe
            .schema()
            .fields()
            .iter()
            .map(|field| Expr::Column(Column::from_name(field.name())))
            .collect();
        dataframe.distinct_on(keys, projection, None)
    }

    pub async fn group_value_counts(&self, table: &str, columns: &[&str]) -> Result<DataFrame> {
        if columns.is_empty() {
            return Err(DataFusionError::Plan(
                "group_value_counts requires at least one column".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, columns)?;
        if columns
            .iter()
            .any(|name| name.eq_ignore_ascii_case("frequency"))
        {
            return Err(DataFusionError::Plan(
                "group_value_counts reserves the frequency column name".into(),
            ));
        }
        let groups = columns
            .iter()
            .map(|name| quote_join_identifier(name))
            .collect::<Vec<_>>()
            .join(", ");
        let source = quote_catalog_table(table, "group_value_counts requires a table path")?;
        self.sql_read_only(&format!(
            "SELECT {groups}, COUNT(*) AS frequency FROM {source} GROUP BY {groups}"
        ))
        .await
    }

    pub async fn value_counts(&self, table: &str, column: &str) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[column])?;
        let table_name =
            quote_catalog_table(table, "value_counts requires a non-empty table path")?;
        let column_name = quote_join_identifier(column);
        self.sql_read_only(&format!(
            "SELECT {column_name} AS value, COUNT(*) AS frequency FROM {table_name} GROUP BY {column_name}"
        ))
        .await
    }

    pub async fn column_distinct_count(
        &self,
        table: &str,
        column: &str,
        include_null: bool,
    ) -> Result<usize> {
        let distinct = self.select_columns(table, &[column]).await?.distinct()?;
        if include_null {
            distinct.count().await
        } else {
            use datafusion::{common::Column, logical_expr::Expr};
            distinct
                .filter(Expr::Column(Column::from_name(column)).is_not_null())?
                .count()
                .await
        }
    }

    pub async fn distinct_row_count(&self, table: &str) -> Result<usize> {
        self.table(table).await?.distinct()?.count().await
    }

    fn sort_dataframe_with_nulls(
        dataframe: DataFrame,
        order: &[(&str, bool, bool)],
    ) -> Result<DataFrame> {
        use datafusion::{common::Column, logical_expr::Expr};
        use std::collections::HashSet;

        let mut seen = HashSet::new();
        let mut expressions = Vec::with_capacity(order.len());
        for &(name, ascending, nulls_first) in order {
            if !seen.insert(name.to_ascii_lowercase()) {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate sort column: {name}"
                )));
            }
            if !dataframe
                .schema()
                .fields()
                .iter()
                .any(|field| field.name().as_str() == name)
            {
                return Err(DataFusionError::Plan(format!(
                    "Unknown sort column: {name}"
                )));
            }
            expressions.push(Expr::Column(Column::from_name(name)).sort(ascending, nulls_first));
        }
        dataframe.sort(expressions)
    }

    fn sort_dataframe(dataframe: DataFrame, order: &[(&str, bool)]) -> Result<DataFrame> {
        let keys = order
            .iter()
            .map(|&(name, ascending)| (name, ascending, ascending))
            .collect::<Vec<_>>();
        Self::sort_dataframe_with_nulls(dataframe, &keys)
    }

    pub async fn top_rows_with_nulls(
        &self,
        table: &str,
        order: &[(&str, bool, bool)],
        limit: usize,
    ) -> Result<DataFrame> {
        if order.is_empty() {
            return Err(DataFusionError::Plan(
                "top_rows_with_nulls requires at least one sort key".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        Self::sort_dataframe_with_nulls(dataframe, order)?.limit(0, Some(limit))
    }

    pub async fn sort_rows(&self, table: &str, order: &[(&str, bool)]) -> Result<DataFrame> {
        if order.is_empty() {
            return Err(DataFusionError::Plan(
                "sort_rows requires at least one key".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        Self::sort_dataframe(dataframe, order)
    }

    pub async fn top_rows(
        &self,
        table: &str,
        order: &[(&str, bool)],
        limit: usize,
    ) -> Result<DataFrame> {
        if order.is_empty() {
            return Err(DataFusionError::Plan(
                "top_rows requires at least one sort key".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        Self::sort_dataframe(dataframe, order)?.limit(0, Some(limit))
    }

    pub async fn take_random_rows(&self, table: &str, limit: usize) -> Result<DataFrame> {
        if limit == 0 {
            return self.table(table).await?.limit(0, Some(0));
        }
        let source = quote_catalog_table(table, "take_random_rows requires a table path")?;
        self.sql_read_only(&format!(
            "SELECT * FROM {source} ORDER BY random() LIMIT {limit}"
        ))
        .await
    }

    pub async fn sort_rows_with_nulls(
        &self,
        table: &str,
        order: &[(&str, bool, bool)],
    ) -> Result<DataFrame> {
        if order.is_empty() {
            return Err(DataFusionError::Plan(
                "sort_rows_with_nulls requires at least one key".into(),
            ));
        }
        Self::sort_dataframe_with_nulls(self.table(table).await?, order)
    }

    pub async fn top_frequent_values(
        &self,
        table: &str,
        column: &str,
        limit: usize,
    ) -> Result<DataFrame> {
        use datafusion::{common::Column, logical_expr::Expr};

        let counts = self.value_counts(table, column).await?;
        counts
            .sort(vec![
                Expr::Column(Column::from_name("frequency")).sort(false, false),
            ])?
            .limit(0, Some(limit))
    }

    async fn plan_union_sql_inputs(&self, queries: &[&str]) -> Result<Vec<DataFrame>> {
        use futures::{StreamExt, TryStreamExt, stream};

        stream::iter(queries.iter().map(|query| self.sql(query)))
            .buffered(16)
            .try_collect()
            .await
    }

    async fn union_sql_internal(&self, queries: &[&str], by_name: bool) -> Result<DataFrame> {
        let operation = if by_name {
            "union_sql_by_name"
        } else {
            "union_sql"
        };
        if queries.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "{operation} requires at least one query"
            )));
        }
        let mut frames = self.plan_union_sql_inputs(queries).await?.into_iter();
        let mut dataframe = frames.next().expect("non-empty query list");
        for frame in frames {
            dataframe = if by_name {
                dataframe.union_by_name(frame)?
            } else {
                dataframe.union(frame)?
            };
        }
        Ok(dataframe)
    }

    pub async fn union_sql(&self, queries: &[&str]) -> Result<DataFrame> {
        self.union_sql_internal(queries, false).await
    }

    pub async fn union_sql_distinct(&self, queries: &[&str]) -> Result<DataFrame> {
        self.union_sql(queries).await?.distinct()
    }

    pub async fn union_sql_by_name(&self, queries: &[&str]) -> Result<DataFrame> {
        self.union_sql_internal(queries, true).await
    }

    pub async fn union_sql_distinct_by_name(&self, queries: &[&str]) -> Result<DataFrame> {
        self.union_sql_by_name(queries).await?.distinct()
    }

    pub async fn sql(&self, query: &str) -> Result<DataFrame> {
        self.context.sql(query).await
    }

    async fn has_rows(dataframe: DataFrame) -> Result<bool> {
        use futures::StreamExt;

        let mut batches = dataframe.limit(0, Some(1))?.execute_stream().await?;
        while let Some(batch) = batches.next().await {
            if batch?.num_rows() > 0 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub async fn table_has_rows(&self, table: &str) -> Result<bool> {
        Self::has_rows(self.table(table).await?).await
    }

    pub async fn sql_exists(&self, query: &str) -> Result<bool> {
        Self::has_rows(self.sql_read_only(query).await?).await
    }

    pub async fn sql_exists_with_params(
        &self,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<bool> {
        Self::has_rows(self.sql_read_only_with_params(query, params).await?).await
    }

    pub async fn sql_read_only_row_count(&self, query: &str) -> Result<usize> {
        self.sql_read_only(query).await?.count().await
    }

    pub async fn sql_read_only_row_count_with_params(
        &self,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<usize> {
        self.sql_read_only_with_params(query, params)
            .await?
            .count()
            .await
    }

    async fn extract_scalar(dataframe: DataFrame) -> Result<datafusion::common::ScalarValue> {
        use datafusion::common::ScalarValue;

        if dataframe.schema().fields().len() != 1 {
            return Err(DataFusionError::Plan(
                "Scalar query must return exactly one column".into(),
            ));
        }
        use futures::StreamExt;

        let mut batches = dataframe.limit(0, Some(2))?.execute_stream().await?;
        let mut result = None;
        while let Some(batch) = batches.next().await {
            let batch = batch?;
            if batch.num_rows() > 1 || (batch.num_rows() > 0 && result.is_some()) {
                return Err(DataFusionError::Plan(
                    "Scalar query must return exactly one row".into(),
                ));
            }
            if batch.num_rows() == 1 {
                result = Some(ScalarValue::try_from_array(batch.column(0).as_ref(), 0)?);
            }
        }
        result.ok_or_else(|| DataFusionError::Plan("Scalar query returned no rows".into()))
    }

    pub async fn sql_scalar(&self, query: &str) -> Result<datafusion::common::ScalarValue> {
        Self::extract_scalar(self.sql(query).await?).await
    }

    pub async fn sql_read_only_scalar(
        &self,
        query: &str,
    ) -> Result<datafusion::common::ScalarValue> {
        Self::extract_scalar(self.sql_read_only(query).await?).await
    }

    pub async fn sql_scalar_with_params(
        &self,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<datafusion::common::ScalarValue> {
        Self::extract_scalar(self.sql_with_params(query, params).await?).await
    }

    pub async fn sql_read_only_scalar_with_params(
        &self,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<datafusion::common::ScalarValue> {
        Self::extract_scalar(self.sql_read_only_with_params(query, params).await?).await
    }

    fn validate_page_window(offset: usize, limit: usize) -> Result<()> {
        if limit == 0 {
            return Err(DataFusionError::Plan(
                "SQL page limit must be positive".into(),
            ));
        }
        if offset.checked_add(limit).is_none() {
            return Err(DataFusionError::Plan(
                "SQL page offset plus limit overflows".into(),
            ));
        }
        Ok(())
    }

    async fn paginate<F>(frame: F, offset: usize, limit: usize) -> Result<DataFrame>
    where
        F: std::future::Future<Output = Result<DataFrame>>,
    {
        Self::validate_page_window(offset, limit)?;
        frame.await?.limit(offset, Some(limit))
    }

    pub async fn sql_page(&self, query: &str, offset: usize, limit: usize) -> Result<DataFrame> {
        Self::paginate(self.sql(query), offset, limit).await
    }

    pub async fn sql_read_only_page(
        &self,
        query: &str,
        offset: usize,
        limit: usize,
    ) -> Result<DataFrame> {
        Self::paginate(self.sql_read_only(query), offset, limit).await
    }

    pub async fn sql_read_only_page_with_params(
        &self,
        query: &str,
        offset: usize,
        limit: usize,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<DataFrame> {
        Self::paginate(self.sql_read_only_with_params(query, params), offset, limit).await
    }

    pub async fn sql_page_with_params(
        &self,
        query: &str,
        offset: usize,
        limit: usize,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<DataFrame> {
        Self::paginate(self.sql_with_params(query, params), offset, limit).await
    }

    async fn extract_first_row(
        dataframe: DataFrame,
    ) -> Result<Option<Vec<datafusion::common::ScalarValue>>> {
        use datafusion::common::ScalarValue;

        use futures::StreamExt;

        let mut batches = dataframe.limit(0, Some(1))?.execute_stream().await?;
        while let Some(batch) = batches.next().await {
            let batch = batch?;
            if batch.num_rows() > 0 {
                let values = (0..batch.num_columns())
                    .map(|index| ScalarValue::try_from_array(batch.column(index).as_ref(), 0))
                    .collect::<Result<Vec<_>>>()?;
                return Ok(Some(values));
            }
        }
        Ok(None)
    }

    pub async fn sql_first_row(
        &self,
        query: &str,
    ) -> Result<Option<Vec<datafusion::common::ScalarValue>>> {
        Self::extract_first_row(self.sql(query).await?).await
    }

    pub async fn sql_read_only_first_row(
        &self,
        query: &str,
    ) -> Result<Option<Vec<datafusion::common::ScalarValue>>> {
        Self::extract_first_row(self.sql_read_only(query).await?).await
    }

    pub async fn sql_first_row_with_params(
        &self,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<Option<Vec<datafusion::common::ScalarValue>>> {
        Self::extract_first_row(self.sql_with_params(query, params).await?).await
    }

    pub async fn sql_read_only_first_row_with_params(
        &self,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<Option<Vec<datafusion::common::ScalarValue>>> {
        Self::extract_first_row(self.sql_read_only_with_params(query, params).await?).await
    }

    pub async fn sql_with_params(
        &self,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<DataFrame> {
        self.sql(query).await?.with_param_values(params)
    }

    pub async fn sql_read_only_with_params(
        &self,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<DataFrame> {
        self.sql_read_only(query).await?.with_param_values(params)
    }

    pub async fn sql_read_only_stream(
        &self,
        query: &str,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        self.sql_read_only(query).await?.execute_stream().await
    }

    pub async fn sql_read_only_stream_with_params(
        &self,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        self.sql_read_only_with_params(query, params)
            .await?
            .execute_stream()
            .await
    }

    pub async fn sql_read_only(&self, query: &str) -> Result<DataFrame> {
        use datafusion::execution::context::SQLOptions;
        let options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        self.context.sql_with_options(query, options).await
    }
}

impl Default for RustSparkSession {
    fn default() -> Self {
        Self::new()
    }
}
