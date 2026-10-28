use std::path::Path;

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    error::Result,
    logical_expr::Expr,
    prelude::{CsvReadOptions, DataFrame, cast, ident, lit, nullif},
};
use rustspark::RustSparkSession;

// DataFusion 55 uses null_regex for CSV inference but not for decoding rows.
// Read as strings and normalize before casting to the inferred input types.
fn raw_string_schema<'a>(names: impl IntoIterator<Item = &'a str>) -> Schema {
    Schema::new(
        names
            .into_iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    )
}

fn null_normalizing_projection(field: &Field, marker: &str) -> Expr {
    let value = nullif(ident(field.name()), lit(marker));
    let typed = if field.data_type() == &DataType::Utf8 {
        value
    } else {
        cast(value, field.data_type().clone())
    };
    typed.alias(field.name())
}

async fn read_normalized_csv(
    session: &RustSparkSession,
    path: impl AsRef<Path>,
    options: CsvReadOptions<'_>,
    marker: &str,
) -> Result<DataFrame> {
    let inferred = session
        .read_csv_with_options(&path, options.clone())
        .await?;
    let fields = inferred.schema().fields();
    let string_schema = raw_string_schema(fields.iter().map(|field| field.name().as_str()));
    let raw = session
        .read_csv_with_options(path, options.schema(&string_schema).null_regex(None))
        .await?;
    let expressions = fields
        .iter()
        .map(|field| null_normalizing_projection(field, marker))
        .collect::<Vec<_>>();
    raw.select(expressions)
}

pub(super) async fn register_csv_with_null_value(
    session: &RustSparkSession,
    name: &str,
    path: impl AsRef<Path>,
    options: CsvReadOptions<'_>,
    marker: &str,
) -> Result<()> {
    let normalized = read_normalized_csv(session, path, options, marker).await?;
    session.register_dataframe(name, normalized)
}

#[cfg(test)]
mod tests {
    use super::raw_string_schema;
    use datafusion::arrow::datatypes::DataType;

    #[test]
    fn raw_schema_keeps_names_and_uses_nullable_strings() {
        let schema = raw_string_schema(["id", "name"]);
        assert_eq!(schema.fields()[0].name(), "id");
        assert_eq!(schema.fields()[1].name(), "name");
        assert!(
            schema
                .fields()
                .iter()
                .all(|field| field.is_nullable() && field.data_type() == &DataType::Utf8)
        );
    }
}
