use std::path::Path;

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    error::Result,
    prelude::{CsvReadOptions, cast, ident, lit, nullif},
};
use rustspark::RustSparkSession;

// DataFusion 55 uses null_regex for CSV inference but not for decoding rows.
// Read as strings and normalize before casting to the inferred input types.
pub(super) async fn register_csv_with_null_value(
    session: &RustSparkSession,
    name: &str,
    path: impl AsRef<Path>,
    options: CsvReadOptions<'_>,
    marker: &str,
) -> Result<()> {
    let inferred = session
        .read_csv_with_options(&path, options.clone())
        .await?;
    let fields = inferred.schema().fields();
    let string_schema = Schema::new(
        fields
            .iter()
            .map(|field| Field::new(field.name(), DataType::Utf8, true))
            .collect::<Vec<_>>(),
    );
    let raw = session
        .read_csv_with_options(path, options.schema(&string_schema).null_regex(None))
        .await?;
    let expressions = fields
        .iter()
        .map(|field| {
            let value = nullif(ident(field.name()), lit(marker));
            let typed = if field.data_type() == &DataType::Utf8 {
                value
            } else {
                cast(value, field.data_type().clone())
            };
            typed.alias(field.name())
        })
        .collect::<Vec<_>>();
    session.register_dataframe(name, raw.select(expressions)?)?;
    Ok(())
}
