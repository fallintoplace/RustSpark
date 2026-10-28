use std::{collections::HashSet, sync::Arc};

use datafusion::arrow::datatypes::{DataType, Field, Schema};

use super::{parse_schema_data_type, valid_identifier};

fn split_top_level(input: &str) -> Result<Vec<&str>, String> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    for (index, character) in input.char_indices() {
        match character {
            '<' => depth += 1,
            '>' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| "Unexpected '>' in JSON schema".to_owned())?;
            }
            ',' if depth == 0 => {
                parts.push(&input[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err("Unclosed '<' in JSON schema".into());
    }
    parts.push(&input[start..]);
    Ok(parts)
}

fn parse_fields(specification: &str) -> Result<Vec<Field>, String> {
    let mut fields = Vec::new();
    let mut names = HashSet::new();
    for entry in split_top_level(specification)? {
        let (raw_name, type_name) = entry
            .split_once(':')
            .ok_or_else(|| "JSON schema needs NAME:TYPE entries".to_owned())?;
        let name = raw_name.trim();
        if !valid_identifier(name) {
            return Err("JSON schema column names must be SQL identifiers".into());
        }
        if !names.insert(name.to_ascii_lowercase()) {
            return Err(format!("Duplicate JSON schema column: {name}"));
        }
        fields.push(Field::new(name, parse_type(type_name)?, true));
    }
    Ok(fields)
}

fn parse_type(specification: &str) -> Result<DataType, String> {
    let name = specification.trim();
    let Some(open) = name.find('<') else {
        return parse_schema_data_type(name, "JSON");
    };
    if !name.ends_with('>') {
        return Err(format!("Unclosed JSON type: {name}"));
    }
    let kind = name[..open].trim().to_ascii_lowercase();
    let inner = &name[open + 1..name.len() - 1];
    match kind.as_str() {
        "array" | "list" => {
            // Arrow List is nullable, including its elements, to preserve JSON nulls.
            let element = parse_type(inner)?;
            Ok(DataType::List(Arc::new(Field::new("item", element, true))))
        }
        "struct" => Ok(DataType::Struct(
            parse_fields(inner)?
                .into_iter()
                .map(Arc::new)
                .collect::<Vec<_>>()
                .into(),
        )),
        "map" => {
            let types = split_top_level(inner)?;
            if types.len() != 2 {
                return Err("JSON map type must be map<KEY,VALUE>".into());
            }
            let key = parse_type(types[0])?;
            let value = parse_type(types[1])?;
            // Arrow maps require a non-null key in a non-null entries struct.
            Ok(DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("key", key, false)),
                            Arc::new(Field::new("value", value, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ))
        }
        _ => Err(format!("Unsupported JSON type: {name}")),
    }
}

pub(super) fn parse_json_schema(specification: &str) -> Result<Schema, String> {
    Ok(Schema::new(parse_fields(specification)?))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;

    use super::parse_json_schema;

    #[test]
    fn supports_nested_array_struct_and_map_types() {
        let schema = parse_json_schema(
            "id:int64,tags:ARRAY<STRING>,meta:struct<city:string,flags:array<bool>>,             attrs:map<string,array<int64>>",
        )
        .unwrap();
        assert_eq!(schema.fields().len(), 4);
        assert!(matches!(schema.field(1).data_type(), DataType::List(_)));
        assert!(matches!(schema.field(2).data_type(), DataType::Struct(_)));
        let DataType::Map(entries, false) = schema.field(3).data_type() else {
            panic!("expected an Arrow map");
        };
        let DataType::Struct(fields) = entries.data_type() else {
            panic!("map entries must be a struct");
        };
        assert!(!fields[0].is_nullable(), "map keys must be non-null");
        assert!(fields[1].is_nullable(), "map values may be null");
    }

    #[test]
    fn preserves_primitive_json_schema_types() {
        let schema = parse_json_schema("city:utf8,amount:int64").unwrap();
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
    }

    #[test]
    fn rejects_invalid_nested_schemas() {
        for input in [
            "", "tags:array<>", "tags:array<int32",
            "tags:array<int32>>", "map:map<utf8>",
            "map:map<,int32>", "thing:struct<>",
            "thing:struct<id:int32,ID:utf8>",
            "x:array<int32>,X:array<int64>", "x:unknown<int32>",
            "x:map<utf8,array<int32>",
        ] {
            assert!(parse_json_schema(input).is_err(), "{input}");
        }
    }
}
