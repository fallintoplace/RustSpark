use std::{collections::HashSet, io};

use super::{invalid, validate_key};

const HEADER: &str = "rustspark-table-manifest-v1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableManifest {
    table: String,
    objects: Vec<String>,
}

impl TableManifest {
    pub fn new(table: impl Into<String>, objects: Vec<String>) -> io::Result<Self> {
        let table = table.into();
        validate_key(&table)?;
        if objects.is_empty() {
            return Err(invalid("Table manifest needs at least one object"));
        }
        let mut seen = HashSet::with_capacity(objects.len());
        for key in &objects {
            validate_key(key)?;
            if !seen.insert(key) {
                return Err(invalid("Table manifest contains duplicate objects"));
            }
        }
        Ok(Self { table, objects })
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn objects(&self) -> &[String] {
        &self.objects
    }

    pub fn encode(&self) -> Vec<u8> {
        let capacity = HEADER.len()
            + 1
            + self.table.len()
            + 1
            + self.objects.iter().map(|key| key.len() + 1).sum::<usize>();
        let mut text = String::with_capacity(capacity);
        text.push_str(HEADER);
        text.push('\n');
        text.push_str(&self.table);
        text.push('\n');
        for key in &self.objects {
            text.push_str(key);
            text.push('\n');
        }
        text.into_bytes()
    }

    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        let input = std::str::from_utf8(bytes).map_err(|_| invalid("Manifest must use UTF-8"))?;
        let mut lines = input.lines();
        if lines.next() != Some(HEADER) {
            return Err(invalid("Unknown table manifest version"));
        }
        let table = lines.next().ok_or_else(|| invalid("Missing table name"))?;
        Self::new(table, lines.map(str::to_owned).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_round_trips_and_rejects_duplicate_keys() {
        let manifest = TableManifest::new(
            "sales",
            vec!["part-1.parquet".into(), "part-2.parquet".into()],
        )
        .unwrap();
        assert_eq!(TableManifest::decode(&manifest.encode()).unwrap(), manifest);
        assert!(TableManifest::new("sales", vec!["x".into(), "x".into()]).is_err());
        assert!(TableManifest::decode(b"wrong\nsales\nx\n").is_err());
        assert!(TableManifest::decode(b"rustspark-table-manifest-v1\nsales\n../bad\n").is_err());
    }

    #[test]
    fn encodes_large_manifests_with_exact_serialized_length() {
        let objects = (0..2048)
            .map(|index| format!("part-{index:06}.parquet"))
            .collect::<Vec<_>>();
        let manifest = TableManifest::new("events", objects).unwrap();
        let encoded = manifest.encode();
        let expected_bytes = HEADER.len()
            + 1
            + "events".len()
            + 1
            + manifest
                .objects()
                .iter()
                .map(|key| key.len() + 1)
                .sum::<usize>();
        assert_eq!(encoded.len(), expected_bytes);
        assert_eq!(TableManifest::decode(&encoded).unwrap(), manifest);
    }
}
