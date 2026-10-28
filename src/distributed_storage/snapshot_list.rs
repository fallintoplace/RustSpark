use std::io;

use super::{ReplicaStore, validate_key};

fn parse_snapshot_generation(key: &str, prefix: &str) -> Option<u64> {
    let suffix = key.strip_prefix(prefix)?;
    if suffix.len() != 20 || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    suffix.parse::<u64>().ok().filter(|generation| *generation > 0)
}

impl ReplicaStore {
    pub fn list_snapshots(&self, table: &str) -> io::Result<Vec<u64>> {
        validate_key(table)?;
        let prefix = format!("_snapshot_{table}_");
        let generations = self
            .list_keys()?
            .into_iter()
            .filter_map(|key| parse_snapshot_generation(&key, &prefix))
            .collect::<Vec<_>>();
        Ok(generations)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_storage::StorageNode;

    #[test]
    fn parses_only_positive_padded_generations() {
        let prefix = "_snapshot_sales_";
        assert_eq!(
            parse_snapshot_generation("_snapshot_sales_00000000000000000007", prefix),
            Some(7)
        );
        for key in [
            "_snapshot_sales_7",
            "_snapshot_sales_00000000000000000000",
            "_snapshot_other_00000000000000000007",
            "_snapshot_sales_0000000000000000000a",
        ] {
            assert_eq!(parse_snapshot_generation(key, prefix), None);
        }
    }

    #[test]
    fn discovers_snapshots_without_mixing_tables() {
        let root = tempfile::tempdir().unwrap();
        let store = ReplicaStore::new(
            vec![StorageNode::new("a", root.path()).unwrap()],
            1,
        )
        .unwrap();
        store.create_snapshot("sales", 7, vec!["a.parquet".into()]).unwrap();
        store.create_snapshot("sales", 2, vec!["a.parquet".into()]).unwrap();
        store.create_snapshot("other", 9, vec!["a.parquet".into()]).unwrap();
        // Filename parsing must agree with the canonical snapshot writer.
        store.put("_snapshot_sales_7", b"not a snapshot").unwrap();
        store.put("_snapshot_sales_+7", b"not a snapshot").unwrap();
        store.put("_snapshot_sales_00000000000000000000", b"zero").unwrap();
        assert_eq!(store.list_snapshots("sales").unwrap(), [2, 7]);
        assert!(store.list_snapshots("empty").unwrap().is_empty());
    }
}
