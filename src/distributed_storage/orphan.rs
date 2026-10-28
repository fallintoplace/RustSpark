use std::{collections::HashSet, io};

use super::{ReplicaStore, manifest::TableManifest};

fn referenced_parts(store: &ReplicaStore, keys: &[String]) -> io::Result<HashSet<String>> {
    let mut referenced = HashSet::new();
    for key in keys.iter().filter(|key| key.starts_with("_snapshot_")) {
        let manifest = TableManifest::decode(&store.get(key)?)?;
        referenced.extend(manifest.objects().iter().cloned());
    }
    Ok(referenced)
}

impl ReplicaStore {
    /// Return candidates only. Direct callers may still use objects that are
    /// absent from snapshots, so this method does not delete anything.
    pub fn orphan_candidates(&self) -> io::Result<Vec<String>> {
        let keys = self.list_keys()?;
        let referenced = referenced_parts(self, &keys)?;
        Ok(keys
            .into_iter()
            .filter(|key| key.ends_with(".parquet") && !referenced.contains(key))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_storage::StorageNode;

    #[test]
    fn protects_parts_referenced_by_any_retained_snapshot() {
        let root = tempfile::tempdir().unwrap();
        let store =
            ReplicaStore::new(vec![StorageNode::new("a", root.path()).unwrap()], 1).unwrap();
        store.put("old.parquet", b"old").unwrap();
        store.put("active.parquet", b"active").unwrap();
        store
            .create_snapshot("sales", 1, vec!["active.parquet".into()])
            .unwrap();
        assert_eq!(store.orphan_candidates().unwrap(), ["old.parquet"]);
        assert_eq!(store.get("old.parquet").unwrap(), b"old");
    }
}
