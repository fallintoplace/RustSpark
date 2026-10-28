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

impl ReplicaStore {
    /// Report candidate orphan keys and logical byte lengths. Nothing is
    /// removed. Missing assigned replicas are tolerated, but disagreement
    /// among surviving lengths is rejected.
    pub fn orphan_candidate_sizes(&self) -> io::Result<Vec<(String, u64)>> {
        let mut candidates = Vec::new();
        for key in self.orphan_candidates()? {
            let mut length = None;
            for replica in self.inspect(&key)? {
                if let Some(bytes) = replica.bytes {
                    if length.is_some_and(|previous| previous != bytes) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Orphan replica lengths disagree",
                        ));
                    }
                    length = Some(bytes);
                }
            }
            let length = length.ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "Orphan has no assigned replica")
            })?;
            candidates.push((key, length));
        }
        Ok(candidates)
    }
}

#[cfg(test)]
mod orphan_size_feature_tests {
    use super::*;
    use crate::distributed_storage::StorageNode;

    #[test]
    fn estimates_only_unreferenced_objects_without_deleting() {
        let root = tempfile::tempdir().unwrap();
        let store = ReplicaStore::new(
            vec![StorageNode::new("a", root.path()).unwrap()],
            1,
        )
        .unwrap();
        store.put("unused.parquet", b"12345").unwrap();
        store.put("active.parquet", b"active").unwrap();
        store
            .create_snapshot("sales", 1, vec!["active.parquet".into()])
            .unwrap();
        assert_eq!(
            store.orphan_candidate_sizes().unwrap(),
            [("unused.parquet".to_string(), 5)]
        );
        assert_eq!(store.get("unused.parquet").unwrap(), b"12345");
    }
}
