use std::io;

use super::{ReplicaStore, invalid, snapshots::snapshot_key};

impl ReplicaStore {
    /// Remove older manifests only; object data is never deleted here.
    /// Run during a controlled maintenance window, not alongside readers.
    pub fn retain_snapshots(&self, table: &str, keep: usize) -> io::Result<Vec<u64>> {
        if keep == 0 {
            return Err(invalid("Snapshot retention must keep at least one generation"));
        }
        let generations = self.list_snapshots(table)?;
        let to_remove = generations.len().saturating_sub(keep);
        let mut removed = Vec::new();
        for generation in generations.into_iter().take(to_remove) {
            if self.delete(&snapshot_key(table, generation)?)? == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "Snapshot was not present in assigned replica placements",
                ));
            }
            removed.push(generation);
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_storage::StorageNode;

    #[test]
    fn keeps_the_newest_generations_without_deleting_parts() {
        let root = tempfile::tempdir().unwrap();
        let store = ReplicaStore::new(
            vec![StorageNode::new("a", root.path()).unwrap()],
            1,
        )
        .unwrap();
        store.put("part.parquet", b"content").unwrap();
        for generation in 1..=4 {
            store
                .create_snapshot("sales", generation, vec!["part.parquet".into()])
                .unwrap();
        }
        assert_eq!(store.retain_snapshots("sales", 2).unwrap(), [1, 2]);
        assert_eq!(store.list_snapshots("sales").unwrap(), [3, 4]);
        assert_eq!(store.get("part.parquet").unwrap(), b"content");
        assert!(store.retain_snapshots("sales", 0).is_err());
    }

    #[test]
    fn does_not_report_deleting_an_unassigned_snapshot_copy() {
        use std::fs;

        let root = tempfile::tempdir().unwrap();
        let nodes = ["a", "b"].map(|id| {
            let path = root.path().join(id);
            fs::create_dir(&path).unwrap();
            StorageNode::new(id, path).unwrap()
        });
        let store = ReplicaStore::new(nodes.to_vec(), 1).unwrap();
        for generation in 1..=2 {
            store.create_snapshot("sales", generation, vec!["part.parquet".into()]).unwrap();
        }
        let key = snapshot_key("sales", 1).unwrap();
        let assigned = store.placements(&key).unwrap()[0];
        let other = nodes.iter().find(|node| node.id != assigned.id).unwrap();
        let source = assigned.root.join("objects").join(&key);
        let migrated = other.root.join("objects").join(&key);
        fs::rename(&source, &migrated).unwrap();
        assert_eq!(store.list_snapshots("sales").unwrap(), [1, 2]);
        assert_eq!(store.retain_snapshots("sales", 1).unwrap_err().kind(), io::ErrorKind::NotFound);
        assert!(migrated.is_file());
    }
}
