use std::io;

use super::ReplicaStore;

impl ReplicaStore {
    /// Copy objects into the target topology. Keep original copies until
    /// the destination has been verified and active readers have switched.
    pub fn copy_to_topology(&self, destination: &ReplicaStore) -> io::Result<usize> {
        let mut copied = 0;
        for key in self.list_keys()? {
            destination.put(&key, &self.get(&key)?)?;
            copied += 1;
        }
        Ok(copied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_storage::StorageNode;
    use std::fs;

    #[test]
    fn migrates_objects_without_removing_old_copies() {
        let root = tempfile::tempdir().unwrap();
        let nodes = ["a", "b"].map(|id| {
            let path = root.path().join(id);
            fs::create_dir(&path).unwrap();
            StorageNode::new(id, path).unwrap()
        });
        let original = ReplicaStore::new(vec![nodes[0].clone()], 1).unwrap();
        original.put("part.parquet", b"payload").unwrap();
        let expanded = ReplicaStore::new(nodes.to_vec(), 2).unwrap();
        assert_eq!(original.copy_to_topology(&expanded).unwrap(), 1);
        assert_eq!(expanded.verify("part.parquet").unwrap().healthy, 2);
        assert_eq!(original.get("part.parquet").unwrap(), b"payload");
    }
}
