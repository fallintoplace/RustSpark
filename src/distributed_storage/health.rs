use std::io;

use super::ReplicaStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeHealth {
    pub node_id: String,
    pub writable: bool,
    pub error: Option<String>,
}

impl ReplicaStore {
    /// Probe local mounts only; this does not test remote network reachability.
    pub fn probe_nodes(&self) -> Vec<NodeHealth> {
        self.nodes()
            .iter()
            .map(|node| {
                let result = (|| -> io::Result<()> {
                    let staged = tempfile::NamedTempFile::new_in(node.root.join("objects"))?;
                    staged.as_file().sync_all()
                })();
                NodeHealth {
                    node_id: node.id.clone(),
                    writable: result.is_ok(),
                    error: result.err().map(|error| error.to_string()),
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_storage::StorageNode;
    use std::fs;

    #[test]
    fn detects_unavailable_object_mounts() {
        let root = tempfile::tempdir().unwrap();
        let store =
            ReplicaStore::new(vec![StorageNode::new("a", root.path()).unwrap()], 1).unwrap();
        assert!(store.probe_nodes()[0].writable);
        fs::remove_dir(root.path().join("objects")).unwrap();
        assert!(!store.probe_nodes()[0].writable);
    }
}

impl ReplicaStore {
    /// Return (node ID, regular object count, physical object bytes) per
    /// storage root. Replicas are counted independently on each mount.
    pub fn node_storage_usage(&self) -> io::Result<Vec<(String, usize, u64)>> {
        let mut usage = Vec::with_capacity(self.nodes().len());
        for node in self.nodes() {
            let mut objects = 0usize;
            let mut bytes = 0u64;
            for entry in std::fs::read_dir(node.root.join("objects"))? {
                let entry = entry?;
                let name = entry.file_name();
                let Some(key) = name.to_str() else {
                    continue;
                };
                if super::validate_key(key).is_err() {
                    continue;
                }
                let metadata = std::fs::symlink_metadata(entry.path())?;
                if !metadata.file_type().is_file() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Storage inventory contains an unsafe entry",
                    ));
                }
                objects = objects
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("Object count overflow"))?;
                bytes = bytes
                    .checked_add(metadata.len())
                    .ok_or_else(|| io::Error::other("Stored byte count overflow"))?;
            }
            usage.push((node.id.clone(), objects, bytes));
        }
        Ok(usage)
    }
}

#[cfg(test)]
mod node_usage_feature_tests {
    use super::*;
    use crate::distributed_storage::StorageNode;
    use std::fs;

    #[test]
    fn counts_physical_bytes_and_ignores_temporary_names() {
        let root = tempfile::tempdir().unwrap();
        let store = ReplicaStore::new(
            vec![StorageNode::new("a", root.path()).unwrap()],
            1,
        )
        .unwrap();
        store.put("one", b"abc").unwrap();
        store.put("two", b"hello").unwrap();
        let objects = root.path().join("objects");
        fs::write(objects.join(".tmpunfinished"), b"ignored").unwrap();
        assert_eq!(store.node_storage_usage().unwrap(), [("a".to_string(), 2, 8)]);
        fs::create_dir(objects.join("unsafe")).unwrap();
        assert_eq!(
            store.node_storage_usage().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}
