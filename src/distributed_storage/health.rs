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
