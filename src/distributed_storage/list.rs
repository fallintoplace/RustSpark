use std::{
    collections::BTreeSet,
    fs,
    io::{self, ErrorKind},
};

use super::{ReplicaStore, validate_key};

impl ReplicaStore {
    pub fn list_keys(&self) -> io::Result<Vec<String>> {
        let mut keys = BTreeSet::new();
        for node in self.nodes() {
            for entry in fs::read_dir(node.root.join("objects"))? {
                let entry = entry?;
                let name = entry.file_name();
                let Some(key) = name.to_str() else {
                    continue;
                };
                // NamedTempFile creates .tmp entries during unpublished writes.
                if key.starts_with(".tmp") {
                    continue;
                }
                if validate_key(key).is_err() {
                    continue;
                }
                if !entry.file_type()?.is_file() {
                    return Err(io::Error::new(
                        ErrorKind::InvalidData,
                        "Object inventory contains a non-file entry",
                    ));
                }
                keys.insert(key.to_owned());
            }
        }
        Ok(keys.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_storage::StorageNode;

    #[test]
    fn lists_replicated_keys_once_in_sorted_order() {
        let root = tempfile::tempdir().unwrap();
        let nodes = ["a", "b"].map(|id| {
            let path = root.path().join(id);
            fs::create_dir(&path).unwrap();
            StorageNode::new(id, path).unwrap()
        });
        let store = ReplicaStore::new(nodes.to_vec(), 2).unwrap();
        store.put("z.parquet", b"z").unwrap();
        store.put("a.parquet", b"a").unwrap();
        fs::write(nodes[0].root.join("objects/.tmpincomplete"), b"ignored").unwrap();
        assert_eq!(store.list_keys().unwrap(), ["a.parquet", "z.parquet"]);
    }
}
