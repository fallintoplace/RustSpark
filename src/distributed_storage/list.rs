use std::{
    collections::HashSet,
    fs,
    io::{self, ErrorKind},
};

use super::{ReplicaStore, validate_key};

fn inventory_key(entry: &fs::DirEntry) -> io::Result<Option<String>> {
    let name = entry.file_name();
    let Some(key) = name.to_str() else {
        return Ok(None);
    };
    // Ignore staged writes and invalid object names before checking file type.
    if key.starts_with(".tmp") || validate_key(key).is_err() {
        return Ok(None);
    }
    if !entry.file_type()?.is_file() {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "Object inventory contains a non-file entry",
        ));
    }
    Ok(Some(key.to_owned()))
}

impl ReplicaStore {
    pub fn list_keys(&self) -> io::Result<Vec<String>> {
        let mut keys = HashSet::new();
        for node in self.nodes() {
            for entry in fs::read_dir(node.root.join("objects"))? {
                let entry = entry?;
                if let Some(key) = inventory_key(&entry)? {
                    keys.insert(key);
                }
            }
        }
        // Hash membership is cheaper than maintaining tree order for every file.
        // Sort once so the API stays deterministic across nodes and platforms.
        let mut keys: Vec<_> = keys.into_iter().collect();
        keys.sort_unstable();
        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_storage::StorageNode;

    #[test]
    fn inventory_skips_invalid_names_but_rejects_named_directories() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("bad?name"), b"ignored").unwrap();
        fs::write(root.path().join(".tmpunfinished"), b"ignored").unwrap();
        let mut entries = fs::read_dir(root.path()).unwrap();
        for entry in entries.by_ref() {
            assert_eq!(inventory_key(&entry.unwrap()).unwrap(), None);
        }
        fs::create_dir(root.path().join("valid.parquet")).unwrap();
        let entry = fs::read_dir(root.path())
            .unwrap()
            .map(Result::unwrap)
            .find(|entry| entry.file_name() == "valid.parquet")
            .unwrap();
        assert_eq!(
            inventory_key(&entry).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }

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

    #[test]
    fn large_multi_node_inventory_is_deduplicated_and_sorted() {
        let root = tempfile::tempdir().unwrap();
        let nodes = ["a", "b"].map(|id| {
            let path = root.path().join(id);
            fs::create_dir(&path).unwrap();
            StorageNode::new(id, path).unwrap()
        });
        let store = ReplicaStore::new(nodes.to_vec(), 2).unwrap();
        for index in (0..256).rev() {
            store
                .put(&format!("part-{index:04}.parquet"), b"test")
                .unwrap();
        }
        let keys = store.list_keys().unwrap();
        assert_eq!(keys.len(), 256);
        assert_eq!(keys.first().unwrap(), "part-0000.parquet");
        assert_eq!(keys.last().unwrap(), "part-0255.parquet");
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
    }
}

impl ReplicaStore {
    /// List existing keys beginning with a prefix, preserving lexicographic
    /// order. An empty prefix selects the whole inventory.
    pub fn list_keys_with_prefix(&self, prefix: &str) -> io::Result<Vec<String>> {
        let keys = self.list_keys()?;
        let start = keys.partition_point(|key| key.as_str() < prefix);
        Ok(keys
            .into_iter()
            .skip(start)
            .take_while(|key| key.starts_with(prefix))
            .collect())
    }
}

#[cfg(test)]
mod prefix_listing_feature_tests {
    use super::*;
    use crate::distributed_storage::StorageNode;

    #[test]
    fn returns_only_matching_keys_in_sorted_order() {
        let root = tempfile::tempdir().unwrap();
        let store = ReplicaStore::new(
            vec![StorageNode::new("a", root.path()).unwrap()],
            1,
        )
        .unwrap();
        for key in ["year-2025", "year-2024-b", "other", "year-2024-a"] {
            store.put(key, b"data").unwrap();
        }
        assert_eq!(
            store.list_keys_with_prefix("year-2024").unwrap(),
            ["year-2024-a", "year-2024-b"]
        );
        assert_eq!(store.list_keys_with_prefix("missing").unwrap(), Vec::<String>::new());
        assert_eq!(store.list_keys_with_prefix("").unwrap().len(), 4);
    }
}
