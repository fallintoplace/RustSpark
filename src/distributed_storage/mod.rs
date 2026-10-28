//! Filesystem-backed replicated object placement for shared cluster mounts.
//!
//! All storage roots must already be mounted and visible to callers. This
//! module does not provide a network protocol or distributed consensus.
use std::{
    collections::HashSet,
    fs,
    io::{self, ErrorKind},
    path::PathBuf,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageNode {
    pub id: String,
    pub root: PathBuf,
}

impl StorageNode {
    pub fn new(id: impl Into<String>, root: impl Into<PathBuf>) -> io::Result<Self> {
        let id = id.into();
        if !valid_name(&id) || id.len() > 64 {
            return Err(invalid("Storage node id must be 1-64 ASCII filename characters"));
        }
        let root = root.into();
        if !fs::symlink_metadata(&root)?.file_type().is_dir() {
            return Err(invalid("Storage root must be a regular directory"));
        }
        Ok(Self {
            id,
            root: root.canonicalize()?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct ReplicaStore {
    nodes: Vec<StorageNode>,
    factor: usize,
}

impl ReplicaStore {
    pub fn new(mut nodes: Vec<StorageNode>, factor: usize) -> io::Result<Self> {
        if nodes.is_empty() || factor == 0 || factor > nodes.len() {
            return Err(invalid("Replication factor must be between 1 and node count"));
        }
        let mut ids = HashSet::new();
        let mut roots = HashSet::new();
        for node in &nodes {
            if !ids.insert(node.id.to_ascii_lowercase()) {
                return Err(invalid("Duplicate storage node id"));
            }
            if !roots.insert(node.root.clone()) {
                return Err(invalid("Duplicate storage root"));
            }
        }
        nodes.sort_by(|a, b| a.id.cmp(&b.id));
        for node in &nodes {
            let objects = node.root.join("objects");
            match fs::symlink_metadata(&objects) {
                Ok(meta) if meta.file_type().is_dir() => {}
                Ok(_) => return Err(invalid("Storage objects directory must not be a symlink or file")),
                Err(error) if error.kind() == ErrorKind::NotFound => fs::create_dir(&objects)?,
                Err(error) => return Err(error),
            }
        }
        Ok(Self { nodes, factor })
    }

    pub fn nodes(&self) -> &[StorageNode] {
        &self.nodes
    }

    pub fn replication_factor(&self) -> usize {
        self.factor
    }

    /// Rendezvous hashing keeps placement stable across input order and
    /// minimizes movement when membership changes.
    pub fn placements(&self, key: &str) -> io::Result<Vec<&StorageNode>> {
        validate_key(key)?;
        let mut ranked: Vec<&StorageNode> = self.nodes.iter().collect();
        ranked.sort_by(|a, b| {
            placement_score(key, &b.id)
                .cmp(&placement_score(key, &a.id))
                .then_with(|| a.id.cmp(&b.id))
        });
        ranked.truncate(self.factor);
        Ok(ranked)
    }

    fn object_path(&self, node: &StorageNode, key: &str) -> PathBuf {
        node.root.join("objects").join(key)
    }
}

fn placement_score(key: &str, node: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in key.bytes().chain(std::iter::once(0)).chain(node.bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn validate_key(key: &str) -> io::Result<()> {
    if key.len() > 120 || !valid_name(key) || key.starts_with(".tmp") {
        return Err(invalid("Object key must be 1-120 safe ASCII filename characters"));
    }
    Ok(())
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn placements_are_stable_across_node_input_order() {
        let root = tempfile::tempdir().unwrap();
        let nodes = ["a", "b", "c"]
            .map(|id| {
                let path = root.path().join(id);
                fs::create_dir(&path).unwrap();
                StorageNode::new(id, path).unwrap()
            });
        let first = ReplicaStore::new(nodes.to_vec(), 2).unwrap();
        let second = ReplicaStore::new(nodes.into_iter().rev().collect(), 2).unwrap();
        let ids = |store: &ReplicaStore| {
            store
                .placements("part-0001.parquet")
                .unwrap()
                .into_iter()
                .map(|node| node.id.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(&first), ids(&second));
        assert_eq!(ids(&first).len(), 2);
        assert!(first.placements("../outside").is_err());
    }

    #[test]
    fn rejects_duplicate_nodes_and_bad_replication() {
        let root = tempfile::tempdir().unwrap();
        let node = StorageNode::new("node", root.path()).unwrap();
        assert!(ReplicaStore::new(vec![node.clone()], 0).is_err());
        assert!(ReplicaStore::new(vec![node.clone()], 2).is_err());
        assert!(ReplicaStore::new(vec![node.clone(), node], 2).is_err());
        assert!(StorageNode::new("bad/name", root.path()).is_err());
    }

    #[test]
    fn rejects_symlinked_storage_root() {
        #[cfg(unix)]
        {
            let root = tempfile::tempdir().unwrap();
            let link = root.path().join("alias");
            std::os::unix::fs::symlink(root.path(), &link).unwrap();
            assert!(StorageNode::new("node", link).is_err());
        }
    }
}
