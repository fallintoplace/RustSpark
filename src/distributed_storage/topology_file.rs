use std::{
    fs::File,
    io::{self, BufRead, BufReader},
    path::Path,
};

use super::{ReplicaStore, StorageNode, invalid};

impl ReplicaStore {
    /// Parse one node=absolute_mount_path per line. Comments start with #.
    pub fn from_topology_file(path: &Path, factor: usize) -> io::Result<Self> {
        let input = BufReader::new(File::open(path)?);
        let mut nodes = Vec::new();
        for (index, line) in input.lines().enumerate() {
            let line = line?;
            let line = if index == 0 {
                line.strip_prefix('\u{feff}').unwrap_or(&line)
            } else {
                &line
            };
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (name, root) = line
                .split_once('=')
                .ok_or_else(|| invalid("Topology line must contain node=absolute_path"))?;
            let path = Path::new(root.trim());
            if !path.is_absolute() {
                return Err(invalid("Storage node path must be absolute"));
            }
            nodes.push(StorageNode::new(name.trim(), path)?);
            if nodes.len() > 256 {
                return Err(invalid("Topology supports at most 256 nodes"));
            }
        }
        Self::new(nodes, factor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn loads_roots_and_rejects_relative_or_duplicate_entries() {
        let root = tempfile::tempdir().unwrap();
        let node = root.path().join("node");
        fs::create_dir(&node).unwrap();
        let config = root.path().join("nodes.txt");
        fs::write(&config, format!("# cluster\na={}\n", node.display())).unwrap();
        let store = ReplicaStore::from_topology_file(&config, 1).unwrap();
        assert_eq!(store.nodes()[0].id, "a");
        fs::write(&config, "a=relative/path\n").unwrap();
        assert!(ReplicaStore::from_topology_file(&config, 1).is_err());
        fs::write(&config, format!("a={}\na={}\n", node.display(), node.display())).unwrap();
        assert!(ReplicaStore::from_topology_file(&config, 1).is_err());
    }
}
