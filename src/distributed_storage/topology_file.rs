use std::{
    fs::File,
    io::{self, BufRead, BufReader},
    path::Path,
};

use super::{ReplicaStore, StorageNode, invalid};

fn parse_topology_node(line: &str) -> io::Result<StorageNode> {
    let (name, root) = line
        .split_once('=')
        .ok_or_else(|| invalid("Topology line must contain node=absolute_path"))?;
    let path = Path::new(root.trim());
    if !path.is_absolute() {
        return Err(invalid("Storage node path must be absolute"));
    }
    StorageNode::new(name.trim(), path)
}

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
            nodes.push(parse_topology_node(line)?);
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
        fs::write(
            &config,
            format!("a={}\na={}\n", node.display(), node.display()),
        )
        .unwrap();
        assert!(ReplicaStore::from_topology_file(&config, 1).is_err());
    }
}

impl ReplicaStore {
    /// Save the current node configuration as a new topology file.
    /// The destination must not already exist. Its parent directory must
    /// exist, and paths with ambiguous line or field delimiters are rejected.
    pub fn save_topology_file(&self, destination: &Path) -> io::Result<()> {
        use std::io::Write;

        let mut body = String::new();
        for node in self.nodes() {
            let root = node
                .root
                .to_str()
                .ok_or_else(|| invalid("Storage root path must be UTF-8"))?;
            if root.trim() != root || root.contains(['=', '\n', '\r']) {
                return Err(invalid("Storage root contains unsafe topology delimiters"));
            }
            body.push_str(&node.id);
            body.push('=');
            body.push_str(root);
            body.push('\n');
        }
        let parent = destination.parent().unwrap_or_else(|| Path::new("."));
        let mut staged = tempfile::NamedTempFile::new_in(parent)?;
        staged.write_all(body.as_bytes())?;
        staged.as_file().sync_all()?;
        staged
            .persist_noclobber(destination)
            .map_err(|error| error.error)?;
        Ok(())
    }
}

#[cfg(test)]
mod topology_export_feature_tests {
    use super::*;

    #[test]
    fn round_trips_sorted_nodes_without_overwriting_a_file() {
        let root = tempfile::tempdir().unwrap();
        let dirs = ["b", "a"]
            .into_iter()
            .map(|id| {
                let directory = root.path().join(id);
                fs::create_dir(&directory).unwrap();
                StorageNode::new(id, directory).unwrap()
            })
            .collect();
        let store = ReplicaStore::new(dirs, 2).unwrap();
        let path = root.path().join("cluster.txt");
        store.save_topology_file(&path).unwrap();
        let content = fs::read_to_string(&path).unwrap();
        assert!(content.starts_with("a="));
        let recovered = ReplicaStore::from_topology_file(&path, 2).unwrap();
        assert_eq!(recovered.nodes(), store.nodes());
        assert_eq!(
            store.save_topology_file(&path).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
    }
}
