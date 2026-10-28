# Replicated storage foundation

RustSpark has an opt-in filesystem-backed storage placement API:
`rustspark::distributed_storage::{StorageNode, ReplicaStore}`.

It assigns immutable object keys to distinct storage roots using rendezvous
hashing. Pass nodes as `StorageNode::new(id, mounted_directory)` and create a
`ReplicaStore::new(nodes, replication_factor)`. Placement is stable regardless
of node input order.

This is intended for storage volumes mounted on the processes that access them.
It is **not** an S3 service, a consensus system, or network replication by
itself. Root paths must exist; the owner manages mounts, access control, and
node lifecycle. Keep local `--database-dir` storage separate from these roots.

Future changes add the read, write, snapshot, and recovery APIs.
