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

## Immutable Parquet snapshots

Store Parquet objects under safe names such as `part-0001.parquet` with
`ReplicaStore::put`. Publish a generation with `create_snapshot(table,
generation, object_keys)`. Generations are immutable; use a new generation
for updates. Missing replicas can be restored with `repair_missing`.

`RustSparkSession::register_snapshot_parquet` materializes the selected
generation into a **new** directory and registers those Parquet parts.
Only the materialized files are used by the SQL planner.

For the optional Ballista client, configure all five values:

```sh
export RUSTSPARK_STORAGE_TOPOLOGY_FILE=/mnt/shared/storage-nodes.txt
export RUSTSPARK_STORAGE_REPLICAS=2
export RUSTSPARK_STORAGE_TABLE=events
export RUSTSPARK_STORAGE_GENERATION=1
export RUSTSPARK_STORAGE_MATERIALIZE_DIR=/mnt/shared/query-events-v1
cargo run --manifest-path integrations/ballista/Cargo.toml
```

The topology file contains one `node_id=/absolute/mounted/root` per
line. The selected snapshot must already exist. The materialized path
must not exist when registration begins.

**Limits:** This backend uses mounted filesystem roots and does not
implement network transport, membership consensus, or atomic writes
across nodes. An interrupted write can leave a subset of replicas;
a retry with identical bytes can repair it. The local database remains
unchanged. For multi-host Ballista, mount the same object roots and
materialized directory at identical paths on all executors. This
does not configure remote S3/GCS credentials.
