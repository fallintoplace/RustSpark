# Remote Ballista task partition tuning


Set `RUSTSPARK_TARGET_PARTITIONS` to control how many DataFusion physical
partitions Ballista plans for the remote query. This is useful when scaling
shuffles and large scans across several executors:

```sh
BALLISTA_URL=df://scheduler:50050 RUSTSPARK_TARGET_PARTITIONS=16 \
RUSTSPARK_SQL='SELECT COUNT(*) FROM (VALUES (1), (2), (3)) AS t(id)' \
cargo run --manifest-path integrations/ballista/Cargo.toml
```

The count must be between 1 and 65536 to avoid extreme partition counts.
Without the setting Ballista uses its
normal partition default. The value controls query partitions, not the number
of scheduler or executor processes. Oversized values can increase scheduling
and shuffle overhead, so benchmark against the executor capacity.


These settings change remote query planning only. They do not create new executors.
