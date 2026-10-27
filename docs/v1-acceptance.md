# RustSpark V1 acceptance

RustSpark V1 is an **experimental local SQL engine** built on Apache DataFusion.
It is not a stable database, Spark API-compatible runtime, or production
distributed SQL service. The current Cargo package version is `0.1.0`.
Treat the first public release as an experimental preview, not a promise of
stable `v1.0.0` semantics.

## Release gates

- The exact `main` commit to be tagged has completed, successful Rust CI:
  formatting, Clippy, all Rust tests, Rust 1.95 and Windows. A cancelled or
  pending run is **not** a passing release gate.
- The release workflow's Linux and Windows `storage-qualification` tests
  pass, including `cli_insert_recovery`, `cli_database_lock`,
  `cli_catalog_recovery`, `cli_database_backup` and `v1_workflows`.
- Core local CLI workflows work across fresh processes: persistent table
  create, INSERT, restart, rename and DROP; named CSV joins, typed SQL
  parameters, Parquet round trips and nonzero exit status on invalid SQL.
- Storage tests verify that failed writes preserve previously committed data,
  and that interrupted insert publication recovers on restart at the tested
  process-exit boundaries. Backup tests verify independent copied Parquet parts.
- The packaged release binaries complete a backup smoke check: independent
  snapshots remain queryable after writes and drops to the original database,
  and invalid backup destinations are rejected.
- The release workflow builds Linux x86-64, Windows x86-64 and macOS arm64
  binaries. Each final archive includes the executable, README and LICENSE,
  matches the checked build output, and passes smoke tests after extraction.
- The tag matches `Cargo.toml`, is based on the verified `main` commit,
  and has not been moved after publication.
- Record measured benchmark results and known Spark SQL differences before
  publishing comparative performance or compatibility claims. The optional
  comparison harnesses are not replacements for the core release checks.

## Run local checks

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
cargo test --locked --test v1_workflows
cargo test --locked --test cli_insert_recovery --test cli_database_lock \
  --test cli_catalog_recovery --test cli_database_backup
python3 scripts/quality.py test --suite all
```

These tests exercise the actual CLI in separate processes. On GitHub, also
run the release workflow through `workflow_dispatch` before tagging.
A local pass does not replace the cross-platform checks or archive smoke tests.

## Known limitations to publish

- Persistent `INSERT INTO` builds a private replacement and uses a recovery
  protocol. Tests cover SQL errors, cancellation and process exits at selected
  commit boundaries. On the next startup, RustSpark validates snapshots and
  recovers the old or published table where possible.
- **This is not ACID storage.** Multi-statement transactions, power-loss/fsync
  durability and recovery from arbitrary file corruption are **not guaranteed**.
  Do not store irreplaceable data without independent backups.
- Local persisted operations hold an exclusive filesystem lock for the whole
  database session, including read-only sessions. Multiple writers are not
  supported. External writers are not coordinated with this lock.
- Staged inserts may hard-link existing immutable Parquet parts. Altering these
  files outside RustSpark can corrupt both the live and staged snapshots.
  Database backups copy independent files rather than sharing hard links.
- Ballista-based distributed execution is an optional feasibility integration,
  not a production scheduler or worker-failure recovery contract.
- RustSpark uses DataFusion SQL semantics and is **not** a drop-in replacement
  for Apache Spark SQL or the Spark APIs.

Include these limits in the release notes. Do not convert passing smoke tests
into claims of guaranteed durability, compatibility or performance.
