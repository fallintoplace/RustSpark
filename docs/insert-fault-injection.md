# Local INSERT fault injection

RustSpark has debug-build-only fault injection for the persistent local INSERT
commit protocol. Production release binaries do not contain the hooks.

The hook `RUSTSPARK_TEST_INSERT_EXIT` immediately exits with status 86, without
running Rust destructors. It models abrupt process termination but **does not**
simulate storage-controller faults, torn filesystem metadata writes, or power loss.

| Value | Termination point | State visible after restart |
| --- | --- | --- |
| `after-stage` | Private Parquet snapshot copied | Previous complete table |
| `after-backup` | Old table directory moved to backup | Previous complete table |
| `after-publish` | New table moved into catalog | New complete table |

Run `cargo test --locked --test cli_insert_crash_recovery` to exercise all
three boundaries and corrupt-data rejection. The tests run separate RustSpark
processes and verify startup recovery and journal cleanup.

The tests require the database lock and catalog recovery PRs. Recovery occurs
under the exclusive database lock before any saved tables are registered.

These tests do not establish full SQL transactions or exactly-once client
retries. A retried INSERT after a lost commit acknowledgement can duplicate
rows. Power-loss guarantees depend on OS/filesystem directory synchronization
support.
