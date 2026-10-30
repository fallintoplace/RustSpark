# Security policy

RustSpark is an **experimental native Rust SQL query engine**, currently version
0.1.0. The default CLI and library have no DataFusion or Ballista fallback.
RustSpark does not promise production-grade availability, authenticated access,
tenant isolation, crash-safe persistence, or a vulnerability response SLA.

## Reporting a vulnerability

Use GitHub's **Report a vulnerability** feature in the Security tab if enabled.
Otherwise contact a maintainer privately before sharing a reproduction.
Do not post credentials, private query inputs, or working exploits in public PRs.
Use synthetic data and include the tested commit, OS, minimal reproduction,
expected result, and security impact. Do not probe systems without permission.

## Current security boundaries

- **Local execution:** The CLI opens CSV, Parquet, and SQL files with its
  operating-system privileges. It is not a sandbox for untrusted inputs.
- **Storage:** The native catalog is in memory or references existing CSV and
  Parquet files. The default CLI does not provide a durable SQL database,
  transactions, WAL, automatic backups, or recovery on restart.
- **Spills:** Streaming sort, join, and aggregation operators write temporary
  query data to the configured scratch directory. Use a private, trusted
  directory on protected storage and clear stale crash artifacts.
- **Resource exhaustion:** The engine has some row, record, memory, and plan
  limits. These are not a complete defense against adversarial SQL or datasets.
- **Networking:** The default CLI is local, not an authenticated SQL server.
  Experimental integrations are not hardened multi-tenant services.
- **Dependencies:** RustSec audits help detect known advisories, but passing a
  scanner does not establish a secure deployment.

Run RustSpark with minimal filesystem permissions. Do not allow untrusted users
to submit queries to a privileged shared service or place secrets in shared
scratch directories. Keep independent backups of input datasets.
