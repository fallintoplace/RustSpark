# Security policy

RustSpark is an experimental Apache-2.0 SQL engine built on DataFusion.
The current package is version 0.1.0. No production-grade security,
availability, multi-tenant isolation, or vulnerability response SLA is
promised by this repository.

## Reporting a vulnerability

Please use GitHub's **Report a vulnerability** private reporting feature
under the repository's Security tab, if it is enabled. If private
reporting is not available, contact a repository maintainer to establish
a private reporting channel **before** sharing exploit details.
Do not post credentials, sensitive files, private data or working
exploit details in a public issue or pull request.

Include a minimal reproduction using synthetic data, the RustSpark
version or commit, host operating system, expected behavior and the
security impact. Do not test third-party infrastructure without permission.

## Current security boundaries

- The CLI executes SQL with the filesystem access of its operating
  system process. Do not let untrusted users execute arbitrary SQL.
- The local persistent database is experimental and single-writer.
  It does **not** guarantee full ACID transactions or power-loss
  durability. Keep independent backups.
- The optional distributed storage and Ballista integrations do not
  provide a hardened, authenticated multi-tenant SQL service.
- Use the principle of least privilege, isolate query inputs and
  writable directories, and avoid granting access to secrets.
- Do not expose example development UIs, gateways or agent tools
  directly on the public internet.

Dependency vulnerability checks are informative. Passing a scan
does not mean RustSpark is safe to expose as a shared SQL service.
