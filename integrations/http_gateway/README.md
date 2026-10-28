# Experimental authenticated SQL template gateway

This optional Python 3.10+ standard-library example exposes **only SQL files
owned by the operator**. It deliberately does not accept arbitrary SQL text
from HTTP requests. This is a loopback-only development integration, not an
internet-facing service or multi-tenant security boundary.

1. Create a persistent RustSpark database and a table named `events`.
2. Create a directory with operator-reviewed SQL files such as `queries/example.sql`.
3. Set a random token of at least 24 characters and start the server:

```sh
export RUSTSPARK_GATEWAY_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
python3 integrations/http_gateway/server.py \
  --database-dir /tmp/rustspark-db \
  --queries-dir integrations/http_gateway/queries \
  --rustspark ./target/release/rustspark
```

```sh
curl -X POST http://127.0.0.1:8787/v1/query/example \
  -H "Authorization: Bearer $RUSTSPARK_GATEWAY_TOKEN" \
  -H "Content-Type: application/json" \
  --data '{"params":{"kind":"utf8:purchase"}}'
```

Returns `{"rows":[...]}`. Paths cannot be selected by clients.
The handler caps requests at 64 KiB and limits returned rows.
Each query launches a new RustSpark process. Database sessions use a
single-writer file lock, so this serial HTTP server should not be scaled
out against the same local database.

**Security limitations:** SQL template authors can still run mutations
in the database. Review every template and use OS-level file permissions,
read-only backup copies, and a separate low-privilege service account.
The example does not implement TLS, audit logging, per-tenant isolation,
distributed scheduling, a connection pool, or hardened request/body limits
for internet deployments.

Test the parser and command construction:

```sh
cd integrations/http_gateway && python3 -m unittest -v test_gateway.py
```
