# RustSpark MCP server (local, template-only)

The local MCP stdio server exposes two tools: `list_queries` and
`run_named_query`. An agent can **only invoke approved SQL files**,
not submit arbitrary SQL or filesystem paths. Data returned by those
queries is untrusted model input.

This example targets the official Python MCP SDK v2:

```sh
python3 -m pip install 'mcp>=2,<3'
export RUSTSPARK_MCP_QUERIES_DIR="$PWD/integrations/mcp/queries"
export RUSTSPARK_MCP_DATABASE_DIR="/tmp/rustspark-db"
export RUSTSPARK_BIN="$PWD/target/release/rustspark"
python3 integrations/mcp/server.py
```

Create a persistent RustSpark database and an `events` table before
running the example template. Add SQL files only after reviewing them.
Template SQL can still mutate local data if an operator puts DDL or DML
in a file; this is not a database-level read-only sandbox.

Configure your MCP host to launch `python3 integrations/mcp/server.py`
with the environment variables above. The server uses **stdio only**,
not unauthenticated HTTP. The host is responsible for tool approvals
and local process access. No credentials or SQL arguments are printed
to the protocol stream.

```sh
cd integrations/mcp && python3 -m unittest -v test_query_templates.py
```
