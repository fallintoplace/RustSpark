# Product integration CI

`.github/workflows/product-integrations.yml` runs fast unit tests for
Python, Node.js and Go clients, optional data frame adapters, and
development integration examples. The jobs trigger only for integration
and related workflow changes, and can also be invoked manually.

Missing optional integration directories are skipped, so this CI can
land independently before the adapter PRs. Once an integration merges,
its unit tests run on relevant changes.

No external database, PyArrow, Polars, Pandas, Streamlit or MCP server
is required for the first-level tests. These mock or validate subprocess
argument construction. **They are not substitutes for end-to-end**
tests with an actual RustSpark binary and production datasets.

The main Rust workflow continues to test the engine. Do not interpret
a green integration-unit workflow as an enterprise deployment gate.
