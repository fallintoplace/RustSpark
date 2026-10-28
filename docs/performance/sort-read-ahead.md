# tune sort-pushdown scan read-ahead

Pass `--sort-read-ahead-mb N`, measured in MiB, to set DataFusion's `execution.sort_pushdown_buffer_capacity`. 

Buffered scans can overlap I/O with a downstream sort-preserving merge, reducing stalls on ordered large Parquet inputs.

No default changes. On a release build, compare actual Parquet query timings, memory use and `--explain-analyze` plans before increasing this value for production.
