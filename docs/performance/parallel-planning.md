# tune multi-branch SQL planning concurrency

Pass `--planning-concurrency N`, measured in concurrent planning tasks, to set DataFusion's `execution.planning_concurrency`. 

Parallelizing independent UNION branches can reduce planning latency when schemas or file listings are expensive.

No default changes. On a release build, compare actual Parquet query timings, memory use and `--explain-analyze` plans before increasing this value for production.
