# tune in-place sort size threshold

Pass `--sort-in-place-kb N`, measured in KiB, to set DataFusion's `execution.sort_in_place_threshold_bytes`. 

Small in-memory sorts can skip batch-merging overhead; larger thresholds may increase peak allocation and hurt wide partitions.

No default changes. On a release build, compare actual Parquet query timings, memory use and `--explain-analyze` plans before increasing this value for production.
