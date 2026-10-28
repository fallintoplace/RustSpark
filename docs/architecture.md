# RustSpark architecture and product boundaries

RustSpark 0.1.x is an **experimental local analytical SQL tool** powered by
Apache DataFusion. The local Rust CLI and Rust API are the primary product.
It is not a drop-in Apache Spark replacement, a multi-writer ACID database,
or a production distributed query service.

This document describes what the existing components own and the guarantees
new features should preserve. It is a design boundary, not a claim that all
experimental features are production ready.

## The local query path

1. The CLI in `src/cli.rs` parses input and execution settings. Registration,
   execution, output, persistence, and validation live in `src/cli/`.
2. `RustSparkSession` in `src/lib.rs` provides typed registration, DataFrames,
   SQL access, compatibility functions, and local helper operations.
3. **DataFusion** owns SQL planning, optimization, physical operators, memory
   accounting, and supported spilling. Keep its default behavior unless an
   opt-in setting has measured value for a defined workload.
4. **Arrow and Parquet** are the columnar types and default analytical storage
   format. CSV and JSON readers are available for ingestion and ad hoc input.
5. `--database-dir` manages a separate, single-writer local persistent catalog
   backed by Parquet snapshots. Its crash-recovery protocol is **not ACID**.

A local query must never require the optional Ballista integration or a mounted
replicated-storage topology.

## Experimental integrations

**Ballista** lives in `integrations/ballista/` with an independent Cargo
project. It is a remote-session feasibility integration, not a `rustspark
--distributed` product. Real multi-host operation needs shared data access
and independent failure/recovery qualification.

**Replicated storage** lives in `src/distributed_storage/`. Its immutable
objects, manifests, snapshot generations, placement, and repair operate on
filesystem roots already mounted by the caller. It does not implement a
network protocol, consensus, automatic node membership, or cross-node atomic
publication. Snapshot materialization copies Parquet parts into a new query
directory. Copies and replicas are not independent backups unless explicitly
created as such.

Do not expose either experimental subsystem through the main CLI as a
transparent local-mode replacement without a documented ownership, security,
failure, and consistency contract.

## Boundaries for new features

- **SQL compatibility:** implement semantic differences only with documented
  DataFusion behavior and regression cases. A passing function-level test
  does not mean Spark SQL or Spark API compatibility.
- **Performance:** prefer improvements tied to a profile of scan, decode,
  filter, join, aggregation, repartition, spill, or result-output costs.
  Avoid multiplying user-facing optimizer aliases for equivalent settings.
- **Storage:** explicitly state durability, concurrent-reader/writer behavior,
  file ownership, failure atomicity, and recovery limits. Never convert a
  successful process-crash test into a power-loss durability guarantee.
- **Data access:** avoid loading multi-gigabyte objects into a single Vec or
  copying complete Parquet datasets on each query when direct safe reads are
  possible. Keep memory usage bounded and protect immutable objects.
- **Configuration:** each DataFusion tuning target should have one canonical
  internal assignment. Preserve existing CLI names during migrations but
  reject combinations that otherwise silently override each other.
- **Public API:** add focused operations rather than more near-duplicate
  convenience variants. Keep the low-level DataFusion escape hatch available.
- **Testing:** prefer a small failure reproduction, a correctness oracle, and
  cross-process tests for persistence. Split slow integration or benchmark
  tests from quick unit tests; document the limits of each.

## Evidence before performance claims

A benchmark script or passing smoke test is not evidence of speed.

For meaningful TPC-H or ClickBench comparisons, record the exact source
revision, SQL fingerprints, Parquet input checksums and source row counts,
hardware/OS, engine versions, memory budget, partition count, repeat count,
warm/cold protocol, median and p95 wall-clock latency, and independent result
verification. Do not compare a warm RustSpark query with a cold competitor
query. Metadata-only COUNT(*) does not establish raw billion-row scan speed.

See [billion-row performance](billion-row-performance.md),
[engine comparison](../benchmarks/engine-comparison.md), and
[V1 acceptance](v1-acceptance.md). Feature-level tuning options are
experiments until the measured report demonstrates their effect.

## Merge and release qualification

The repository includes Rust, Python, TPC-H, Spark-compatibility, and storage
checks. Passing unit tests and scripts do not automatically enforce a GitHub
merge rule. **Repository admins must enable branch protection or a ruleset**
requiring the desired completed checks, blocking direct pushes to main, and
preventing merging while checks are pending or cancelled.

Recommended minimum before a release:

1. Require the core Rust workflow to pass for the exact commit on main.
2. Complete relevant local persistence, cross-platform, and optional
   distributed-storage checks. Do not treat a skipped path-filtered workflow
   as a passing run.
3. Run representative real SQL results against an independent reference.
4. Run the release workflow on the intended commit, including archive and
   cross-process storage smoke checks.
5. Publish explicit experimental limitations and benchmark conditions.

## Product scope decisions

**Near term:** a trustworthy single-node analytics CLI and Rust library, with
a focused local persistence mode. Optimize measurable user workflows, support
documented inputs, and improve failure handling.

**Separate research:** distributed execution, replica placement, and
Spark-semantic compatibility. They should progress under explicit integration
contracts and failure tests, not feature-count goals.

**Out of scope for 0.1.x guarantees:** ACID multi-statement transactions,
multi-writer storage, Spark API equivalence, transparent multi-host execution,
self-managing distributed storage, and unmeasured speedup claims.
