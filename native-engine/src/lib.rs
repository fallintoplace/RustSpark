//! RustSpark's independent native query engine.
//!
//! The native engine does not depend on DataFusion. It is developed beside the
//! existing RustSpark CLI until correctness and performance gates permit migration.
#![forbid(unsafe_code)]

pub mod error;

pub use error::{Error, Result};

pub mod scalar;

pub mod schema;

pub mod batch;

pub mod catalog;

pub mod expr;

pub mod filter;

pub mod project;

pub mod limit;

pub mod sort;

pub mod aggregate;

pub mod distinct;

pub mod join;

pub mod union;

pub mod plan;

pub mod executor;

pub mod lexer;

pub mod sql;

pub mod csv;

pub mod memory;

pub mod batch_utils;

pub mod cancel;

pub mod pipeline;

pub mod hash_partition;

pub mod csv_stream;

pub mod statistics;

pub mod pruning;

pub mod partial_aggregate;

pub mod stage_graph;

pub mod transport;

pub mod task_scheduler;

pub mod metrics;

pub mod optimizer;

pub mod scale;

pub mod stream_scan;

pub mod vector_filter;

pub mod topk;

pub mod parallel_map;

pub mod bloom;

pub mod stages;

pub mod cancellation;

pub mod batch_chunks;

pub mod arrow_bridge;

pub mod spill_ipc;

pub mod external_sort;

pub mod parquet_scan;

pub mod builtin;
