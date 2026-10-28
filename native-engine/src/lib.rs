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

pub mod parallel_map;
