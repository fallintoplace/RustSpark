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
