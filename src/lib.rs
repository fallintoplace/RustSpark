//! RustSpark's native SQL engine.
//!
//! The previous DataFusion-backed session and DataFrame API has been removed.
//! All public exports now originate in the independent RustSpark-native engine.
#![forbid(unsafe_code)]

pub use rustspark_native::*;
