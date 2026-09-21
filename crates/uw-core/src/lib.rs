//! uw-core: domain types and the `HarnessAdapter` trait for usagewindow.
//!
//! This crate is the public library surface — other applications depend on
//! it to query resume state and usage without pulling in harness-specific
//! adapter code or a storage backend. See docs/architecture.md.

pub mod adapter;
pub mod api;
pub mod compaction;
pub mod model;
pub mod summarizer;
pub mod threshold;
