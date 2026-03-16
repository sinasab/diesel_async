//! Postgres COPY FROM support
//!
//! This module provides asynchronous support for PostgreSQL's `COPY FROM` command,
//! bridging Diesel's synchronous `COPY FROM` DSL to `tokio-postgres`.
//!
//! There are two approaches available:
//!
//! ## Synchronous (buffered) — via Diesel's `CopyFromExpression`
//!
//! Uses Diesel's existing synchronous `CopyFromExpression` trait. The entire payload is
//! buffered in memory before being sent to the database. This is the simplest approach
//! and works well for small-to-medium datasets.
//!
//! See [`sync_compat`] for details and the [`AsyncExecuteCopyFromDsl`] trait.
//!
//! ## Asynchronous (streaming) — via `AsyncCopyFromExpression`
//!
//! For large datasets where buffering everything in memory is undesirable, implement
//! [`AsyncCopyFromExpression`] which yields data as an async stream of `Bytes` chunks.
//! Each chunk is forwarded to the database incrementally, keeping memory usage constant.

mod async_copy_from;
pub mod sync_compat;

pub use async_copy_from::*;
pub use sync_compat::AsyncExecuteCopyFromDsl;
