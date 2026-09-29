//! mainlineNERD passive Matrix ingestion.
//!
//! This crate contains the ingestion MVP: a durable SQLite event archive with a
//! transport-agnostic engine. The real Matrix adapter (matrix-sdk) is a
//! separate, not-yet-implemented piece; see `docs/architecture.md`.

pub mod engine;
pub mod event;
pub mod store;
