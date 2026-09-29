//! mainlineNERD passive Matrix ingestion.
//!
//! A durable SQLite event archive, a transport-agnostic engine, a live
//! matrix-sdk transport and a single-writer runtime that keeps one long-poll
//! `/sync` and paced `/messages` backfill concurrently in flight.

pub mod config;
pub mod engine;
pub mod event;
pub mod matrix;
pub mod runtime;
pub mod store;
