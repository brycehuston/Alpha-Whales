//! alpha-agents-core — Reusable execution infrastructure.
//!
//! This library crate contains every piece of the Alpha-Agents execution engine
//! that is strategy-agnostic: Jito dispatch, bundle tracking, tipping, database,
//! Telegram alerts, WebSocket ingestion, pool caching, and bot state management.
//!
//! Trading-strategy logic lives in the binary crates (e.g. `alpha-whales`).

#![allow(
    clippy::result_large_err,
    clippy::too_many_arguments,
    clippy::enum_variant_names
)]

pub mod bundle_tracker;
pub mod config;
pub mod db;
pub mod dispatcher;
pub mod error;
pub mod pool_cache;
pub mod state;
pub mod geyser_stream;
pub mod tipping;
pub mod types;
pub mod websocket;
