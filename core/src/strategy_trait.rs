//! strategy_trait.rs — Generic trading strategy interface.
//!
//! Any struct implementing `TradingStrategy` can be wired into the core
//! execution loop without modifying infrastructure code.  The whale
//! copy-trading strategy is implemented in the `alpha-whales` binary crate.
//! Future strategies (e.g. mean-reversion bands) live in their own binary
//! crates and implement this same interface.

use crate::types::{SwapEvent, WhaleSignal};
use async_trait::async_trait;

/// A pluggable signal evaluator.
///
/// # Contract
/// - `should_enter` is called on the **hot signal path** immediately after a
///   whale webhook is received.  Implementations MUST be fast — no blocking
///   I/O, no expensive computation.
/// - `should_exit` is called for **every** incoming `SwapEvent` while a
///   position is open.  Implementations should use pre-computed baselines
///   stored in the strategy struct rather than re-deriving them each tick.
/// - Both methods receive shared references and are therefore safe to call
///   concurrently from multiple position watchers.
#[async_trait]
pub trait TradingStrategy: Send + Sync + 'static {
    /// Human-readable identifier used in log lines and telemetry.
    fn name(&self) -> &'static str;

    /// Evaluate an incoming whale signal.
    ///
    /// Returns `true` if the strategy wants to open a position for this
    /// signal, `false` to skip it.
    async fn should_enter(&self, signal: &WhaleSignal) -> bool;

    /// Evaluate a live price tick against an open position.
    ///
    /// - `event`: the latest `SwapEvent` from the Raydium WebSocket feed.
    /// - `entry_price_lamports`: WSOL lamports per token-raw-unit at the
    ///   moment of the confirmed buy.
    ///
    /// Returns `true` if the strategy's exit conditions (TP / SL / velocity
    /// breaker) are met and the position should be closed immediately.
    async fn should_exit(&self, event: &SwapEvent, entry_price_lamports: u64) -> bool;
}
