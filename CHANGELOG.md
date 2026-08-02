# Alpha Whales Changelog

All notable changes to this project will be documented in this file.

## [0.2.0] - 2026-08-02
### Added
- Embedded `axum` HTTP server directly into `webhook.rs` to receive real-time Helius Enhanced Webhook payloads.
- Added 15% algorithmic Take-Profit trigger to `exits.rs`.
- `approved_watchlist.csv` with 26 curated whale wallets (Degen, Scalper, Swing, Conservative).

### Removed
- Dismantled the entire VWAP (Volume-Weighted Average Price) mean-reversion algorithmic trading pipeline.
- Deleted `security.rs` and the 250ms honeypot evaluation delay.
- Purged all shadow-mode SQLite telemetry logging and associated structs.
- Removed the standalone Python webhook server.

### Changed
- Refactored `execution.rs` to accept external `WhaleSignal` webhooks instead of internal Raydium state signals.
- Stripped `ActivePosition` of legacy VWAP baseline attributes.
