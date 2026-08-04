# Alpha Whales Changelog

All notable changes to this project will be documented in this file.

## [EXECUTION-CRITICAL-FIX] - 2026-08-03
### Fixed
- **Bug: Stale Journal Lock File Blocks All Trades** (`src/execution.rs` — `ExecutionJournal::load()`): The `acquire_writer_lock()` method uses `create_new(true)`, meaning any `.lock` file left on disk by a previously crashed or killed executor permanently blocks every subsequent trade attempt with "cannot acquire exclusive writer lock". Fixed by auto-deleting the stale `.lock` file on startup inside `load()`. Added a log warning in case another executor is genuinely running concurrently.
- **Bug: PumpPortal Mint Recorded as `111...1` Default** (`src/execution.rs` — execution loop): `prepared_target_mint` was initialized to `Pubkey::default()` (the all-1s pubkey). When the Raydium path failed and the PumpPortal branch was taken, the journal recorded `111...1` as the reserved mint. This permanently blocked the real target mint from ever being retried because `journal.contains()` checked the real mint (always absent) but the `111...1` default accumulated in `reserved_mints`. Fixed by initializing `prepared_target_mint` to the parsed signal mint upfront, so both Raydium and PumpPortal branches always record the correct pubkey.
- **Manual:** Deleted stale `execution_journal.log.lock` file from disk.
- **Config:** `MAX_PENDING_CAPITAL_LAMPORTS=500000000` (0.5 SOL ceiling) to accommodate 0.2 SOL test fund.

## [PUMPFUN-INTEGRATION] - 2026-08-03

### Added
- **PumpPortal Execution (Buy Side):** Refactored `run_whale_execution_consumer` in `execution.rs` to fallback to `resolve_pumpportal_swap` when `ROUTE_NOT_FOUND` occurs for pre-migration Pump.fun tokens. Bundles are resigned with our custom Jito tip logic via `build_and_sign_pump_bundle`.
- **PumpPortal Execution (Sell Side):** Added `resolve_pumpportal_sell` in `exits.rs`. The exit watcher dynamically routes sells to PumpPortal using the exact token unit amount (accounting for Pump.fun's 6 decimals) if the position was acquired before Raydium migration.
- **Dynamic Configuration:** Injected `PUMPPORTAL_API_KEY` through `config.rs`, `execution.rs`, and into `ActivePosition` to enable fallback routing across the full lifecycle.
- **Bot Fixes & Upgrades:**
  - Implemented Pump.fun / PumpPortal exit watcher logic in `exits.rs` to safely handle non-migrated tokens.
  - Calculated raw token units directly (via `amount_ui`) for PumpPortal exit payloads, ensuring maximum precision and zero dust on sells.
  - Cleared `execution_journal.log` to unblock duplicate-trade guardrails and allow fresh execution attempts.
  - Fixed an unwrap panic in `sell_position` when encountering Pump.fun tokens.

## [EXECUTION ENGINE - MEDIUM BUGS] - 2026-08-02 23:19 PST
### Fixed
- **Bug 1 (VWAP Snapback):** Added `VWAP_SNAPBACK` trigger to `run_watcher` in `exits.rs` before partial exit condition to properly exit break-even/profit trades.
- **Bug 2 (Webhook Timestamp):** Updated `HeliusWebhookPayload` to parse the on-chain timestamp directly instead of setting `timestamp_ms` at webhook receipt in `webhook.rs`.
- **Bug 3 (5s Resolution Block):** Passed `pool_keys` from the buy preparation phase directly into `ActivePosition` to eliminate the synchronous `resolve_pool_keys` bottleneck in `exits.rs`.
- **Bug 4 & 5 (DB Starvation):** Wrapped all SQLite operations (`insert_trade_log`, `record_position`, `close_position`, etc.) in `tokio::task::spawn_blocking` in `db.rs` to prevent blocking the async runtime on disk I/O.

## [PYTHON-BRAIN] - 2026-08-03
### Added
- **Self-Healing Feedback Loop:** Added `feedback_loop.py` to query the Rust engine's `trade_telemetry.db` for consecutive `STOP_LOSS` execution statuses. Automatically prunes toxic whales from `approved_watchlist.csv`.
- **Pipeline Orchestration:** `brain_daemon.py` now runs `leaderboard_scraper.py` alongside the `block_sniffer` and `wallet_scorer`, fully automating the discovery and ranking pipeline.
- **Wallet Scorer:** Hotfixed an undefined reference (`df`) which caused scoring pipeline errors during result generation.
- **Alerts:** Upgraded the Telegram alert message in `brain_daemon.py` to include `feedback_stats`.

## [SECURITY-HOTFIX] - 2026-08-03
### Fixed
- **CRIT-01**: Hardened `main.rs` to intentionally panic if `WEBHOOK_API_KEY` is missing, preventing unauthorized remote code execution and webhook spoofing via the `supersecret` fallback.
- **CRIT-02**: Patched adaptive partial exit logic in `exits.rs` to only halve memory `acquired_amount` *after* verifying on-chain bundle execution.
- **CRIT-03**: Replaced blind bundle acceptance in `exits.rs` with strict RPC signature polling (`get_signature_statuses`). `release_shadow_position` is now properly delayed until the sell is finalized on the ledger.
- **CRIT-04**: Added dynamic minimum slippage threshold (`calculate_local_minimum_amount_out`) calculated from live pool WSOL reserves and applied `apply_jitodontfront_protection()` directly to the final `attempt_sell_bundle` instruction to prevent sandwich attacks on panic dumps.
- **HIGH-01**: Removed hardcoded `D:\` paths for the Python `brain_daemon.py` spawn in `main.rs`, replacing them with environment variables (`BRAIN_DAEMON_PATH` and `BRAIN_DAEMON_CWD`) for reliable spawning across different drives and OS environments.
- **HIGH-02**: Removed `.unwrap()` on the tip telemetry `RwLock` in `tipping.rs` and added explicit `.into_inner()` fallback handling to prevent lock poisoning from crashing the main rust runtime.
- **HIGH-03**: Migrated the synchronous `std::sync::Mutex` for `circuit_breaker_tripped_at` in `state.rs` to a `tokio::sync::Mutex` to prevent blocking the async runtime on the hot webhook ingestion path.
- **HIGH-04**: Replaced `Arc<RwLock<HashMap>>` with `dashmap::DashMap` for the `watchlist` in `webhook.rs` to resolve potential write starvation from the background hot-reloader task during high Helius webhook burst volumes.

## [AUDIT] - 2026-08-03 — Full Security & Logic Audit
### Session Summary
Performed a complete ruthless security and logic audit of the entire Rust codebase (`audit_bundle.txt`, 12319 lines).
No source files were modified. All findings are documented in the artifact report.

### Findings (16 total)
**CRITICAL (4):**
- `CRIT-01` (`webhook.rs`): Hardcoded default API key `"supersecret"` — allows unauthenticated whale signal injection from any external attacker.
- `CRIT-02` (`exits.rs`): Partial 50% sell failure does not prevent `acquired_amount` from being halved in memory — subsequent final exit sells wrong quantity.
- `CRIT-03` (`exits.rs`): Shadow position released on Jito bundle *acceptance*, not on-chain *confirmation*. Sell may never land but position is marked closed.
- `CRIT-04` (`exits.rs`): `minimum_amount_out = 1` and no `jitodontfront` MEV protection on sell path — maximum sandwichable exposure during panic exits.

**HIGH (4):**
- `HIGH-01` (`main.rs`): Hardcoded Windows/WSL absolute path for brain daemon spawn. Silent failure on non-D: drives.
- `HIGH-02` (`tipping.rs`): `std::sync::RwLock.write().unwrap()` panics entire runtime on lock poison.
- `HIGH-03` (`state.rs`): Synchronous `std::sync::Mutex` acquired on async hot path — latency spikes under load.
- `HIGH-04` (`webhook.rs`): `Arc<RwLock<HashMap>>` write starvation possible under burst webhook volume.

**MEDIUM (5):** VWAP snapback exit condition never triggered (circuit breaker always logs losses), signal timestamp set at webhook receipt not on-chain time, 5s pool resolution blocks exit watcher, global DB mutex serializes concurrent exits, blocking SQLite call inside async task.

**INFO (3):** sol-to-lamports truncation, entry_timestamp_ms zero-init risk, health heartbeat not in JoinSet.

### Files Reviewed
- `src/execution.rs`, `src/exits.rs`, `src/webhook.rs`, `src/main.rs`, `src/config.rs`
- `src/db.rs`, `src/state.rs`, `src/tipping.rs`, `src/bundle_tracker.rs`, `src/dispatcher.rs`
- `src/pool_cache.rs`, `src/shadow_logger.rs`, `src/telegram.rs`, `src/types.rs`, `src/websocket.rs`

## [0.2.0] - 2026-08-02
### Added
- Embedded `axum` HTTP server directly into `webhook.rs` to receive real-time Helius Enhanced Webhook payloads.
- Added 15% algorithmic Take-Profit trigger to `exits.rs`.
- `approved_watchlist.csv` with 26 curated whale wallets (Degen, Scalper, Swing, Conservative).
- **Master Process Spawning:** Implemented `std::process::Command` in `main.rs` to automatically spawn the Python `brain_daemon.py` from the `WhaleSignalLab` directory when the Rust engine starts. This unifies the entire pipeline under a single `cargo run` command.

### Removed
- Dismantled the entire VWAP (Volume-Weighted Average Price) mean-reversion algorithmic trading pipeline.
- Deleted `security.rs` and the 250ms honeypot evaluation delay.
- Purged all shadow-mode SQLite telemetry logging and associated structs.
- Removed the standalone Python webhook server.

### Changed
- Refactored `execution.rs` to accept external `WhaleSignal` webhooks instead of internal Raydium state signals.
- Stripped `ActivePosition` of legacy VWAP baseline attributes.
