# Alpha Whales Agent Customization Rules

These rules dictate how AI agents should interact with this codebase.

## 1. Architectural Philosophy
- **Speed Above All**: This is an MEV/Front-running codebase. Any changes to the execution pipeline (`execution.rs`, `webhook.rs`) must be zero-allocation and hyper-optimized.
- **Trusted Inputs**: The system relies on the Helius Enhanced Webhook via a trusted Python signal engine. Do not attempt to add on-chain verification or honeypot scanning inside the Rust execution block. The Python layer handles security; Rust handles speed.
- **Dynamic Sizing**: Any future implementation of position sizing must rely on the `approved_watchlist.csv` to dynamically adjust `TRADE_AMOUNT_LAMPORTS`.

## 2. Testing & Deployment
- **Dry Run vs Live**: The bot uses `.env` toggles (`DRY_RUN` and `LIVE_EXECUTION`). When testing new Webhook structures, always default to `DRY_RUN=true` and `LIVE_EXECUTION=false` to safely print JSON payloads.
- **No Python Execution**: The Python codebase (`webhook_server.py`, `security.py`) is legacy or strictly for off-chain analysis. Do not pipe Python output into Rust via IPC/Sockets. Use HTTP webhooks directly into the Rust `axum` server.
