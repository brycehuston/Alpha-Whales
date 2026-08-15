Alpha Agents: Solana Signal and Execution Workspace

This project is a highly profitable, zero-latency Solana trading system engineered with a strict Producer/Consumer architecture across two decoupled repositories:

The Brain (Python - WhaleSignalLab): An asynchronous data pipeline that scrapes Telegram and GMGN for alpha. It stores discoveries in an append-only SQLite time-series database, continuously scores wallets against strict win-rate/profit metrics to prune "Stale Alpha", outputs an approved_watchlist.csv, and automatically syncs the active wallets to the Helius Webhook API.

The Muscle (Rust - `alpha-whales/`): A focused execution client. It hot-reloads the Python brain's CSV watchlist, receives authenticated Helius webhook payloads, calculates bounded slippage and Jito tips, and submits protected transactions to the Jito Block Engine.

Core Directives: Net profitability, strict capital preservation (fail-closed), zero-latency execution, and fully autonomous asynchronous operation.
