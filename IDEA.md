Alpha-X-Whales: Solana MEV & Copy-Trading Execution Engine

This project is a highly profitable, zero-latency Solana trading system engineered with a strict Producer/Consumer architecture across two decoupled repositories:

The Brain (Python - WhaleSignalLab): An asynchronous data pipeline that scrapes Telegram and GMGN for alpha. It stores discoveries in an append-only SQLite time-series database, continuously scores wallets against strict win-rate/profit metrics to prune "Stale Alpha", outputs an approved_watchlist.csv, and automatically syncs the active wallets to the Helius Webhook API.

The Muscle (Rust - Alpha-Whales): A hyper-optimized execution client. It automatically hot-reloads the Python brain's CSV watchlist into memory without restarting. It listens on a local port for Helius Webhook payloads, calculates dynamic slippage and Jito tips, and fires sub-second atomic transactions directly to the Jito Block Engine to snipe or copy-trade the elite whales.

Core Directives: Net profitability, strict capital preservation (fail-closed), zero-latency execution, and fully autonomous asynchronous operation.
