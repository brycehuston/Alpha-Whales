<div align="center">
  <h1>🐋 Alpha-Agents (Alpha-Whales)</h1>
  <p><strong>A high-performance Solana MEV & sniping bot architecture</strong></p>
  <p>
    <img src="https://img.shields.io/badge/Solana-MEV-blueviolet?style=for-the-badge&logo=solana" alt="Solana MEV" />
    <img src="https://img.shields.io/badge/Rust-Blazing_Fast-orange?style=for-the-badge&logo=rust" alt="Rust" />
    <img src="https://img.shields.io/badge/Jito-Block_Engine-green?style=for-the-badge" alt="Jito" />
  </p>
</div>

---

## ⚡ Overview

Alpha-Agents is a high-frequency, ultra-low latency execution engine built in Rust. Currently pivoting to zero-latency **Geyser gRPC streams** (via Yellowstone) to snipe Slot-0 state and front-run optimal MEV opportunities.

### 🎯 Key Features
- **Zero-Latency Ingestion**: Sub-millisecond banking-stage extraction using Helius Geyser gRPC streams.
- **Jito Block Engine Integration**: Direct bundle assembly and transmission with dynamic tipping for sandwich protection and guaranteed execution.
- **Pump.fun Targetting**: Dedicated logic for identifying and sniping bonding curves on `6EF8rrecthR5Dkzon8Nwu78hRvfX9MLnqiX+`.
- **Position Management**: Built-in 16% Trailing Stop Loss (TSL) and 50% Take Profit (TP) lifecycle management.

## 🏗️ Architecture

- `core/`: The foundational ingestion pipeline and infrastructure (Geyser stream, connection handling, DB, configurations).
- `alpha-whales/`: The main execution binary consuming the `token_mint` stream, routing Jito bundles, and handling position tracking.
- `alpha-dbo/`: Database object and historical telemetry management crate.

## 🚀 Getting Started

### Prerequisites
- [Rust & Cargo](https://rustup.rs/)
- [Solana CLI](https://docs.solana.com/cli/install-solana-cli-tools) `v2.1.0+`
- A Helius WSS/gRPC endpoint
- WSL 2 (Windows Subsystem for Linux) is **required** for compiling Jito dependencies.

### Environment Setup

Create a `.env` file in the root directory:
```env
WALLET_PRIVATE_KEY=your_base58_private_key
HELIUS_WSS_URL=wss://mainnet.helius-rpc.com/?api-key=your_key
HELIUS_X_TOKEN=your_grpc_token
MAX_SLIPPAGE_BPS=50
MAX_SIGNAL_AGE_MS=1500
MAX_PENDING_CAPITAL_LAMPORTS=1000000000
EXECUTION_JOURNAL_PATH=/path/to/journal.log
JITO_DONT_FRONT_PUBKEY=...
```

### Build & Run
Run all Cargo commands within a WSL bash environment:

```bash
# Verify the build
cargo check

# Run the execution binary
cargo run --release -p alpha-whales
```

## ⚠️ Disclaimer
**Use at your own risk.** MEV and high-frequency trading involve substantial financial risk. The bot is strictly designed to fail-closed to protect capital, but market volatility on Solana is unpredictable. Always test with small amounts or on Devnet first.
