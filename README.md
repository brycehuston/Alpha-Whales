<div align="center">
  <h1>🐋 Alpha-Whales</h1>
  <p><strong>High-Frequency, MEV-Protected Solana Execution Engine</strong></p>
</div>

---

## ⚡ Overview
**Alpha-Whales** is an ultra-low latency, Rust-based algorithmic execution engine designed for the Solana blockchain. It acts as a highly optimized shadowing bot that listens for transaction signals from top-tier, highly profitable wallets (Whales) via **Helius Webhooks**, and front-runs the public mempool by routing execution bundles directly through the **Jito Block Engine**.

Designed specifically for memecoin trading, it features dynamic capital allocation, real-time Time-Weighted ROI tracking, and aggressive adaptive scaling logic to secure initial capital during parabolic price action.

---

## 🧠 Core Architecture

### 1. Helius Webhook Ingestion
The bot runs a blazing-fast, authenticated `axum` HTTP server that ingests Enhanced Transaction JSON payloads pushed directly from Helius nodes in milliseconds. It filters for `SWAP` events triggered by monitored wallets, parses the exact token mint, and skips the slow polling of typical RPCs.

### 2. Dynamic Capital Allocation ("Whale Lanes")
Not all whales trade the same. The engine loads an `approved_watchlist.csv` database containing the historical win-rates and profit factors of every tracked wallet. When a signal is received, it performs an $O(1)$ lookup and automatically sizes the trade risk based on the wallet's historical performance tier:
* 🛡️ **Conservative Lane:** Max Size
* 🌊 **Swing Lane:** Moderate Size
* 🎲 **Degen Lane:** Low Size

### 3. MEV Protection & Dynamic Jito Tipping
Public mempools on Solana are infested with sandwich bots. Alpha-Whales bypasses this entirely by building and signing atomic transaction bundles and sending them directly to the Jito Block Engine. 
Furthermore, it actively monitors the `Jito Tip Stream` over gRPC to dynamically calculate the exact bribe required to land the bundle in the next block, ensuring execution even during massive network congestion.

### 4. Hybrid Exit Engine
Exit logic executes strictly in-memory on a dedicated asynchronous Tokio thread, reading live WebSocket tick data from Raydium:
* 🎯 **Adaptive Partial Scale-Out:** The engine calculates *Time-Weighted ROI* tick-by-tick. If a token hits **100% ROI in under 60 seconds**, the bot triggers a Velocity Breaker—instantly splitting the position in memory and dispatching a Jito bundle to sell exactly **50%** of the tokens to secure initial capital.
* 🛑 **Trailing Stop-Loss:** The remaining 50% "moonbag" is trailed by a dynamic stop-loss that locks in a floor and dumps the remainder if the price pulls back 20% from its absolute peak.
* 🚨 **Panic Velocity Breaker:** If a single-tick log-return drops below a lethal threshold (e.g., a dev rug-pull), the bot immediately abandons the trailing logic and dumps the entire bag. If the network is congested, it mathematically escalates the Jito tip by `1.5x` on every retry until the validator accepts the bundle.

---

## 🛠️ Installation & Setup

### Requirements
* Rust (`cargo`)
* Linux or Windows Subsystem for Linux (WSL2)
* Helius API Key (For Webhooks and RPC)
* Funded Solana Wallet Private Key

### Quick Start
1. **Clone the repository:**
   ```bash
   git clone https://github.com/brycehuston/Alpha-Whales.git
   cd Alpha-Whales
   ```

2. **Environment Configuration:**
   Create a `.env` file in the root directory and add your private credentials:
   ```env
   # API Keys
   HELIUS_API_KEY=your_helius_key_here
   WALLET_PRIVATE_KEY=your_base58_private_key_here

   # Execution Switches
   DRY_RUN=true
   LIVE_EXECUTION=false
   
   # Tipping & Safety Guardrails
   MAX_SLIPPAGE_BPS=500
   MAX_SIGNAL_AGE_MS=5000
   MAX_PENDING_CAPITAL_LAMPORTS=100000000
   ```

3. **Configure Watchlist:**
   Modify the `approved_watchlist.csv` to include the specific whale wallets you wish to track.

4. **Run the Daemon:**
   ```bash
   cargo run --release
   ```

---

## ⚠️ Disclaimer
**This software is for educational and research purposes only.** Algorithmic trading on highly volatile networks (like Solana) using highly volatile assets (like memecoins) carries extreme financial risk. Bugs, RPC latency, or unexpected network congestion can result in total loss of funds. The developers accept zero liability for any financial losses incurred while operating this software.

<div align="center">
  <p>Built for Speed. Built for Alpha.</p>
</div>
