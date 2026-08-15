# Alpha Agents

Shared Solana execution infrastructure with strategy-specific bot crates. The active workspace currently contains:

- `core/` — shared WebSocket ingestion, Jito dispatch, tipping, state, database, and Telegram infrastructure.
- `alpha-whales/` — whale-wallet signal execution and automated exit logic.

Planned strategies are `alpha-telegram-whales`, `alpha-dbo`, `alpha-trends`, and paper-only `alpha-x`. They are not registered until each has distinct, tested signal logic. Do not clone `alpha-whales` and relabel it as a new strategy.

## Safety state

- Default development mode is paper/shadow execution: `DRY_RUN=true` and `LIVE_EXECUTION=false`.
- Live execution requires the inverse pair explicitly and a funded signer.
- Never commit private keys, bot tokens, RPC credentials, `.env` files, journals, or telemetry databases.
- Jito bundle acceptance is not treated as settlement; transaction confirmation remains required.

## Setup

Requirements: Rust, WSL2, a private HTTPS Solana RPC endpoint, and a WSS transaction stream.

Create an ignored `.env` file in the repository root:

```env
RPC_URL=https://YOUR_PRIVATE_RPC_ENDPOINT
RAYDIUM_WS_URL=wss://YOUR_PRIVATE_TRANSACTION_STREAM
WALLET_PRIVATE_KEY=YOUR_BASE58_PRIVATE_KEY
WEBHOOK_API_KEY=YOUR_RANDOM_WEBHOOK_AUTH_SECRET

DRY_RUN=true
LIVE_EXECUTION=false

TELEGRAM_BOT_TOKEN=YOUR_TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID=YOUR_TELEGRAM_CHAT_ID
PUMPPORTAL_API_KEY=YOUR_PUMPPORTAL_API_KEY

MIN_SWAP_LAMPORTS=10000000
JITO_REGIONS=amsterdam,frankfurt,ny,slc,tokyo
```

Keep optional Telegram and PumpPortal values unset when those integrations are unused.

Configure `approved_watchlist.csv`, then validate from WSL:

```bash
cargo check --workspace --all-targets --offline
cargo test --workspace --offline
cargo clippy --workspace --all-targets --offline -- -D warnings
```

Run the current strategy only after validation:

```bash
cargo run -p alpha-whales-bot --release
```

## Current boundary

The current bot consumes authenticated Helius whale-wallet webhook events and an on-chain Raydium transaction stream. The Telegram-channel adapter, Alpha DBO, Alpha Trends, and isolated Alpha X still require separate paper-mode implementations and replay evidence before registration.
# Alpha-Agents
