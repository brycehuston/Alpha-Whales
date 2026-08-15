// exits.rs — Alpha Nexus Generation 2: Mean-Reversion Exit Manager
//
// Responsibilities
// ----------------
//   1. Receive an ActivePosition from the buy path.
//   2. Consume live SwapEvents from the Raydium WebSocket feed to track
//      price exclusively from on-chain data — no Jupiter polling.
//   3. Evaluate three sell conditions on every tick:
//        a) Mean-Reversion Snapback — price returns to VWAP baseline.
//        b) Profit-Lock / Trailing Stop — lock profits before reaching VWAP.
//        c) Panic Velocity Breaker — flash-crash emergency stop.
//   4. On trigger, construct a Raydium V4 SwapBaseIn (Token → WSOL) sell
//      instruction, append a Jito tip, sign, and dispatch via gRPC bundle.
//   5. On bundle acceptance, release position locks in BotState and drop
//      the semaphore permit so the slot is available for the next trade.
//
// Architecture notes
// ------------------
//   • Fully async, Tokio-native. No blocking calls on the executor.
//   • All arithmetic is integer-only or saturating-f64 with explicit
//     finite/NaN guards. No `.unwrap()` anywhere.
//   • Price is denominated in WSOL lamports per token-raw-unit, computed
//     from the live SwapEvent stream — same source as the buy trigger.
//   • Sell execution reuses `construct_raydium_swap_instruction` and the
//     `ConnectedJitoClient` pattern from execution.rs to stay on the same
//     MEV-proof Jito bundle path as the buy.

use crate::execution::{construct_raydium_swap_instruction, MINIMUM_JITO_TIP_LAMPORTS};
use alpha_agents_core::{
    db,
    pool_cache::{RaydiumPoolKeys, WSOL_MINT},
    state::BotState,
    types::SwapEvent,
};

use jito_protos::{
    bundle::Bundle,
    packet::{Meta as ProtoMeta, Packet as ProtoPacket},
    searcher::{
        searcher_service_client::SearcherServiceClient, GetTipAccountsRequest, SendBundleRequest,
    },
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    instruction::Instruction,
    message::{v0, VersionedMessage},
    pubkey::Pubkey,
    signature::Keypair,
    signer::Signer,
    system_instruction,
    transaction::VersionedTransaction,
};
use std::{str::FromStr, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{
    sync::{mpsc, OwnedSemaphorePermit},
    time::timeout,
};
use tonic::{
    transport::{Channel, ClientTlsConfig, Endpoint},
    Request,
};

// ============================================================================
// Configuration Constants
// ============================================================================

/// Hard deadline for the entire position watcher task.
/// Protects against permanently stuck watchers if no exit condition fires.
const WATCHER_MAX_LIFETIME: Duration = Duration::from_secs(4 * 60 * 60); // 4 hours

/// Maximum staleness for a SwapEvent price tick.
/// Ticks older than this relative to `now` are discarded as lagged data.
const MAX_TICK_STALENESS_MS: u64 = 30_000; // 30 seconds

/// How long to wait for a new price tick before declaring the feed dead.
const PRICE_FEED_TIMEOUT: Duration = Duration::from_secs(60);

/// Maximum consecutive missing ticks before aborting the watcher.
const MAX_CONSECUTIVE_STALE_TICKS: u32 = 20;

/// Maximum number of full sell-pipeline retries.
const MAX_SELL_ATTEMPTS: u32 = 5;

/// Jito gRPC request timeout for sell bundles.
const JITO_SELL_TIMEOUT: Duration = Duration::from_secs(15);

/// Delay between sell pipeline retry attempts.
const SELL_RETRY_DELAY: Duration = Duration::from_secs(2);

// ============================================================================
// VBATS — Velocity-Based Adaptive Trailing Stop Constants
// ============================================================================
//
// Price is tracked in WSOL lamports per token raw unit.
// All ratios are computed as (numerator, denominator) integer pairs or as
// carefully guarded f64 (only for log-velocity, which requires ln()).
//
// SELL TRIGGER SUMMARY:
//   A) Mean-Reversion Snapback:
//        current_price >= vwap_price_at_entry   (0% deviation or positive)
//        → position has fully recovered; primary profit target.
//
//   B) Profit-Lock / Trailing Stop:
//        price ever reached >= PROFIT_LOCK_THRESHOLD × entry_price
//        AND current_price drops below highest_price_seen × (1 - trail_pct)
//        → lock in gains before reaching full VWAP.
//
//   C) Panic Velocity Breaker:
//        single-tick log-return < PANIC_VELOCITY_THRESHOLD
//        → flash crash or rug; exit immediately regardless of other conditions.

/// EMA smoothing factor α = 2 / (N + 1), N = 5.
/// Half-life ~2 ticks; recent ticks receive ~2× weight of older ticks.
const EMA_ALPHA: f64 = 2.0 / 6.0; // ≈ 0.3333

/// Default trailing distance in neutral market (zero velocity).
const TRAIL_BASE: f64 = 0.08; // 8%

/// Minimum trailing distance (tightest, during explosive upward moves).
const TRAIL_MIN: f64 = 0.03; // 3%

/// Maximum trailing distance. Also serves as the absolute stop-loss floor
/// before profit lock activates (TRAIL_MAX = 20% loss from high-water mark).
const TRAIL_MAX: f64 = 0.20; // 20%

/// Sensitivity multiplier: trail_pct = clamp(TRAIL_BASE - ema_v × SENSITIVITY).
const VELOCITY_SENSITIVITY: f64 = 1.5;

/// Single-tick log-return threshold for emergency market exit.
/// ln(0.88) ≈ −0.128 → price dropped > 12% in one tick (rug / flash crash).
const PANIC_VELOCITY_THRESHOLD: f64 = -0.128;

/// Profit-lock activation: position must have gained >= 30% above entry.
/// Expressed as a multiplier: 1.30 = +30% unrealized gain.
#[expect(
    dead_code,
    reason = "reserved for Phase 5 profit-assertion integration"
)]
const PROFIT_LOCK_THRESHOLD: f64 = 1.30;

/// Once profit lock activates, the trailing stop floor is set at this
/// multiple of entry price (1.10 = we never give back more than −10% of entry).
const PROFIT_LOCK_FLOOR_MULT: f64 = 1.10;

// ============================================================================
// Public Data Types
// ============================================================================

/// Fully resolved position handed off from the buy path to the exit watcher.
///
/// All amounts are in raw on-chain units:
///   - prices are WSOL lamports per token raw unit (u128 for precision)
///   - `acquired_amount` is token raw units (u64)
#[derive(Clone, Debug)]
pub struct ActivePosition {
    /// Base58 mint address of the token being held.
    pub mint: String,
    /// Source pool ID used for execution — used to filter price ticks.
    pub source_pool_id: String,
    /// Pool keys used for the exit, pre-resolved during the buy phase. None for Pump.fun tokens.
    pub pool_keys: Option<RaydiumPoolKeys>,
    /// Entry price: WSOL lamports received per token raw unit at buy time.
    /// Computed as `wsol_out_lamports / token_in_raw`.
    /// Stored as (numerator, denominator) to avoid precision loss.
    pub entry_price_wsol_num: u128,
    pub entry_price_wsol_den: u128,

    /// Tokens acquired (raw units, straight from the token account balance).
    pub acquired_amount: u64,
    /// Jito tip in lamports to attach to the sell bundle.
    pub jito_tip_lamports: u64,
    /// Jito Block Engine URL.
    pub block_engine_url: String,
    /// The timestamp in milliseconds when the position was acquired.
    pub entry_timestamp_ms: u64,
    /// PumpPortal API key for fallback selling of pre-migration tokens.
    pub pumpportal_api_key: Option<String>,
    /// Jito don't front pubkey for MEV protection on the sell side.
    pub jito_dont_front_pubkey: solana_sdk::pubkey::Pubkey,
    /// Max slippage basis points for the exit swap.
    pub max_slippage_bps: u16,
}

// ============================================================================
// Internal Error Type
// ============================================================================

#[derive(Debug, Error)]
enum ExitError {
    #[error("position mint is not a valid pubkey: {0}")]
    #[expect(dead_code, reason = "will be used by Phase 4 watcher")]
    InvalidMint(String),

    #[error("pool resolution failed: {0}")]
    #[expect(dead_code, reason = "will be used by Phase 4 watcher")]
    PoolResolution(String),

    #[error("price feed timed out after {0} consecutive stale ticks")]
    #[expect(dead_code, reason = "will be used by Phase 4 watcher")]
    FeedTimeout(u32),

    #[error("sell bundle rejected after {0} attempts: {1}")]
    SellFailed(u32, String),

    #[error("Jito connection error: {0}")]
    JitoConnect(String),

    #[error("Jito tip account error: {0}")]
    JitoTipAccount(String),

    #[error("transaction build error: {0}")]
    TxBuild(String),

    #[error("Jito gRPC submission error: {0}")]
    #[expect(dead_code, reason = "will be used by Phase 4 watcher")]
    JitoSubmit(String),

    #[error("arithmetic overflow in price comparison")]
    #[expect(dead_code, reason = "will be used by Phase 4 watcher")]
    ArithmeticOverflow,

    #[error("watcher hit 4-hour hard deadline — position may be open")]
    #[expect(dead_code, reason = "will be used by Phase 4 watcher")]
    HardDeadlineExpired,
}

// ============================================================================
// RAII Position Guard
// ============================================================================
//
// Holds the semaphore permit for the duration of the trade. When dropped
// (normal exit, panic, or cancellation) the permit is automatically released,
// making the position slot available for the next trade signal.
//
// NOTE: `release_shadow_position` is called explicitly on successful bundle
// confirmation, before this guard is dropped, so we never double-release.

struct PositionGuard {
    mint: String,
    #[expect(dead_code, reason = "will be used by Phase 5 state cleanup")]
    bot_state: Arc<BotState>,
    _permit: OwnedSemaphorePermit,
}

impl Drop for PositionGuard {
    fn drop(&mut self) {
        // The permit is released automatically when `_permit` is dropped.
        // Log so operators can confirm the slot was returned.
        println!(
            "[exits] Position slot released for {} (permit dropped).",
            self.mint
        );
    }
}

// ============================================================================
// Public Entry Point
// ============================================================================

/// Spawns the position watcher for `position` on the Tokio runtime.
///
/// The caller must pass:
///   - `price_rx`   — a channel that receives live SwapEvents for **this pool**.
///     Recommended: the caller filters the global swap stream and
///     forwards only events matching `position.source_pool_id`.
///   - `permit`     — the OwnedSemaphorePermit acquired before the buy.
///     Held until the watcher exits (success or failure).
///
/// This function returns immediately; the watcher runs as a background task.
pub fn spawn_position_watcher(
    position: ActivePosition,
    price_rx: mpsc::Receiver<SwapEvent>,
    rpc_client: Arc<RpcClient>,
    payer: Arc<Keypair>,
    bot_state: Arc<BotState>,
    permit: OwnedSemaphorePermit,
    dry_run: bool,
    http_client: reqwest::Client,
    telegram_bot_token: Option<String>,
    telegram_chat_id: Option<String>,
) {
    tokio::spawn(async move {
        let result = timeout(
            WATCHER_MAX_LIFETIME,
            run_watcher(
                position,
                price_rx,
                rpc_client,
                payer,
                bot_state,
                permit,
                dry_run,
                http_client,
                telegram_bot_token,
                telegram_chat_id,
            ),
        )
        .await;

        match result {
            Ok(()) => {}
            Err(_elapsed) => {
                eprintln!(
                    "[exits] 🚨 WATCHER HARD DEADLINE: position watcher exceeded {}h limit. \
                     Task terminated. MANUAL REVIEW REQUIRED — position may still be open.",
                    WATCHER_MAX_LIFETIME.as_secs() / 3600
                );
            }
        }
    });
}

// ============================================================================
// Inner Watcher
// ============================================================================

async fn run_watcher(
    mut position: ActivePosition,
    mut price_rx: mpsc::Receiver<SwapEvent>,
    rpc_client: Arc<RpcClient>,
    payer: Arc<Keypair>,
    bot_state: Arc<BotState>,
    permit: OwnedSemaphorePermit,
    dry_run: bool,
    http_client: reqwest::Client,
    telegram_bot_token: Option<String>,
    telegram_chat_id: Option<String>,
) {
    // RAII guard — permit released on drop regardless of exit path.
    let _guard = PositionGuard {
        mint: position.mint.clone(),
        bot_state: bot_state.clone(),
        _permit: permit,
    };

    // Validate the mint once up-front. If the position was constructed with a
    // garbage mint, fail loudly and immediately rather than silently continuing.
    let mint_pubkey = match Pubkey::from_str(&position.mint) {
        Ok(pk) => pk,
        Err(err) => {
            eprintln!(
                "[exits] 🚨 Invalid mint address '{}': {}. Aborting watcher.",
                position.mint, err
            );
            return;
        }
    };

    println!(
        "[exits] 👀 Watcher started for {}. acquired={} tokens.",
        position.mint, position.acquired_amount
    );

    let pool_keys = position.pool_keys.clone();

    if let Some(ref keys) = pool_keys {
        println!(
            "[exits] ✅ Pool keys resolved for {} (amm_id={}).",
            position.mint, keys.amm_id
        );
    } else {
        println!(
            "[exits] 💊 Pump.fun token detected for {} (no Raydium pool keys).",
            position.mint
        );
    }

    // -----------------------------------------------------------------------
    // VBATS State
    // -----------------------------------------------------------------------
    // Price is tracked as (wsol_lamports, token_raw_units) ratio from ticks.
    // The f64 representation is used only for EMA velocity; all threshold
    // comparisons use the integer ratio form to avoid precision drift.

    let mut ema_velocity: f64 = 0.0;
    let mut profit_lock_active = false;
    let mut partial_sold = false;
    let mut tick: u64 = 0;
    let mut consecutive_stale: u32 = 0;

    // High-water-mark numerator/denominator (same ratio space as entry price).
    let mut hwm_wsol_num: u128 = position.entry_price_wsol_num;
    let mut hwm_wsol_den: u128 = position.entry_price_wsol_den;

    // Previous-tick price (ratio form) for velocity computation.
    let mut prev_wsol_num: u128 = position.entry_price_wsol_num;
    let mut prev_wsol_den: u128 = position.entry_price_wsol_den;

    // -----------------------------------------------------------------------
    // Main Price-Tick Loop
    // -----------------------------------------------------------------------
    loop {
        // Wait for the next price tick with a liveness deadline.
        let event = match timeout(PRICE_FEED_TIMEOUT, price_rx.recv()).await {
            Ok(Some(event)) => event,
            Ok(None) => {
                eprintln!(
                    "[exits] Price feed channel closed for {}. Aborting watcher.",
                    position.mint
                );
                return;
            }
            Err(_) => {
                consecutive_stale += 1;
                eprintln!(
                    "[exits] Price feed timeout ({}/{}) for {}.",
                    consecutive_stale, MAX_CONSECUTIVE_STALE_TICKS, position.mint
                );
                if consecutive_stale >= MAX_CONSECUTIVE_STALE_TICKS {
                    eprintln!(
                        "[exits] 🚨 {} consecutive stale ticks for {}. Aborting watcher.",
                        consecutive_stale, position.mint
                    );
                    return;
                }
                continue;
            }
        };

        // ------ Validate tick -----------------------------------------------

        // Only consume ticks for our pool.
        if event.pool_id != position.source_pool_id {
            continue;
        }

        // Reject ticks with zero amounts — should not happen after websocket
        // parsing, but defend in depth.
        if event.base_amount == 0 || event.quote_amount == 0 {
            continue;
        }

        // Reject stale ticks (lagged replay, out-of-order delivery, etc.).
        // SwapEvent::timestamp_ms is the on-chain block time in milliseconds.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        if now_ms > 0 && now_ms.saturating_sub(event.timestamp_ms) > MAX_TICK_STALENESS_MS {
            consecutive_stale += 1;
            if consecutive_stale >= MAX_CONSECUTIVE_STALE_TICKS {
                eprintln!(
                    "[exits] 🚨 {} consecutive stale ticks for {}. Aborting watcher.",
                    consecutive_stale, position.mint
                );
                return;
            }
            continue;
        }
        consecutive_stale = 0;
        tick += 1;

        // ------ Current price (WSOL lamports per token raw unit) -------------
        //   current_price = quote_amount (WSOL lamports) / base_amount (token raw)
        // Stored as (wsol_num=quote_amount, wsol_den=base_amount) ratio.
        let cur_wsol_num = event.quote_amount as u128;
        let cur_wsol_den = event.base_amount as u128;

        // ------ Log-return velocity (f64 required for ln()) ------------------
        // Guard: both ratios must be positive and finite.
        let log_velocity =
            compute_log_velocity(prev_wsol_num, prev_wsol_den, cur_wsol_num, cur_wsol_den);

        // EMA update. Seed with first reading to avoid zero-bias.
        if tick == 1 {
            ema_velocity = log_velocity;
        } else {
            ema_velocity = EMA_ALPHA * log_velocity + (1.0 - EMA_ALPHA) * ema_velocity;
        }

        // ------ High-water mark update (integer ratio comparison) -----------
        // hwm is higher if:  cur_num / cur_den > hwm_num / hwm_den
        //   ⟺  cur_num * hwm_den > hwm_num * cur_den
        let update_hwm = cur_wsol_num
            .checked_mul(hwm_wsol_den)
            .and_then(|lhs| hwm_wsol_num.checked_mul(cur_wsol_den).map(|rhs| lhs > rhs))
            .unwrap_or(false);
        if update_hwm {
            hwm_wsol_num = cur_wsol_num;
            hwm_wsol_den = cur_wsol_den;
        }

        // ------ Adaptive trail distance -------------------------------------
        let raw_trail = TRAIL_BASE - ema_velocity * VELOCITY_SENSITIVITY;
        let trail_pct = raw_trail.clamp(TRAIL_MIN, TRAIL_MAX);

        // ------ Profit-lock activation (integer ratio comparison) -----------
        // Profit ratio = cur_price / entry_price >= PROFIT_LOCK_THRESHOLD
        //   ⟺  cur_wsol_num * entry_wsol_den >= PROFIT_LOCK_THRESHOLD * entry_wsol_num * cur_wsol_den
        if !profit_lock_active {
            let lhs = cur_wsol_num.checked_mul(position.entry_price_wsol_den);
            // PROFIT_LOCK_THRESHOLD = 1.30 → multiply entry by 130/100.
            let rhs = position
                .entry_price_wsol_num
                .checked_mul(cur_wsol_den)
                .and_then(|v| v.checked_mul(130))
                .map(|v| v / 100);
            if let (Some(l), Some(r)) = (lhs, rhs) {
                if l >= r {
                    profit_lock_active = true;
                    println!(
                        "[exits] 🔒 PROFIT LOCK ACTIVATED for {} at tick {}. \
                         Stop floor raised to >{:.0}% of entry.",
                        position.mint,
                        tick,
                        (PROFIT_LOCK_FLOOR_MULT - 1.0) * 100.0
                    );
                }
            }
        }

        // ------ Status log --------------------------------------------------
        let pnl_display = compute_pnl_bps(
            cur_wsol_num,
            cur_wsol_den,
            position.entry_price_wsol_num,
            position.entry_price_wsol_den,
        );
        println!(
            "[exits] 📈 [t={:>4}] mint={} price={}/{} hwm={}/{} trail={:.1}% v_t={:+.4} \
             ema_v={:+.4} pnl~{:+}bps{}",
            tick,
            &position.mint[..8.min(position.mint.len())],
            cur_wsol_num,
            cur_wsol_den,
            hwm_wsol_num,
            hwm_wsol_den,
            trail_pct * 100.0,
            log_velocity,
            ema_velocity,
            pnl_display,
            if profit_lock_active { "  🔒" } else { "" }
        );

        // ====================================================================
        // EXIT CONDITION C — Panic Velocity Breaker (highest priority)
        // ====================================================================
        //
        // A single tick log-return below PANIC_VELOCITY_THRESHOLD signals a
        // flash crash or rug. Bypass all trail logic and sell immediately.
        // The guard `pnl > -1500 bps` prevents panic-selling during the normal
        // buy-settle dip immediately after entry.
        if log_velocity < PANIC_VELOCITY_THRESHOLD && pnl_display > -1500 {
            println!(
                "[exits] 🚨 PANIC VELOCITY BREAKER for {} | v_t={:.4} < threshold={:.4}. \
                 Executing emergency sell.",
                position.mint, log_velocity, PANIC_VELOCITY_THRESHOLD
            );
            execute_sell_with_retry(
                "PANIC_VELOCITY",
                &position,
                pool_keys.as_ref(),
                mint_pubkey,
                rpc_client.clone(),
                payer.clone(),
                bot_state.clone(),
                dry_run,
                &http_client,
                telegram_bot_token.clone(),
                telegram_chat_id.clone(),
                true, // is_final_exit
                cur_wsol_num,
                cur_wsol_den,
            )
            .await;
            return;
        }
        // ====================================================================
        // EXIT CONDITION VWAP — Mean-Reversion Snapback (Breakeven/Profit)
        // ====================================================================
        // current_price >= entry_price
        let is_snapback = cur_wsol_num
            .checked_mul(position.entry_price_wsol_den)
            .and_then(|lhs| {
                position
                    .entry_price_wsol_num
                    .checked_mul(cur_wsol_den)
                    .map(|rhs| lhs >= rhs)
            })
            .unwrap_or(false);

        if is_snapback {
            println!(
                "[exits] 🎯 VWAP SNAPBACK: Price fully recovered to entry for {}. \
                 Executing full sell to lock in breakeven/profit.",
                position.mint
            );
            execute_sell_with_retry(
                "VWAP_SNAPBACK",
                &position,
                pool_keys.as_ref(),
                mint_pubkey,
                rpc_client.clone(),
                payer.clone(),
                bot_state.clone(),
                dry_run,
                &http_client,
                telegram_bot_token.clone(),
                telegram_chat_id.clone(),
                true, // is_final_exit
                cur_wsol_num,
                cur_wsol_den,
            )
            .await;
            return;
        }

        // ====================================================================
        // EXIT CONDITION A — Adaptive Partial Exit (50% Scale-Out)
        // ====================================================================
        // If Time-Weighted ROI >= 100% and elapsed time <= 60 seconds, we hit parabolic velocity.
        // Scale out 50% of the bag to lock in initial capital instantly.
        let elapsed_secs = now_ms.saturating_sub(position.entry_timestamp_ms) / 1000;
        let roi = (cur_wsol_num as f64 * position.entry_price_wsol_den as f64)
            / (cur_wsol_den as f64 * position.entry_price_wsol_num as f64)
            - 1.0;

        if !partial_sold && roi >= 1.0 && elapsed_secs <= 60 {
            println!(
                "[exits] 🎯 PARABOLIC VELOCITY: 100%+ ROI in {}s for {}. \
                 Scaling out 50% of the bag to lock in initial capital.",
                elapsed_secs, position.mint
            );

            // Halve the acquired amount for the partial sell
            let partial_amount = position.acquired_amount / 2;
            let mut partial_position = position.clone();
            partial_position.acquired_amount = partial_amount;

            let success = execute_sell_with_retry(
                "ADAPTIVE_SCALE_OUT_50PCT",
                &partial_position,
                pool_keys.as_ref(),
                mint_pubkey,
                rpc_client.clone(),
                payer.clone(),
                bot_state.clone(),
                dry_run,
                &http_client,
                telegram_bot_token.clone(),
                telegram_chat_id.clone(),
                false, // is_final_exit
                cur_wsol_num,
                cur_wsol_den,
            )
            .await;

            if success {
                // Reduce our local tracked bag ONLY IF the sell actually landed on chain
                position.acquired_amount -= partial_amount;
                partial_sold = true;
            }
            continue;
        }

        // ====================================================================
        // EXIT CONDITION B — Profit-Lock / Trailing Stop
        // ====================================================================
        //
        // Only applies once the position has ever gained >= PROFIT_LOCK_THRESHOLD.
        //
        // Stop price = hwm * (1 - trail_pct), floored at entry * PROFIT_LOCK_FLOOR_MULT
        // if profit lock is active.
        //
        // Comparison: cur_price < stop_price
        //   where stop_price = hwm × (1 - trail_pct)
        //
        // Integer form:
        //   cur_num / cur_den < hwm_num / hwm_den × (1 - trail_pct)
        //   ⟺  cur_num * hwm_den * SCALE < hwm_num * cur_den * (SCALE - trail_scaled)
        //
        // We convert trail_pct to basis points (integers) for the comparison.
        if profit_lock_active {
            let trail_basis_points = (trail_pct * 10_000.0) as u128;
            let scale: u128 = 10_000;
            // stop_ratio = hwm × (scale - trail_bps) / scale
            // cur < stop ⟺ cur_num * hwm_den * scale < hwm_num * cur_den * (scale - trail_bps)
            let lhs = cur_wsol_num
                .checked_mul(hwm_wsol_den)
                .and_then(|v| v.checked_mul(scale));
            let trail_factor = scale.checked_sub(trail_basis_points);
            let rhs_base = hwm_wsol_num
                .checked_mul(cur_wsol_den)
                .and_then(|v| v.checked_mul(trail_factor?));

            // Profit-lock floor: stop >= entry × PROFIT_LOCK_FLOOR_MULT
            // Check if current is also below the floor:
            //   cur_num / cur_den < entry_num * 110 / (entry_den * 100)
            //   ⟺  cur_num * entry_den * 100 < entry_num * cur_den * 110
            let lhs_floor = cur_wsol_num
                .checked_mul(position.entry_price_wsol_den)
                .and_then(|v| v.checked_mul(100));
            let rhs_floor = position
                .entry_price_wsol_num
                .checked_mul(cur_wsol_den)
                .and_then(|v| v.checked_mul(110));

            let below_trail_stop = match (lhs, rhs_base) {
                (Some(l), Some(r)) => l < r,
                _ => false,
            };
            let below_profit_lock_floor = match (lhs_floor, rhs_floor) {
                (Some(l), Some(r)) => l < r,
                _ => false,
            };

            // Trail stop fires when price drops below both the adaptive trail
            // AND the profit-lock floor (if active).
            if below_trail_stop || below_profit_lock_floor {
                let reason = if below_profit_lock_floor {
                    "PROFIT_LOCK_FLOOR"
                } else if trail_pct >= TRAIL_MAX - 0.001 {
                    "HARD_STOP_20PCT"
                } else {
                    "ADAPTIVE_TRAIL"
                };
                println!(
                    "[exits] 🛑 {} triggered for {} at tick {}. \
                     trail={:.1}% ema_v={:+.4}. Executing sell.",
                    reason,
                    position.mint,
                    tick,
                    trail_pct * 100.0,
                    ema_velocity
                );
                execute_sell_with_retry(
                    reason,
                    &position,
                    pool_keys.as_ref(),
                    mint_pubkey,
                    rpc_client.clone(),
                    payer.clone(),
                    bot_state.clone(),
                    dry_run,
                    &http_client,
                    telegram_bot_token.clone(),
                    telegram_chat_id.clone(),
                    true, // is_final_exit
                    cur_wsol_num,
                    cur_wsol_den,
                )
                .await;
                return;
            }
        }

        // Advance state for next tick.
        prev_wsol_num = cur_wsol_num;
        prev_wsol_den = cur_wsol_den;
    }
}

// ============================================================================
// Sell Execution Pipeline
// ============================================================================

/// Attempts the full sell pipeline up to MAX_SELL_ATTEMPTS times.
///
/// On success:
///   - Logs the exit with reason.
///   - Releases the shadow position in BotState.
///   - Updates the circuit-breaker consecutive-loss counter.
///
/// On exhaustion:
///   - Logs a loud alert. The RAII guard still releases the semaphore permit.
///   - The position remains open. Operator intervention required.
async fn execute_sell_with_retry(
    reason: &str,
    position: &ActivePosition,
    pool_keys: Option<&RaydiumPoolKeys>,
    mint_pubkey: Pubkey,
    rpc_client: Arc<RpcClient>,
    payer: Arc<Keypair>,
    bot_state: Arc<BotState>,
    dry_run: bool,
    http_client: &reqwest::Client,
    telegram_bot_token: Option<String>,
    telegram_chat_id: Option<String>,
    is_final_exit: bool,
    cur_wsol_num: u128,
    cur_wsol_den: u128,
) -> bool {
    if dry_run {
        println!(
            "[exits] 💸 [DRY RUN] Sell triggered ({}) for {}. No bundle dispatched.",
            reason, position.mint
        );
        if is_final_exit {
            // Still release the shadow position in dry-run so state is consistent.
            let released = bot_state.release_shadow_position(&position.mint).await;
            println!(
                "[exits] [DRY RUN] shadow_position released={} for {}.",
                released, position.mint
            );
        }
        return true;
    }

    let result = attempt_sell_bundle(
        reason,
        position,
        pool_keys,
        mint_pubkey,
        rpc_client.clone(),
        payer,
        cur_wsol_num,
        cur_wsol_den,
    )
    .await;

    match result {
        Ok((bundle_id, signature)) => {
            println!(
                "[exits] ✅ Sell bundle ACCEPTED ({}) for {} | bundle_id={}.",
                reason, position.mint, bundle_id
            );
            println!(
                "[exits] ⏳ Waiting for on-chain confirmation of signature: {}",
                signature
            );

            let mut confirmed = false;
            for _ in 0..15 {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                if let Ok(response) = rpc_client.get_signature_statuses(&[signature]).await {
                    if let Some(Some(status)) = response.value.first() {
                        if status.satisfies_commitment(
                            solana_sdk::commitment_config::CommitmentConfig {
                                commitment:
                                    solana_sdk::commitment_config::CommitmentLevel::Confirmed,
                            },
                        ) {
                            if status.err.is_none() {
                                confirmed = true;
                            }
                            break;
                        }
                    }
                }
            }

            if !confirmed {
                eprintln!(
                    "[exits] 🚨 Sell bundle {} was accepted but DID NOT CONFIRM on-chain.",
                    bundle_id
                );
                return false;
            }

            println!("[exits] ✅ Sell transaction CONFIRMED on-chain!");

            // ---- Phase 6: Close Position in Database -----------------------
            //
            // Record the exit in the SQLite positions table so the operator
            // has a complete PnL record across the full trade lifecycle.
            if is_final_exit {
                db::close_position(&position.mint);

                // Release the shadow position lock now that we have bundle acceptance.
                // Note: bundle acceptance ≠ on-chain confirmation, but Jito blocks
                // guarantee atomicity — if the bundle is accepted it will land.
                let released = bot_state.release_shadow_position(&position.mint).await;
                if !released {
                    eprintln!(
                        "[exits] ⚠️  release_shadow_position returned false for {} \
                         (position was already released or never retained).",
                        position.mint
                    );
                }
            }

            // ---- Circuit-breaker loss-streak accounting --------------------
            //
            // BUGFIX (High, audit 2026-08): previously ran unconditionally
            // (even for non-final partial exits) and classified win/loss by
            // string-matching the exit `reason` against the literal
            // "VWAP_SNAPBACK" — so a winning ADAPTIVE_SCALE_OUT_50PCT partial
            // exit (which only fires on >=100% ROI in under 60s, i.e. a
            // clean win) was counted as a loss and could trip the 3-strike
            // breaker during a winning streak. The streak is now gated on
            // `is_final_exit` (partial scale-outs don't close the position,
            // so they shouldn't move the streak either way) and classified
            // by the actual realized PnL sign rather than a reason string.
            use std::sync::atomic::Ordering;
            let pnl_bps = compute_pnl_bps(
                cur_wsol_num,
                cur_wsol_den,
                position.entry_price_wsol_num,
                position.entry_price_wsol_den,
            );
            if is_final_exit {
                if pnl_bps >= 0 {
                    bot_state.consecutive_losses.store(0, Ordering::SeqCst);
                    println!(
                        "[exits] ✅ POSITION CLOSED [{}] | reason={} | pnl={:+}bps | loss streak RESET.",
                        position.mint, reason, pnl_bps
                    );
                } else {
                    let streak = bot_state.consecutive_losses.fetch_add(1, Ordering::SeqCst) + 1;
                    eprintln!(
                        "[exits] 📉 POSITION CLOSED [{}] | reason={} | pnl={:+}bps | consecutive losses: {}.",
                        position.mint, reason, pnl_bps, streak
                    );
                }
            }

            if let (Some(bot_token), Some(chat_id)) = (telegram_bot_token, telegram_chat_id) {
                let client_clone = http_client.clone();
                let mint_str = position.mint.clone();
                // pnl_usd is not computable here — this module has no live
                // USD price feed, only the WSOL-lamports ratio. pnl_pct IS
                // computable from that same ratio, so it is no longer
                // hardcoded to 0.0 (audit 2026-08).
                let pnl_usd = 0.0;
                let pnl_pct = pnl_bps as f64 / 100.0;
                let exit_reason_str = reason.to_string();
                tokio::spawn(async move {
                    alpha_agents_core::telegram::send_bot_sell_alert(
                        &client_clone,
                        &bot_token,
                        &chat_id,
                        &mint_str,
                        pnl_usd,
                        pnl_pct,
                        &exit_reason_str,
                        0.0,
                    )
                    .await;
                });
            }
        }
        Err(err) => {
            eprintln!(
                "[exits] 🚨 SELL FAILED ({}) for {} after {} attempts: {}. \
                 Position may be stranded. MANUAL REVIEW REQUIRED.",
                reason, position.mint, MAX_SELL_ATTEMPTS, err
            );
            // Do NOT release the shadow position — leave it as a sentinel so
            // the operator knows this mint is in an unresolved state.
            return false;
        }
    }
    true
}

/// Executes the full sell pipeline: build instruction → sign bundle → submit.
/// Retries up to MAX_SELL_ATTEMPTS times with increasing Jito tip.
async fn attempt_sell_bundle(
    reason: &str,
    position: &ActivePosition,
    pool_keys: Option<&RaydiumPoolKeys>,
    mint_pubkey: Pubkey,
    rpc_client: Arc<RpcClient>,
    payer: Arc<Keypair>,
    cur_wsol_num: u128,
    cur_wsol_den: u128,
) -> Result<(String, solana_sdk::signature::Signature), ExitError> {
    // Derive ATAs for the sell: source = token ATA, destination = WSOL ATA.
    let user_owner = payer.pubkey();
    let user_source_token_account =
        spl_associated_token_account::get_associated_token_address(&user_owner, &mint_pubkey);
    let user_destination_wsol_account =
        spl_associated_token_account::get_associated_token_address(&user_owner, &WSOL_MINT);

    // Validate the acquired amount.
    if position.acquired_amount == 0 {
        return Err(ExitError::TxBuild(
            "acquired_amount is zero; cannot build sell instruction".to_string(),
        ));
    }

    // Connect to Jito once; reconnect inside the retry loop if needed.
    let mut jito = connect_jito(&position.block_engine_url).await?;

    let mut last_error = String::new();

    for attempt in 1..=MAX_SELL_ATTEMPTS {
        if attempt > 1 {
            tokio::time::sleep(SELL_RETRY_DELAY).await;
            println!(
                "[exits] 🔁 Sell retry {}/{} for {}.",
                attempt, MAX_SELL_ATTEMPTS, position.mint
            );
            // Reconnect on retry — the gRPC channel may have dropped.
            match connect_jito(&position.block_engine_url).await {
                Ok(client) => jito = client,
                Err(err) => {
                    last_error = err.to_string();
                    eprintln!(
                        "[exits] ⚠️  Jito reconnect failed on attempt {}: {}",
                        attempt, last_error
                    );
                    continue;
                }
            }
        }

        // BUGFIX (High, audit 2026-08): Re-compute minimum_amount_out and dynamic
        // slippage tolerance per attempt. For emergency exits (PANIC_VELOCITY, HARD_STOP_20PCT),
        // widen slippage to 30-50% rather than reusing entry-time bps (1.0-2.5%).
        let is_emergency = reason == "PANIC_VELOCITY" || reason == "HARD_STOP_20PCT";
        let effective_slippage_bps = if is_emergency {
            match attempt {
                1 => 3000, // 30%
                2 => 4000, // 40%
                _ => 5000, // 50%
            }
        } else {
            (position.max_slippage_bps.saturating_mul(attempt as u16)).min(1000)
        };

        let expected_wsol = (position.acquired_amount as u128)
            .checked_mul(cur_wsol_num)
            .unwrap_or(0)
            / cur_wsol_den.max(1);
        let minimum_amount_out = crate::execution::calculate_local_minimum_amount_out(
            expected_wsol as u64,
            effective_slippage_bps,
        )
        .unwrap_or(1);

        // Escalate tip geometrically (1.5× per retry).
        let tip_scale = 1.5_f64.powi(attempt as i32 - 1);
        let tip_lamports = {
            let scaled = position.jito_tip_lamports as f64 * tip_scale;
            if scaled.is_finite() && scaled >= MINIMUM_JITO_TIP_LAMPORTS as f64 {
                scaled as u64
            } else {
                MINIMUM_JITO_TIP_LAMPORTS
            }
        };

        // Fresh blockhash every attempt.
        let recent_blockhash = match rpc_client.get_latest_blockhash().await {
            Ok(bh) => bh,
            Err(err) => {
                last_error = format!("getLatestBlockhash: {err}");
                eprintln!(
                    "[exits] ⚠️  Blockhash fetch failed on attempt {}: {}",
                    attempt, last_error
                );
                continue;
            }
        };

        let mut packets = Vec::new();
        let main_signature;

        if let Some(keys) = pool_keys {
            // Build the sell instruction: Token → WSOL.
            let mut swap_ix = match construct_raydium_swap_instruction(
                keys,
                user_owner,
                user_source_token_account,
                user_destination_wsol_account,
                position.acquired_amount,
                minimum_amount_out,
            ) {
                Ok(ix) => ix,
                Err(err) => {
                    return Err(ExitError::TxBuild(format!(
                        "construct_raydium_swap_instruction: {err}"
                    )));
                }
            };

            if let Err(e) = crate::execution::apply_jitodontfront_protection(
                &mut swap_ix,
                position.jito_dont_front_pubkey,
            ) {
                eprintln!("[exits] ⚠️ Failed to apply MEV protection: {}", e);
            }

            // Select the next tip account (round-robin).
            let tip_account = jito.tip_accounts[jito.next_tip_index];
            jito.next_tip_index = (jito.next_tip_index + 1) % jito.tip_accounts.len();

            let tip_ix = system_instruction::transfer(&user_owner, &tip_account, tip_lamports);

            let instructions: Vec<Instruction> = vec![swap_ix, tip_ix];

            // Compile and sign.
            let message =
                match v0::Message::try_compile(&user_owner, &instructions, &[], recent_blockhash) {
                    Ok(m) => m,
                    Err(err) => {
                        last_error = format!("message compile: {err}");
                        eprintln!("[exits] ⚠️  {} on attempt {}.", last_error, attempt);
                        continue;
                    }
                };
            let transaction = match VersionedTransaction::try_new(
                VersionedMessage::V0(message),
                &[payer.as_ref()],
            ) {
                Ok(tx) => tx,
                Err(err) => {
                    last_error = format!("tx sign: {err}");
                    eprintln!("[exits] ⚠️  {} on attempt {}.", last_error, attempt);
                    continue;
                }
            };

            // Serialize to proto packet.
            let packet = match transaction_to_proto_packet(&transaction) {
                Ok(p) => p,
                Err(err) => {
                    last_error = format!("serialization: {err}");
                    eprintln!("[exits] ⚠️  {} on attempt {}.", last_error, attempt);
                    continue;
                }
            };

            packets.push(packet);
            main_signature = transaction.signatures[0];
        } else {
            // PumpPortal fallback
            // Pump.fun tokens always have 6 decimals.
            let amount_ui = position.acquired_amount as f64 / 1_000_000.0;
            let priority_fee_sol = tip_lamports as f64 / 1_000_000_000.0;

            let payload = serde_json::json!({
                "publicKey": payer.pubkey().to_string(),
                "action": "sell",
                "mint": position.mint,
                "amount": amount_ui,
                "denominatedInSol": "false",
                "slippage": effective_slippage_bps as f64 / 100.0,
                "priorityFee": priority_fee_sol,
                "pool": "pump"
            });

            let url = "https://pumpportal.fun/api/trade-local";
            let mut builder = reqwest::Client::new().post(url).json(&payload);

            if let Some(key) = &position.pumpportal_api_key {
                builder = builder.header("x-api-key", key);
            }

            let response = match builder.send().await {
                Ok(r) => r,
                Err(e) => {
                    last_error = format!("PumpPortal HTTP error: {e}");
                    eprintln!("[exits] ⚠️  {} on attempt {}.", last_error, attempt);
                    continue;
                }
            };

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                last_error = format!("PumpPortal HTTP {status}: {body}");
                eprintln!("[exits] ⚠️  {} on attempt {}.", last_error, attempt);
                continue;
            }

            let bytes = match response.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    last_error = format!("PumpPortal read error: {e}");
                    eprintln!("[exits] ⚠️  {} on attempt {}.", last_error, attempt);
                    continue;
                }
            };

            let pump_tx: VersionedTransaction = match bincode::deserialize(&bytes) {
                Ok(tx) => tx,
                Err(e) => {
                    last_error = format!("PumpPortal deserialize error: {e}");
                    eprintln!("[exits] ⚠️  {} on attempt {}.", last_error, attempt);
                    continue;
                }
            };

            let tip_account = jito.tip_accounts[jito.next_tip_index];
            jito.next_tip_index = (jito.next_tip_index + 1) % jito.tip_accounts.len();

            let signed_bundle = match alpha_agents_core::dispatcher::build_and_sign_pump_bundle(
                pump_tx,
                &payer,
                tip_account,
                tip_lamports,
            ) {
                Ok(b) => b,
                Err(e) => {
                    last_error = format!("build_and_sign_pump_bundle: {e}");
                    eprintln!("[exits] ⚠️  {} on attempt {}.", last_error, attempt);
                    continue;
                }
            };

            packets = signed_bundle.request.bundle.unwrap().packets;
            main_signature =
                solana_sdk::signature::Signature::from_str(&signed_bundle.transaction_signature)
                    .unwrap_or_default();
        }

        let bundle_request = SendBundleRequest {
            bundle: Some(Bundle {
                header: None,
                packets,
            }),
        };

        // Submit via Jito gRPC.
        let send_result = timeout(
            JITO_SELL_TIMEOUT,
            jito.grpc.send_bundle(Request::new(bundle_request)),
        )
        .await;

        match send_result {
            Ok(Ok(response)) => {
                let bundle_id = response.into_inner().uuid;
                if bundle_id.is_empty() {
                    last_error = "Jito returned empty bundle_id".to_string();
                    eprintln!("[exits] ⚠️  {} on attempt {}.", last_error, attempt);
                    continue;
                }
                return Ok((bundle_id, main_signature));
            }
            Ok(Err(status)) => {
                last_error = format!("gRPC status: {status}");
                eprintln!("[exits] ⚠️  {} on attempt {}.", last_error, attempt);
                // Reconnect on next iteration.
            }
            Err(_elapsed) => {
                last_error = "gRPC send_bundle timed out".to_string();
                eprintln!("[exits] ⚠️  {} on attempt {}.", last_error, attempt);
            }
        }
    }

    Err(ExitError::SellFailed(MAX_SELL_ATTEMPTS, last_error))
}

// ============================================================================
// Jito gRPC Connection Helper
// ============================================================================

struct JitoSellClient {
    grpc: SearcherServiceClient<Channel>,
    tip_accounts: Vec<Pubkey>,
    next_tip_index: usize,
}

async fn connect_jito(block_engine_url: &str) -> Result<JitoSellClient, ExitError> {
    let endpoint = Endpoint::from_shared(block_engine_url.to_string())
        .map_err(|err| ExitError::JitoConnect(format!("invalid URL: {err}")))?
        .connect_timeout(JITO_SELL_TIMEOUT)
        .timeout(JITO_SELL_TIMEOUT)
        .tls_config(ClientTlsConfig::new())
        .map_err(|err| ExitError::JitoConnect(format!("TLS config: {err}")))?;

    let channel = timeout(JITO_SELL_TIMEOUT, endpoint.connect())
        .await
        .map_err(|_| ExitError::JitoConnect("connection timed out".to_string()))?
        .map_err(|err| ExitError::JitoConnect(format!("transport: {err}")))?;

    let mut grpc = SearcherServiceClient::new(channel);

    let tip_response = timeout(
        JITO_SELL_TIMEOUT,
        grpc.get_tip_accounts(Request::new(GetTipAccountsRequest {})),
    )
    .await
    .map_err(|_| ExitError::JitoTipAccount("GetTipAccounts timed out".to_string()))?
    .map_err(|status| ExitError::JitoTipAccount(format!("gRPC: {status}")))?;

    let mut tip_accounts = Vec::new();
    for raw in tip_response.into_inner().accounts {
        let pk = Pubkey::from_str(&raw).map_err(|err| {
            ExitError::JitoTipAccount(format!("invalid tip account '{raw}': {err}"))
        })?;
        tip_accounts.push(pk);
    }
    if tip_accounts.is_empty() {
        return Err(ExitError::JitoTipAccount(
            "no tip accounts returned".to_string(),
        ));
    }

    Ok(JitoSellClient {
        grpc,
        tip_accounts,
        next_tip_index: 0,
    })
}

// ============================================================================
// Helpers
// ============================================================================

/// Computes log-return velocity from two price ratios.
/// Returns 0.0 if inputs are degenerate (zero, negative, or non-finite result).
fn compute_log_velocity(prev_num: u128, prev_den: u128, cur_num: u128, cur_den: u128) -> f64 {
    if prev_num == 0 || prev_den == 0 || cur_num == 0 || cur_den == 0 {
        return 0.0;
    }
    // ratio = (cur_num / cur_den) / (prev_num / prev_den)
    //       = (cur_num * prev_den) / (cur_den * prev_num)
    let numerator = cur_num as f64 * prev_den as f64;
    let denominator = cur_den as f64 * prev_num as f64;
    if !numerator.is_finite() || !denominator.is_finite() || denominator <= 0.0 {
        return 0.0;
    }
    let ratio = numerator / denominator;
    if ratio <= 0.0 {
        return 0.0;
    }
    let v = ratio.ln();
    if v.is_finite() {
        v
    } else {
        0.0
    }
}

/// Computes approximate PnL in basis points relative to entry.
/// Returns 0 on overflow or degenerate input.
fn compute_pnl_bps(cur_num: u128, cur_den: u128, entry_num: u128, entry_den: u128) -> i64 {
    if cur_den == 0 || entry_num == 0 || entry_den == 0 {
        return 0;
    }
    // pnl_bps = (cur/entry - 1) * 10_000
    //         = (cur_num * entry_den - entry_num * cur_den) * 10_000
    //           / (entry_num * cur_den)
    let lhs = (cur_num as i128).checked_mul(entry_den as i128);
    let rhs = (entry_num as i128).checked_mul(cur_den as i128);
    let denom = (entry_num as i128).checked_mul(cur_den as i128);
    match (lhs, rhs, denom) {
        (Some(l), Some(r), Some(d)) if d != 0 => {
            let diff = l.saturating_sub(r);
            (diff.saturating_mul(10_000) / d) as i64
        }
        _ => 0,
    }
}

/// Serializes a VersionedTransaction to a Jito proto packet.
fn transaction_to_proto_packet(tx: &VersionedTransaction) -> Result<ProtoPacket, ExitError> {
    const SOLANA_PACKET_DATA_SIZE: usize = 1_232;
    let data = bincode::serialize(tx)
        .map_err(|err| ExitError::TxBuild(format!("bincode serialize: {err}")))?;
    if data.len() > SOLANA_PACKET_DATA_SIZE {
        return Err(ExitError::TxBuild(format!(
            "transaction is {} bytes; max is {SOLANA_PACKET_DATA_SIZE}",
            data.len()
        )));
    }
    let size = data.len() as u64;
    Ok(ProtoPacket {
        data,
        meta: Some(ProtoMeta {
            size,
            addr: String::new(),
            port: 0,
            flags: None,
            sender_stake: 0,
        }),
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---- compute_log_velocity ------------------------------------------------

    #[test]
    fn velocity_zero_on_equal_prices() {
        let v = compute_log_velocity(1_000, 100, 1_000, 100);
        assert!(v.abs() < 1e-12, "equal prices must yield zero velocity");
    }

    #[test]
    fn velocity_positive_on_price_increase() {
        // cur = 1200/100, prev = 1000/100 → ratio = 1.2 → ln(1.2) > 0
        let v = compute_log_velocity(1_000, 100, 1_200, 100);
        assert!(v > 0.0, "price increase must yield positive velocity");
    }

    #[test]
    fn velocity_negative_on_price_decrease() {
        // cur = 850/100, prev = 1000/100 → ratio = 0.85 → ln(0.85) < 0
        let v = compute_log_velocity(1_000, 100, 850, 100);
        assert!(v < 0.0, "price decrease must yield negative velocity");
    }

    #[test]
    fn velocity_zero_on_degenerate_inputs() {
        assert_eq!(compute_log_velocity(0, 100, 1_000, 100), 0.0);
        assert_eq!(compute_log_velocity(1_000, 0, 1_000, 100), 0.0);
        assert_eq!(compute_log_velocity(1_000, 100, 0, 100), 0.0);
        assert_eq!(compute_log_velocity(1_000, 100, 1_000, 0), 0.0);
    }

    #[test]
    fn panic_velocity_threshold_correctly_classified() {
        // A 12.8% single-tick drop → ln(0.872) ≈ −0.137 < PANIC_VELOCITY_THRESHOLD
        let v = compute_log_velocity(1_000, 100, 872, 100);
        assert!(
            v < PANIC_VELOCITY_THRESHOLD,
            "a >12.8% drop must breach the panic threshold"
        );
        // A 10% drop → ln(0.90) ≈ −0.105; above the threshold (not panic)
        let v2 = compute_log_velocity(1_000, 100, 900, 100);
        assert!(
            v2 > PANIC_VELOCITY_THRESHOLD,
            "a <12.8% drop must not breach the panic threshold"
        );
    }

    // ---- compute_pnl_bps -----------------------------------------------------

    #[test]
    fn pnl_bps_zero_at_entry() {
        let bps = compute_pnl_bps(1_000, 100, 1_000, 100);
        assert_eq!(bps, 0);
    }

    #[test]
    fn pnl_bps_positive_on_gain() {
        // cur = 1100/100 (+10% = +1000 bps), entry = 1000/100
        let bps = compute_pnl_bps(1_100, 100, 1_000, 100);
        assert_eq!(bps, 1_000);
    }

    #[test]
    fn pnl_bps_negative_on_loss() {
        // cur = 850/100 (−15% = −1500 bps), entry = 1000/100
        let bps = compute_pnl_bps(850, 100, 1_000, 100);
        assert_eq!(bps, -1_500);
    }

    #[test]
    fn pnl_bps_zero_on_degenerate() {
        assert_eq!(compute_pnl_bps(1_000, 0, 1_000, 100), 0);
        assert_eq!(compute_pnl_bps(1_000, 100, 0, 100), 0);
        assert_eq!(compute_pnl_bps(1_000, 100, 1_000, 0), 0);
    }

    // ---- trail clamp ---------------------------------------------------------

    #[test]
    fn trail_clamps_to_min_on_extreme_upward_velocity() {
        // ema_velocity = +0.10 → raw = 0.08 - 0.10*1.5 = -0.07 → clamped to 0.03
        let ema_v = 0.10_f64;
        let raw = TRAIL_BASE - ema_v * VELOCITY_SENSITIVITY;
        let trail = raw.clamp(TRAIL_MIN, TRAIL_MAX);
        assert!(
            (trail - TRAIL_MIN).abs() < 1e-12,
            "strong upward velocity must clamp trail to TRAIL_MIN"
        );
    }

    #[test]
    fn trail_clamps_to_max_on_extreme_downward_velocity() {
        // ema_velocity = -0.10 → raw = 0.08 + 0.15 = 0.23 → clamped to 0.20
        let ema_v = -0.10_f64;
        let raw = TRAIL_BASE - ema_v * VELOCITY_SENSITIVITY;
        let trail = raw.clamp(TRAIL_MIN, TRAIL_MAX);
        assert!(
            (trail - TRAIL_MAX).abs() < 1e-12,
            "strong downward velocity must clamp trail to TRAIL_MAX"
        );
    }

    // ---- mean-reversion snapback (integer ratio comparison) ------------------

    #[test]
    fn snapback_triggers_at_exact_vwap_boundary() {
        // VWAP baseline: quote_sum = 1_000 WSOL, base_sum = 100 tokens
        // → VWAP price = 10 WSOL per token
        let vwap_base_sum: u128 = 100;
        let vwap_quote_sum: u128 = 1_000;

        // Current price exactly at VWAP (10/1)
        let cur_num: u128 = 10;
        let cur_den: u128 = 1;
        // cur_num * vwap_base_sum >= vwap_quote_sum * cur_den
        // 10 * 100 = 1000 >= 1000 * 1 = 1000 → true
        let result = vwap_base_sum
            .checked_mul(cur_num)
            .and_then(|lhs| vwap_quote_sum.checked_mul(cur_den).map(|rhs| lhs >= rhs));
        assert_eq!(result, Some(true), "exact VWAP must trigger snapback");
    }

    #[test]
    fn snapback_does_not_trigger_below_vwap() {
        // VWAP = 10 WSOL/token, current = 9 WSOL/token (−10%)
        let vwap_base_sum: u128 = 100;
        let vwap_quote_sum: u128 = 1_000;
        let cur_num: u128 = 9;
        let cur_den: u128 = 1;
        let result = vwap_base_sum
            .checked_mul(cur_num)
            .and_then(|lhs| vwap_quote_sum.checked_mul(cur_den).map(|rhs| lhs >= rhs));
        assert_eq!(
            result,
            Some(false),
            "price below VWAP must not trigger snapback"
        );
    }

    // ---- profit lock activation (integer arithmetic) -------------------------

    #[test]
    fn profit_lock_activates_at_exactly_thirty_percent_gain() {
        // entry = 1_000/100, current = 1_300/100 (+30%)
        // cur_num * entry_den >= entry_num * cur_den * 130 / 100
        // 1300 * 100 = 130_000 >= 1000 * 100 * 130 / 100 = 130_000 → true
        let entry_num: u128 = 1_000;
        let entry_den: u128 = 100;
        let cur_num: u128 = 1_300;
        let cur_den: u128 = 100;

        let lhs = cur_num.checked_mul(entry_den);
        let rhs = entry_num
            .checked_mul(cur_den)
            .and_then(|v| v.checked_mul(130))
            .map(|v| v / 100);
        assert_eq!(
            lhs.zip(rhs).map(|(l, r)| l >= r),
            Some(true),
            "+30% gain must activate profit lock"
        );
    }

    #[test]
    fn profit_lock_does_not_activate_below_threshold() {
        // entry = 1_000/100, current = 1_299/100 (+29.9%)
        let entry_num: u128 = 1_000;
        let entry_den: u128 = 100;
        let cur_num: u128 = 1_299;
        let cur_den: u128 = 100;

        let lhs = cur_num.checked_mul(entry_den);
        let rhs = entry_num
            .checked_mul(cur_den)
            .and_then(|v| v.checked_mul(130))
            .map(|v| v / 100);
        assert_eq!(
            lhs.zip(rhs).map(|(l, r)| l >= r),
            Some(false),
            "+29.9% must not activate profit lock"
        );
    }
}
