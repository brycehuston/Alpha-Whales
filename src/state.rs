use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};

// ============================================================================
// MAX_CONCURRENT_POSITIONS — Open Position Concurrency Cap
// ============================================================================
//
// WHY THIS MATTERS:
//   Without a cap, a coordinated whale group buying 10+ tokens in 2 seconds
//   causes the bot to open 10 simultaneous positions. Each position runs:
//     - 30 RPC broadcast calls (buy)
//     - 1 Jupiter price poll every 3 seconds (VBATS)
//     - Up to 5×3 RPC calls per sell attempt
//
//   At 10 concurrent positions, that's 300 concurrent buy broadcasts + 10
//   Jupiter polls/3s = ~200 req/min just for price monitoring. This saturates
//   Helius standard plan limits (~1000 req/s but with burst penalties), causes
//   priority fee starvation as all positions compete to sell simultaneously,
//   and creates a Jupiter API thundering herd on exit.
//
// FIX:
//   A Tokio Semaphore with MAX_CONCURRENT_POSITIONS permits. Each call to
//   `try_acquire_position` either returns a SemaphorePermit (held for the
//   lifetime of the trade) or returns None (position skipped, logged).
//   The permit is automatically released when the watcher task completes.
//
// TUNING:
//   Set based on your Helius plan limits and acceptable capital exposure:
//     - Paper Trading: 3 (conservative, easy to track in logs)
//     - Standard plan: 5
//     - Growth/Business plan: 10-15
pub const MAX_CONCURRENT_POSITIONS: usize = 3;

/// Accepted shadow positions retain their capacity for the same four-hour
/// window used by the duplicate-mint guard.
#[expect(dead_code, reason = "will be used by Phase 4 shadow-position tracking")]
pub(crate) const SHADOW_POSITION_TTL_MS: u64 = 4 * 60 * 60 * 1000;

/// How long the circuit breaker blocks new trades after tripping.
/// 30 minutes gives the market time to stabilize after 3 consecutive stop-losses
/// without requiring a manual server restart.
const CIRCUIT_BREAKER_COOLDOWN: Duration = Duration::from_secs(30 * 60);

/// Number of consecutive stop-losses that trigger the circuit breaker.
const CIRCUIT_BREAKER_THRESHOLD: usize = 3;

struct ShadowPosition {
    #[expect(dead_code, reason = "will be used by Phase 4 expiry pruning")]
    opened_at_ms: u64,
    _position_permit: OwnedSemaphorePermit,
}

pub struct BotState {
    /// Tracks mints we are currently trading or have already traded.
    /// Maps mint address -> time of first entry. Entries expire after 4 hours,
    /// allowing the bot to re-enter the same token on a second pump.
    pub traded_mints: RwLock<HashMap<String, Instant>>,

    /// Tracks consecutive stop-losses to trigger the circuit breaker.
    pub consecutive_losses: AtomicUsize,

    /// Hard cap on simultaneous open positions.
    /// Wrapped in Arc so we can call acquire_owned(), which returns a permit
    /// with 'static lifetime that is safe to move into tokio::spawn tasks.
    pub position_semaphore: Arc<Semaphore>,

    /// Accepted hypothetical positions keyed by mint. Owning the semaphore
    /// permit here keeps the production position cap active after the decision
    /// function returns.
    shadow_positions: RwLock<HashMap<String, ShadowPosition>>,

    /// Timestamp of the moment the circuit breaker first tripped.
    /// `None` when the breaker is not active or has been auto-reset.
    /// Uses std::sync::Mutex (not tokio) because reads are fast and
    /// is_circuit_breaker_active must remain a synchronous fn for use
    /// in the hot websocket loop without .await overhead.
    circuit_breaker_tripped_at: Mutex<Option<Instant>>,
}

#[expect(
    dead_code,
    reason = "shadow-position methods reserved for Phase 4 tracking"
)]
impl BotState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            traded_mints: RwLock::new(HashMap::new()),
            consecutive_losses: AtomicUsize::new(0),
            position_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_POSITIONS)),
            shadow_positions: RwLock::new(HashMap::new()),
            circuit_breaker_tripped_at: Mutex::new(None),
        })
    }

    /// Atomically checks if we can trade this token, and locks it if we can.
    /// Returns true if the mint was freshly inserted (trade is allowed).
    /// Returns false if it was already present (duplicate — skip).
    ///
    /// NOTE: This only guards against duplicate mints. The caller must ALSO
    /// call `try_acquire_position` to enforce the concurrent position cap.
    pub async fn try_lock_mint(&self, mint: &str) -> bool {
        const MINT_TTL: Duration = Duration::from_secs(4 * 3600); // 4-hour re-entry window
        let mut mints = self.traded_mints.write().await;

        // Prune entries older than TTL on every write. This is O(n) but n is
        // bounded by MAX_CONCURRENT_POSITIONS and is called at most once per
        // trade signal — negligible overhead vs. the RPC calls that follow.
        mints.retain(|_, inserted_at| inserted_at.elapsed() < MINT_TTL);

        if mints.contains_key(mint) {
            return false; // Active or recently traded — skip.
        }
        mints.insert(mint.to_string(), Instant::now());
        true
    }

    /// Releases a mint lock if execution fails early.
    pub async fn unlock_mint(&self, mint: &str) {
        let mut mints = self.traded_mints.write().await;
        mints.remove(mint);
    }

    /// Attempts to acquire a position slot from the semaphore.
    ///
    /// Returns `Some(permit)` if a slot is available. The caller MUST hold
    /// this permit for the duration of the trade and drop it when the watcher
    /// exits (dropping an OwnedSemaphorePermit releases it automatically).
    ///
    /// Returns `None` if MAX_CONCURRENT_POSITIONS are already open.
    /// The caller should log and skip the signal — do NOT call try_lock_mint first;
    /// check the semaphore first to avoid polluting traded_mints with mints
    /// we never actually traded.
    ///
    /// Returns OwnedSemaphorePermit (not borrowed) so it can safely be moved
    /// into tokio::spawn tasks across 'static lifetime boundaries.
    pub fn try_acquire_position(&self) -> Option<OwnedSemaphorePermit> {
        self.position_semaphore.clone().try_acquire_owned().ok()
    }

    /// Returns the number of currently open positions.
    pub fn open_position_count(&self) -> usize {
        MAX_CONCURRENT_POSITIONS - self.position_semaphore.available_permits()
    }

    /// Retains an accepted shadow position and its capacity permit.
    ///
    /// Returns false if the mint already has a retained shadow position. The
    /// supplied permit is then dropped on return and capacity is restored.
    pub(crate) async fn retain_shadow_position(
        &self,
        mint: &str,
        opened_at_ms: u64,
        position_permit: OwnedSemaphorePermit,
    ) -> bool {
        let mut positions = self.shadow_positions.write().await;
        if positions.contains_key(mint) {
            return false;
        }

        positions.insert(
            mint.to_string(),
            ShadowPosition {
                opened_at_ms,
                _position_permit: position_permit,
            },
        );
        true
    }

    /// Explicitly closes a retained shadow position and releases its permit.
    ///
    /// The duplicate-mint guard is deliberately left intact until its normal
    /// four-hour expiry, preventing an early release from enabling a re-buy.
    pub(crate) async fn release_shadow_position(&self, mint: &str) -> bool {
        self.shadow_positions.write().await.remove(mint).is_some()
    }

    /// Releases retained shadow positions whose deterministic event-time
    /// lifetime has expired.
    pub(crate) async fn prune_expired_shadow_positions(&self, now_ms: u64) {
        let mut positions = self.shadow_positions.write().await;
        positions.retain(|_, position| {
            now_ms.saturating_sub(position.opened_at_ms) < SHADOW_POSITION_TTL_MS
        });
    }

    pub(crate) async fn has_shadow_position(&self, mint: &str) -> bool {
        self.shadow_positions.read().await.contains_key(mint)
    }

    pub(crate) async fn shadow_position_count(&self) -> usize {
        self.shadow_positions.read().await.len()
    }

    pub(crate) async fn has_traded_mint(&self, mint: &str) -> bool {
        self.traded_mints.read().await.contains_key(mint)
    }

    /// Checks if the circuit breaker is active.
    ///
    /// # State Machine
    ///
    /// - **Below threshold** (`consecutive_losses < 3`): Returns `false`.
    ///   Clears the trip timestamp in case it was set during a previous cycle.
    ///
    /// - **First trip** (threshold reached, no timestamp yet): Records
    ///   `circuit_breaker_tripped_at = Instant::now()`, returns `true`.
    ///   Logs a loud warning so operators know trading has been paused.
    ///
    /// - **In cooldown** (threshold reached, elapsed < 30 min): Returns `true`
    ///   silently. Logging here would flood the output on every signal.
    ///
    /// - **Cooldown expired** (elapsed >= 30 min): Resets `consecutive_losses`
    ///   to 0, clears the timestamp, returns `false`. Trading resumes.
    ///   Logs a confirmation so operators know the bot self-recovered.
    pub fn is_circuit_breaker_active(&self) -> bool {
        let losses = self.consecutive_losses.load(Ordering::SeqCst);

        if losses < CIRCUIT_BREAKER_THRESHOLD {
            // Below threshold — clear the trip timer from any previous cycle.
            let mut guard = self
                .circuit_breaker_tripped_at
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *guard = None;
            return false;
        }

        let mut guard = self
            .circuit_breaker_tripped_at
            .lock()
            .unwrap_or_else(|e| e.into_inner());

        match *guard {
            None => {
                // Circuit breaker tripping for the first time — record timestamp.
                *guard = Some(Instant::now());
                eprintln!(
                    "🔴 CIRCUIT BREAKER TRIPPED: {} consecutive stop-losses. \
                     Trading PAUSED for {} minutes. Will auto-reset.",
                    losses,
                    CIRCUIT_BREAKER_COOLDOWN.as_secs() / 60
                );
                true
            }
            Some(tripped_at) => {
                if tripped_at.elapsed() >= CIRCUIT_BREAKER_COOLDOWN {
                    // Cooldown expired — auto-reset and resume trading.
                    self.consecutive_losses.store(0, Ordering::SeqCst);
                    *guard = None;
                    eprintln!(
                        "🔄 Circuit breaker AUTO-RESET after {} minute cooldown. Trading RESUMED.",
                        CIRCUIT_BREAKER_COOLDOWN.as_secs() / 60
                    );
                    false
                } else {
                    // Still in cooldown — silent return to avoid log flooding.
                    true
                }
            }
        }
    }
}
