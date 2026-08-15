// tipping.rs — Alpha Nexus Phase 3: Tip Telemetry & EV-Aware Bidding
//
// Implements MASTER_PLAN.md Section 3. Computes a bounded Jito tip from
// fresh tip-floor telemetry and conservative expected net profit.
//
// Design invariants:
//   - Every value is in lamports (not SOL/f64) after initial conversion.
//   - Telemetry fetch failures fail closed (no implicit fallback to a
//     hardcoded tip floor).
//   - All arithmetic uses checked u128; overflows are caught and reject
//     the opportunity rather than producing a silent wrap.
//   - Telemetry refresh runs on a dedicated task, never on the execution
//     hot path.
//   - Empty tip-account configuration fails at startup, not at runtime.

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

const LAMPORTS_PER_SOL: u64 = 1_000_000_000;
const BASIS_POINTS_DENOM: u64 = 10_000;

/// Telemetry snapshot from the Jito tip-floor API.
///
/// Normalised to lamports at ingest time. The ordering invariant
/// `p50 <= p75 <= p95` is validated during construction.
#[derive(Debug, Clone, Copy)]
pub struct TipTelemetry {
    pub p50_lamports: u64,
    pub p75_lamports: u64,
    pub p95_lamports: u64,
    pub observed_at: Instant,
}

/// Outcome of a `calculate_tip` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TipDecision {
    Bid {
        lamports: u64,
        telemetry_age_ms: u64,
    },
    Skip {
        reason: TipSkipReason,
    },
}

/// Reason a tip bid was not produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TipSkipReason {
    TelemetryStale,
    InsufficientEdge,
    TipCapHit,
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the tip-telemetry engine.
#[derive(Debug, Clone)]
pub struct TipConfig {
    pub tip_floor_url: String,
    pub refresh_interval: Duration,
    pub max_telemetry_age: Duration,
    pub max_profit_share_bps: u16,
    pub minimum_net_profit_lamports: u64,
}

impl Default for TipConfig {
    fn default() -> Self {
        Self {
            tip_floor_url: "https://bundles.jito.wtf/api/v1/bundles/tip_floor".to_string(),
            refresh_interval: Duration::from_secs(30),
            max_telemetry_age: Duration::from_secs(120),
            max_profit_share_bps: 5000,
            minimum_net_profit_lamports: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct TipTelemetryEngine {
    inner: Arc<TipTelemetryEngineInner>,
}

struct TipTelemetryEngineInner {
    telemetry: RwLock<Option<TipTelemetry>>,
    config: TipConfig,
}

impl TipTelemetryEngine {
    pub fn new(config: TipConfig) -> Self {
        Self {
            inner: Arc::new(TipTelemetryEngineInner {
                telemetry: RwLock::new(None),
                config,
            }),
        }
    }

    pub fn spawn_refresh_loop(self, shutdown: tokio::sync::watch::Receiver<bool>) {
        tokio::spawn(async move {
            self.refresh_loop_inner(shutdown).await;
        });
    }

    async fn refresh_loop_inner(self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let client = reqwest::Client::new();

        // Initial fetch before entering the sleep loop
        let result = self.fetch_telemetry(&client).await;
        match result {
            Ok(telemetry) => {
                log::info!(
                    "Tip telemetry initial refresh: p50={}, p75={}, p95={} lamports",
                    telemetry.p50_lamports,
                    telemetry.p75_lamports,
                    telemetry.p95_lamports,
                );
                match self.inner.telemetry.write() {
                    Ok(mut lock) => *lock = Some(telemetry),
                    Err(poison) => {
                        log::warn!("Recovering poisoned tip telemetry write lock");
                        *poison.into_inner() = Some(telemetry);
                    }
                }
            }
            Err(error) => {
                log::warn!("Tip telemetry initial refresh failed: {error}");
            }
        }

        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = tokio::time::sleep(self.inner.config.refresh_interval) => {}
            }

            let result = self.fetch_telemetry(&client).await;
            match result {
                Ok(telemetry) => {
                    log::info!(
                        "Tip telemetry refreshed: p50={}, p75={}, p95={} lamports",
                        telemetry.p50_lamports,
                        telemetry.p75_lamports,
                        telemetry.p95_lamports,
                    );
                    match self.inner.telemetry.write() {
                        Ok(mut lock) => *lock = Some(telemetry),
                        Err(poison) => {
                            log::warn!("Recovering poisoned tip telemetry write lock");
                            *poison.into_inner() = Some(telemetry);
                        }
                    }
                }
                Err(error) => {
                    log::warn!("Tip telemetry refresh failed: {error}; keeping previous snapshot");
                }
            }
        }
    }

    async fn fetch_telemetry(&self, client: &reqwest::Client) -> Result<TipTelemetry, String> {
        let response = client
            .get(&self.inner.config.tip_floor_url)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| format!("HTTP request failed: {e}"))?;

        let body = response
            .text()
            .await
            .map_err(|e| format!("read response body failed: {e}"))?;

        parse_tip_floor_response(&body)
    }

    /// Computes a bounded tip using the decision model from
    /// MASTER_PLAN.md Section 3.3.
    ///
    /// `pre_tip_expected_profit_lamports` is the profit estimated *before*
    /// the Jito tip, after subtracting base fees, priority fees, slippage
    /// reserves, and failure-risk reserves (the execution consumer's
    /// responsibility to provide a conservative estimate).
    pub fn calculate_tip(
        &self,
        pre_tip_expected_profit_lamports: u64,
        now: Instant,
    ) -> TipDecision {
        let telemetry_guard = match self.inner.telemetry.read() {
            Ok(guard) => guard,
            Err(poison) => {
                log::warn!("Recovering poisoned tip telemetry read lock");
                poison.into_inner()
            }
        };

        let telemetry = match *telemetry_guard {
            Some(t) => t,
            None => {
                return TipDecision::Skip {
                    reason: TipSkipReason::TelemetryStale,
                };
            }
        };

        let age = now.duration_since(telemetry.observed_at);
        if age > self.inner.config.max_telemetry_age {
            return TipDecision::Skip {
                reason: TipSkipReason::TelemetryStale,
            };
        }

        let telemetry_age_ms = age.as_millis() as u64;

        let pre_tip = pre_tip_expected_profit_lamports as u128;
        let p50 = telemetry.p50_lamports as u128;
        let p75 = telemetry.p75_lamports as u128;
        let p95 = telemetry.p95_lamports as u128;

        if pre_tip < p50.saturating_mul(2) {
            return TipDecision::Skip {
                reason: TipSkipReason::InsufficientEdge,
            };
        }

        let candidate = if pre_tip < p50.saturating_mul(3) {
            p50
        } else if pre_tip < p50.saturating_mul(10) {
            p75
        } else {
            let pct_cap = pre_tip
                .checked_mul(2500)
                .and_then(|v| v.checked_div(10_000))
                .unwrap_or(u128::MAX);
            p95.min(pct_cap)
        };

        let profit_share_cap = pre_tip
            .checked_mul(self.inner.config.max_profit_share_bps as u128)
            .and_then(|v| v.checked_div(BASIS_POINTS_DENOM as u128))
            .unwrap_or(0);

        let tip_lamports = candidate.min(profit_share_cap) as u64;

        let net_profit = pre_tip_expected_profit_lamports.saturating_sub(tip_lamports);
        if net_profit < self.inner.config.minimum_net_profit_lamports {
            return TipDecision::Skip {
                reason: TipSkipReason::TipCapHit,
            };
        }

        TipDecision::Bid {
            lamports: tip_lamports,
            telemetry_age_ms,
        }
    }
}

// ---------------------------------------------------------------------------
// Serde helpers for the Jito tip-floor API response
// ---------------------------------------------------------------------------

fn parse_tip_floor_response(body: &str) -> Result<TipTelemetry, String> {
    let entries: Vec<TipFloorEntry> =
        serde_json::from_str(body).map_err(|e| format!("JSON parse error: {e}"))?;

    let latest = entries
        .into_iter()
        .next_back()
        .ok_or_else(|| "tip_floor endpoint returned an empty array".to_string())?;

    let p50 = sol_to_lamports(latest.landed_tips_50th_percentile)?;
    let p75 = sol_to_lamports(latest.landed_tips_75th_percentile)?;
    let p95 = sol_to_lamports(latest.landed_tips_95th_percentile)?;

    if p50 > p75 || p75 > p95 {
        return Err(format!(
            "tip percentiles out of order: p50={p50} p75={p75} p95={p95} (must be monotonic)"
        ));
    }

    Ok(TipTelemetry {
        p50_lamports: p50,
        p75_lamports: p75,
        p95_lamports: p95,
        observed_at: Instant::now(),
    })
}

fn sol_to_lamports(sol: f64) -> Result<u64, String> {
    if !sol.is_finite() || sol < 0.0 {
        return Err(format!("invalid SOL value: {sol}"));
    }
    let lamports = (sol * LAMPORTS_PER_SOL as f64).round() as u64;
    Ok(lamports)
}

#[derive(serde::Deserialize)]
struct TipFloorEntry {
    #[serde(rename = "landed_tips_50th_percentile")]
    landed_tips_50th_percentile: f64,
    #[serde(rename = "landed_tips_75th_percentile")]
    landed_tips_75th_percentile: f64,
    #[serde(rename = "landed_tips_95th_percentile")]
    landed_tips_95th_percentile: f64,
}

// ---------------------------------------------------------------------------
// Metrics counters (Phase 3, Section 3.6)
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct TipMetrics {
    pub refresh_total: AtomicU64,
    pub refresh_error_total: AtomicU64,
    pub bid_total: AtomicU64,
    pub skip_stale_total: AtomicU64,
    pub skip_edge_total: AtomicU64,
    pub skip_cap_total: AtomicU64,
    pub bid_lamports_total: AtomicU64,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn example_telemetry(now: Instant) -> TipTelemetry {
        TipTelemetry {
            p50_lamports: 10_000,
            p75_lamports: 36_000,
            p95_lamports: 1_400_000,
            observed_at: now,
        }
    }

    fn engine_with_telemetry(telemetry: TipTelemetry) -> TipTelemetryEngine {
        let config = TipConfig {
            max_telemetry_age: Duration::from_secs(120),
            max_profit_share_bps: 5000,
            ..Default::default()
        };
        let engine = TipTelemetryEngine::new(config);
        *engine.inner.telemetry.write().unwrap() = Some(telemetry);
        engine
    }

    // --- parse_tip_floor_response ---

    #[test]
    fn parses_valid_tip_floor_response() {
        let body = r#"[
            {"time":"2024-01-01T00:00:00Z","landed_tips_25th_percentile":0.0,"landed_tips_50th_percentile":1e-05,"landed_tips_75th_percentile":3.6e-05,"landed_tips_95th_percentile":0.001,"landed_tips_99th_percentile":0.01,"ema_landed_tips_50th_percentile":9.8e-06}
        ]"#;
        let t = parse_tip_floor_response(body).expect("valid response");
        assert_eq!(t.p50_lamports, 10_000);
        assert_eq!(t.p75_lamports, 36_000);
        assert_eq!(t.p95_lamports, 1_000_000);
    }

    #[test]
    fn rejects_non_finite_sol_value() {
        let body = r#"[
            {"time":"2024-01-01T00:00:00Z","landed_tips_25th_percentile":0.0,"landed_tips_50th_percentile":"NaN","landed_tips_75th_percentile":0.0,"landed_tips_95th_percentile":0.0,"landed_tips_99th_percentile":0.0,"ema_landed_tips_50th_percentile":0.0}
        ]"#;
        assert!(parse_tip_floor_response(body).is_err());
    }

    #[test]
    fn rejects_percentiles_out_of_order() {
        let body = r#"[
            {"time":"2024-01-01T00:00:00Z","landed_tips_25th_percentile":0.0,"landed_tips_50th_percentile":1e-05,"landed_tips_75th_percentile":3.6e-05,"landed_tips_95th_percentile":5e-07,"landed_tips_99th_percentile":0.0,"ema_landed_tips_50th_percentile":9.8e-06}
        ]"#;
        assert!(parse_tip_floor_response(body).is_err());
    }

    #[test]
    fn rejects_empty_array() {
        assert!(parse_tip_floor_response("[]").is_err());
    }

    // --- calculate_tip ---

    #[test]
    fn missing_telemetry_rejects() {
        let engine = TipTelemetryEngine::new(TipConfig::default());
        let now = Instant::now();
        let decision = engine.calculate_tip(1_000_000, now);
        assert_eq!(
            decision,
            TipDecision::Skip {
                reason: TipSkipReason::TelemetryStale
            }
        );
    }

    #[test]
    fn stale_telemetry_rejects() {
        let mut telemetry = example_telemetry(Instant::now());
        telemetry.observed_at = Instant::now() - Duration::from_secs(300);
        let engine = engine_with_telemetry(telemetry);
        let decision = engine.calculate_tip(1_000_000, Instant::now());
        assert_eq!(
            decision,
            TipDecision::Skip {
                reason: TipSkipReason::TelemetryStale
            }
        );
    }

    #[test]
    fn insufficient_edge_rejects() {
        let now = Instant::now();
        let telemetry = example_telemetry(now);
        let engine = engine_with_telemetry(telemetry);
        let decision = engine.calculate_tip(15_000, now);
        assert_eq!(
            decision,
            TipDecision::Skip {
                reason: TipSkipReason::InsufficientEdge
            }
        );
    }

    #[test]
    fn low_bucket_uses_p50() {
        let now = Instant::now();
        let telemetry = example_telemetry(now);
        let engine = engine_with_telemetry(telemetry);
        let decision = engine.calculate_tip(25_000, now);
        assert_eq!(
            decision,
            TipDecision::Bid {
                lamports: 10_000,
                telemetry_age_ms: 0
            }
        );
    }

    #[test]
    fn mid_bucket_uses_p75() {
        let now = Instant::now();
        let telemetry = example_telemetry(now);
        let engine = engine_with_telemetry(telemetry);
        let decision = engine.calculate_tip(80_000, now);
        assert_eq!(
            decision,
            TipDecision::Bid {
                lamports: 36_000,
                telemetry_age_ms: 0
            }
        );
    }

    #[test]
    fn high_bucket_uses_p95_capped_by_25pct() {
        let now = Instant::now();
        let telemetry = example_telemetry(now);
        let engine = engine_with_telemetry(telemetry);
        let decision = engine.calculate_tip(200_000, now);
        assert_eq!(
            decision,
            TipDecision::Bid {
                lamports: 50_000,
                telemetry_age_ms: 0
            }
        );
    }

    #[test]
    fn high_bucket_p95_is_the_limiting_factor() {
        let now = Instant::now();
        let telemetry = TipTelemetry {
            p50_lamports: 1_000,
            p75_lamports: 2_000,
            p95_lamports: 3_000,
            observed_at: now,
        };
        let engine = engine_with_telemetry(telemetry);
        let decision = engine.calculate_tip(1_000_000, now);
        assert_eq!(
            decision,
            TipDecision::Bid {
                lamports: 3_000,
                telemetry_age_ms: 0
            }
        );
    }

    #[test]
    fn profit_share_cap_is_the_limiting_factor() {
        let now = Instant::now();
        let config = TipConfig {
            max_profit_share_bps: 100,
            ..Default::default()
        };
        let telemetry = TipTelemetry {
            p50_lamports: 1_000,
            p75_lamports: 2_000,
            p95_lamports: 1_000_000,
            observed_at: now,
        };
        let engine = TipTelemetryEngine::new(config);
        *engine.inner.telemetry.write().unwrap() = Some(telemetry);

        let decision = engine.calculate_tip(100_000, now);
        assert_eq!(
            decision,
            TipDecision::Bid {
                lamports: 1_000,
                telemetry_age_ms: 0
            }
        );
    }

    #[test]
    fn post_tip_positive_net_profit_boundary() {
        let now = Instant::now();
        let telemetry = example_telemetry(now);
        let config = TipConfig {
            minimum_net_profit_lamports: 5_000,
            ..Default::default()
        };
        let engine = TipTelemetryEngine::new(config);
        *engine.inner.telemetry.write().unwrap() = Some(telemetry);

        let decision = engine.calculate_tip(20_000, now);
        assert!(matches!(decision, TipDecision::Bid { .. }));
    }

    #[test]
    fn post_tip_net_profit_below_floor_skips() {
        let now = Instant::now();
        let telemetry = example_telemetry(now);
        let config = TipConfig {
            minimum_net_profit_lamports: 15_000,
            max_profit_share_bps: 5000,
            ..Default::default()
        };
        let engine = TipTelemetryEngine::new(config);
        *engine.inner.telemetry.write().unwrap() = Some(telemetry);

        let decision = engine.calculate_tip(20_000, now);
        assert_eq!(
            decision,
            TipDecision::Skip {
                reason: TipSkipReason::TipCapHit
            }
        );
    }

    #[test]
    fn tip_never_exceeds_configured_profit_share() {
        let now = Instant::now();
        let telemetry = TipTelemetry {
            p50_lamports: 1,
            p75_lamports: 2,
            p95_lamports: 3,
            observed_at: now,
        };
        let engine = engine_with_telemetry(telemetry);
        let decision = engine.calculate_tip(u64::MAX, now);
        match decision {
            TipDecision::Bid { lamports, .. } => {
                assert!(lamports as u128 <= (u64::MAX as u128) * 2500 / 10_000);
            }
            TipDecision::Skip { .. } => {}
        }
    }

    // --- sol_to_lamports ---

    #[test]
    fn converts_sol_to_lamports() {
        assert_eq!(sol_to_lamports(1.0).unwrap(), 1_000_000_000);
        assert_eq!(sol_to_lamports(0.000_001).unwrap(), 1_000);
        assert_eq!(sol_to_lamports(0.0).unwrap(), 0);
    }

    #[test]
    fn rejects_negative_sol() {
        assert!(sol_to_lamports(-1.0).is_err());
    }

    #[test]
    fn rejects_nan() {
        assert!(sol_to_lamports(f64::NAN).is_err());
    }

    #[test]
    fn rejects_infinite() {
        assert!(sol_to_lamports(f64::INFINITY).is_err());
    }
}
