use reqwest::Client;
use serde_json::{json, Value};

// ============================================================================
// SHARED CONSTANTS — Priority Fee
// ============================================================================

/// Maximum priority fee cap to prevent fee runaway during extreme congestion spikes.
/// Exported so exits.rs can apply the same cap during sell fee escalation.
pub const MAX_PRIORITY_FEE_LAMPORTS: u64 = 5_000_000;

/// Fallback priority fee if the dynamic estimator API call fails.
/// Kept private — callers receive this value transparently via get_dynamic_priority_fee.
const FALLBACK_PRIORITY_FEE_LAMPORTS: u64 = 1_000_000;

// ============================================================================
// SHARED HELIUS DYNAMIC PRIORITY FEE ESTIMATOR
// ============================================================================
//
// WHY THIS IS SHARED:
//   Previously this lived only in exits.rs (sells). The buy path in execution.rs
//   used a hardcoded 0.0005 SOL fee — fine during low congestion, catastrophic
//   during a memecoin pump where the network is saturated and the buy is the
//   most time-critical transaction in the entire lifecycle.
//
//   Moving this here ensures both sides of the trade — entry AND exit — use the
//   same market-responsive fee logic with identical fallback behavior.
//
// CALLERS:
//   - crate::exits::execute_jupiter_sell  (lamports, used directly)
//   - crate::execution::execute_pump_buy  (must convert: lamports / 1e9 = SOL,
//                                          because PumpPortal trade-local API
//                                          expects priorityFee in SOL, not lamports)

/// Fetches a dynamic priority fee estimate from the Helius RPC endpoint.
///
/// Returns the fee in **lamports**, capped at `MAX_PRIORITY_FEE_LAMPORTS`.
/// Falls back to `FALLBACK_PRIORITY_FEE_LAMPORTS` on any API or parse failure
/// so the caller always gets a usable value — never a panic, never a zero fee.
///
/// # Conversion for PumpPortal
/// The PumpPortal trade-local API expects `priorityFee` in **SOL**, not lamports:
/// ```rust
/// let fee_sol = get_dynamic_priority_fee(...).await as f64 / 1_000_000_000.0;
/// ```
pub async fn get_dynamic_priority_fee(
    http_client: &Client,
    rpc_url: &str,
    account_keys: &[String],
) -> u64 {
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getPriorityFeeEstimate",
        "params": [{
            "accountKeys": account_keys,
            "options": { "priorityLevel": "veryHigh" }
        }]
    });

    let res = match http_client.post(rpc_url).json(&payload).send().await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("⚠️  Priority fee API call failed: {}. Using fallback.", e);
            return FALLBACK_PRIORITY_FEE_LAMPORTS;
        }
    };

    let body: Value = match res.json().await {
        Ok(v) => v,
        Err(e) => {
            eprintln!(
                "⚠️  Priority fee response malformed: {}. Using fallback.",
                e
            );
            return FALLBACK_PRIORITY_FEE_LAMPORTS;
        }
    };

    let estimated = body
        .get("result")
        .and_then(|r| r.get("priorityFeeEstimate"))
        .and_then(|f| f.as_f64())
        .map(|f| f as u64)
        .unwrap_or_else(|| {
            eprintln!("⚠️  Priority fee estimate missing in response. Using fallback.");
            FALLBACK_PRIORITY_FEE_LAMPORTS
        });

    let capped = estimated.min(MAX_PRIORITY_FEE_LAMPORTS);
    println!(
        "💡 Dynamic priority fee: {} lamports (raw: {}, cap: {})",
        capped, estimated, MAX_PRIORITY_FEE_LAMPORTS
    );
    capped
}
