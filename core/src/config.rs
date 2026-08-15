use dashmap::DashMap;
use std::{collections::HashMap, env, sync::Arc};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShadowStartupPolicy {
    pub position_recovery_allowed: bool,
    pub capital_execution_allowed: bool,
}

fn parse_flag(
    name: &'static str,
    value: Option<&str>,
    required: bool,
) -> Result<bool, crate::error::BotError> {
    match value.map(str::trim) {
        Some(value) if value.eq_ignore_ascii_case("true") || value == "1" => Ok(true),
        Some(value) if value.eq_ignore_ascii_case("false") || value == "0" => Ok(false),
        Some(_) => Err(crate::error::BotError::ConfigError(format!(
            "{name} must be `true` or `false`"
        ))),
        None if required => Err(crate::error::BotError::ConfigError(format!(
            "{name} environment variable is REQUIRED"
        ))),
        None => Ok(false),
    }
}

pub fn startup_policy(
    dry_run_value: Option<&str>,
    live_execution_value: Option<&str>,
) -> Result<ShadowStartupPolicy, crate::error::BotError> {
    let dry_run = parse_flag("DRY_RUN", dry_run_value, true)?;
    let live_execution = parse_flag("LIVE_EXECUTION", live_execution_value, false)?;

    match (dry_run, live_execution) {
        (true, false) => Ok(ShadowStartupPolicy {
            position_recovery_allowed: false,
            capital_execution_allowed: false,
        }),
        (false, true) => Ok(ShadowStartupPolicy {
            position_recovery_allowed: false,
            capital_execution_allowed: true,
        }),
        _ => Err(crate::error::BotError::ConfigError(
            "exactly one execution mode must be selected: use DRY_RUN=true with \
             LIVE_EXECUTION=false for shadow mode, or DRY_RUN=false with \
             LIVE_EXECUTION=true for capital execution"
                .to_string(),
        )),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub enum WhaleLane {
    Conservative,
    Swing,
    Degen,
    Sniper,
    Unknown,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct WhaleProfile {
    pub total_trades: u32,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub net_profit: f64,
    pub lane: WhaleLane,
}

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub rpc_url: String,
    pub raydium_ws_url: String,
    pub target_mints: Option<Vec<String>>,
    pub min_swap_lamports: u64,
    pub telegram_bot_token: Option<String>,
    pub telegram_chat_id: Option<String>,
    pub pumpportal_api_key: Option<String>,
    pub startup_policy: ShadowStartupPolicy,
    pub watchlist: Arc<DashMap<String, WhaleProfile>>,
}

impl AppConfig {
    pub fn load_from_env() -> Result<Self, crate::error::BotError> {
        // -----------------------------------------------------------------------
        // HARDENING: RPC_URL is now REQUIRED — no silent public-node fallback.
        //
        // WHY: The public Solana RPC (api.mainnet-beta.solana.com) does NOT
        // support `getPriorityFeeEstimate` (Helius-specific), rate-limits
        // aggressively, and silently degrades every exit transaction to the
        // FALLBACK_PRIORITY_FEE with no warning. A misconfigured deploy would
        // produce real trades with systematically underpriced sell fees.
        // -----------------------------------------------------------------------
        let dry_run_env = env::var("DRY_RUN").ok();
        let live_execution_env = env::var("LIVE_EXECUTION").ok();
        let startup_policy = startup_policy(dry_run_env.as_deref(), live_execution_env.as_deref())?;
        let dry_run = !startup_policy.capital_execution_allowed;

        let rpc_url = env::var("RPC_URL").map_err(|_| {
            crate::error::BotError::ConfigError(
                "RPC_URL environment variable is REQUIRED. \
                 Set it to your Helius (or equivalent) RPC endpoint. \
                 Example: https://mainnet.helius-rpc.com/?api-key=YOUR_KEY"
                    .to_string(),
            )
        })?;
        if !rpc_url.starts_with("https://") {
            return Err(crate::error::BotError::ConfigError(
                "RPC_URL must use HTTPS.".to_string(),
            ));
        }

        let raydium_ws_url = env::var("RAYDIUM_WS_URL").map_err(|_| {
            crate::error::BotError::ConfigError(
                "RAYDIUM_WS_URL environment variable is REQUIRED.".to_string(),
            )
        })?;

        if !raydium_ws_url.starts_with("wss://") {
            return Err(crate::error::BotError::ConfigError(
                "RAYDIUM_WS_URL must use WSS.".to_string(),
            ));
        }

        let telegram_bot_token = env::var("TELEGRAM_BOT_TOKEN")
            .ok()
            .filter(|s| !s.is_empty());
        let telegram_chat_id = env::var("TELEGRAM_CHAT_ID").ok().filter(|s| !s.is_empty());

        let pumpportal_api_key = env::var("PUMPPORTAL_API_KEY")
            .ok()
            .filter(|s| !s.is_empty());

        if telegram_bot_token.is_none() || telegram_chat_id.is_none() {
            println!("⚠️  TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set — alerts disabled.");
        }

        // BUGFIX (Critical #2, audit 2026-08): this value was parsed and
        // validated but then discarded — the struct below hardcoded
        // `min_swap_lamports: 10_000` regardless of what the operator set.
        // Now actually threaded through.
        let min_swap_lamports = match env::var("MIN_SWAP_LAMPORTS") {
            Ok(val) => val.trim().parse::<u64>().map_err(|error| {
                crate::error::BotError::ConfigError(format!(
                    "MIN_SWAP_LAMPORTS must be an unsigned integer: {error}"
                ))
            })?,
            Err(_) => 10_000,
        };

        if dry_run {
            println!("DRY RUN MODE ENABLED: execution signals will not be transmitted.");
        } else {
            println!("LIVE EXECUTION MODE ENABLED: capital transmission is authorized.");
        }

        let watchlist = Self::load_csv_watchlist("approved_watchlist.csv");
        log::info!(
            "Loaded {} approved whale wallets into memory",
            watchlist.len()
        );

        let watchlist_arc = Arc::new(watchlist.into_iter().collect::<DashMap<_, _>>());

        Ok(Self {
            rpc_url,
            raydium_ws_url,
            // BUGFIX (Critical #1, audit 2026-08): this was previously
            // `Some(vec![])`, which is NOT equivalent to "no filter" under
            // `Option::is_none_or` in websocket.rs::configured_target_matches
            // — `Some(vec![])` runs `.any()` against an empty iterator,
            // which is always `false`. Every decoded swap was therefore
            // silently routed into FILTER_DROPS and never reached the exit
            // watchers, leaving every live position with zero VWAP/trailing
            // stop/panic-velocity protection. `None` is the actual "accept
            // every mint" sentinel this codebase has no env wiring to
            // override yet; `configured_target_matches` also now treats an
            // empty `Some(vec![])` as unrestricted so this class of bug
            // can't resurface if target-mint filtering is wired to an env
            // var later and left unset/empty.
            target_mints: None,
            min_swap_lamports,
            telegram_bot_token,
            telegram_chat_id,
            pumpportal_api_key,
            startup_policy,
            watchlist: watchlist_arc,
        })
    }

    pub fn load_csv_watchlist(path: &str) -> HashMap<String, WhaleProfile> {
        let mut watchlist = HashMap::new();
        if let Ok(content) = std::fs::read_to_string(path) {
            for (i, line) in content.lines().enumerate() {
                if i == 0 || line.trim().is_empty() {
                    continue;
                } // skip header and empty
                let parts: Vec<&str> = line.split(',').collect();
                if parts.len() >= 5 {
                    let wallet = parts[0].trim().to_string();
                    let total_trades = parts[1].parse::<u32>().unwrap_or(0);
                    let win_rate = parts[2].parse::<f64>().unwrap_or(0.0);
                    let profit_factor = parts[3].parse::<f64>().unwrap_or(0.0);
                    let net_profit = parts[4].parse::<f64>().unwrap_or(0.0);

                    let lane_str = if parts.len() > 5 { parts[5].trim() } else { "" };
                    let lane = match lane_str {
                        "Conservative" => WhaleLane::Conservative,
                        "Swing" => WhaleLane::Swing,
                        "Degen" => WhaleLane::Degen,
                        "Sniper" => WhaleLane::Sniper,
                        _ => {
                            // Fallback if lane is missing
                            if win_rate >= 0.6 && profit_factor > 2.0 {
                                WhaleLane::Conservative
                            } else if win_rate < 0.4 && profit_factor > 2.0 {
                                WhaleLane::Degen
                            } else {
                                WhaleLane::Swing
                            }
                        }
                    };

                    watchlist.insert(
                        wallet,
                        WhaleProfile {
                            total_trades,
                            win_rate,
                            profit_factor,
                            net_profit,
                            lane,
                        },
                    );
                }
            }
        }
        watchlist
    }
}

pub async fn spawn_hot_reloader(watchlist_arc: Arc<DashMap<String, WhaleProfile>>, path: String) {
    log::info!("Starting Hot Reloader for {}", path);
    let mut last_modified = std::time::SystemTime::UNIX_EPOCH;

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;

        if let Ok(metadata) = std::fs::metadata(&path) {
            if let Ok(modified) = metadata.modified() {
                if modified > last_modified {
                    // Only reload if this isn't the first loop iteration
                    if last_modified != std::time::SystemTime::UNIX_EPOCH {
                        log::info!("[Hot-Reload] Detected changes in {}. Reloading...", path);
                        let new_watchlist = AppConfig::load_csv_watchlist(&path);

                        watchlist_arc.clear();
                        for (k, v) in new_watchlist {
                            watchlist_arc.insert(k, v);
                        }
                        log::info!(
                            "[Hot-Reload] Watchlist updated from disk! Loaded {} whales.",
                            watchlist_arc.len()
                        );
                    }
                    last_modified = modified;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_dry_run_fails_closed() {
        assert!(startup_policy(None, None).is_err());
    }

    #[test]
    fn ambiguous_mode_fails_closed() {
        assert!(startup_policy(Some("false"), None).is_err());
        assert!(startup_policy(Some("true"), Some("true")).is_err());
    }

    #[test]
    fn invalid_dry_run_fails_closed() {
        assert!(startup_policy(Some("shadow"), None).is_err());
    }

    #[test]
    fn explicit_dry_run_allows_only_shadow_startup() {
        let policy = startup_policy(Some("true"), None).expect("explicit shadow mode");

        assert!(!policy.position_recovery_allowed);
        assert!(!policy.capital_execution_allowed);
    }

    #[test]
    fn live_execution_requires_both_explicit_flags() {
        let policy =
            startup_policy(Some("false"), Some("true")).expect("explicit live execution mode");

        assert!(!policy.position_recovery_allowed);
        assert!(policy.capital_execution_allowed);
    }
}
