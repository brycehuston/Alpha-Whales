#![allow(
    clippy::result_large_err,
    clippy::too_many_arguments,
    clippy::enum_variant_names
)]

mod bundle_tracker;
mod config;
mod db;
mod dispatcher;
mod error;
mod execution;
mod exits;
mod pool_cache;

mod webhook;
mod state;
pub mod telegram;
mod tipping;
mod types;
mod websocket;

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair, signer::Signer};
use std::{
    env,
    error::Error,
    path::PathBuf,
    str::FromStr,
    sync::atomic::{AtomicUsize, Ordering},
    sync::Arc,
    time::Duration,
};
use tokio::task::JoinSet;

type WorkerResult = Result<(), String>;
const MAX_SIGNAL_AGE_LIMIT_MS: u64 = 30_000;
const SWAP_CHANNEL_CAPACITY: usize = 8_192;

/// Capacity of the tokio::sync::broadcast channel that delivers SwapEvents to
/// exit watchers. Each active position watcher subscribes independently.
///
/// 4096 is sized for worst-case: ~1 000 events/s × 4 s lag budget = 4 000.
/// A lagged watcher loses the oldest events but continues operating — the
/// broadcast channel never blocks senders.
const EXIT_BROADCAST_CAPACITY: usize = 4096;

fn required_u64(name: &'static str) -> Result<u64, error::BotError> {
    let value = env::var(name).map_err(|_| {
        error::BotError::ConfigError(format!("{name} environment variable is REQUIRED"))
    })?;
    value.trim().parse::<u64>().map_err(|error| {
        error::BotError::ConfigError(format!("{name} must be an unsigned integer: {error}"))
    })
}

fn optional_u64(name: &'static str, default: u64) -> Result<u64, error::BotError> {
    match env::var(name) {
        Ok(value) => value.trim().parse::<u64>().map_err(|error| {
            error::BotError::ConfigError(format!("{name} must be an unsigned integer: {error}"))
        }),
        Err(_) => Ok(default),
    }
}

fn optional_u16(name: &'static str, default: u16) -> Result<u16, error::BotError> {
    match env::var(name) {
        Ok(value) => value.trim().parse::<u16>().map_err(|error| {
            error::BotError::ConfigError(format!("{name} must be an unsigned integer: {error}"))
        }),
        Err(_) => Ok(default),
    }
}

fn required_path(name: &'static str) -> Result<PathBuf, error::BotError> {
    let value = env::var(name).map_err(|_| {
        error::BotError::ConfigError(format!("{name} environment variable is REQUIRED"))
    })?;
    let path = PathBuf::from(value.trim());
    if !path.is_absolute() {
        return Err(error::BotError::ConfigError(format!(
            "{name} must be an absolute path"
        )));
    }
    Ok(path)
}

fn load_wallet_private_key() -> Result<Keypair, error::BotError> {
    let encoded = env::var("WALLET_PRIVATE_KEY").map_err(|_| {
        error::BotError::ConfigError(
            "WALLET_PRIVATE_KEY environment variable is REQUIRED for live execution".to_string(),
        )
    })?;
    let encoded = encoded.trim();
    if encoded.is_empty() {
        return Err(error::BotError::ConfigError(
            "WALLET_PRIVATE_KEY must not be empty".to_string(),
        ));
    }

    let keypair =
        std::panic::catch_unwind(|| Keypair::from_base58_string(encoded)).map_err(|_| {
            error::BotError::ConfigError(
                "WALLET_PRIVATE_KEY is not a valid base58-encoded Solana keypair".to_string(),
            )
        })?;
    if keypair.pubkey() == solana_sdk::pubkey::Pubkey::default() {
        return Err(error::BotError::ConfigError(
            "WALLET_PRIVATE_KEY produced the default pubkey".to_string(),
        ));
    }
    Ok(keypair)
}

fn required_pubkey(name: &'static str) -> Result<solana_sdk::pubkey::Pubkey, error::BotError> {
    let value = env::var(name).map_err(|_| {
        error::BotError::ConfigError(format!("{name} environment variable is REQUIRED"))
    })?;
    solana_sdk::pubkey::Pubkey::from_str(value.trim()).map_err(|error| {
        error::BotError::ConfigError(format!("{name} is not a valid base58 pubkey: {error}"))
    })
}

fn load_live_executor_config() -> Result<execution::JitoExecutorConfig, error::BotError> {

    let max_slippage_bps = u16::try_from(required_u64("MAX_SLIPPAGE_BPS")?).map_err(|error| {
        error::BotError::ConfigError(format!("MAX_SLIPPAGE_BPS is out of range: {error}"))
    })?;
    let max_signal_age_ms = required_u64("MAX_SIGNAL_AGE_MS")?;
    if max_signal_age_ms == 0 || max_signal_age_ms > MAX_SIGNAL_AGE_LIMIT_MS {
        return Err(error::BotError::ConfigError(format!(
            "MAX_SIGNAL_AGE_MS must be between 1 and {MAX_SIGNAL_AGE_LIMIT_MS}"
        )));
    }
    let max_pending_capital_lamports = required_u64("MAX_PENDING_CAPITAL_LAMPORTS")?;
    let execution_journal_path = required_path("EXECUTION_JOURNAL_PATH")?;
    // Phase 2 (MASTER_PLAN.md Section 2): the jitodontfront sentinel used
    // to request Jito Block Engine anti-sandwich ordering. Required (not
    // defaulted) so an operator cannot accidentally run live execution
    // without sandwich protection configured.
    let jito_dont_front_pubkey = required_pubkey("JITO_DONT_FRONT_PUBKEY")?;

    execution::JitoExecutorConfig::new(
        execution::DEFAULT_JITO_BLOCK_ENGINE_URL.to_string(),
        execution::MINIMUM_JITO_TIP_LAMPORTS,
        Duration::from_secs(10),
        Duration::from_secs(2),
        max_slippage_bps,
        Duration::from_millis(max_signal_age_ms),
        max_pending_capital_lamports,
        execution_journal_path,
        jito_dont_front_pubkey,
    )
    .map(|config| config.with_alt_address(None))
    .map_err(|error| error::BotError::ConfigError(error.to_string()))
}

/// Waits for either SIGINT (Ctrl+C) or SIGTERM (systemctl stop).
/// On non-Unix platforms only SIGINT is caught.
async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
    }

    #[cfg(not(unix))]
    ctrl_c.await;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    if dotenvy::dotenv().is_err() && dotenvy::from_path("../.env").is_err() {
        let _ = dotenvy::from_path("../../.env");
    }

    // Initialise structured logging. Must happen after dotenv so
    // RUST_LOG from .env is respected. Without this, all log::info!
    // and log::warn! calls are silently discarded.
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let config = config::AppConfig::load_from_env()?;

    // ---- Telegram Startup Alert ------------------------------
    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap_or_default();
        
    if let (Some(bot_token), Some(chat_id)) = (
        config.telegram_bot_token.clone(),
        config.telegram_chat_id.clone(),
    ) {
        let client_clone = http_client.clone();
        let mode_str = if config.startup_policy.capital_execution_allowed { "LIVE_TRADING" } else { "DRY_RUN_MODE" }.to_string();
        tokio::spawn(async move {
            crate::telegram::send_startup_alert(&client_clone, &bot_token, &chat_id, &mode_str).await;
        });
    }
    if config.startup_policy.position_recovery_allowed {
        return Err(error::BotError::ConfigError(
            "position recovery is not implemented for this execution path".to_string(),
        )
        .into());
    }
    let live_execution = config.startup_policy.capital_execution_allowed;
    if live_execution {
        // The legacy tracked telemetry database remains a live-only dependency.
        if let Err(error) = db::init_db() {
            log::warn!("[db] Failed to initialise SQLite: {error}");
        }
    }
    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        config.rpc_url.clone(),
        CommitmentConfig::processed(),
    ));

    // -----------------------------------------------------------------------
    // SwapEvent Fan-Out Architecture
    // -----------------------------------------------------------------------
    //
    // The websocket ingestion loop pushes each SwapEvent into a crossbeam
    // bounded channel (the "ingestion channel"). A splitter task reads
    // from this channel and clones each event to:
    //
    //   1. A second crossbeam channel ("math channel") → consumed by the
    //      VWAP math engine on a dedicated OS thread (signals::run_math_loop).
    //
    //   2. A tokio::sync::broadcast channel ("exit broadcast") → subscribed
    //      by each active exit watcher spawned after a confirmed buy.
    //
    // The bounded crossbeam hops keep ingestion and VWAP math nonblocking,
    // while exit watchers get independent streams via broadcast::Receiver.

    // Ingestion channel: websocket → splitter
    let (ws_tx, ws_rx) = crossbeam::channel::bounded::<types::SwapEvent>(SWAP_CHANNEL_CAPACITY);
    // Exit broadcast: splitter → exit watchers (each subscribes independently)
    let (exit_broadcast_tx, _exit_broadcast_rx) =
        tokio::sync::broadcast::channel::<types::SwapEvent>(EXIT_BROADCAST_CAPACITY);

    let (signal_tx, signal_rx) = tokio::sync::mpsc::channel::<types::WhaleSignal>(16);

    // -----------------------------------------------------------------------
    // Phase 1 — Security Preflight Coordinator
    // -----------------------------------------------------------------------
    //
    // The coordinator owns per-mint SecurityState and is solely responsible
    // for issuing security-scan RPC calls. It never runs on the signal hot
    // path (INV-01): near-miss events arrive over an unbounded mpsc channel
    // and are consumed by a dedicated task below, which fires-and-forgets
    // `request_scan` (itself a bounded, single-flight spawn per Section 1.5).
    
    
    

    // Phase 3 — Tip telemetry engine. The refresh loop runs in the
    // background until `tip_shutdown_tx` is dropped.
    let tip_config = tipping::TipConfig {
        refresh_interval: Duration::from_secs(optional_u64("TIP_REFRESH_INTERVAL_SECS", 30)?),
        max_telemetry_age: Duration::from_secs(optional_u64("TIP_MAX_TELEMETRY_AGE_SECS", 120)?),
        max_profit_share_bps: optional_u16("TIP_MAX_PROFIT_SHARE_BPS", 2500)?,
        minimum_net_profit_lamports: optional_u64("TIP_MINIMUM_NET_PROFIT_LAMPORTS", 0)?,
        ..Default::default()
    };
    let tip_engine = tipping::TipTelemetryEngine::new(tip_config);
    let (_tip_shutdown_tx, tip_shutdown_rx) = tokio::sync::watch::channel(true);
    tip_engine.clone().spawn_refresh_loop(tip_shutdown_rx);

    // Phase 4 — Multi-region bundle dispatcher region definitions.
    // Constructed inside each branch below so ownership is clean.
    // Regions specified as semicolon-separated label:url pairs in JITO_REGIONS.
    let region_defs: Vec<dispatcher::RegionDefinition> = match env::var("JITO_REGIONS") {
        Ok(val) => val
            .split(';')
            .filter_map(|pair| {
                let mut parts = pair.splitn(2, ':');
                let label = parts.next()?.to_string();
                let url = parts.next()?.to_string();
                if label.is_empty() || url.is_empty() {
                    log::warn!("Skipping malformed JITO_REGIONS entry: {pair}");
                    return None;
                }
                Some(dispatcher::RegionDefinition {
                    label,
                    block_engine_url: url,
                })
            })
            .collect(),
        Err(_) => vec![dispatcher::RegionDefinition {
            label: "default".to_string(),
            block_engine_url: execution::DEFAULT_JITO_BLOCK_ENGINE_URL.to_string(),
        }],
    };
    let dedupe_ttl =
        std::time::Duration::from_secs(optional_u64("OPPORTUNITY_DEDUPE_TTL_SECS", 300)?);

    // Splitter task: runs on a dedicated OS thread because it blocks on
    // crossbeam::Receiver::recv(). 
    let exit_broadcast_tx_clone = exit_broadcast_tx.clone();
    let splitter_thread = std::thread::spawn(move || {
        for event in ws_rx {
            // Forward to exit broadcast for Take Profit / Stop Loss tracking.
            let _ = exit_broadcast_tx_clone.send(event);
        }
        Ok::<(), String>(())
    });

    let bot_state = state::BotState::new();
    let mut workers = JoinSet::<WorkerResult>::new();
    let tasks_spawned: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));

    // Phase 1 — Webhook Server Ingestion
    tasks_spawned.fetch_add(1, Ordering::Relaxed);
    let webhook_state = webhook::WebhookState {
        signal_tx,
        api_key: std::env::var("WEBHOOK_API_KEY").unwrap_or_else(|_| "supersecret".to_string()),
        watchlist: config.watchlist.clone(),
    };
    workers.spawn(async move {
        webhook::run_server(webhook_state, 5000).await
    });

    tasks_spawned.fetch_add(1, Ordering::Relaxed);
    let config_ws = config.clone();
    workers.spawn(async move {
        match websocket::run_listener(config_ws, ws_tx).await {
            Ok(()) => Err("Raydium ingestion loop exited unexpectedly".to_string()),
            Err(error) => Err(format!("Raydium ingestion loop failed: {error}")),
        }
    });

    if live_execution {
        let payer = load_wallet_private_key()?;
        println!("Loaded live trading wallet: {}", payer.pubkey());
        let jito_config = load_live_executor_config()?;
        let dispatcher = dispatcher::BundleDispatcher::connect_all(
            &region_defs,
            std::time::Duration::from_secs(10),
            dedupe_ttl,
        )
        .await;



        // ---- Phase 5: BundleTracker initialization -------------------------
        let bundle_tracker = Arc::new(bundle_tracker::BundleTracker::new());
        // Register region clients with the tracker so the SubscribeBundleResults
        // subscription can connect to the correct Jito Block Engine regions.
        for region_arc in dispatcher.region_arcs() {
            let region = region_arc.lock().await;
            if let Some(client) = region.grpc_client().cloned() {
                bundle_tracker
                    .register_region_client(&region.label, client)
                    .await;
            }
        }
        // Spawn the background inclusion polling loop.
        bundle_tracker.clone().spawn_polling_loop();
        log::info!("[Phase 5] BundleTracker polling loop spawned");

        let payer_arc = Arc::new(payer);
        let bot_state_clone = bot_state.clone();
        let rpc_clone = rpc_client.clone();
        let exit_tx = exit_broadcast_tx.clone();
        
        let tip_clone = tip_engine.clone();
        
        let tg_bot_token = config.telegram_bot_token.clone();
        let tg_chat_id = config.telegram_chat_id.clone();
        
        tasks_spawned.fetch_add(1, Ordering::Relaxed);
        workers.spawn(async move {
            match execution::run_whale_execution_consumer(
                signal_rx,
                rpc_clone,
                payer_arc,
                jito_config,
                bot_state_clone,
                exit_tx,
                
                tip_clone,
                dispatcher,
                false, // dry_run
                Some(bundle_tracker.clone()),
                http_client,
                tg_bot_token,
                tg_chat_id,
            )
            .await
            {
                Ok(()) => Err("Jito execution consumer exited unexpectedly".to_string()),
                Err(error) => Err(format!("Jito execution consumer failed: {error}")),
            }
        });
    }


    // Monitor the splitter thread alongside the math thread.
    tasks_spawned.fetch_add(1, Ordering::Relaxed);
    workers.spawn_blocking(move || match splitter_thread.join() {
        Ok(Ok(())) => Err("SwapEvent splitter exited unexpectedly".to_string()),
        Ok(Err(error)) => Err(format!("SwapEvent splitter failed: {error}")),
        Err(_) => Err("SwapEvent splitter panicked".to_string()),
    });


    // -----------------------------------------------------------------------
    // Shadow-Mode Health Heartbeat
    // -----------------------------------------------------------------------
    //
    // Every 60 seconds, emit structured telemetry for the soak test. This
    // task runs as a plain tokio::spawn (not inside `workers`) so it is not
    // counted in tasks_spawned and is not subject to JoinSet lifecycle
    // management — it lives until the runtime drops it on shutdown.
    //
    // Log level: info. In shadow mode these messages are the primary
    // operator visibility into soak health. In live mode (never before
    // Gate D), they serve as a low-noise pulse check.
    let health_start = std::time::Instant::now();
    
    let health_state = bot_state.clone();
    let health_tasks = tasks_spawned.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;

            let uptime_secs = health_start.elapsed().as_secs();
            
            
            
            
            
            
            
            let cb = health_state.is_circuit_breaker_active();
            let open_pos = health_state.open_position_count();
            let notifications_received = websocket::NOTIFICATIONS_RECEIVED.load(Ordering::Relaxed);
            let swaps_decoded = websocket::SWAPS_DECODED.load(Ordering::Relaxed);
            let decode_failures = websocket::DECODE_FAILURES.load(Ordering::Relaxed);
            let filter_drops = websocket::FILTER_DROPS.load(Ordering::Relaxed);
            let ws_queue_full_drops = websocket::WS_QUEUE_FULL_DROPS.load(Ordering::Relaxed);
            let epoch_resets_observed = 0; // Removed signals module
            let execution_signals_emitted = 0; // Removed signals module
            if live_execution {
                let db_ok = crate::db::check_db_healthy();
                log::info!(
                    "shadow_health_heartbeat \
                     uptime={uptime_secs}s \
                     tasks_spawned={} \
                     circuit_breaker={cb} \
                     open_positions={open_pos} \
                     notifications_received={notifications_received} \
                     swaps_decoded={swaps_decoded} \
                     decode_failures={decode_failures} \
                     filter_drops={filter_drops} \
                     ws_queue_full_drops={ws_queue_full_drops} \
                     epoch_resets_observed={epoch_resets_observed} \
                     execution_signals_emitted={execution_signals_emitted} \
                     db_healthy={db_ok}",
                    health_tasks.load(Ordering::Relaxed),
                );
            }
        }
    });

    let fatal_error = tokio::select! {
        _ = wait_for_shutdown_signal() => None,
        outcome = workers.join_next() => {
            Some(match outcome {
                Some(Ok(Ok(()))) => "worker exited unexpectedly".to_string(),
                Some(Ok(Err(error))) => error,
                Some(Err(error)) => format!("worker task failed: {error}"),
                None => "all worker tasks exited unexpectedly".to_string(),
            })
        }
    };

    // Aborting ingestion drops the final crossbeam sender. That closes the
    // splitter, which drops math_tx and exit_broadcast_tx. The math loop and
    // exit watchers then terminate. The blocking monitors join the OS threads.
    workers.abort_all();
    while workers.join_next().await.is_some() {}

    if let Some(error) = fatal_error {
        return Err(error::BotError::ConfigError(error).into());
    }

    Ok(())
}
