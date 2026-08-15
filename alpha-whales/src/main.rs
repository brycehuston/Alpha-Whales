#![allow(
    clippy::result_large_err,
    clippy::too_many_arguments,
    clippy::enum_variant_names
)]

use alpha_agents_core::geyser_stream;
use log::{error, info};
use std::env;
use std::error::Error;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    if dotenvy::dotenv().is_err() && dotenvy::from_path("../.env").is_err() {
        let _ = dotenvy::from_path("../../.env");
    }

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    info!("Starting Alpha-Whales Execution Binary...");

    // Channel Setup: Initialize a tokio::sync::mpsc::channel
    let (mint_tx, mut mint_rx) = mpsc::channel::<String>(10_000);

    let mut workers = JoinSet::new();

    // Spawn WSS: tokio::spawn the ingestion loop
    let helius_wss_url = env::var("HELIUS_WSS_URL").unwrap_or_else(|_| {
        error!("HELIUS_WSS_URL must be set in .env");
        std::process::exit(1);
    });
    
    // Optional auth token for gRPC/WSS
    let helius_x_token = env::var("HELIUS_X_TOKEN").ok();

    workers.spawn(async move {
        info!("Spawning Helius Geyser ingestion stream...");
        if let Err(e) = geyser_stream::run_geyser_stream(helius_wss_url, helius_x_token, mint_tx).await {
            error!("Ingestion stream exited with error: {}", e);
        }
    });

    // The Receiver Loop
    workers.spawn(async move {
        info!("Execution Receiver Loop initialized. Awaiting Slot-0 token mints...");
        
        while let Some(mint) = mint_rx.recv().await {
            info!("🔥 Sniped Mint Candidate: {}", mint);

            // TODO: Python-ported validation checks (Liquidity/Dev Allocation)
            // - Extract initial liquidity pool size.
            // - Verify dev allocation percentages against strict thresholds.
            // - Check for bundled honeypot/rug indicators.

            // TODO: Jito Bundle Assembly & Transmission
            // - Formulate the optimal swap routing instruction.
            // - Calculate and append dynamic Jito tip based on network congestion.
            // - Dispatch signed transaction payload to the Jito Block Engine.

            // TODO: Initializing the PositionManager (16% TSL / 50% TP)
            // - Hand off confirmed position state to the async watcher.
            // - Enforce strict 16% trailing stop loss.
            // - Trigger 50% take profit execution upon target hit.
        }
    });

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received SIGINT, shutting down...");
        }
        Some(res) = workers.join_next() => {
            if let Err(e) = res {
                error!("A critical worker thread panicked: {}", e);
            } else {
                error!("A critical worker thread exited unexpectedly.");
            }
        }
    }

    workers.abort_all();
    Ok(())
}
