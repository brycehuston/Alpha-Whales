use crate::types::WhaleSignal;
use axum::{
    extract::{State, Json},
    http::{HeaderMap, StatusCode},
    routing::post,
    Router,
};
use serde::Deserialize;
use std::net::SocketAddr;
use tokio::sync::mpsc::Sender;
use std::sync::Arc;
use dashmap::DashMap;

#[derive(Clone)]
pub struct WebhookState {
    pub signal_tx: Sender<WhaleSignal>,
    pub api_key: String,
    pub watchlist: Arc<DashMap<String, crate::config::WhaleProfile>>,
}

#[derive(Deserialize, Debug)]
pub struct HeliusWebhookPayload {
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(rename = "feePayer")]
    pub fee_payer: String,
    pub timestamp: u64,
    pub events: Option<HeliusEvents>,
}

#[derive(Deserialize, Debug)]
pub struct HeliusEvents {
    pub swap: Option<HeliusSwap>,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub struct HeliusSwap {
    #[serde(rename = "nativeInput")]
    pub native_input: Option<HeliusNativeInput>,
    #[serde(rename = "tokenOutputs")]
    pub token_outputs: Option<Vec<HeliusTokenOutput>>,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub struct HeliusNativeInput {
    pub amount: String,
}

#[derive(Deserialize, Debug)]
pub struct HeliusTokenOutput {
    pub mint: String,
}

pub async fn run_server(state: WebhookState, port: u16) -> Result<(), String> {
    let app = Router::new()
        .route("/webhook", post(handle_webhook))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    log::info!("Webhook server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| format!("Failed to bind webhook port: {}", e))?;

    axum::serve(listener, app)
        .await
        .map_err(|e| format!("Webhook server failed: {}", e))?;

    Ok(())
}

async fn handle_webhook(
    State(state): State<WebhookState>,
    headers: HeaderMap,
    Json(payload): Json<Vec<HeliusWebhookPayload>>,
) -> Result<StatusCode, (StatusCode, String)> {
    
    // 1. Authenticate Request
    let auth_header = headers.get("authorization")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
        
    if auth_header != state.api_key {
        log::warn!("Unauthorized webhook attempt");
        return Err((StatusCode::UNAUTHORIZED, "Unauthorized".to_string()));
    }

    log::info!("📡 Helius Webhook Received! Processing {} transaction(s)...", payload.len());

    for trade in payload {
        if trade.event_type != "SWAP" { 
            log::info!("   [Skip] Ignored transaction from {}: Event is {} (Not a SWAP)", trade.fee_payer, trade.event_type);
            continue; 
        }
        
        let Some(events) = trade.events else { 
            log::info!("   [Skip] Ignored SWAP from {}: Missing 'events' data", trade.fee_payer);
            continue; 
        };
        let Some(swap) = events.swap else { 
            log::info!("   [Skip] Ignored SWAP from {}: Missing 'swap' payload", trade.fee_payer);
            continue; 
        };
        let Some(token_outputs) = swap.token_outputs else { 
            log::info!("   [Skip] Ignored SWAP from {}: Missing 'tokenOutputs' (Probably a sell or no tokens received)", trade.fee_payer);
            continue; 
        };
        
        if let Some(output) = token_outputs.first() {
            let token_mint = &output.mint;
            let whale_wallet = &trade.fee_payer;

            log::info!("Received Whale Signal from {} for mint: {}", whale_wallet, token_mint);
            
            // Look up the whale in our dynamic sizing watchlist
            let mut trade_size_lamports = 10_000_000.0; // Default 0.01 SOL fallback
            
            if let Some(profile) = state.watchlist.get(whale_wallet) {
                trade_size_lamports = match profile.lane {
                    crate::config::WhaleLane::Conservative => 30_000_000.0, // 0.03 SOL
                    crate::config::WhaleLane::Swing => 20_000_000.0,       // 0.02 SOL
                    crate::config::WhaleLane::Degen => 10_000_000.0,       // 0.01 SOL
                    crate::config::WhaleLane::Sniper => 50_000_000.0,      // 0.05 SOL (Highest confidence)
                    _ => 10_000_000.0, // 0.01 SOL
                };
                log::info!("🐋 Whale Lane: {:?} | Dynamically sized trade to {} lamports", profile.lane, trade_size_lamports);
            } else {
                log::warn!("Wallet {} not found in watchlist. Using fallback size.", whale_wallet);
            }

            let timestamp_ms = trade.timestamp * 1000;

            let signal = WhaleSignal {
                target_mint: token_mint.clone(),
                whale_wallet: whale_wallet.clone(),
                trade_size_sol: trade_size_lamports / 1_000_000_000.0,
                timestamp_ms,
            };

            if let Err(e) = state.signal_tx.try_send(signal) {
                log::error!("Failed to send WhaleSignal to execution queue (Queue Full): {}", e);
            }
        }
    }
    
    Ok(StatusCode::OK)
}
