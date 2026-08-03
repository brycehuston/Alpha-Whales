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
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct WebhookState {
    pub signal_tx: Sender<WhaleSignal>,
    pub api_key: String,
    pub watchlist: Arc<RwLock<std::collections::HashMap<String, crate::config::WhaleProfile>>>,
}

#[derive(Deserialize, Debug)]
pub struct HeliusWebhookPayload {
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(rename = "feePayer")]
    pub fee_payer: String,
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
            
            let watchlist_read = state.watchlist.read().await;
            if let Some(profile) = watchlist_read.get(whale_wallet) {
                trade_size_lamports = match profile.lane {
                    crate::config::WhaleLane::Conservative => 100_000_000.0, // 0.1 SOL
                    crate::config::WhaleLane::Swing => 50_000_000.0, // 0.05 SOL
                    crate::config::WhaleLane::Degen => 20_000_000.0, // 0.02 SOL
                    _ => 10_000_000.0, // 0.01 SOL
                };
                log::info!("🐋 Whale Lane: {:?} | Dynamically sized trade to {} lamports", profile.lane, trade_size_lamports);
            } else {
                log::warn!("Wallet {} not found in watchlist. Using fallback size.", whale_wallet);
            }
            drop(watchlist_read);

            let timestamp_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

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
