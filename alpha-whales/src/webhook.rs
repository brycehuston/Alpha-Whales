use alpha_agents_core::types::WhaleSignal;
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
use subtle::ConstantTimeEq;

#[derive(Clone)]
pub struct WebhookState {
    pub signal_tx: Sender<WhaleSignal>,
    pub api_key: String,
    pub watchlist: Arc<DashMap<String, alpha_agents_core::config::WhaleProfile>>,
}

#[derive(Deserialize, Debug)]
pub struct HeliusWebhookPayload {
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(rename = "feePayer")]
    pub fee_payer: String,
    pub timestamp: u64,
    pub events: Option<HeliusEvents>,
    #[serde(rename = "tokenTransfers")]
    pub token_transfers: Option<Vec<HeliusTokenTransfer>>,
}

#[derive(Deserialize, Debug)]
pub struct HeliusTokenTransfer {
    #[serde(rename = "toUserAccount")]
    pub to_user_account: String,
    pub mint: String,
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
    
    // AUDIT FIX: CRITICAL #1 — Webhook Auth TLS & Timing
    let auth_header = headers.get("authorization")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
        
    if auth_header.as_bytes().ct_eq(state.api_key.as_bytes()).unwrap_u8() == 0 {
        log::warn!("Unauthorized webhook attempt");
        return Err((StatusCode::UNAUTHORIZED, "Unauthorized".to_string()));
    }

    log::info!("📡 Helius Webhook Received! Processing {} transaction(s)...", payload.len());

    for trade in payload {
        if trade.event_type != "SWAP" { 
            log::info!("   [Skip] Ignored transaction from {}: Event is {} (Not a SWAP)", trade.fee_payer, trade.event_type);
            continue; 
        }
        
        // Try to get token mint from events.swap first (Jupiter/Raydium)
        let mut target_mint = None;
        
        if let Some(events) = &trade.events {
            if let Some(swap) = &events.swap {
                if let Some(token_outputs) = &swap.token_outputs {
                    if let Some(output) = token_outputs.first() {
                        target_mint = Some(output.mint.clone());
                    }
                }
            }
        }
        
        // Fallback to tokenTransfers (Pump.fun)
        if target_mint.is_none() {
            if let Some(transfers) = &trade.token_transfers {
                for transfer in transfers {
                    // Find the transfer where the whale received the token (and ignore wrapped SOL)
                    if transfer.to_user_account == trade.fee_payer && transfer.mint != "So11111111111111111111111111111111111111112" {
                        target_mint = Some(transfer.mint.clone());
                        break;
                    }
                }
            }
        }
        
        let Some(token_mint) = target_mint else {
            log::info!("   [Skip] Ignored SWAP from {}: Could not determine purchased token mint", trade.fee_payer);
            continue;
        };
        
        let whale_wallet = &trade.fee_payer;

            log::info!("Received Whale Signal from {} for mint: {}", whale_wallet, token_mint);
            
            // Look up the whale in our dynamic sizing watchlist
            let mut trade_size_lamports = 10_000_000.0; // Default 0.01 SOL fallback
            let mut signal_lane = alpha_agents_core::config::WhaleLane::Unknown;
            
            if let Some(profile) = state.watchlist.get(whale_wallet) {
                signal_lane = profile.lane;
                trade_size_lamports = match profile.lane {
                    alpha_agents_core::config::WhaleLane::Conservative => 30_000_000.0, // 0.03 SOL
                    alpha_agents_core::config::WhaleLane::Swing => 20_000_000.0,       // 0.02 SOL
                    alpha_agents_core::config::WhaleLane::Degen => 10_000_000.0,       // 0.01 SOL
                    alpha_agents_core::config::WhaleLane::Sniper => 50_000_000.0,      // 0.05 SOL (Highest confidence)
                    _ => 10_000_000.0, // 0.01 SOL
                };
                log::info!("🐋 Whale Lane: {:?} | Dynamically sized trade to {} lamports", profile.lane, trade_size_lamports);
            } else {
                log::warn!("Wallet {} not found in watchlist. Using fallback size.", whale_wallet);
            }

            let timestamp_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;

            let signal = WhaleSignal {
                target_mint: token_mint.clone(),
                whale_wallet: whale_wallet.clone(),
                trade_size_sol: trade_size_lamports / 1_000_000_000.0,
                timestamp_ms,
                lane: signal_lane,
            };

        if let Err(e) = state.signal_tx.try_send(signal) {
            log::error!("Failed to send WhaleSignal to execution queue (Queue Full): {}", e);
        }
    }
    
    Ok(StatusCode::OK)
}
