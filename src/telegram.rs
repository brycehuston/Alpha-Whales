use crate::db;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde_json::{json, Value};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct TokenMetadata {
    pub name: String,
    pub symbol: String,
    pub mc_formatted: String,
    pub age_formatted: String,
    /// Raw `pairCreatedAt` timestamp from DexScreener in milliseconds.
    /// Used by the token age filter in websocket.rs.
    /// 0 means DexScreener did not return a timestamp (token too new or not indexed).
    pub created_at_ms: u64,
    /// Raw market cap in USD. 0.0 if not found.
    pub raw_mc: f64,
}

fn empty_token_metadata() -> TokenMetadata {
    TokenMetadata {
        name: "Unknown".to_string(),
        symbol: "???".to_string(),
        mc_formatted: "N/A".to_string(),
        age_formatted: "0m".to_string(),
        created_at_ms: 0,
        raw_mc: 0.0,
    }
}

pub async fn fetch_token_metadata_strict(
    http_client: &Client,
    mint_address: &str,
) -> Result<TokenMetadata, String> {
    let url = format!(
        "https://api.dexscreener.com/latest/dex/tokens/{}",
        mint_address
    );
    let response = http_client
        .get(&url)
        .timeout(std::time::Duration::from_millis(1500))
        .send()
        .await
        .map_err(|error| format!("DexScreener request failed: {error}"))?;

    if !response.status().is_success() {
        return Err(format!("DexScreener returned HTTP {}", response.status()));
    }

    let data = response
        .json::<Value>()
        .await
        .map_err(|error| format!("DexScreener response was invalid JSON: {error}"))?;
    let pairs = data
        .get("pairs")
        .and_then(|value| value.as_array())
        .ok_or_else(|| "DexScreener response did not contain a pairs array".to_string())?;
    let pair = pairs
        .first()
        .ok_or_else(|| "DexScreener returned no token pair".to_string())?;
    let created_at_ms = pair
        .get("pairCreatedAt")
        .and_then(|value| value.as_u64())
        .filter(|created_at| *created_at > 0)
        .ok_or_else(|| "DexScreener token pair is missing pairCreatedAt".to_string())?;
    let raw_mc = pair
        .get("marketCap")
        .and_then(|value| value.as_f64())
        .filter(|market_cap| market_cap.is_finite() && *market_cap > 0.0)
        .ok_or_else(|| "DexScreener token pair is missing a positive marketCap".to_string())?;

    let empty_obj = json!({});
    let base_token = pair.get("baseToken").unwrap_or(&empty_obj);
    let name = base_token
        .get("name")
        .and_then(|value| value.as_str())
        .unwrap_or("Unknown")
        .chars()
        .take(15)
        .collect();
    let symbol = base_token
        .get("symbol")
        .and_then(|value| value.as_str())
        .unwrap_or("???")
        .to_string();

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let minutes_old = now_ms.saturating_sub(created_at_ms) / 60_000;
    let hours_old = minutes_old / 60;
    let age_formatted = if hours_old > 24 {
        format!("{}d {}h", hours_old / 24, hours_old % 24)
    } else if hours_old > 0 {
        format!("{}h {}m", hours_old, minutes_old % 60)
    } else {
        format!("{}m", minutes_old)
    };
    let mc_formatted = if raw_mc > 1_000_000.0 {
        format!("${:.2}M", raw_mc / 1_000_000.0)
    } else {
        format!("${:.1}K", raw_mc / 1_000.0)
    };

    Ok(TokenMetadata {
        name,
        symbol,
        mc_formatted,
        age_formatted,
        created_at_ms,
        raw_mc,
    })
}

pub async fn fetch_token_metadata(http_client: &Client, mint_address: &str) -> TokenMetadata {
    fetch_token_metadata_strict(http_client, mint_address)
        .await
        .unwrap_or_else(|_| empty_token_metadata())
}

pub async fn send_telegram_alert(
    http_client: &Client,
    bot_token: &str,
    chat_id: &str,
    mint: &str,
    net_change: f64,
    wallet: &str,
    signature: &str,
    execution_status: &str,
    trade_size_formatted: &str,
) {
    let meta = fetch_token_metadata(http_client, mint).await;
    let history = db::get_whale_history(wallet, mint);

    let direction_header = if net_change > 0.0 {
        "<b>🟢 BUY DETECTED 🚀</b>"
    } else {
        "<b>🔴 SELL DETECTED 📉</b>"
    };

    let net_str = if net_change > 0.0 {
        format!("+{:.4}", net_change)
    } else {
        format!("{:.4}", net_change)
    };

    let trade_size_line = if !trade_size_formatted.is_empty() {
        format!("{}\n", trade_size_formatted)
    } else {
        "".to_string()
    };

    let message = format!(
        "{}\n\
        ━━━━━━━━━━━━━━━━━━━━━━━━\n\
        <b>🔑 Wallet:</b>\n\
        <code>{}</code>\n\n\
        <b>💎 Token:</b> {} ({})\n\
        <code>{}</code>\n\n\
        <b>📊 Net Position Change</b>: <code>{}</code> tokens\n\
        📊 {}<b>Market Cap</b>: {}\n\
        <b>Token Age</b>: {}\n\
        🕵️ <b>WHALE HISTORY:</b>\n\
        🛒 Buys: {} | 📤 Sells: {}\n\
        💰 Net Flow: {} SOL\n\
        ⚖️ Status: {}\n\n\
        <b>Execution</b>: {}\n\n\
        <a href=\"https://dexscreener.com/solana/{}\">DexScreener</a> | \
        <a href=\"https://photon-sol.tinyastro.io/en/lp/{}\">Photon</a> | \
        <a href=\"https://solscan.io/tx/{}\">Solscan</a>\n\
        ━━━━━━━━━━━━━━━━━━━━━━━━\n\
        ʜᴛꜰ © ᴀʟᴘʜᴀ ᴀʟᴇʀᴛꜱ | v1.01",
        direction_header,
        wallet,
        meta.name,
        meta.symbol,
        mint,
        net_str,
        trade_size_line,
        meta.mc_formatted,
        meta.age_formatted,
        history.buys,
        history.sells,
        history.net_sol,
        history.status,
        execution_status,
        mint,
        mint,
        signature
    );

    let url = format!("https://api.telegram.org/bot{}/sendMessage", bot_token);
    let payload = json!({
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": true,
    });

    let _ = http_client
        .post(&url)
        .json(&payload)
        .timeout(std::time::Duration::from_secs(8))
        .send()
        .await;
}

pub async fn send_startup_alert(
    http_client: &Client,
    bot_token: &str,
    chat_id: &str,
    pubkey: &str,
) {
    let now: DateTime<Utc> = Utc::now();
    let time_str = now.format("%Y-%m-%d %H:%M:%S UTC").to_string();

    let message = format!(
        "<b>🟢 ALPHA NEXUS ONLINE 🟢</b>\n\
        ━━━━━━━━━━━━━━━━━━━━━━━━\n\
        <b>Bot State:</b> Active & Listening\n\
        <b>Wallet:</b> <code>{}</code>\n\
        <b>Time:</b> {}\n\
        ━━━━━━━━━━━━━━━━━━━━━━━━\n\
        ʜᴛꜰ © ᴀʟᴘʜᴀ ᴀʟᴇʀᴛꜱ | v1.01",
        pubkey, time_str
    );

    let url = format!("https://api.telegram.org/bot{}/sendMessage", bot_token);
    let payload = json!({
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": true,
    });

    let _ = http_client
        .post(&url)
        .json(&payload)
        .timeout(std::time::Duration::from_secs(8))
        .send()
        .await;
}

/// Sends a Telegram alert when the BOT closes one of its own positions.
///
/// Distinct from `send_telegram_alert` which fires on incoming whale signals.
/// Called from exits.rs after the on-chain sell is confirmed.
pub async fn send_bot_sell_alert(
    http_client: &Client,
    bot_token: &str,
    chat_id: &str,
    mint: &str,
    pnl_usd: f64,
    pnl_pct: f64,
    exit_reason: &str,
    exit_value_usd: f64,
) {
    let meta = fetch_token_metadata(http_client, mint).await;

    let (icon, outcome) = if pnl_usd >= 0.0 {
        ("✅", "PROFIT")
    } else {
        ("🛑", "STOP LOSS")
    };
    let pnl_sign = if pnl_usd >= 0.0 { "+" } else { "" };

    let message = format!(
        "<b>{icon} BOT EXIT — {outcome}</b>\n\
        ━━━━━━━━━━━━━━━━━━━━━━━━\n\
        <b>💎 Token:</b> {name} ({symbol})\n\
        <code>{mint}</code>\n\n\
        <b>💰 P&amp;L:</b> <code>{pnl_sign}${pnl_usd:.4} ({pnl_sign}{pnl_pct:.1}%)</code>\n\
        <b>📤 Exit Value:</b> ${exit_value_usd:.4}\n\
        <b>⚡ Trigger:</b> {exit_reason}\n\
        <b>📊 Market Cap:</b> {mc}\n\n\
        <a href=\"https://dexscreener.com/solana/{mint}\">DexScreener</a> | \
        <a href=\"https://photon-sol.tinyastro.io/en/lp/{mint}\">Photon</a>\n\
        ━━━━━━━━━━━━━━━━━━━━━━━━\n\
        ʜᴛꜰ © ᴀʟᴘʜᴀ ᴀʟᴇʀᴛꜱ | v1.01",
        icon = icon,
        outcome = outcome,
        name = meta.name,
        symbol = meta.symbol,
        mint = mint,
        pnl_sign = pnl_sign,
        pnl_usd = pnl_usd,
        pnl_pct = pnl_pct.abs(),
        exit_value_usd = exit_value_usd,
        exit_reason = exit_reason,
        mc = meta.mc_formatted,
    );

    let url = format!("https://api.telegram.org/bot{}/sendMessage", bot_token);
    let payload = json!({
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": true,
    });

    let _ = http_client
        .post(&url)
        .json(&payload)
        .timeout(std::time::Duration::from_secs(8))
        .send()
        .await;
}
