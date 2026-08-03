use crate::{
    error::BotError,
    pool_cache::{RAYDIUM_LIQUIDITY_POOL_V4_PROGRAM_ID, WSOL_MINT},
    types::SwapEvent,
};
use crossbeam::channel::{Sender, TrySendError};
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use serde::Deserialize;
use serde_json::Value;
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use std::{
    cmp::Ordering as Comparison,
    str::{self, FromStr},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{net::TcpStream, time::MissedTickBehavior};
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};

const SUBSCRIPTION_ACK_TIMEOUT: Duration = Duration::from_secs(10);
const WS_RECEIVE_TIMEOUT: Duration = Duration::from_secs(86400); // 24 hours
const WS_PING_INTERVAL: Duration = Duration::from_secs(10);
const RAYDIUM_SWAP_BASE_IN: u8 = 9;
const RAYDIUM_SWAP_BASE_OUT: u8 = 11;
const CURRENT_SWAP_ACCOUNT_COUNT: usize = 17;
const LEGACY_SWAP_ACCOUNT_COUNT: usize = 18;

pub(crate) static STREAM_EPOCH: AtomicU64 = AtomicU64::new(0);
pub(crate) static NOTIFICATIONS_RECEIVED: AtomicU64 = AtomicU64::new(0);
pub(crate) static SWAPS_DECODED: AtomicU64 = AtomicU64::new(0);
pub(crate) static DECODE_FAILURES: AtomicU64 = AtomicU64::new(0);
pub(crate) static FILTER_DROPS: AtomicU64 = AtomicU64::new(0);
pub(crate) static WS_QUEUE_FULL_DROPS: AtomicU64 = AtomicU64::new(0);


type HeliusSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DecodedSwap {
    signature: Signature,
    pool_id: Pubkey,
    target_mint: Pubkey,
    base_amount: u64,
    quote_amount: u64,
    observed_at_ms: u64,
    source_slot: u64,
    outer_instruction_index: u8,
    inner_instruction_index: Option<u8>,
}

impl DecodedSwap {
    fn into_swap_event(self, stream_epoch: u64) -> SwapEvent {
        // This conversion intentionally performs exactly two allocations:
        // one String for each existing textual identifier.
        SwapEvent {
            target_mint: self.target_mint.to_string(),
            pool_id: self.pool_id.to_string(),
            base_amount: self.base_amount,
            quote_amount: self.quote_amount,
            timestamp_ms: self.observed_at_ms,
            source_signature: self.signature,
            source_slot: self.source_slot,
            outer_instruction_index: self.outer_instruction_index,
            inner_instruction_index: self.inner_instruction_index,
            stream_epoch,
        }
    }
}

#[derive(Debug, Deserialize)]
struct TransactionNotification<'a> {
    #[serde(borrow)]
    method: &'a str,
    #[serde(borrow)]
    params: NotificationParams<'a>,
}

#[derive(Debug, Deserialize)]
struct NotificationParams<'a> {
    subscription: u64,
    #[serde(borrow)]
    result: NotificationResult<'a>,
}

#[derive(Debug, Deserialize)]
struct NotificationResult<'a> {
    slot: u64,
    #[serde(default, borrow)]
    signature: Option<&'a str>,
    #[serde(borrow)]
    transaction: TransactionWithMeta<'a>,
}

#[derive(Debug, Deserialize)]
struct TransactionWithMeta<'a> {
    #[serde(borrow)]
    transaction: RawTransaction<'a>,
    #[serde(borrow)]
    meta: RawMeta<'a>,
}

#[derive(Debug, Deserialize)]
struct RawTransaction<'a> {
    #[serde(borrow)]
    signatures: Vec<&'a str>,
    #[serde(borrow)]
    message: RawMessage<'a>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawMessage<'a> {
    #[serde(borrow)]
    account_keys: Vec<RawAccountKey<'a>>,
    #[serde(default, borrow)]
    instructions: Vec<RawInstruction<'a>>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawAccountKey<'a> {
    Parsed {
        #[serde(borrow)]
        pubkey: &'a str,
    },
    Address(#[serde(borrow)] &'a str),
}

impl RawAccountKey<'_> {
    fn as_str(&self) -> &str {
        match self {
            Self::Parsed { pubkey } | Self::Address(pubkey) => pubkey,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawInstruction<'a> {
    #[serde(default, borrow)]
    program_id: Option<&'a str>,
    #[serde(default)]
    program_id_index: Option<u64>,
    #[serde(default, borrow)]
    accounts: Vec<RawAccountReference<'a>>,
    #[serde(default, borrow)]
    data: Option<&'a str>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawAccountReference<'a> {
    Address(#[serde(borrow)] &'a str),
    Index(u64),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawMeta<'a> {
    #[serde(default)]
    err: Option<Value>,
    #[serde(default, borrow)]
    inner_instructions: Option<Vec<RawInnerInstructions<'a>>>,
    #[serde(default, borrow)]
    pre_token_balances: Option<Vec<RawTokenBalance<'a>>>,
    #[serde(default, borrow)]
    post_token_balances: Option<Vec<RawTokenBalance<'a>>>,
}

#[derive(Debug, Deserialize)]
struct RawInnerInstructions<'a> {
    index: u64,
    #[serde(default, borrow)]
    instructions: Vec<RawInstruction<'a>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawTokenBalance<'a> {
    account_index: u64,
    #[serde(borrow)]
    mint: &'a str,
    #[serde(borrow)]
    ui_token_amount: RawTokenAmount<'a>,
}

#[derive(Debug, Deserialize)]
struct RawTokenAmount<'a> {
    #[serde(borrow)]
    amount: &'a str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SwapInstruction {
    pool_id: Pubkey,
    base_vault: Pubkey,
    quote_vault: Pubkey,
    outer_instruction_index: u8,
    inner_instruction_index: Option<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BalanceDirection {
    Increase,
    Decrease,
}

#[derive(Debug)]
enum NotificationError {
    Protocol(String),
    ChannelClosed,
}

fn validate_subscription_ack(payload: &str) -> Result<u64, String> {
    let response: Value =
        serde_json::from_str(payload).map_err(|error| format!("invalid JSON-RPC ACK: {error}"))?;

    if response.get("error").is_some_and(|error| !error.is_null()) {
        return Err(format!(
            "transactionSubscribe rejected by provider: {}",
            response["error"]
        ));
    }

    let id = response
        .get("id")
        .and_then(Value::as_u64)
        .ok_or_else(|| "transactionSubscribe ACK id must be numeric".to_string())?;
    if id != 1 {
        return Err(format!(
            "transactionSubscribe ACK id mismatch: expected 1, got {id}"
        ));
    }

    response
        .get("result")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            "transactionSubscribe ACK result must be a numeric subscription id".to_string()
        })
}

async fn subscribe_and_wait_for_ack(
    write: &mut SplitSink<HeliusSocket, Message>,
    read: &mut SplitStream<HeliusSocket>,
    subscription_payload: &str,
) -> Result<u64, String> {
    tokio::time::timeout(SUBSCRIPTION_ACK_TIMEOUT, async {
        write
            .send(Message::Text(subscription_payload.to_string()))
            .await
            .map_err(|error| format!("failed to send transactionSubscribe: {error}"))?;

        loop {
            let message = read
                .next()
                .await
                .ok_or_else(|| "WebSocket closed before transactionSubscribe ACK".to_string())?
                .map_err(|error| {
                    format!("WebSocket read failed before transactionSubscribe ACK: {error}")
                })?;

            match message {
                Message::Text(payload) => return validate_subscription_ack(payload.as_str()),
                Message::Binary(payload) => {
                    let payload = str::from_utf8(payload.as_slice()).map_err(|error| {
                        format!("transactionSubscribe ACK is not UTF-8: {error}")
                    })?;
                    return validate_subscription_ack(payload);
                }
                Message::Ping(payload) => {
                    write.send(Message::Pong(payload)).await.map_err(|error| {
                        format!("failed to answer Ping while awaiting subscription ACK: {error}")
                    })?;
                }
                Message::Pong(_) | Message::Frame(_) => {}
                Message::Close(frame) => {
                    return Err(format!(
                        "WebSocket closed before transactionSubscribe ACK: {frame:?}"
                    ));
                }
            }
        }
    })
    .await
    .map_err(|_| {
        format!(
            "transactionSubscribe ACK timed out after {}s",
            SUBSCRIPTION_ACK_TIMEOUT.as_secs()
        )
    })?
}

fn resolve_message_key(
    message: &RawMessage<'_>,
    reference: &RawAccountReference<'_>,
) -> Result<Pubkey, String> {
    let text = match reference {
        RawAccountReference::Address(address) => *address,
        RawAccountReference::Index(index) => {
            let index = usize::try_from(*index)
                .map_err(|error| format!("instruction account index is out of range: {error}"))?;
            message
                .account_keys
                .get(index)
                .ok_or_else(|| format!("instruction account index {index} is missing"))?
                .as_str()
        }
    };
    Pubkey::from_str(text).map_err(|error| format!("invalid instruction account pubkey: {error}"))
}

fn resolve_program_id(
    message: &RawMessage<'_>,
    instruction: &RawInstruction<'_>,
) -> Result<Option<Pubkey>, String> {
    match (instruction.program_id, instruction.program_id_index) {
        (Some(program_id), None) => Pubkey::from_str(program_id)
            .map(Some)
            .map_err(|error| format!("invalid instruction program id: {error}")),
        (None, Some(index)) => {
            let reference = RawAccountReference::Index(index);
            resolve_message_key(message, &reference).map(Some)
        }
        (None, None) => Ok(None),
        (Some(_), Some(_)) => {
            Err("instruction contains both programId and programIdIndex".to_string())
        }
    }
}

fn decode_swap_instruction(
    message: &RawMessage<'_>,
    instruction: &RawInstruction<'_>,
    outer_instruction_index: usize,
    inner_instruction_index: Option<usize>,
) -> Result<Option<SwapInstruction>, String> {
    if resolve_program_id(message, instruction)? != Some(RAYDIUM_LIQUIDITY_POOL_V4_PROGRAM_ID) {
        return Ok(None);
    }

    let data = instruction
        .data
        .ok_or_else(|| "Raydium instruction is missing raw data".to_string())?;
    let mut decoded_data = [0_u8; 64];
    let decoded_len = bs58::decode(data)
        .onto(&mut decoded_data)
        .map_err(|error| format!("Raydium instruction data is not valid base58: {error}"))?;
    if decoded_len == 0
        || !matches!(
            decoded_data[0],
            RAYDIUM_SWAP_BASE_IN | RAYDIUM_SWAP_BASE_OUT
        )
    {
        return Ok(None);
    }

    let (base_vault_index, quote_vault_index) = match instruction.accounts.len() {
        CURRENT_SWAP_ACCOUNT_COUNT => (4, 5),
        LEGACY_SWAP_ACCOUNT_COUNT => (5, 6),
        count => {
            return Err(format!(
                "Raydium swap account layout must contain 17 or 18 accounts, got {count}"
            ));
        }
    };

    let token_program = resolve_message_key(message, &instruction.accounts[0])?;
    if token_program != spl_token::id() {
        return Err("Raydium swap token-program account is invalid".to_string());
    }

    let pool_id = resolve_message_key(message, &instruction.accounts[1])?;
    let base_vault = resolve_message_key(message, &instruction.accounts[base_vault_index])?;
    let quote_vault = resolve_message_key(message, &instruction.accounts[quote_vault_index])?;
    if pool_id == Pubkey::default()
        || base_vault == Pubkey::default()
        || quote_vault == Pubkey::default()
        || pool_id == base_vault
        || pool_id == quote_vault
        || base_vault == quote_vault
    {
        return Err("Raydium pool/vault account mapping is ambiguous".to_string());
    }

    Ok(Some(SwapInstruction {
        pool_id,
        base_vault,
        quote_vault,
        outer_instruction_index: u8::try_from(outer_instruction_index)
            .map_err(|error| format!("outer instruction index is out of range: {error}"))?,
        inner_instruction_index: inner_instruction_index
            .map(u8::try_from)
            .transpose()
            .map_err(|error| format!("inner instruction index is out of range: {error}"))?,
    }))
}

fn account_index_for_pubkey(message: &RawMessage<'_>, expected: Pubkey) -> Result<u64, String> {
    let mut found = None;
    for (index, account_key) in message.account_keys.iter().enumerate() {
        let account_key = Pubkey::from_str(account_key.as_str())
            .map_err(|error| format!("invalid transaction account key: {error}"))?;
        if account_key == expected {
            if found.is_some() {
                return Err(format!(
                    "transaction account key {expected} appears more than once"
                ));
            }
            found = Some(
                u64::try_from(index)
                    .map_err(|error| format!("transaction account index is too large: {error}"))?,
            );
        }
    }
    found.ok_or_else(|| format!("vault {expected} is absent from transaction account keys"))
}

fn unique_token_balance(
    balances: &[RawTokenBalance<'_>],
    account_index: u64,
    phase: &str,
) -> Result<(Pubkey, u64), String> {
    let mut found = None;
    for balance in balances
        .iter()
        .filter(|balance| balance.account_index == account_index)
    {
        if found.is_some() {
            return Err(format!(
                "{phase} token balances contain duplicate account index {account_index}"
            ));
        }
        let mint = Pubkey::from_str(balance.mint)
            .map_err(|error| format!("{phase} token-balance mint is invalid: {error}"))?;
        let amount = balance
            .ui_token_amount
            .amount
            .parse::<u64>()
            .map_err(|error| format!("{phase} raw token amount is invalid: {error}"))?;
        found = Some((mint, amount));
    }
    found.ok_or_else(|| {
        format!("{phase} token balance is missing for account index {account_index}")
    })
}

fn balance_delta(pre_amount: u64, post_amount: u64) -> Result<(BalanceDirection, u64), String> {
    match post_amount.cmp(&pre_amount) {
        Comparison::Greater => Ok((
            BalanceDirection::Increase,
            post_amount
                .checked_sub(pre_amount)
                .ok_or_else(|| "token balance increase underflowed".to_string())?,
        )),
        Comparison::Less => Ok((
            BalanceDirection::Decrease,
            pre_amount
                .checked_sub(post_amount)
                .ok_or_else(|| "token balance decrease underflowed".to_string())?,
        )),
        Comparison::Equal => Err("pool vault token balance did not change".to_string()),
    }
}

fn resolve_source_signature(result: &NotificationResult<'_>) -> Result<Signature, String> {
    let nested = result
        .transaction
        .transaction
        .signatures
        .first()
        .copied()
        .ok_or_else(|| "transaction has no source signature".to_string())?;
    if result
        .signature
        .is_some_and(|signature| signature != nested)
    {
        return Err("notification signature differs from transaction signature".to_string());
    }
    Signature::from_str(result.signature.unwrap_or(nested))
        .map_err(|error| format!("source transaction signature is invalid: {error}"))
}

fn decode_notification(
    notification: &TransactionNotification<'_>,
    observed_at_ms: u64,
) -> Result<Vec<DecodedSwap>, String> {
    let result = &notification.params.result;
    let transaction = &result.transaction;
    if transaction.meta.err.is_some() {
        return Err("failed transaction notification was received".to_string());
    }

    let signature = resolve_source_signature(result)?;
    let message = &transaction.transaction.message;
    let mut candidates = Vec::new();

    for (outer_index, instruction) in message.instructions.iter().enumerate() {
        if let Some(candidate) = decode_swap_instruction(message, instruction, outer_index, None)? {
            candidates.push(candidate);
        }
    }

    for inner_group in transaction
        .meta
        .inner_instructions
        .as_deref()
        .unwrap_or_default()
    {
        let outer_index = usize::try_from(inner_group.index)
            .map_err(|error| format!("inner-instruction outer index is invalid: {error}"))?;
        if outer_index >= message.instructions.len() {
            return Err(format!(
                "inner-instruction group references absent outer index {outer_index}"
            ));
        }
        for (inner_index, instruction) in inner_group.instructions.iter().enumerate() {
            if let Some(candidate) =
                decode_swap_instruction(message, instruction, outer_index, Some(inner_index))?
            {
                candidates.push(candidate);
            }
        }
    }

    for (index, candidate) in candidates.iter().enumerate() {
        for other in candidates.iter().skip(index + 1) {
            if candidate.pool_id == other.pool_id
                || candidate.base_vault == other.base_vault
                || candidate.base_vault == other.quote_vault
                || candidate.quote_vault == other.base_vault
                || candidate.quote_vault == other.quote_vault
            {
                return Err(
                    "multiple Raydium swaps have ambiguous aggregate pool-vault deltas".to_string(),
                );
            }
        }
    }

    let pre_balances = transaction
        .meta
        .pre_token_balances
        .as_deref()
        .ok_or_else(|| "preTokenBalances is missing".to_string())?;
    let post_balances = transaction
        .meta
        .post_token_balances
        .as_deref()
        .ok_or_else(|| "postTokenBalances is missing".to_string())?;

    let mut decoded_swaps = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let base_index = account_index_for_pubkey(message, candidate.base_vault)?;
        let quote_index = account_index_for_pubkey(message, candidate.quote_vault)?;
        let (pre_base_mint, pre_base_amount) =
            unique_token_balance(pre_balances, base_index, "pre")?;
        let (post_base_mint, post_base_amount) =
            unique_token_balance(post_balances, base_index, "post")?;
        let (pre_quote_mint, pre_quote_amount) =
            unique_token_balance(pre_balances, quote_index, "pre")?;
        let (post_quote_mint, post_quote_amount) =
            unique_token_balance(post_balances, quote_index, "post")?;

        if pre_base_mint != post_base_mint || pre_quote_mint != post_quote_mint {
            return Err("pool vault mint changed between pre/post balances".to_string());
        }

        let (base_direction, base_delta) = balance_delta(pre_base_amount, post_base_amount)?;
        let (quote_direction, quote_delta) = balance_delta(pre_quote_amount, post_quote_amount)?;
        if base_direction == quote_direction {
            return Err("pool vault deltas must have opposite signs".to_string());
        }

        let (target_mint, base_amount, quote_amount) =
            if pre_base_mint == WSOL_MINT && pre_quote_mint != WSOL_MINT {
                (pre_quote_mint, quote_delta, base_delta)
            } else if pre_quote_mint == WSOL_MINT && pre_base_mint != WSOL_MINT {
                (pre_base_mint, base_delta, quote_delta)
            } else {
                return Err("pool vault mints must contain exactly one WSOL mint".to_string());
            };

        decoded_swaps.push(DecodedSwap {
            signature,
            pool_id: candidate.pool_id,
            target_mint,
            base_amount,
            quote_amount,
            observed_at_ms,
            source_slot: result.slot,
            outer_instruction_index: candidate.outer_instruction_index,
            inner_instruction_index: candidate.inner_instruction_index,
        });
    }

    Ok(decoded_swaps)
}

/// Returns true when `target_mint` should be accepted (i.e. is not filtered
/// out) by the configured target-mint allowlist.
///
/// REGRESSION GUARD (Critical #1, audit 2026-08): `None` means "no filter,
/// accept everything" — the production default. `Some(vec![])` must ALSO be
/// treated as "no filter", not as "reject everything", because an empty
/// allowlist is never an intentional "block all swaps" configuration in
/// this codebase; it previously arose from a config bug and silently
/// starved every exit watcher of price ticks. A genuinely non-empty
/// allowlist still filters normally.
fn configured_target_matches(config: &crate::config::AppConfig, target_mint: Pubkey) -> bool {
    match config.target_mints.as_ref() {
        None => true,
        Some(target_mints) if target_mints.is_empty() => true,
        Some(target_mints) => target_mints
            .iter()
            .any(|configured| Pubkey::from_str(configured).is_ok_and(|mint| mint == target_mint)),
    }
}

fn local_receipt_time_ms() -> Result<u64, String> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock error: {error}"))?;
    u64::try_from(duration.as_millis())
        .map_err(|error| format!("receipt time overflows u64 milliseconds: {error}"))
}

fn enqueue_decoded_swap(
    decoded: DecodedSwap,
    swap_tx: &Sender<SwapEvent>,
    config: &crate::config::AppConfig,
) -> Result<(), BotError> {
    SWAPS_DECODED.fetch_add(1, Ordering::Relaxed);
    if !configured_target_matches(config, decoded.target_mint)
        || (config.min_swap_lamports > 0 && decoded.quote_amount < config.min_swap_lamports)
    {
        FILTER_DROPS.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }

    let stream_epoch = STREAM_EPOCH.load(Ordering::Relaxed);
    let event = decoded.into_swap_event(stream_epoch);
    match swap_tx.try_send(event) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(_)) => {
            WS_QUEUE_FULL_DROPS.fetch_add(1, Ordering::Relaxed);
            STREAM_EPOCH.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        Err(TrySendError::Disconnected(_)) => Err(BotError::ConfigError(
            "WebSocket ingestion channel is disconnected".to_string(),
        )),
    }
}

fn process_notification_payload(
    payload: &str,
    expected_subscription: u64,
    observed_at_ms: u64,
    swap_tx: &Sender<SwapEvent>,
    config: &crate::config::AppConfig,
) -> Result<(), NotificationError> {
    let notification: TransactionNotification<'_> =
        serde_json::from_str(payload).map_err(|error| {
            NotificationError::Protocol(format!("invalid transactionNotification JSON: {error}"))
        })?;
    if notification.method != "transactionNotification" {
        return Err(NotificationError::Protocol(format!(
            "unexpected JSON-RPC method after subscription: {}",
            notification.method
        )));
    }

    NOTIFICATIONS_RECEIVED.fetch_add(1, Ordering::Relaxed);
    if notification.params.subscription != expected_subscription {
        return Err(NotificationError::Protocol(format!(
            "transactionNotification subscription mismatch: expected {}, got {}",
            expected_subscription, notification.params.subscription
        )));
    }

    let decoded_swaps = match decode_notification(&notification, observed_at_ms) {
        Ok(decoded_swaps) => decoded_swaps,
        Err(error) => {
            DECODE_FAILURES.fetch_add(1, Ordering::Relaxed);
            log::warn!("Discarding undecodable transaction notification: {error}");
            return Ok(());
        }
    };

    for decoded_swap in decoded_swaps {
        enqueue_decoded_swap(decoded_swap, swap_tx, config)
            .map_err(|_| NotificationError::ChannelClosed)?;
    }
    Ok(())
}

pub async fn run_listener(
    config: crate::config::AppConfig,
    swap_tx: Sender<SwapEvent>,
) -> Result<(), BotError> {
    if !config.raydium_ws_url.starts_with("wss://") {
        return Err(BotError::ConfigError(
            "Raydium WebSocket URL must use WSS".to_string(),
        ));
    }

    let watchlist_content = std::fs::read_to_string("approved_watchlist.csv")
        .map_err(|e| BotError::ConfigError(format!("Failed to read approved_watchlist.csv: {}", e)))?;
    
    let mut wallets = Vec::new();
    for (i, line) in watchlist_content.lines().enumerate() {
        if i == 0 { continue; } // Skip header
        if let Some(wallet) = line.split(',').next() {
            if !wallet.trim().is_empty() {
                wallets.push(format!("\"{}\"", wallet.trim()));
            }
        }
    }
    
    if wallets.is_empty() {
        return Err(BotError::ConfigError("approved_watchlist.csv is empty or invalid".to_string()));
    }
    
    let account_include = wallets.join(", ");
    let subscription_request = format!(
        r#"{{"jsonrpc": "2.0", "id": 1, "method": "transactionSubscribe", "params": [{{"vote": false, "failed": false, "accountInclude": [{}]}}, {{"commitment": "processed", "encoding": "jsonParsed", "transactionDetails": "full", "showRewards": false, "maxSupportedTransactionVersion": 0}}]}}"#,
        account_include
    );

    loop {
        log::info!("Connecting to Helius Raydium transaction stream");
        let (ws_stream, _) = match connect_async(config.raydium_ws_url.as_str()).await {
            Ok(stream) => stream,
            Err(error) => {
                log::warn!("Helius WebSocket connection failed: {error}; retrying in 2s");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        let (mut write, mut read) = ws_stream.split();
        let subscription_id = match subscribe_and_wait_for_ack(&mut write, &mut read, &subscription_request).await {
            Ok(subscription_id) => subscription_id,
            Err(error) => {
                log::warn!("Helius subscription handshake failed: {error}; reconnecting");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        STREAM_EPOCH.fetch_add(1, Ordering::Relaxed);
        log::info!("Helius Raydium transaction subscription active: {subscription_id}");

        let mut ping_interval = tokio::time::interval(WS_PING_INTERVAL);
        ping_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ping_interval.tick().await;

        let receive_deadline = tokio::time::sleep(WS_RECEIVE_TIMEOUT);
        tokio::pin!(receive_deadline);

        'connection: loop {
            tokio::select! {
                _ = &mut receive_deadline => {
                    log::warn!(
                        "Helius WebSocket received no transaction notifications for {}s; reconnecting",
                        WS_RECEIVE_TIMEOUT.as_secs()
                    );
                    break 'connection;
                }
                _ = ping_interval.tick() => {
                    if let Err(error) = write.send(Message::Ping(Vec::new())).await {
                        log::warn!("Helius WebSocket ping failed: {error}; reconnecting");
                        break 'connection;
                    }
                }
                message = read.next() => {
                    let payload = match message {
                        None => {
                            log::warn!("Helius WebSocket stream closed; reconnecting");
                            break 'connection;
                        }
                        Some(Err(error)) => {
                            log::warn!("Helius WebSocket read failed: {error}; reconnecting");
                            break 'connection;
                        }
                        Some(Ok(Message::Text(payload))) => payload,
                        Some(Ok(Message::Binary(payload))) => {
                            match String::from_utf8(payload) {
                                Ok(payload) => payload,
                                Err(error) => {
                                    log::warn!("Helius data frame is not UTF-8: {error}; reconnecting");
                                    break 'connection;
                                }
                            }
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            if let Err(error) = write.send(Message::Pong(payload)).await {
                                log::warn!("Helius WebSocket pong failed: {error}; reconnecting");
                                break 'connection;
                            }
                            continue;
                        }
                        Some(Ok(Message::Pong(_))) | Some(Ok(Message::Frame(_))) => {
                            continue;
                        }
                        Some(Ok(Message::Close(frame))) => {
                            log::warn!("Helius WebSocket closed ({frame:?}); reconnecting");
                            break 'connection;
                        }
                    };

                    let observed_at_ms = match local_receipt_time_ms() {
                        Ok(observed_at_ms) => observed_at_ms,
                        Err(error) => {
                            DECODE_FAILURES.fetch_add(1, Ordering::Relaxed);
                            log::warn!(
                                "Discarding transaction notification: local receipt time failed: {error}"
                            );
                            continue;
                        }
                    };

                    match process_notification_payload(
                        payload.as_str(),
                        subscription_id,
                        observed_at_ms,
                        &swap_tx,
                        &config,
                    ) {
                        Ok(()) => {
                            receive_deadline
                                .as_mut()
                                .reset(tokio::time::Instant::now() + WS_RECEIVE_TIMEOUT);
                        }
                        Err(NotificationError::Protocol(error)) => {
                            log::warn!("Helius subscription protocol failure: {error}; reconnecting");
                            break 'connection;
                        }
                        Err(NotificationError::ChannelClosed) => {
                            return Err(BotError::ConfigError(
                                "WebSocket ingestion channel is disconnected".to_string(),
                            ));
                        }
                    }
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Map};
    use std::sync::Mutex;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    static EPOCH_TEST_LOCK: Mutex<()> = Mutex::new(());

    #[derive(Clone, Copy)]
    enum InstructionLocation {
        Outer,
        Inner,
    }

    struct Fixture {
        value: Value,
        signature: Signature,
        pool_id: Pubkey,
        target_mint: Pubkey,
        base_amount: u64,
        quote_amount: u64,
    }

    fn test_config() -> crate::config::AppConfig {
        crate::config::AppConfig {
            rpc_url: "https://localhost".to_string(),
            raydium_ws_url: "wss://localhost".to_string(),
            target_mints: None,
            min_swap_lamports: 0,
            telegram_bot_token: None,
            telegram_chat_id: None,
            dry_run: true,
            startup_policy: crate::config::ShadowStartupPolicy {
                position_recovery_allowed: false,
                capital_execution_allowed: false,
            },
        }
    }

    fn token_balance(account_index: usize, mint: Pubkey, amount: u64) -> Value {
        json!({
            "accountIndex": account_index,
            "mint": mint.to_string(),
            "uiTokenAmount": {
                "amount": amount.to_string(),
                "decimals": 9,
                "uiAmount": null,
                "uiAmountString": "ignored"
            }
        })
    }

    fn notification_fixture(
        account_count: usize,
        discriminator: u8,
        location: InstructionLocation,
        wsol_first: bool,
    ) -> Fixture {
        let signature = Signature::default();
        let pool_id = Pubkey::new_unique();
        let target_mint = Pubkey::new_unique();
        let mut accounts: Vec<Pubkey> = (0..account_count).map(|_| Pubkey::new_unique()).collect();
        accounts[0] = spl_token::id();
        accounts[1] = pool_id;
        let (base_vault_index, quote_vault_index) = match account_count {
            CURRENT_SWAP_ACCOUNT_COUNT => (4, 5),
            LEGACY_SWAP_ACCOUNT_COUNT => (5, 6),
            _ => panic!("test fixture requires a supported layout"),
        };

        let (base_mint, quote_mint) = if wsol_first {
            (WSOL_MINT, target_mint)
        } else {
            (target_mint, WSOL_MINT)
        };
        let (pre_base, post_base, pre_quote, post_quote) =
            (1_000_u64, 1_321_u64, 10_000_u64, 9_211_u64);
        let (expected_base, expected_quote) = if wsol_first { (789, 321) } else { (321, 789) };

        let mut instruction_data = [0_u8; 17];
        instruction_data[0] = discriminator;
        instruction_data[1..9].copy_from_slice(&u64::MAX.to_le_bytes());
        instruction_data[9..17].copy_from_slice(&1_u64.to_le_bytes());
        let instruction = json!({
            "programId": RAYDIUM_LIQUIDITY_POOL_V4_PROGRAM_ID.to_string(),
            "accounts": accounts.iter().map(Pubkey::to_string).collect::<Vec<_>>(),
            "data": bs58::encode(instruction_data).into_string(),
            "stackHeight": null
        });

        let (outer_instructions, inner_instructions) = match location {
            InstructionLocation::Outer => (vec![instruction], Vec::new()),
            InstructionLocation::Inner => (
                vec![json!({
                    "program": "system",
                    "programId": solana_sdk::system_program::id().to_string(),
                    "parsed": {"type": "transfer"},
                    "stackHeight": null
                })],
                vec![json!({"index": 0, "instructions": [instruction]})],
            ),
        };

        let value = json!({
            "jsonrpc": "2.0",
            "method": "transactionNotification",
            "params": {
                "subscription": 77,
                "result": {
                    "signature": signature.to_string(),
                    "slot": 123456_u64,
                    "transaction": {
                        "transaction": {
                            "signatures": [signature.to_string()],
                            "message": {
                                "accountKeys": accounts.iter().map(|key| {
                                    json!({
                                        "pubkey": key.to_string(),
                                        "signer": false,
                                        "source": "transaction",
                                        "writable": true
                                    })
                                }).collect::<Vec<_>>(),
                                "instructions": outer_instructions
                            }
                        },
                        "meta": {
                            "err": null,
                            "innerInstructions": inner_instructions,
                            "preTokenBalances": [
                                token_balance(base_vault_index, base_mint, pre_base),
                                token_balance(quote_vault_index, quote_mint, pre_quote)
                            ],
                            "postTokenBalances": [
                                token_balance(base_vault_index, base_mint, post_base),
                                token_balance(quote_vault_index, quote_mint, post_quote)
                            ]
                        }
                    }
                }
            }
        });

        Fixture {
            value,
            signature,
            pool_id,
            target_mint,
            base_amount: expected_base,
            quote_amount: expected_quote,
        }
    }

    fn decode_fixture(fixture: &Fixture, observed_at_ms: u64) -> Result<Vec<DecodedSwap>, String> {
        let payload = fixture.value.to_string();
        let notification: TransactionNotification<'_> =
            serde_json::from_str(&payload).expect("fixture notification parses");
        decode_notification(&notification, observed_at_ms)
    }

    #[test]
    fn subscription_payload_is_exactly_approved_json_rpc() {
        assert_eq!(
            SUBSCRIPTION_REQUEST,
            r#"{"jsonrpc": "2.0", "id": 1, "method": "transactionSubscribe", "params": [{"vote": false, "failed": false, "accountInclude": ["675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"]}, {"commitment": "processed", "encoding": "jsonParsed", "transactionDetails": "full", "showRewards": false, "maxSupportedTransactionVersion": 0}]}"#
        );
    }

    #[test]
    fn ack_requires_id_one_and_numeric_result() {
        assert_eq!(SUBSCRIPTION_ACK_TIMEOUT, Duration::from_secs(10));
        assert_eq!(
            validate_subscription_ack(r#"{"jsonrpc":"2.0","id":1,"result":77}"#),
            Ok(77)
        );
        for invalid in [
            r#"{"jsonrpc":"2.0","id":2,"result":77}"#,
            r#"{"jsonrpc":"2.0","id":"1","result":77}"#,
            r#"{"jsonrpc":"2.0","id":1,"result":"77"}"#,
            r#"{"jsonrpc":"2.0","id":1,"result":null}"#,
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-1,"message":"denied"}}"#,
        ] {
            assert!(validate_subscription_ack(invalid).is_err(), "{invalid}");
        }
    }

    #[tokio::test]
    async fn handshake_sends_exact_payload_and_ignores_ping_before_ack() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let address = listener.local_addr().expect("loopback listener address");
        let server = tokio::spawn(async move {
            let (tcp_stream, _) = listener.accept().await.expect("accept loopback client");
            let mut socket = accept_async(tcp_stream)
                .await
                .expect("accept WebSocket handshake");

            let request = socket
                .next()
                .await
                .expect("subscription request frame")
                .expect("valid subscription frame");
            assert_eq!(
                request.into_text().expect("subscription request is text"),
                SUBSCRIPTION_REQUEST
            );

            socket
                .send(Message::Ping(vec![1, 2, 3]))
                .await
                .expect("send pre-ACK ping");
            assert!(matches!(
                socket
                    .next()
                    .await
                    .expect("pong frame")
                    .expect("valid pong frame"),
                Message::Pong(payload) if payload == [1, 2, 3]
            ));
            socket
                .send(Message::Text(
                    r#"{"jsonrpc":"2.0","id":1,"result":77}"#.to_string(),
                ))
                .await
                .expect("send subscription ACK");
        });

        let (socket, _) = connect_async(format!("ws://{address}"))
            .await
            .expect("connect loopback WebSocket");
        let (mut write, mut read) = socket.split();
        assert_eq!(
            subscribe_and_wait_for_ack(&mut write, &mut read)
                .await
                .expect("valid subscription ACK"),
            77
        );
        server.await.expect("loopback server task");
    }

    #[test]
    fn both_discriminators_decode_in_outer_and_inner_17_and_18_account_layouts() {
        for account_count in [CURRENT_SWAP_ACCOUNT_COUNT, LEGACY_SWAP_ACCOUNT_COUNT] {
            for discriminator in [RAYDIUM_SWAP_BASE_IN, RAYDIUM_SWAP_BASE_OUT] {
                for location in [InstructionLocation::Outer, InstructionLocation::Inner] {
                    let fixture =
                        notification_fixture(account_count, discriminator, location, false);
                    let decoded =
                        decode_fixture(&fixture, 7_777).expect("supported swap must decode");
                    assert_eq!(decoded.len(), 1);
                    assert_eq!(decoded[0].pool_id, fixture.pool_id);
                    assert_eq!(decoded[0].target_mint, fixture.target_mint);
                    assert_eq!(decoded[0].base_amount, fixture.base_amount);
                    assert_eq!(decoded[0].quote_amount, fixture.quote_amount);
                    assert_eq!(
                        decoded[0].inner_instruction_index,
                        match location {
                            InstructionLocation::Outer => None,
                            InstructionLocation::Inner => Some(0),
                        }
                    );
                }
            }
        }
    }

    #[test]
    fn decodes_outer_swap_base_in_from_current_vault_deltas() {
        let fixture = notification_fixture(
            CURRENT_SWAP_ACCOUNT_COUNT,
            RAYDIUM_SWAP_BASE_IN,
            InstructionLocation::Outer,
            false,
        );
        let decoded = decode_fixture(&fixture, 9_999).expect("outer swap decodes");
        assert_eq!(decoded.len(), 1);
        assert_eq!(
            decoded[0],
            DecodedSwap {
                signature: fixture.signature,
                pool_id: fixture.pool_id,
                target_mint: fixture.target_mint,
                base_amount: fixture.base_amount,
                quote_amount: fixture.quote_amount,
                observed_at_ms: 9_999,
                source_slot: 123_456,
                outer_instruction_index: 0,
                inner_instruction_index: None,
            }
        );
    }

    #[test]
    fn decodes_inner_swap_base_out_from_legacy_vault_deltas() {
        let fixture = notification_fixture(
            LEGACY_SWAP_ACCOUNT_COUNT,
            RAYDIUM_SWAP_BASE_OUT,
            InstructionLocation::Inner,
            true,
        );
        let decoded = decode_fixture(&fixture, 8_888).expect("inner swap decodes");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].target_mint, fixture.target_mint);
        assert_eq!(decoded[0].base_amount, fixture.base_amount);
        assert_eq!(decoded[0].quote_amount, fixture.quote_amount);
        assert_eq!(decoded[0].outer_instruction_index, 0);
        assert_eq!(decoded[0].inner_instruction_index, Some(0));
    }

    #[test]
    fn rejects_missing_balance_mapping() {
        let mut fixture = notification_fixture(
            CURRENT_SWAP_ACCOUNT_COUNT,
            RAYDIUM_SWAP_BASE_IN,
            InstructionLocation::Outer,
            false,
        );
        fixture.value["params"]["result"]["transaction"]["meta"]["postTokenBalances"] = json!([]);
        let payload = fixture.value.to_string();
        let (tx, rx) = crossbeam::channel::bounded(1);
        let failures_before = DECODE_FAILURES.load(Ordering::Relaxed);
        process_notification_payload(&payload, 77, 1, &tx, &test_config())
            .expect("decode failure is a nonfatal notification drop");
        assert_eq!(DECODE_FAILURES.load(Ordering::Relaxed), failures_before + 1);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn rejects_ambiguous_duplicate_balance_mapping() {
        let mut fixture = notification_fixture(
            CURRENT_SWAP_ACCOUNT_COUNT,
            RAYDIUM_SWAP_BASE_IN,
            InstructionLocation::Outer,
            false,
        );
        let pre_balances = fixture.value["params"]["result"]["transaction"]["meta"]
            ["preTokenBalances"]
            .as_array_mut()
            .expect("pre balances array");
        pre_balances.push(pre_balances[0].clone());
        assert!(decode_fixture(&fixture, 1).is_err());
    }

    #[test]
    fn rejects_same_direction_pool_vault_deltas() {
        let mut fixture = notification_fixture(
            CURRENT_SWAP_ACCOUNT_COUNT,
            RAYDIUM_SWAP_BASE_IN,
            InstructionLocation::Outer,
            false,
        );
        let post = fixture.value["params"]["result"]["transaction"]["meta"]["postTokenBalances"]
            .as_array_mut()
            .expect("post balances array");
        post[1]["uiTokenAmount"]["amount"] = json!("10789");
        assert!(decode_fixture(&fixture, 1).is_err());
    }

    #[test]
    fn rejects_multiple_swaps_using_the_same_pool_deltas() {
        let mut fixture = notification_fixture(
            CURRENT_SWAP_ACCOUNT_COUNT,
            RAYDIUM_SWAP_BASE_IN,
            InstructionLocation::Outer,
            false,
        );
        let instructions = fixture.value["params"]["result"]["transaction"]["transaction"]
            ["message"]["instructions"]
            .as_array_mut()
            .expect("outer instruction array");
        instructions.push(instructions[0].clone());
        assert!(decode_fixture(&fixture, 1).is_err());
    }

    #[test]
    fn subscription_mismatch_is_a_protocol_failure() {
        let fixture = notification_fixture(
            CURRENT_SWAP_ACCOUNT_COUNT,
            RAYDIUM_SWAP_BASE_IN,
            InstructionLocation::Outer,
            false,
        );
        let payload = fixture.value.to_string();
        let (tx, _rx) = crossbeam::channel::bounded(1);
        assert!(matches!(
            process_notification_payload(&payload, 78, 1, &tx, &test_config()),
            Err(NotificationError::Protocol(_))
        ));
    }

    #[test]
    fn filter_runs_before_swap_event_materialization() {
        let fixture = notification_fixture(
            CURRENT_SWAP_ACCOUNT_COUNT,
            RAYDIUM_SWAP_BASE_IN,
            InstructionLocation::Outer,
            false,
        );
        let decoded = decode_fixture(&fixture, 1)
            .expect("fixture decodes")
            .pop()
            .expect("one swap");
        let mut config = test_config();
        config.target_mints = Some(vec![Pubkey::new_unique().to_string()]);
        let (tx, rx) = crossbeam::channel::bounded(1);
        enqueue_decoded_swap(decoded, &tx, &config).expect("filter drop is nonfatal");
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn empty_target_mints_allowlist_does_not_drop_swaps() {
        // REGRESSION GUARD (Critical #1, audit 2026-08): `AppConfig::load_from_env`
        // previously produced `target_mints: Some(vec![])`, which under the old
        // `is_none_or` implementation silently rejected every swap — starving
        // every exit watcher of price ticks in production. `Some(vec![])` must
        // behave identically to `None` (accept everything).
        let fixture = notification_fixture(
            CURRENT_SWAP_ACCOUNT_COUNT,
            RAYDIUM_SWAP_BASE_IN,
            InstructionLocation::Outer,
            false,
        );
        let decoded = decode_fixture(&fixture, 1)
            .expect("fixture decodes")
            .pop()
            .expect("one swap");
        let mut config = test_config();
        config.target_mints = Some(vec![]);
        let (tx, rx) = crossbeam::channel::bounded(1);
        enqueue_decoded_swap(decoded, &tx, &config).expect("empty allowlist must not drop swaps");
        assert!(
            rx.try_recv().is_ok(),
            "an empty target_mints allowlist must be treated as unrestricted, not as reject-all"
        );
    }

    #[test]
    fn minimum_lamport_filter_drops_executed_quote_delta() {
        let fixture = notification_fixture(
            CURRENT_SWAP_ACCOUNT_COUNT,
            RAYDIUM_SWAP_BASE_IN,
            InstructionLocation::Outer,
            false,
        );
        let decoded = decode_fixture(&fixture, 1)
            .expect("fixture decodes")
            .pop()
            .expect("one swap");
        let mut config = test_config();
        config.min_swap_lamports = decoded
            .quote_amount
            .checked_add(1)
            .expect("fixture quote amount has headroom");
        let (tx, rx) = crossbeam::channel::bounded(1);
        enqueue_decoded_swap(decoded, &tx, &config).expect("filter drop is nonfatal");
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn full_ws_queue_drops_event_and_bumps_epoch() {
        let _guard = EPOCH_TEST_LOCK.lock().expect("epoch test lock");
        STREAM_EPOCH.store(500, Ordering::Relaxed);
        let fixture = notification_fixture(
            CURRENT_SWAP_ACCOUNT_COUNT,
            RAYDIUM_SWAP_BASE_IN,
            InstructionLocation::Outer,
            false,
        );
        let decoded = decode_fixture(&fixture, 1)
            .expect("fixture decodes")
            .pop()
            .expect("one swap");
        let (tx, _rx) = crossbeam::channel::bounded(0);
        let drops_before = WS_QUEUE_FULL_DROPS.load(Ordering::Relaxed);
        enqueue_decoded_swap(decoded, &tx, &test_config()).expect("full queue is nonfatal");
        assert_eq!(STREAM_EPOCH.load(Ordering::Relaxed), 501);
        assert_eq!(
            WS_QUEUE_FULL_DROPS.load(Ordering::Relaxed),
            drops_before + 1
        );
        STREAM_EPOCH.store(0, Ordering::Relaxed);
    }

    #[test]
    fn queued_event_retains_stamp_when_math_drop_advances_shared_epoch() {
        let _guard = EPOCH_TEST_LOCK.lock().expect("epoch test lock");
        STREAM_EPOCH.store(700, Ordering::Relaxed);
        let fixture = notification_fixture(
            CURRENT_SWAP_ACCOUNT_COUNT,
            RAYDIUM_SWAP_BASE_IN,
            InstructionLocation::Outer,
            false,
        );
        let decoded = decode_fixture(&fixture, 1)
            .expect("fixture decodes")
            .pop()
            .expect("one swap");
        let (tx, rx) = crossbeam::channel::bounded(1);
        enqueue_decoded_swap(decoded, &tx, &test_config()).expect("queue accepts event");
        let event = rx.try_recv().expect("stamped event");
        let drops_before = MATH_QUEUE_FULL_DROPS.load(Ordering::Relaxed);

        record_math_queue_full_drop();

        assert_eq!(event.stream_epoch, 700);
        assert_eq!(STREAM_EPOCH.load(Ordering::Relaxed), 701);
        assert_eq!(
            MATH_QUEUE_FULL_DROPS.load(Ordering::Relaxed),
            drops_before + 1
        );
        STREAM_EPOCH.store(0, Ordering::Relaxed);
    }

    #[test]
    fn into_swap_event_preserves_typed_provenance_and_epoch() {
        let fixture = notification_fixture(
            CURRENT_SWAP_ACCOUNT_COUNT,
            RAYDIUM_SWAP_BASE_IN,
            InstructionLocation::Outer,
            false,
        );
        let decoded = decode_fixture(&fixture, 123)
            .expect("fixture decodes")
            .pop()
            .expect("one swap");
        let event = decoded.into_swap_event(42);
        assert_eq!(event.target_mint, fixture.target_mint.to_string());
        assert_eq!(event.pool_id, fixture.pool_id.to_string());
        assert_eq!(event.source_signature, fixture.signature);
        assert_eq!(event.source_slot, 123_456);
        assert_eq!(event.outer_instruction_index, 0);
        assert_eq!(event.inner_instruction_index, None);
        assert_eq!(event.stream_epoch, 42);
        assert_eq!(event.timestamp_ms, 123);
    }

    #[test]
    fn parsed_non_raydium_instruction_without_raw_fields_is_ignored() {
        let mut fixture = notification_fixture(
            CURRENT_SWAP_ACCOUNT_COUNT,
            RAYDIUM_SWAP_BASE_IN,
            InstructionLocation::Outer,
            false,
        );
        let instructions = fixture.value["params"]["result"]["transaction"]["transaction"]
            ["message"]["instructions"]
            .as_array_mut()
            .expect("outer instruction array");
        instructions.insert(
            0,
            json!({
                "program": "system",
                "programId": solana_sdk::system_program::id().to_string(),
                "parsed": {"type": "transfer"}
            }),
        );
        let decoded = decode_fixture(&fixture, 1).expect("Raydium instruction still decodes");
        assert_eq!(decoded[0].outer_instruction_index, 1);
    }

    #[test]
    fn malformed_provider_error_cannot_be_acknowledged() {
        let mut error = Map::new();
        error.insert("code".to_string(), json!(-32000));
        error.insert("message".to_string(), json!("subscription failed"));
        let payload = json!({"jsonrpc":"2.0","id":1,"error":error}).to_string();
        assert!(validate_subscription_ack(&payload).is_err());
    }
}
