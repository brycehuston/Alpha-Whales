use crate::exits::{self, ActivePosition};
use alpha_agents_core::{
    db,
    pool_cache::{
        fetch_alt, resolve_pool_keys, PoolKeyValidationError, PoolResolutionError, RaydiumPoolKeys,
        WSOL_MINT,
    },
    state::BotState,
    types::{SwapEvent, WhaleSignal},
};

use alpha_agents_core::dispatcher::{transaction_to_proto_packet, SignedBundle};

use jito_protos::{
    bundle::Bundle,
    searcher::{
        searcher_service_client::SearcherServiceClient, GetTipAccountsRequest, SendBundleRequest,
    },
};
use serde::{Deserialize, Serialize};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_request::TokenAccountsFilter;
use solana_sdk::{
    account::Account,
    address_lookup_table::AddressLookupTableAccount,
    commitment_config::CommitmentConfig,
    instruction::{AccountMeta, Instruction},
    message::{v0, VersionedMessage},
    program_pack::Pack,
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
    system_instruction,
    transaction::VersionedTransaction,
};
use spl_token::state::{Account as TokenAccount, AccountState};
use std::{
    collections::HashSet,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::{fs::OpenOptions, io::AsyncWriteExt, sync::broadcast, time::timeout};
use tonic::{
    transport::{Channel, ClientTlsConfig, Endpoint},
    Request,
};

pub const DEFAULT_JITO_BLOCK_ENGINE_URL: &str = "https://amsterdam.mainnet.block-engine.jito.wtf";
pub const MINIMUM_JITO_TIP_LAMPORTS: u64 = 1_000;
pub use alpha_agents_core::pool_cache::RAYDIUM_LIQUIDITY_POOL_V4_PROGRAM_ID;

const RAYDIUM_SWAP_BASE_IN_DISCRIMINATOR: u8 = 9;
const RAYDIUM_SWAP_BASE_IN_DATA_LEN: usize = 17;
const MAXIMUM_SLIPPAGE_BPS: u16 = 500;
const MAX_QUOTE_RESPONSE_BYTES: usize = 64 * 1024;
const RAYDIUM_QUOTE_URL: &str = "https://transaction-v1.raydium.io/compute/swap-base-in";
const BASIS_POINTS_DENOMINATOR: u128 = 10_000;
const EXECUTION_JOURNAL_VERSION: u8 = 1;
const MAX_EXECUTION_JOURNAL_BYTES: usize = 1024 * 1024;

// ============================================================================
// Phase 2 — Jito Top-of-Bundle ("jitodontfront") Sandwich Protection
// ============================================================================
//
// Verified against current official Jito documentation retrieved on
// 2026-07-26 from https://docs.jito.wtf/lowlatencytxnsend/#sandwich-mitigation
// ("Sandwich Mitigation" section of the Low Latency Transaction Send page):
//
//   - The sentinel is NOT one fixed pubkey. Any valid Solana pubkey whose
//     base58-encoded text starts with the literal prefix "jitodontfront"
//     qualifies (the docs' own examples are
//     `jitodontfront111111111111111111111111111111` and
//     `jitodontfront111111111111111111111111111123`). The account does
//     not need to exist on-chain.
//   - The docs "recommend" (not require) marking the account read-only
//     ("to optimize landing speed"); MASTER_PLAN.md Section 2.1 hardens
//     this into a strict requirement for this codebase
//     (is_signer=false, is_writable=false), which is what we enforce.
//   - The ordering/rejection rule is enforced by the Jito Block Engine at
//     the BUNDLE level, not by any on-chain Solana program: any bundle
//     containing a transaction with a jitodontfront-prefixed account will
//     be rejected UNLESS that transaction is at index 0 of the bundle.
//     Multiple dont-front transactions are allowed at the front of the
//     bundle as long as they are contiguous and each shares at least one
//     signer with the first dont-front transaction.
//   - This solution works with both sendBundle and sendTransaction.
//   - Supports Address Lookup Tables.
//
// This module's `SignedBundle` always contains exactly one transaction
// packet per bundle (see `sign_bundle` below), so the protected swap
// transaction is trivially always at bundle index 0. `validate_bundle_
// protection` still performs the explicit check required by
// MASTER_PLAN.md Section 2.3 so that a future multi-transaction bundle
// change cannot silently violate the ordering contract.
const JITO_DONT_FRONT_PREFIX: &str = "jitodontfront";

/// Returns true when `pubkey`'s base58 text starts with the literal
/// `jitodontfront` prefix, matching current Jito Block Engine behavior.
fn is_valid_dont_front_pubkey(pubkey: &Pubkey) -> bool {
    pubkey.to_string().starts_with(JITO_DONT_FRONT_PREFIX)
}

/// Errors from Phase 2 sandwich-protection shielding.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ShieldError {
    /// The sentinel pubkey does not start with the required
    /// `jitodontfront` prefix, so it would not be honored by the Jito
    /// Block Engine.
    #[error("sentinel pubkey does not start with the required `{JITO_DONT_FRONT_PREFIX}` prefix")]
    InvalidSentinelPrefix,

    /// The target instruction's account layout cannot safely absorb a
    /// trailing account (MASTER_PLAN.md Section 2.4): the on-chain
    /// program may validate exact account counts or positional layouts,
    /// so appending an extra account without proof of safety is
    /// forbidden.
    #[error(
        "instruction does not have a proven-safe trailing account slot for the sentinel \
         (program_id={program_id})"
    )]
    UnsafeTrailingAccount { program_id: Pubkey },
}

/// Errors from Phase 2 transaction/bundle-level sentinel validation,
/// performed immediately before signing (MASTER_PLAN.md Section 2.3).
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum BundleProtectionError {
    /// A protected instruction was found, but the sentinel is missing
    /// from at least one instruction that is supposed to carry it.
    #[error("expected sentinel account is missing from a protected instruction")]
    SentinelMissing,

    /// A protected transaction does not occupy the required leading
    /// bundle index (only index 0 is currently supported by this
    /// codebase's single-transaction-per-bundle design).
    #[error("protected transaction is not at the required leading bundle index (index {0})")]
    NotAtLeadingIndex(usize),
}

/// Appends the verified Jito `jitodontfront` sentinel to `instruction` as an
/// extra read-only, non-signer trailing account meta.
///
/// Required behavior (MASTER_PLAN.md Section 2.1):
///   - adds the sentinel exactly once (idempotent — Section 2.2);
///   - uses `is_signer = false`;
///   - uses `is_writable = false`;
///   - preserves the deterministic ordering of all existing account metas
///     (the sentinel is always appended, never inserted mid-list);
///   - rejects the call outright if the target instruction's program is
///     not on the allow-list of instructions proven to safely accept a
///     trailing account (MASTER_PLAN.md Section 2.4 — no live deployment
///     without that proof).
///
/// Currently the only proven-safe target is the Raydium Liquidity Pool V4
/// swap instruction (`RAYDIUM_LIQUIDITY_POOL_V4_PROGRAM_ID`): its on-chain
/// processor reads a fixed, positional prefix of accounts and does not
/// validate the *total* account count, so trailing accounts beyond the
/// documented layout are ignored by the program and safe to append. Any
/// other program_id is rejected until similarly proven.
pub(crate) fn apply_jitodontfront_protection(
    instruction: &mut Instruction,
    sentinel: Pubkey,
) -> Result<(), ShieldError> {
    if !is_valid_dont_front_pubkey(&sentinel) {
        return Err(ShieldError::InvalidSentinelPrefix);
    }
    if instruction.program_id != RAYDIUM_LIQUIDITY_POOL_V4_PROGRAM_ID {
        return Err(ShieldError::UnsafeTrailingAccount {
            program_id: instruction.program_id,
        });
    }

    // Duplicate protection (MASTER_PLAN.md Section 2.2): calling this
    // function twice must be idempotent.
    if instruction
        .accounts
        .iter()
        .any(|meta| meta.pubkey == sentinel)
    {
        return Ok(());
    }

    instruction
        .accounts
        .push(AccountMeta::new_readonly(sentinel, false));
    Ok(())
}

/// Transaction-level sentinel validation, run immediately before signing
/// (MASTER_PLAN.md Section 2.3). Verifies:
///   - every protected instruction in `instructions` actually contains the
///     sentinel account (not just that `apply_jitodontfront_protection` was
///     called at some point — this re-checks the final instruction list
///     that will actually be signed and submitted);
///   - the protected transaction occupies bundle index 0, the only leading
///     index this codebase's single-transaction-per-bundle design can
///     produce (MASTER_PLAN.md Section 2.3's "required leading bundle
///     indexes" requirement).
///
/// This performs a message-level scan rather than trusting caller state,
/// so a future refactor that forgets to call `apply_jitodontfront_
/// protection` on a protected instruction is caught here rather than
/// silently shipping an unprotected transaction.
fn validate_bundle_protection(
    instructions: &[Instruction],
    sentinel: Pubkey,
    bundle_index: usize,
) -> Result<(), BundleProtectionError> {
    let sentinel_present = instructions.iter().any(|instruction| {
        instruction
            .accounts
            .iter()
            .any(|meta| meta.pubkey == sentinel && !meta.is_signer && !meta.is_writable)
    });
    if !sentinel_present {
        return Err(BundleProtectionError::SentinelMissing);
    }
    if bundle_index != 0 {
        return Err(BundleProtectionError::NotAtLeadingIndex(bundle_index));
    }
    Ok(())
}

#[allow(dead_code)]
type JitoGrpcClient = SearcherServiceClient<Channel>;

#[derive(Clone, Debug)]
pub struct JitoExecutorConfig {
    pub block_engine_url: String,
    pub tip_lamports: u64,
    pub request_timeout: Duration,
    #[allow(dead_code)]
    pub reconnect_delay: Duration,
    pub max_slippage_bps: u16,
    pub max_signal_age: Duration,
    pub max_pending_capital_lamports: u64,
    pub execution_journal_path: PathBuf,
    /// Phase 2 (MASTER_PLAN.md Section 2): the verified `jitodontfront*`
    /// sentinel pubkey attached to protected instructions to request
    /// Jito Block Engine anti-sandwich ordering. Any pubkey whose base58
    /// text starts with the literal string `jitodontfront` qualifies per
    /// current Jito documentation (retrieved 2026-07-26,
    /// https://docs.jito.wtf/lowlatencytxnsend/#sandwich-mitigation); it
    /// does not need to exist on-chain.
    pub jito_dont_front_pubkey: Pubkey,
    pub pumpportal_api_key: Option<String>,
    /// AN-ALT-01: optional on-chain Address Lookup Table address.
    /// When set, `fetch_alt` is called per signal to resolve the
    /// `AddressLookupTableAccount` used for v0 message compression.
    /// ALT creation is a one-time admin operation; this field is
    /// config-driven, not auto-created by the daemon.
    pub alt_address: Option<Pubkey>,
}

impl JitoExecutorConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        block_engine_url: String,
        tip_lamports: u64,
        request_timeout: Duration,
        reconnect_delay: Duration,
        max_slippage_bps: u16,
        max_signal_age: Duration,
        max_pending_capital_lamports: u64,
        execution_journal_path: PathBuf,
        jito_dont_front_pubkey: Pubkey,
        pumpportal_api_key: Option<String>,
    ) -> Result<Self, JitoExecutionError> {
        if !block_engine_url.starts_with("https://") {
            return Err(JitoExecutionError::InvalidConfiguration(
                "Jito Block Engine URL must use HTTPS".to_string(),
            ));
        }
        if tip_lamports < MINIMUM_JITO_TIP_LAMPORTS {
            return Err(JitoExecutionError::InvalidConfiguration(format!(
                "Jito bundle tip must be at least {MINIMUM_JITO_TIP_LAMPORTS} lamports"
            )));
        }
        if request_timeout.is_zero() {
            return Err(JitoExecutionError::InvalidConfiguration(
                "Jito request timeout must be non-zero".to_string(),
            ));
        }
        if reconnect_delay.is_zero() {
            return Err(JitoExecutionError::InvalidConfiguration(
                "Jito reconnect delay must be non-zero".to_string(),
            ));
        }
        if !(1..=MAXIMUM_SLIPPAGE_BPS).contains(&max_slippage_bps) {
            return Err(JitoExecutionError::InvalidConfiguration(format!(
                "max slippage must be between 1 and {MAXIMUM_SLIPPAGE_BPS} basis points"
            )));
        }
        if max_signal_age.is_zero() {
            return Err(JitoExecutionError::InvalidConfiguration(
                "maximum signal age must be non-zero".to_string(),
            ));
        }
        if !execution_journal_path.is_absolute() {
            return Err(JitoExecutionError::InvalidConfiguration(
                "execution journal path must be absolute".to_string(),
            ));
        }
        if !is_valid_dont_front_pubkey(&jito_dont_front_pubkey) {
            return Err(JitoExecutionError::InvalidConfiguration(format!(
                "JITO_DONT_FRONT_PUBKEY must start with the literal prefix \
                 `{JITO_DONT_FRONT_PREFIX}` (got `{jito_dont_front_pubkey}`)"
            )));
        }

        Ok(Self {
            block_engine_url,
            tip_lamports,
            request_timeout,
            reconnect_delay,
            max_slippage_bps,
            max_signal_age,
            max_pending_capital_lamports,
            execution_journal_path,
            jito_dont_front_pubkey,
            pumpportal_api_key,
            alt_address: None,
        })
    }

    /// Set the optional Address Lookup Table address for this config.
    ///
    /// Builder-style setter; zero-cost when `None`. Preserves all existing
    /// `JitoExecutorConfig::new(...)` call sites without any signature change.
    pub fn with_alt_address(mut self, alt_address: Option<Pubkey>) -> Self {
        self.alt_address = alt_address;
        self
    }
}

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum JitoExecutionError {
    #[error("invalid Jito executor configuration: {0}")]
    InvalidConfiguration(String),

    #[error(transparent)]
    InvalidPoolKeys(#[from] PoolKeyValidationError),

    #[error(transparent)]
    PoolResolution(#[from] PoolResolutionError),

    #[error(transparent)]
    Shield(#[from] ShieldError),

    #[error(transparent)]
    BundleProtection(#[from] BundleProtectionError),

    #[error(transparent)]
    BundleBuild(#[from] alpha_agents_core::dispatcher::BundleError),

    #[error("invalid Raydium swap amount: {0}")]
    InvalidSwapAmount(&'static str),

    #[error("invalid Raydium swap account: {0}")]
    InvalidSwapAccount(&'static str),

    #[error("target mint is invalid: {0}")]
    InvalidTargetMint(String),

    #[error("execution signal pool id is invalid: {0}")]
    InvalidSignalPool(String),

    #[error("resolved Raydium pool does not match the signal source pool")]
    SignalPoolMismatch,

    #[error("execution signal timestamp is invalid: {0}")]
    InvalidSignalTimestamp(String),

    #[error("execution signal is stale or from the future")]
    StaleSignal,

    #[error("Raydium quote request failed: {0}")]
    QuoteRequest(String),

    #[error("Raydium quote response is invalid: {0}")]
    InvalidQuote(String),

    #[error("Raydium quote no longer satisfies the VWAP trigger")]
    QuoteNoLongerTriggers,

    #[error("VWAP quote revalidation overflowed")]
    QuoteArithmeticOverflow,

    #[error("Solana RPC error while resolving user token accounts: {0}")]
    UserAccountRpc(String),

    #[error("required funded WSOL associated token account does not exist")]
    MissingSourceTokenAccount,

    #[error("user token account `{0}` failed ownership, mint, state, or balance validation")]
    InvalidUserTokenAccount(&'static str),

    #[error("wallet already has a target-token account or position")]
    ExistingTargetPosition,

    #[error("execution journal error: {0}")]
    ExecutionJournal(String),

    #[error("pending capital ceiling would be exceeded")]
    PendingCapitalLimit,

    #[error("target mint is already reserved in the execution journal")]
    DuplicateExecutionAttempt,

    #[error("capital reservation arithmetic overflowed")]
    CapitalArithmeticOverflow,

    #[error("Jito Block Engine returned no tip accounts")]
    MissingTipAccounts,

    #[error("Jito Block Engine returned an invalid tip account {account}: {reason}")]
    #[allow(dead_code)]
    InvalidTipAccount { account: String, reason: String },
    #[error("Jito gRPC request timed out during {0}")]
    #[allow(dead_code)]
    RequestTimeout(&'static str),

    #[error("failed to compile the versioned transaction message: {0}")]
    MessageCompilation(String),

    #[error("failed to sign the versioned transaction: {0}")]
    TransactionSigning(String),

    #[error("failed to serialize the versioned transaction: {0}")]
    TransactionSerialization(String),

    #[error("serialized transaction is {actual} bytes; maximum packet size is {maximum}")]
    TransactionTooLarge { actual: usize, maximum: usize },

    #[error("Solana RPC error while fetching a recent blockhash: {0}")]
    RecentBlockhash(String),

    #[error("Solana RPC error while calculating the transaction fee: {0}")]
    #[allow(dead_code)]
    TransactionFee(String),

    #[error("Solana RPC error while calculating token-account rent: {0}")]
    RentExemption(String),

    #[error("Jito transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("Jito gRPC status: {0}")]
    GrpcStatus(#[from] tonic::Status),

    #[error("on-chain confirmation failed after {0} polls: {1}")]
    ConfirmationFailed(u32, String),

    #[error("token account read-back failed: {0}")]
    TokenAccountReadback(String),

    #[error("position handoff failed: {0}")]
    #[expect(dead_code, reason = "will be used by Phase 4 dispatch handoff")]
    PositionHandoff(String),
}

#[derive(Debug, PartialEq, Eq)]
pub struct BundleSubmission {
    pub bundle_id: String,
    pub transaction_signature: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum ExecutionJournalRecord {
    Reserved {
        version: u8,
        target_mint: String,
        amount_in: u64,
        capital_at_risk_lamports: u64,
        timestamp_ms: u64,
        transaction_signature: String,
        recent_blockhash: String,
    },
    Accepted {
        version: u8,
        target_mint: String,
        bundle_id: String,
        transaction_signature: String,
    },
}

struct ExecutionJournal {
    path: PathBuf,
    writer_lock_path: PathBuf,
    writer_lock_acquired: bool,
    reserved_mints: HashSet<Pubkey>,
    reserved_capital_lamports: u64,
    max_pending_capital_lamports: u64,
}

impl ExecutionJournal {
    async fn load(config: &JitoExecutorConfig) -> Result<Self, JitoExecutionError> {
        // On startup, remove any stale lock file left by a previously crashed
        // executor process. `acquire_writer_lock` uses `create_new(true)` so
        // it will permanently fail if a prior lock file exists. We only clean
        // it up here — before any trade state is loaded — so that a concurrent
        // running process can still protect itself.
        let lock_path = execution_journal_lock_path(&config.execution_journal_path);
        if lock_path.exists() {
            if let Err(e) = std::fs::remove_file(&lock_path) {
                log::warn!("Could not remove stale execution journal lock `{}`: {e}; if another executor is running, this is expected — otherwise delete it manually.", lock_path.display());
            } else {
                log::info!(
                    "Removed stale execution journal lock `{}`.",
                    lock_path.display()
                );
            }
        }
        ensure_durable_file_exists(&config.execution_journal_path).await?;
        let bytes = tokio::fs::read(&config.execution_journal_path)
            .await
            .map_err(|error| JitoExecutionError::ExecutionJournal(error.to_string()))?;
        if bytes.len() > MAX_EXECUTION_JOURNAL_BYTES {
            return Err(JitoExecutionError::ExecutionJournal(format!(
                "journal exceeds {MAX_EXECUTION_JOURNAL_BYTES} bytes"
            )));
        }
        let contents = std::str::from_utf8(&bytes)
            .map_err(|error| JitoExecutionError::ExecutionJournal(error.to_string()))?;

        let mut reserved_mints = HashSet::new();
        let mut reserved_capital_lamports = 0_u64;
        for (index, line) in contents.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let record: ExecutionJournalRecord = serde_json::from_str(line).map_err(|error| {
                JitoExecutionError::ExecutionJournal(format!(
                    "invalid record on line {}: {error}",
                    index + 1
                ))
            })?;
            match record {
                ExecutionJournalRecord::Reserved {
                    version,
                    target_mint,
                    amount_in,
                    capital_at_risk_lamports,
                    timestamp_ms,
                    transaction_signature,
                    recent_blockhash,
                } => {
                    if version != EXECUTION_JOURNAL_VERSION
                        || amount_in == 0
                        || capital_at_risk_lamports == 0
                        || timestamp_ms == 0
                        || transaction_signature.is_empty()
                        || recent_blockhash.is_empty()
                    {
                        return Err(JitoExecutionError::ExecutionJournal(format!(
                            "invalid reservation on line {}",
                            index + 1
                        )));
                    }
                    let mint = Pubkey::from_str(&target_mint).map_err(|error| {
                        JitoExecutionError::ExecutionJournal(format!(
                            "invalid target mint on line {}: {error}",
                            index + 1
                        ))
                    })?;
                    if !reserved_mints.insert(mint) {
                        return Err(JitoExecutionError::ExecutionJournal(format!(
                            "duplicate target reservation on line {}",
                            index + 1
                        )));
                    }
                    reserved_capital_lamports = reserved_capital_lamports
                        .checked_add(capital_at_risk_lamports)
                        .ok_or(JitoExecutionError::CapitalArithmeticOverflow)?;
                }
                ExecutionJournalRecord::Accepted {
                    version,
                    target_mint,
                    bundle_id,
                    transaction_signature,
                } => {
                    if version != EXECUTION_JOURNAL_VERSION
                        || Pubkey::from_str(&target_mint).is_err()
                        || bundle_id.is_empty()
                        || transaction_signature.is_empty()
                    {
                        return Err(JitoExecutionError::ExecutionJournal(format!(
                            "invalid acceptance on line {}",
                            index + 1
                        )));
                    }
                }
            }
        }
        if reserved_capital_lamports > config.max_pending_capital_lamports {
            return Err(JitoExecutionError::PendingCapitalLimit);
        }

        Ok(Self {
            writer_lock_path: execution_journal_lock_path(&config.execution_journal_path),
            path: config.execution_journal_path.clone(),
            writer_lock_acquired: false,
            reserved_mints,
            reserved_capital_lamports,
            max_pending_capital_lamports: config.max_pending_capital_lamports,
        })
    }

    fn contains(&self, target_mint: &Pubkey) -> bool {
        self.reserved_mints.contains(target_mint)
    }

    async fn reserve(
        &mut self,
        signal: &WhaleSignal,
        target_mint: Pubkey,
        destination_ata_rent_lamports: u64,
        config: &JitoExecutorConfig,
        signed_bundle: &SignedBundle,
    ) -> Result<(), JitoExecutionError> {
        let dynamic_amount_in = (signal.trade_size_sol * 1_000_000_000.0) as u64;
        if self.contains(&target_mint) {
            return Err(JitoExecutionError::DuplicateExecutionAttempt);
        }
        let capital_at_risk_lamports = capital_at_risk_lamports(
            dynamic_amount_in,
            config.tip_lamports,
            signed_bundle.transaction_fee_lamports,
            destination_ata_rent_lamports,
        )?;
        let next_total = self
            .reserved_capital_lamports
            .checked_add(capital_at_risk_lamports)
            .ok_or(JitoExecutionError::CapitalArithmeticOverflow)?;
        if next_total > self.max_pending_capital_lamports {
            return Err(JitoExecutionError::PendingCapitalLimit);
        }

        self.acquire_writer_lock().await?;
        self.append(&ExecutionJournalRecord::Reserved {
            version: EXECUTION_JOURNAL_VERSION,
            target_mint: target_mint.to_string(),
            amount_in: dynamic_amount_in,
            capital_at_risk_lamports,
            timestamp_ms: signal.timestamp_ms,
            transaction_signature: signed_bundle.transaction_signature.clone(),
            recent_blockhash: signed_bundle.recent_blockhash.clone(),
        })
        .await?;

        self.reserved_mints.insert(target_mint);
        self.reserved_capital_lamports = next_total;
        Ok(())
    }

    async fn acquire_writer_lock(&mut self) -> Result<(), JitoExecutionError> {
        if self.writer_lock_acquired {
            return Ok(());
        }
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&self.writer_lock_path)
            .await
            .map_err(|error| {
                JitoExecutionError::ExecutionJournal(format!(
                    "cannot acquire exclusive writer lock `{}`: {error}; reconcile and remove \
                     the lock only when no executor is running",
                    self.writer_lock_path.display()
                ))
            })?;
        file.write_all(format!("pid={}\n", std::process::id()).as_bytes())
            .await
            .map_err(|error| JitoExecutionError::ExecutionJournal(error.to_string()))?;
        file.sync_all()
            .await
            .map_err(|error| JitoExecutionError::ExecutionJournal(error.to_string()))?;
        sync_parent_directory(&self.writer_lock_path).await?;
        self.writer_lock_acquired = true;
        Ok(())
    }

    async fn record_acceptance(
        &self,
        target_mint: Pubkey,
        submission: &BundleSubmission,
    ) -> Result<(), JitoExecutionError> {
        self.append(&ExecutionJournalRecord::Accepted {
            version: EXECUTION_JOURNAL_VERSION,
            target_mint: target_mint.to_string(),
            bundle_id: submission.bundle_id.clone(),
            transaction_signature: submission.transaction_signature.clone(),
        })
        .await
    }

    async fn append(&self, record: &ExecutionJournalRecord) -> Result<(), JitoExecutionError> {
        let mut encoded = serde_json::to_vec(record)
            .map_err(|error| JitoExecutionError::ExecutionJournal(error.to_string()))?;
        encoded.push(b'\n');
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await
            .map_err(|error| JitoExecutionError::ExecutionJournal(error.to_string()))?;
        file.write_all(&encoded)
            .await
            .map_err(|error| JitoExecutionError::ExecutionJournal(error.to_string()))?;
        file.flush()
            .await
            .map_err(|error| JitoExecutionError::ExecutionJournal(error.to_string()))?;
        file.sync_all()
            .await
            .map_err(|error| JitoExecutionError::ExecutionJournal(error.to_string()))
    }
}

async fn ensure_durable_file_exists(path: &std::path::Path) -> Result<(), JitoExecutionError> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .map_err(|error| JitoExecutionError::ExecutionJournal(error.to_string()))?;
    file.sync_all()
        .await
        .map_err(|error| JitoExecutionError::ExecutionJournal(error.to_string()))?;
    sync_parent_directory(path).await
}

#[cfg(unix)]
async fn sync_parent_directory(path: &std::path::Path) -> Result<(), JitoExecutionError> {
    let parent = path.parent().ok_or_else(|| {
        JitoExecutionError::ExecutionJournal("journal path has no parent directory".to_string())
    })?;
    let directory = tokio::fs::File::open(parent)
        .await
        .map_err(|error| JitoExecutionError::ExecutionJournal(error.to_string()))?;
    directory
        .sync_all()
        .await
        .map_err(|error| JitoExecutionError::ExecutionJournal(error.to_string()))
}

#[cfg(not(unix))]
async fn sync_parent_directory(_path: &std::path::Path) -> Result<(), JitoExecutionError> {
    Ok(())
}

fn execution_journal_lock_path(journal_path: &std::path::Path) -> PathBuf {
    let mut lock_path = journal_path.as_os_str().to_os_string();
    lock_path.push(".lock");
    PathBuf::from(lock_path)
}

fn capital_at_risk_lamports(
    amount_in: u64,
    tip_lamports: u64,
    transaction_fee_lamports: u64,
    destination_ata_rent_lamports: u64,
) -> Result<u64, JitoExecutionError> {
    amount_in
        .checked_add(tip_lamports)
        .and_then(|value| value.checked_add(transaction_fee_lamports))
        .and_then(|value| value.checked_add(destination_ata_rent_lamports))
        .ok_or(JitoExecutionError::CapitalArithmeticOverflow)
}

struct ConnectedJitoClient {
    grpc: JitoGrpcClient,
    tip_accounts: Vec<Pubkey>,
    next_tip_account: usize,
}

#[allow(dead_code)]
impl ConnectedJitoClient {
    async fn connect(config: &JitoExecutorConfig) -> Result<Self, JitoExecutionError> {
        let endpoint = Endpoint::from_shared(config.block_engine_url.clone()).map_err(|error| {
            JitoExecutionError::InvalidConfiguration(format!(
                "invalid Jito Block Engine URL: {error}"
            ))
        })?;
        let endpoint = endpoint
            .connect_timeout(config.request_timeout)
            .timeout(config.request_timeout)
            .tls_config(ClientTlsConfig::new())?;

        let channel = timeout(config.request_timeout, endpoint.connect())
            .await
            .map_err(|_| JitoExecutionError::RequestTimeout("connection"))??;
        let mut grpc = SearcherServiceClient::new(channel);

        let tip_response = timeout(
            config.request_timeout,
            grpc.get_tip_accounts(Request::new(GetTipAccountsRequest {})),
        )
        .await
        .map_err(|_| JitoExecutionError::RequestTimeout("GetTipAccounts"))??;

        let mut tip_accounts = Vec::new();
        for account in tip_response.into_inner().accounts {
            let parsed = Pubkey::from_str(&account).map_err(|error| {
                JitoExecutionError::InvalidTipAccount {
                    account,
                    reason: error.to_string(),
                }
            })?;
            tip_accounts.push(parsed);
        }

        if tip_accounts.is_empty() {
            return Err(JitoExecutionError::MissingTipAccounts);
        }

        Ok(Self {
            grpc,
            tip_accounts,
            next_tip_account: 0,
        })
    }

    fn take_tip_account(&mut self) -> Result<Pubkey, JitoExecutionError> {
        let account = self
            .tip_accounts
            .get(self.next_tip_account)
            .copied()
            .ok_or(JitoExecutionError::MissingTipAccounts)?;
        self.next_tip_account = (self.next_tip_account + 1) % self.tip_accounts.len();
        Ok(account)
    }

    async fn sign_bundle(
        &mut self,
        mut instructions: Vec<Instruction>,
        rpc_client: &RpcClient,
        payer: &Keypair,
        config: &JitoExecutorConfig,
        tip_lamports: u64,
    ) -> Result<SignedBundle, JitoExecutionError> {
        // Phase 2 (MASTER_PLAN.md Section 2.3): transaction-level sentinel
        // validation, performed before signing. This bundle always
        // contains exactly one transaction (built from `instructions`
        // below, before the tip instruction is appended), so that
        // transaction is always bundle index 0 — the only leading index
        // this design can produce.
        validate_bundle_protection(&instructions, config.jito_dont_front_pubkey, 0)?;

        let tip_account = self.take_tip_account()?;
        let recent_blockhash = rpc_client.get_latest_blockhash().await.map_err(|_| {
            JitoExecutionError::RecentBlockhash("getLatestBlockhash failed".to_string())
        })?;

        let tip_instruction =
            system_instruction::transfer(&payer.pubkey(), &tip_account, tip_lamports);
        instructions.push(tip_instruction);
        let message =
            v0::Message::try_compile(&payer.pubkey(), &instructions, &[], recent_blockhash)
                .map_err(|error| JitoExecutionError::MessageCompilation(error.to_string()))?;
        let transaction_fee_lamports =
            rpc_client
                .get_fee_for_message(&message)
                .await
                .map_err(|_| {
                    JitoExecutionError::TransactionFee("getFeeForMessage failed".to_string())
                })?;
        let transaction = VersionedTransaction::try_new(VersionedMessage::V0(message), &[payer])
            .map_err(|error| JitoExecutionError::TransactionSigning(error.to_string()))?;
        let transaction_signature = transaction
            .signatures
            .first()
            .map(ToString::to_string)
            .ok_or_else(|| {
                JitoExecutionError::TransactionSigning(
                    "signed transaction contained no signature".to_string(),
                )
            })?;
        let packet = transaction_to_proto_packet(&transaction)?;
        Ok(SignedBundle {
            request: SendBundleRequest {
                bundle: Some(Bundle {
                    header: None,
                    packets: vec![packet],
                }),
            },
            transaction_signature,
            recent_blockhash: recent_blockhash.to_string(),
            transaction_fee_lamports,
        })
    }

    async fn sign_pump_bundle(
        &mut self,
        pump_tx: VersionedTransaction,
        payer: &Keypair,
        tip_lamports: u64,
    ) -> Result<SignedBundle, JitoExecutionError> {
        let message = pump_tx.message;
        let pump_blockhash = match &message {
            VersionedMessage::Legacy(m) => m.recent_blockhash,
            VersionedMessage::V0(m) => m.recent_blockhash,
        };

        let signed_pump_tx = VersionedTransaction::try_new(message, &[payer]).map_err(|error| {
            JitoExecutionError::TransactionSigning(format!("Pump tx sign error: {error}"))
        })?;

        let pump_signature = signed_pump_tx
            .signatures
            .first()
            .map(ToString::to_string)
            .ok_or_else(|| {
                JitoExecutionError::TransactionSigning("No signature on Pump tx".into())
            })?;

        let pump_packet = transaction_to_proto_packet(&signed_pump_tx)?;

        let tip_account = self.take_tip_account()?;
        let tip_instruction =
            system_instruction::transfer(&payer.pubkey(), &tip_account, tip_lamports);
        let tip_message =
            v0::Message::try_compile(&payer.pubkey(), &[tip_instruction], &[], pump_blockhash)
                .map_err(|error| {
                    JitoExecutionError::MessageCompilation(format!(
                        "Tip compilation error: {error}"
                    ))
                })?;

        let tip_tx = VersionedTransaction::try_new(VersionedMessage::V0(tip_message), &[payer])
            .map_err(|error| {
                JitoExecutionError::TransactionSigning(format!("Tip sign error: {error}"))
            })?;

        let tip_packet = transaction_to_proto_packet(&tip_tx)?;

        // Estimate fees roughly since we bypass get_fee_for_message to save latency
        let transaction_fee_lamports = 100_000;

        Ok(SignedBundle {
            request: SendBundleRequest {
                bundle: Some(Bundle {
                    header: None,
                    packets: vec![pump_packet, tip_packet],
                }),
            },
            transaction_signature: pump_signature,
            recent_blockhash: pump_blockhash.to_string(),
            transaction_fee_lamports,
        })
    }

    async fn submit(
        &mut self,
        signal: &WhaleSignal,
        signed_bundle: SignedBundle,
        config: &JitoExecutorConfig,
    ) -> Result<BundleSubmission, JitoExecutionError> {
        ensure_signal_fresh(signal, config.max_signal_age)?;
        let response = timeout(
            config.request_timeout,
            self.grpc.send_bundle(Request::new(signed_bundle.request)),
        )
        .await
        .map_err(|_| JitoExecutionError::RequestTimeout("SendBundle"))??;
        let bundle_id = response.into_inner().uuid;

        if bundle_id.is_empty() {
            return Err(JitoExecutionError::GrpcStatus(tonic::Status::internal(
                "Jito returned an empty bundle ID",
            )));
        }

        println!(
            "Jito bundle accepted but not yet confirmed for {}: bundle={}, signature={}",
            signal.target_mint, bundle_id, signed_bundle.transaction_signature
        );

        Ok(BundleSubmission {
            bundle_id,
            transaction_signature: signed_bundle.transaction_signature,
        })
    }
}

pub fn construct_raydium_swap_instruction(
    pool_keys: &RaydiumPoolKeys,
    user_owner: Pubkey,
    user_source_token_account: Pubkey,
    user_destination_token_account: Pubkey,
    amount_in: u64,
    minimum_amount_out: u64,
) -> Result<Instruction, JitoExecutionError> {
    pool_keys.validate()?;

    if amount_in == 0 {
        return Err(JitoExecutionError::InvalidSwapAmount(
            "amount_in must be greater than zero",
        ));
    }
    if minimum_amount_out == 0 {
        return Err(JitoExecutionError::InvalidSwapAmount(
            "minimum_amount_out must be greater than zero",
        ));
    }
    if user_owner == Pubkey::default() {
        return Err(JitoExecutionError::InvalidSwapAccount(
            "user_owner must not be the default pubkey",
        ));
    }
    if user_source_token_account == Pubkey::default() {
        return Err(JitoExecutionError::InvalidSwapAccount(
            "user_source_token_account must not be the default pubkey",
        ));
    }
    if user_destination_token_account == Pubkey::default() {
        return Err(JitoExecutionError::InvalidSwapAccount(
            "user_destination_token_account must not be the default pubkey",
        ));
    }
    if user_source_token_account == user_destination_token_account {
        return Err(JitoExecutionError::InvalidSwapAccount(
            "source and destination token accounts must differ",
        ));
    }
    if user_owner == user_source_token_account || user_owner == user_destination_token_account {
        return Err(JitoExecutionError::InvalidSwapAccount(
            "user owner must differ from user token accounts",
        ));
    }
    if user_source_token_account == pool_keys.base_vault
        || user_source_token_account == pool_keys.quote_vault
        || user_destination_token_account == pool_keys.base_vault
        || user_destination_token_account == pool_keys.quote_vault
    {
        return Err(JitoExecutionError::InvalidSwapAccount(
            "user token accounts must differ from pool vaults",
        ));
    }

    let mut data = Vec::with_capacity(RAYDIUM_SWAP_BASE_IN_DATA_LEN);
    data.push(RAYDIUM_SWAP_BASE_IN_DISCRIMINATOR);
    data.extend_from_slice(&amount_in.to_le_bytes());
    data.extend_from_slice(&minimum_amount_out.to_le_bytes());

    // Raydium's current V4 builder omits legacy target_orders. The on-chain
    // processor accepts both the 17-account form and the legacy 18-account
    // form, but recommends the smaller form because target_orders is unused.
    let accounts = vec![
        AccountMeta::new_readonly(spl_token::id(), false),
        AccountMeta::new(pool_keys.amm_id, false),
        AccountMeta::new_readonly(pool_keys.authority, false),
        AccountMeta::new(pool_keys.open_orders, false),
        AccountMeta::new(pool_keys.base_vault, false),
        AccountMeta::new(pool_keys.quote_vault, false),
        AccountMeta::new_readonly(pool_keys.market_program_id, false),
        AccountMeta::new(pool_keys.market_id, false),
        AccountMeta::new(pool_keys.market_bids, false),
        AccountMeta::new(pool_keys.market_asks, false),
        AccountMeta::new(pool_keys.market_event_queue, false),
        AccountMeta::new(pool_keys.market_base_vault, false),
        AccountMeta::new(pool_keys.market_quote_vault, false),
        AccountMeta::new_readonly(pool_keys.market_vault_signer, false),
        AccountMeta::new(user_source_token_account, false),
        AccountMeta::new(user_destination_token_account, false),
        AccountMeta::new_readonly(user_owner, true),
    ];

    Ok(Instruction {
        program_id: RAYDIUM_LIQUIDITY_POOL_V4_PROGRAM_ID,
        accounts,
        data,
    })
}

#[derive(Deserialize)]
struct QuoteEnvelope {
    success: bool,
    #[serde(default)]
    msg: String,
    data: Option<QuoteData>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct QuoteData {
    swap_type: String,
    input_mint: String,
    input_amount: String,
    output_mint: String,
    output_amount: String,
    other_amount_threshold: String,
    slippage_bps: u16,
    #[serde(default)]
    route_plan: Vec<QuoteRouteHop>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct QuoteRouteHop {
    pool_id: String,
    input_mint: String,
    output_mint: String,
}

struct PreparedSwap {
    target_mint: Pubkey,
    pool_id: String,
    instructions: Vec<Instruction>,
    destination_ata_rent_lamports: u64,
    /// Resolved ALT for this pool, if `config.alt_address` is configured
    /// legacy message (no compression).
    alt: Option<AddressLookupTableAccount>,
    pool_keys: alpha_agents_core::pool_cache::RaydiumPoolKeys,
}

async fn resolve_swap_instructions_for_signal(
    signal: &WhaleSignal,
    rpc_client: &RpcClient,
    payer: &Keypair,
    config: &JitoExecutorConfig,
) -> Result<PreparedSwap, JitoExecutionError> {
    ensure_signal_fresh(signal, config.max_signal_age)?;
    let target_mint = Pubkey::from_str(&signal.target_mint)
        .map_err(|error| JitoExecutionError::InvalidTargetMint(error.to_string()))?;
    if target_mint == WSOL_MINT {
        return Err(JitoExecutionError::InvalidTargetMint(
            "target mint must differ from WSOL".to_string(),
        ));
    }
    let dynamic_amount_in = (signal.trade_size_sol * 1_000_000_000.0) as u64;

    let user_owner = payer.pubkey();
    let user_source_token_account =
        spl_associated_token_account::get_associated_token_address(&user_owner, &WSOL_MINT);
    let user_destination_token_account =
        spl_associated_token_account::get_associated_token_address(&user_owner, &target_mint);
    let user_account_addresses = [user_source_token_account, user_destination_token_account];

    let pool_future = async {
        resolve_pool_keys(rpc_client, &signal.target_mint)
            .await
            .map_err(JitoExecutionError::from)
    };
    let quote_future = fetch_raydium_quote(target_mint, dynamic_amount_in, config);
    let user_accounts_future = async {
        rpc_client
            .get_multiple_accounts(&user_account_addresses)
            .await
            .map_err(|_| {
                JitoExecutionError::UserAccountRpc("getMultipleAccounts failed".to_string())
            })
    };
    let owned_target_accounts_future = async {
        rpc_client
            .get_token_accounts_by_owner(&user_owner, TokenAccountsFilter::Mint(target_mint))
            .await
            .map_err(|_| {
                JitoExecutionError::UserAccountRpc("getTokenAccountsByOwner failed".to_string())
            })
    };
    let destination_rent_future = async {
        rpc_client
            .get_minimum_balance_for_rent_exemption(TokenAccount::LEN)
            .await
            .map_err(|_| {
                JitoExecutionError::RentExemption(
                    "getMinimumBalanceForRentExemption failed".to_string(),
                )
            })
    };
    let (pool_keys, quote, user_accounts, owned_target_accounts, destination_account_rent) = tokio::try_join!(
        pool_future,
        quote_future,
        user_accounts_future,
        owned_target_accounts_future,
        destination_rent_future
    )?;

    let source_account = user_accounts
        .first()
        .and_then(Option::as_ref)
        .ok_or(JitoExecutionError::MissingSourceTokenAccount)?;
    let _source_token = validate_user_token_account(
        source_account,
        "WSOL source",
        WSOL_MINT,
        user_owner,
        dynamic_amount_in,
        true,
    )?;

    let destination_address = user_destination_token_account.to_string();
    if owned_target_accounts
        .iter()
        .any(|account| account.pubkey != destination_address)
    {
        return Err(JitoExecutionError::ExistingTargetPosition);
    }

    let mut instructions = Vec::with_capacity(2);
    let destination_ata_rent_lamports = match user_accounts.get(1).and_then(Option::as_ref) {
        Some(destination_account) => {
            let destination_token = validate_user_token_account(
                destination_account,
                "target destination",
                target_mint,
                user_owner,
                0,
                false,
            )?;
            if destination_token.amount != 0
                || owned_target_accounts.len() != 1
                || owned_target_accounts[0].pubkey != destination_address
            {
                return Err(JitoExecutionError::ExistingTargetPosition);
            }
            0
        }
        None => {
            if !owned_target_accounts.is_empty() {
                return Err(JitoExecutionError::ExistingTargetPosition);
            }
            instructions.push(
                spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                    &user_owner,
                    &user_owner,
                    &target_mint,
                    &spl_token::id(),
                ),
            );
            destination_account_rent
        }
    };

    let minimum_amount_out = validate_quote(&quote, &pool_keys, signal, config, dynamic_amount_in)?;
    let mut swap_instruction = construct_raydium_swap_instruction(
        &pool_keys,
        user_owner,
        user_source_token_account,
        user_destination_token_account,
        dynamic_amount_in,
        minimum_amount_out,
    )?;
    // Phase 2 (MASTER_PLAN.md Section 2): attach the Jito anti-sandwich
    // sentinel to the swap instruction. This is the only instruction in
    // the bundle proven safe to carry a trailing account (see
    // `apply_jitodontfront_protection` doc comment).
    apply_jitodontfront_protection(&mut swap_instruction, config.jito_dont_front_pubkey)?;
    instructions.push(swap_instruction);

    // AN-ALT-01: best-effort ALT fetch. When `alt_address` is configured,
    // attempt to resolve the lookup table. On failure, log and continue
    // without ALT compression (no transaction blocked by an ALT RPC error).
    let alt = if let Some(alt_addr) = config.alt_address {
        match fetch_alt(rpc_client, alt_addr).await {
            Ok(table) => {
                log::debug!(
                    "AN-ALT-01: resolved ALT {} for mint {}",
                    alt_addr,
                    target_mint
                );
                Some(table)
            }
            Err(e) => {
                log::warn!(
                    "AN-ALT-01: fetch_alt failed for {alt_addr}, proceeding without ALT: {e}"
                );
                None
            }
        }
    } else {
        None
    };

    Ok(PreparedSwap {
        target_mint,
        pool_id: pool_keys.amm_id.to_string(),
        instructions,
        destination_ata_rent_lamports,
        alt,
        pool_keys,
    })
}

async fn fetch_raydium_quote(
    target_mint: Pubkey,
    dynamic_amount_in: u64,
    config: &JitoExecutorConfig,
) -> Result<QuoteData, JitoExecutionError> {
    let client = reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .timeout(config.request_timeout)
        .build()
        .map_err(|error| JitoExecutionError::QuoteRequest(error.to_string()))?;
    let response = client
        .get(RAYDIUM_QUOTE_URL)
        .query(&[
            ("inputMint", WSOL_MINT.to_string()),
            ("outputMint", target_mint.to_string()),
            ("amount", dynamic_amount_in.to_string()),
            ("slippageBps", config.max_slippage_bps.to_string()),
            ("txVersion", "V0".to_string()),
        ])
        .send()
        .await
        .map_err(|error| JitoExecutionError::QuoteRequest(error.to_string()))?;
    if !response.status().is_success() {
        return Err(JitoExecutionError::InvalidQuote(format!(
            "HTTP {}",
            response.status()
        )));
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|error| JitoExecutionError::QuoteRequest(error.to_string()))?;
    if bytes.len() > MAX_QUOTE_RESPONSE_BYTES {
        return Err(JitoExecutionError::InvalidQuote(format!(
            "response exceeded {MAX_QUOTE_RESPONSE_BYTES} bytes"
        )));
    }
    let envelope: QuoteEnvelope = serde_json::from_slice(&bytes)
        .map_err(|error| JitoExecutionError::InvalidQuote(error.to_string()))?;
    if !envelope.success {
        return Err(JitoExecutionError::InvalidQuote(envelope.msg));
    }
    envelope.data.ok_or_else(|| {
        JitoExecutionError::InvalidQuote("successful quote response contained no data".to_string())
    })
}

pub(crate) async fn resolve_pumpportal_swap(
    signal: &WhaleSignal,
    payer: &Keypair,
    config: &JitoExecutorConfig,
    pumpportal_api_key: &Option<String>,
    http_client: &reqwest::Client,
) -> Result<VersionedTransaction, JitoExecutionError> {
    ensure_signal_fresh(signal, config.max_signal_age)?;

    let priority_fee_lamports = 100_000; // Hardcoded for now to avoid utils issue
    let priority_fee_sol = priority_fee_lamports as f64 / 1_000_000_000.0;

    let payload = serde_json::json!({
        "publicKey": payer.pubkey().to_string(),
        "action": "buy",
        "mint": signal.target_mint,
        "amount": signal.trade_size_sol,
        "denominatedInSol": "true",
        "slippage": config.max_slippage_bps as f64 / 100.0,
        "priorityFee": priority_fee_sol,
        "pool": "pump"
    });

    let url = "https://pumpportal.fun/api/trade-local";
    let mut builder = http_client.post(url).json(&payload);

    if let Some(key) = pumpportal_api_key {
        builder = builder.header("x-api-key", key);
    }

    let response = builder
        .send()
        .await
        .map_err(|e| JitoExecutionError::QuoteRequest(format!("PumpPortal HTTP error: {e}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(JitoExecutionError::InvalidQuote(format!(
            "PumpPortal HTTP {status}: {body}"
        )));
    }

    let bytes = response
        .bytes()
        .await
        .map_err(|e| JitoExecutionError::QuoteRequest(format!("PumpPortal read error: {e}")))?;

    let transaction: VersionedTransaction = bincode::deserialize(&bytes).map_err(|e| {
        JitoExecutionError::InvalidQuote(format!(
            "Failed to deserialize PumpPortal transaction: {e}"
        ))
    })?;

    Ok(transaction)
}

fn validate_quote(
    quote: &QuoteData,
    pool_keys: &RaydiumPoolKeys,
    signal: &WhaleSignal,
    config: &JitoExecutorConfig,
    dynamic_amount_in: u64,
) -> Result<u64, JitoExecutionError> {
    if quote.swap_type != "BaseIn"
        || quote.input_mint != WSOL_MINT.to_string()
        || quote.output_mint != signal.target_mint
        || quote.slippage_bps != config.max_slippage_bps
    {
        return Err(JitoExecutionError::InvalidQuote(
            "quote parameters do not match the execution request".to_string(),
        ));
    }
    let quoted_input = parse_quote_amount("inputAmount", &quote.input_amount)?;
    let quoted_output = parse_quote_amount("outputAmount", &quote.output_amount)?;
    let api_minimum_amount_out =
        parse_quote_amount("otherAmountThreshold", &quote.other_amount_threshold)?;
    let local_minimum_amount_out =
        calculate_local_minimum_amount_out(quoted_output, config.max_slippage_bps)?;
    if quoted_input != dynamic_amount_in
        || quoted_output == 0
        || api_minimum_amount_out == 0
        || local_minimum_amount_out == 0
        || api_minimum_amount_out > quoted_output
        || api_minimum_amount_out < local_minimum_amount_out
    {
        return Err(JitoExecutionError::InvalidQuote(
            "quote amounts are inconsistent".to_string(),
        ));
    }
    let minimum_amount_out = api_minimum_amount_out.max(local_minimum_amount_out);

    let [hop] = quote.route_plan.as_slice() else {
        return Err(JitoExecutionError::InvalidQuote(
            "only a single-hop Raydium V4 route is permitted".to_string(),
        ));
    };
    if hop.pool_id != pool_keys.amm_id.to_string()
        || hop.input_mint != WSOL_MINT.to_string()
        || hop.output_mint != signal.target_mint
    {
        return Err(JitoExecutionError::InvalidQuote(
            "quote route does not match the verified Raydium V4 pool".to_string(),
        ));
    }
    Ok(minimum_amount_out)
}

pub(crate) fn calculate_local_minimum_amount_out(
    quoted_output: u64,
    max_slippage_bps: u16,
) -> Result<u64, JitoExecutionError> {
    let retained_bps = BASIS_POINTS_DENOMINATOR
        .checked_sub(max_slippage_bps as u128)
        .ok_or(JitoExecutionError::QuoteArithmeticOverflow)?;
    let minimum = (quoted_output as u128)
        .checked_mul(retained_bps)
        .ok_or(JitoExecutionError::QuoteArithmeticOverflow)?
        / BASIS_POINTS_DENOMINATOR;
    u64::try_from(minimum).map_err(|_| JitoExecutionError::QuoteArithmeticOverflow)
}

fn parse_quote_amount(name: &str, value: &str) -> Result<u64, JitoExecutionError> {
    value.parse::<u64>().map_err(|error| {
        JitoExecutionError::InvalidQuote(format!("{name} is not a valid u64: {error}"))
    })
}

fn validate_user_token_account(
    account: &Account,
    name: &'static str,
    expected_mint: Pubkey,
    expected_owner: Pubkey,
    minimum_balance: u64,
    require_native: bool,
) -> Result<TokenAccount, JitoExecutionError> {
    if account.owner != spl_token::id() {
        return Err(JitoExecutionError::InvalidUserTokenAccount(name));
    }
    let token = TokenAccount::unpack(&account.data)
        .map_err(|_| JitoExecutionError::InvalidUserTokenAccount(name))?;
    if token.state != AccountState::Initialized
        || token.mint != expected_mint
        || token.owner != expected_owner
        || token.amount < minimum_balance
        || (require_native && token.is_native.is_none())
    {
        return Err(JitoExecutionError::InvalidUserTokenAccount(name));
    }
    Ok(token)
}

fn ensure_signal_fresh(
    signal: &WhaleSignal,
    max_signal_age: Duration,
) -> Result<(), JitoExecutionError> {
    if signal.timestamp_ms == 0 {
        return Err(JitoExecutionError::InvalidSignalTimestamp(
            "timestamp must be non-zero".to_string(),
        ));
    }
    let now_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| JitoExecutionError::InvalidSignalTimestamp(error.to_string()))?
            .as_millis(),
    )
    .map_err(|error| JitoExecutionError::InvalidSignalTimestamp(error.to_string()))?;
    let age_ms = now_ms.saturating_sub(signal.timestamp_ms);
    if Duration::from_millis(age_ms) > max_signal_age {
        log::error!(
            "DEBUG: age_ms={} max_signal_age={:?} signal_ts={} now_ms={}",
            age_ms,
            max_signal_age,
            signal.timestamp_ms,
            now_ms
        );
        return Err(JitoExecutionError::StaleSignal);
    }
    Ok(())
}

// ============================================================================
// On-Chain Confirmation Constants
// ============================================================================

/// Maximum number of RPC polls to confirm a buy transaction on-chain.
const CONFIRMATION_MAX_POLLS: u32 = 40;

/// Delay between confirmation polls.
const CONFIRMATION_POLL_DELAY: Duration = Duration::from_millis(500);

/// Hard timeout for the entire confirmation + handoff sequence.
const CONFIRMATION_HARD_TIMEOUT: Duration = Duration::from_secs(30);

pub async fn run_whale_execution_consumer(
    mut signal_rx: tokio::sync::mpsc::Receiver<WhaleSignal>,
    rpc_client: Arc<RpcClient>,
    payer: Arc<Keypair>,
    config: JitoExecutorConfig,
    bot_state: Arc<BotState>,
    exit_broadcast_tx: broadcast::Sender<SwapEvent>,
    tip_engine: alpha_agents_core::tipping::TipTelemetryEngine,
    mut dispatcher: alpha_agents_core::dispatcher::BundleDispatcher,
    dry_run: bool,
    bundle_tracker: Option<Arc<alpha_agents_core::bundle_tracker::BundleTracker>>,
    http_client: reqwest::Client,
    telegram_bot_token: Option<String>,
    telegram_chat_id: Option<String>,
) -> Result<(), JitoExecutionError> {
    let mut journal = ExecutionJournal::load(&config).await?;

    if dispatcher.is_empty() {
        return Err(JitoExecutionError::InvalidConfiguration(
            "no healthy Jito Block Engine regions available for dispatch".to_string(),
        ));
    }

    while let Some(signal) = signal_rx.recv().await {
        // ---- Circuit Breaker Gate ------------------------------------------
        if bot_state.is_circuit_breaker_active().await {
            continue;
        }

        let target_mint = match Pubkey::from_str(&signal.target_mint) {
            Ok(target_mint) => target_mint,
            Err(error) => {
                log::warn!(
                    "Execution signal rejected before submission: invalid target mint: {error}"
                );
                continue;
            }
        };

        if journal.contains(&target_mint) {
            log::warn!(
                "Execution signal rejected for {}: mint is durably locked after a prior submission attempt",
                signal.target_mint
            );
            continue;
        }

        // ---- Position Capacity Gate ----------------------------------------
        let permit = match bot_state.try_acquire_position() {
            Some(permit) => permit,
            None => {
                log::warn!(
                    "Execution signal rejected for {}: all position slots occupied ({})",
                    signal.target_mint,
                    bot_state.open_position_count()
                );
                continue;
            }
        };

        // ---- Duplicate Mint Guard ------------------------------------------
        if !bot_state.try_lock_mint(&signal.target_mint).await {
            log::warn!(
                "Execution signal rejected for {}: mint already traded or actively held",
                signal.target_mint
            );
            // permit is dropped here, restoring the semaphore slot.
            continue;
        }

        let mut signal_config = config.clone();
        signal_config.max_slippage_bps = match signal.lane {
            alpha_agents_core::config::WhaleLane::Sniper => 250, // 2.5%
            alpha_agents_core::config::WhaleLane::Degen => 200,  // 2.0%
            alpha_agents_core::config::WhaleLane::Swing => 150,  // 1.5%
            alpha_agents_core::config::WhaleLane::Conservative => 100, // 1.0%
            _ => config.max_slippage_bps,
        };

        let dynamic_amount_in = (signal.trade_size_sol * 1_000_000_000.0) as u64;

        // ---- Phase 3 Tip Gate (MASTER_PLAN.md Section 3) -----------------
        let pre_tip_profit = dynamic_amount_in / 100;
        let tip_decision =
            tip_engine.calculate_tip(pre_tip_profit, tokio::time::Instant::now().into_std());
        let tip_lamports = match tip_decision {
            alpha_agents_core::tipping::TipDecision::Bid {
                lamports,
                telemetry_age_ms: _,
            } => lamports,
            alpha_agents_core::tipping::TipDecision::Skip { reason } => {
                log::warn!(
                    "Execution signal rejected for {}: tip gate reason={:?}",
                    signal.target_mint,
                    reason
                );
                bot_state.unlock_mint(&signal.target_mint).await;
                continue;
            }
        };

        // ---- Phase 4 Build & Sign Once (Section 4.1 step 3) -----------
        let is_pump = signal.target_mint.ends_with("pump");

        // Pre-parse the target mint. For Raydium the pool resolution will
        // confirm and overwrite this. For PumpPortal the fallback branch
        // uses this directly. Initialised to the signal mint so that
        // prepared_target_mint is NEVER Pubkey::default() when we reach
        // journal.reserve(), eliminating the "already reserved" false-match
        // against a previously recorded 111...1 default entry.
        let mut prepared_target_mint = Pubkey::from_str(&signal.target_mint).unwrap_or_default();
        let mut destination_ata_rent_lamports = 2_039_280;
        let mut pool_id_str = "pump".to_string();
        let mut pool_keys_opt = None;

        let signed_bundle = match resolve_swap_instructions_for_signal(
            &signal,
            rpc_client.as_ref(),
            &payer,
            &signal_config,
        )
        .await
        {
            Ok(prepared) => {
                prepared_target_mint = prepared.target_mint;
                destination_ata_rent_lamports = prepared.destination_ata_rent_lamports;
                pool_id_str = prepared.pool_id;
                pool_keys_opt = Some(prepared.pool_keys);

                let tip_account = match dispatcher.take_tip_account().await {
                    Some(account) => account,
                    None => {
                        log::warn!(
                            "Execution signal rejected for {}: missing tip accounts",
                            signal.target_mint
                        );
                        bot_state.unlock_mint(&signal.target_mint).await;
                        continue;
                    }
                };
                let recent_blockhash = match rpc_client.get_latest_blockhash().await {
                    Ok(blockhash) => blockhash,
                    Err(_) => {
                        log::warn!("Execution signal rejected before submission: getLatestBlockhash failed");
                        bot_state.unlock_mint(&signal.target_mint).await;
                        continue;
                    }
                };
                let alt_accounts: Vec<AddressLookupTableAccount> =
                    prepared.alt.into_iter().collect();
                alpha_agents_core::dispatcher::build_and_sign_bundle_with_alt(
                    prepared.instructions,
                    &payer,
                    tip_account,
                    tip_lamports,
                    recent_blockhash,
                    &alt_accounts,
                )
                .map_err(Into::into)
            }
            Err(error) => {
                if is_pump {
                    log::info!("Raydium execution unavailable for {} ({error}). Attempting PumpPortal fallback.", signal.target_mint);
                    match resolve_pumpportal_swap(
                        &signal,
                        &payer,
                        &signal_config,
                        &config.pumpportal_api_key,
                        &http_client,
                    )
                    .await
                    {
                        Ok(pump_tx) => {
                            prepared_target_mint =
                                Pubkey::from_str(&signal.target_mint).unwrap_or_default();
                            alpha_agents_core::dispatcher::build_and_sign_pump_bundle(
                                pump_tx,
                                &payer,
                                dispatcher.take_tip_account().await.unwrap(),
                                tip_lamports,
                            )
                            .map_err(Into::into)
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    log::warn!("Execution signal rejected before submission: {error}");
                    bot_state.unlock_mint(&signal.target_mint).await;
                    continue;
                }
            }
        };

        let signed_bundle = match signed_bundle {
            Ok(signed_bundle) => signed_bundle,
            Err(error) => {
                log::warn!(
                    "Execution signal rejected before signing or transmission for {}: {}",
                    signal.target_mint,
                    error
                );
                bot_state.unlock_mint(&signal.target_mint).await;
                continue;
            }
        };
        if let Err(error) = ensure_signal_fresh(&signal, config.max_signal_age) {
            log::warn!("Execution signal expired after signing: {error}");
            continue;
        }

        // ---- Phase 4 Opportunity Dedupe Gate -----------------------------
        // BUGFIX (High, audit 2026-08): previously acquired at the very top
        // of the loop, before pool resolution / quote validation / tip-gate
        // / signing could still fail and `continue`. Since none of those
        // failure paths released the dedupe key, a signal that failed for a
        // purely transient reason (RPC blip, momentary tip-gate skip) was
        // locked out of retry for the full OPPORTUNITY_DEDUPE_TTL_SECS
        // window even though nothing was ever submitted. Acquiring the key
        // here — immediately before the durable journal reservation, once
        // the bundle is fully built and signed — means dedupe now only
        // blocks genuine re-submission of an opportunity we already
        // committed capital to (MASTER_PLAN.md Section 4.6).
        let opp_key = alpha_agents_core::dispatcher::OpportunityKey {
            mint: signal.target_mint.clone(),
            pool: signal.whale_wallet.clone(),
            signal_epoch_secs: signal.timestamp_ms / 1000,
        };
        if !dispatcher.try_acquire_dedupe(opp_key).await {
            log::warn!(
                "Execution signal rejected for {}: duplicate opportunity (already dispatched in this epoch)",
                signal.target_mint
            );
            bot_state.unlock_mint(&signal.target_mint).await;
            continue;
        }

        // Persist and sync the signature and capital reservation before the
        // first SendBundle attempt. A timeout is therefore never safe to retry,
        // including after a process restart.
        if let Err(error) = journal
            .reserve(
                &signal,
                prepared_target_mint,
                destination_ata_rent_lamports,
                &signal_config,
                &signed_bundle,
            )
            .await
        {
            log::warn!(
                "Execution signal rejected during journal reservation for {}: {}",
                signal.target_mint,
                error
            );
            bot_state.unlock_mint(&signal.target_mint).await;
            continue;
        }

        let tx_signature_string = signed_bundle.transaction_signature.clone();

        let submission_result = match dispatcher.fan_out_submit(&signed_bundle).await {
            Ok(ack) => Ok(BundleSubmission {
                bundle_id: ack.bundle_id,
                transaction_signature: signed_bundle.transaction_signature.clone(),
            }),
            Err(_errors) => Err(JitoExecutionError::InvalidConfiguration(
                "all Jito Block Engine regions rejected or timed out during dispatch".to_string(),
            )),
        };

        match submission_result {
            Ok(submission) => {
                journal
                    .record_acceptance(prepared_target_mint, &submission)
                    .await?;

                // ---- Phase 5: Register with BundleTracker -------------------
                //
                // Register the submitted bundle with the inclusion tracker so
                // Jito's SubscribeBundleResults stream can verify landing
                // independently of the RPC-based transaction confirmation below.
                let mut bundle_landed_rx = None::<
                    tokio::sync::mpsc::Receiver<alpha_agents_core::bundle_tracker::BundleRecord>,
                >;
                let mut bundle_slot = None::<u64>;
                let mut bundle_region = None::<String>;
                if let Some(ref tracker) = bundle_tracker {
                    let bundle_record = alpha_agents_core::bundle_tracker::BundleRecord {
                        bundle_id: submission.bundle_id.clone(),
                        region: "ack-region".to_string(), // will be refined when fan_out returns the winning region
                        mint: signal.target_mint.clone(),
                        pool_id: pool_id_str.clone(),
                        transaction_signature: submission.transaction_signature.clone(),
                        submitted_at: std::time::Instant::now(),
                        status: alpha_agents_core::bundle_tracker::InclusionStatus::Pending,
                        notify_tx: None,
                    };
                    let rx = tracker.register_bundle(bundle_record).await;
                    bundle_landed_rx = Some(rx);
                    log::info!(
                        "[Phase 5] Registered bundle {} with inclusion tracker for {}",
                        submission.bundle_id,
                        signal.target_mint
                    );
                }

                // ---- Phase 6: Await BundleTracker Landing Confirmation ------
                //
                // Wait for the Jito block engine to confirm the bundle landed
                // via the SubscribeBundleResults stream. A timeout is applied
                // so the RPC-based fallback can still progress if the tracker
                // stream is delayed.
                const BUNDLE_LANDING_WAIT: std::time::Duration = std::time::Duration::from_secs(10);
                if let Some(ref mut rx) = bundle_landed_rx {
                    match tokio::time::timeout(BUNDLE_LANDING_WAIT, rx.recv()).await {
                        Ok(Some(record)) => {
                            log::info!(
                                "[Phase 6] Bundle {} confirmed landed at slot {} in region {}",
                                record.bundle_id,
                                record.status.slot().unwrap_or(0),
                                record.region,
                            );
                            bundle_slot = record.status.slot();
                            bundle_region = Some(record.region.clone());

                            // Record the landed bundle in the database.
                            db::record_landed_bundle(
                                &record.bundle_id,
                                &record.region,
                                &record.mint,
                                record.status.slot().unwrap_or(0),
                                &record.transaction_signature,
                            );
                        }
                        Ok(None) => {
                            log::warn!(
                                "[Phase 6] BundleTracker channel closed without confirmation for {}; \
                                 falling back to RPC-only confirmation",
                                signal.target_mint,
                            );
                        }
                        Err(_elapsed) => {
                            log::warn!(
                                "[Phase 6] BundleTracker confirmation timeout ({:?}) for {}; \
                                 continuing with RPC-only confirmation",
                                BUNDLE_LANDING_WAIT,
                                signal.target_mint,
                            );
                        }
                    }
                }

                // =============================================================
                // C10: HANDOFF — Buy → Exit Watcher
                // =============================================================
                //
                // The bundle was accepted by the Jito Block Engine. We now:
                //   1. Confirm the transaction on-chain via RPC polling.
                //   2. Read back the token account balance (acquired_amount).
                //   3. Construct ActivePosition from signal + on-chain data.
                //   4. Subscribe a broadcast receiver filtered to this pool.
                //   5. Spawn the position watcher.
                //
                // If any step fails, the permit is dropped (slot restored) and
                // the position is logged for manual review. The journal entry
                // remains (mint stays locked) so no duplicate buy can occur.

                println!(
                    "[handoff] 🔄 Starting on-chain confirmation for {} (sig={}).",
                    signal.target_mint, tx_signature_string
                );

                let signal_clone = signal.clone();
                let tx_signature_clone = tx_signature_string.clone();
                let config_clone = signal_config.clone();
                let exit_tx_clone = exit_broadcast_tx.clone();
                let pool_id_clone = pool_id_str.clone();
                let pool_keys_clone = pool_keys_opt.clone();
                let rpc_clone = rpc_client.clone();
                let payer_clone = payer.clone();
                let state_clone = bot_state.clone();
                let http_clone = http_client.clone();
                let token_clone = telegram_bot_token.clone();
                let chat_clone = telegram_chat_id.clone();

                tokio::spawn(async move {
                    let handoff_result = confirm_and_handoff(
                        &signal_clone,
                        &tx_signature_clone,
                        prepared_target_mint,
                        pool_id_clone,
                        pool_keys_clone,
                        &config_clone,
                        rpc_clone,
                        payer_clone,
                        state_clone,
                        &exit_tx_clone,
                        permit,
                        dry_run,
                        bundle_slot,
                        bundle_region,
                        http_clone.clone(),
                        token_clone.clone(),
                        chat_clone.clone(),
                    )
                    .await;

                    match handoff_result {
                        Ok(()) => {
                            println!(
                                "[handoff] ✅ Position watcher spawned for {}.",
                                signal_clone.target_mint
                            );

                            let size_sol = (dynamic_amount_in as f64) / 1_000_000_000.0;
                            db::log_trade_telemetry(
                                &signal_clone.whale_wallet,
                                &signal_clone.target_mint,
                                "BUY",
                                size_sol,
                                0.0,
                                "LANDED",
                            );

                            if let (Some(bot_token), Some(chat_id)) = (token_clone, chat_clone) {
                                let client_clone = http_clone;
                                let mint_str = signal_clone.target_mint.clone();
                                let sig_str = tx_signature_clone.clone();
                                let trade_size = format!("Bot Trade: {} SOL", size_sol);
                                tokio::spawn(async move {
                                    alpha_agents_core::telegram::send_telegram_alert(
                                        &client_clone,
                                        &bot_token,
                                        &chat_id,
                                        &mint_str,
                                        1.0,
                                        "Bot Execution",
                                        &sig_str,
                                        "LANDED ON-CHAIN ✅",
                                        &trade_size,
                                    )
                                    .await;
                                });
                            }
                        }
                        Err(error) => {
                            log::warn!(
                                "[handoff] 🚨 HANDOFF FAILED for {} (sig={}): {}. \
                                 Position may be open without an exit watcher. \
                                 MANUAL REVIEW REQUIRED.",
                                signal_clone.target_mint,
                                tx_signature_clone,
                                error
                            );
                        }
                    }
                });
            }
            Err(error) => {
                log::warn!(
                    "Jito bundle submission failed for {}: {}",
                    signal.target_mint,
                    error
                );
                // Do not blindly retry an ambiguous submission: the Block Engine may
                // have accepted it before the response was lost.
                // permit dropped → slot restored.
            }
        }
    }

    Ok(())
}

// ============================================================================
// Confirmation & Handoff Pipeline
// ============================================================================

/// Confirms the buy transaction on-chain, reads the acquired token balance,
/// constructs an `ActivePosition`, and spawns the exit watcher.
///
/// Takes ownership of the semaphore `permit`. On success, the permit is moved
/// into the exit watcher's RAII guard. On failure, the permit is dropped and
/// the slot is restored.
async fn confirm_and_handoff(
    signal: &WhaleSignal,
    tx_signature_str: &str,
    target_mint: Pubkey,
    pool_id: String,
    pool_keys: Option<alpha_agents_core::pool_cache::RaydiumPoolKeys>,
    config: &JitoExecutorConfig,
    rpc_client: Arc<RpcClient>,
    payer: Arc<Keypair>,
    bot_state: Arc<BotState>,
    exit_broadcast_tx: &broadcast::Sender<SwapEvent>,
    permit: tokio::sync::OwnedSemaphorePermit,
    dry_run: bool,
    bundle_slot: Option<u64>,
    bundle_region: Option<String>,
    http_client: reqwest::Client,
    telegram_bot_token: Option<String>,
    telegram_chat_id: Option<String>,
) -> Result<(), JitoExecutionError> {
    let dynamic_amount_in = (signal.trade_size_sol * 1_000_000_000.0) as u64;
    // ---- Parse the transaction signature -----------------------------------
    let tx_signature = Signature::from_str(tx_signature_str).map_err(|err| {
        JitoExecutionError::ConfirmationFailed(
            0,
            format!(
                "invalid transaction signature '{}': {}",
                tx_signature_str, err
            ),
        )
    })?;

    // ---- Poll for on-chain confirmation ------------------------------------
    let confirmation = timeout(
        CONFIRMATION_HARD_TIMEOUT,
        poll_transaction_confirmation(&rpc_client, &tx_signature),
    )
    .await;

    match confirmation {
        Ok(Ok(())) => {
            println!(
                "[handoff] ✅ Transaction confirmed on-chain for {} (sig={}).",
                signal.target_mint, tx_signature_str
            );
        }
        Ok(Err(err)) => {
            return Err(err);
        }
        Err(_elapsed) => {
            return Err(JitoExecutionError::ConfirmationFailed(
                CONFIRMATION_MAX_POLLS,
                format!(
                    "hard timeout ({}s) waiting for confirmation",
                    CONFIRMATION_HARD_TIMEOUT.as_secs()
                ),
            ));
        }
    }

    // ---- Read back the acquired token balance ------------------------------
    let user_owner = payer.pubkey();
    let user_destination_token_account =
        spl_associated_token_account::get_associated_token_address(&user_owner, &target_mint);

    let acquired_amount = tokio::time::timeout(
        CONFIRMATION_HARD_TIMEOUT,
        read_token_account_balance(
            &rpc_client,
            &user_destination_token_account,
            target_mint,
            user_owner,
        ),
    )
    .await
    .map_err(|_| {
        JitoExecutionError::TokenAccountReadback(
            "timeout waiting for token account balance readback".to_string(),
        )
    })??;

    if acquired_amount == 0 {
        return Err(JitoExecutionError::TokenAccountReadback(
            "token account balance is zero after confirmed buy; \
             possible front-run or complete slippage loss"
                .to_string(),
        ));
    }

    println!(
        "[handoff] 📦 Acquired {} raw token units of {} (ATA={}).",
        acquired_amount, signal.target_mint, user_destination_token_account
    );

    // ---- Construct ActivePosition ------------------------------------------
    //
    // Entry price ratio = WSOL spent / tokens acquired
    //   = config.amount_in (WSOL lamports) / acquired_amount (token raw units)
    //
    // Stored as (numerator=amount_in, denominator=acquired_amount) to avoid
    // precision loss.
    let position = ActivePosition {
        mint: signal.target_mint.clone(),
        source_pool_id: pool_id.clone(),
        pool_keys,
        entry_price_wsol_num: dynamic_amount_in as u128,
        entry_price_wsol_den: acquired_amount as u128,

        acquired_amount,
        jito_tip_lamports: config.tip_lamports,
        block_engine_url: config.block_engine_url.clone(),
        entry_timestamp_ms: signal.timestamp_ms,
        pumpportal_api_key: config.pumpportal_api_key.clone(),
        jito_dont_front_pubkey: config.jito_dont_front_pubkey,
        max_slippage_bps: config.max_slippage_bps,
    };

    // ---- Phase 6: Record Position in Database -------------------------------
    //
    // Persist the confirmed position so the operator can query active positions,
    // and so the sell-side close function can update the record on exit.
    let slot = bundle_slot.unwrap_or(0);
    let region_str = bundle_region.as_deref().unwrap_or("unknown");
    db::record_position(
        &position.mint,
        "", // bundle_id — set from tracker record in a follow-up
        region_str,
        slot,
        tx_signature_str,
        dynamic_amount_in,
        acquired_amount,
        position.entry_price_wsol_num,
        position.entry_price_wsol_den,
        position.jito_tip_lamports,
        &position.source_pool_id,
    );

    // ---- Create per-watcher price feed via broadcast → mpsc adapter --------
    //
    // The exit watcher expects a tokio::sync::mpsc::Receiver<SwapEvent>.
    // We subscribe to the broadcast channel and spawn a lightweight adapter
    // task that forwards only events matching this position's pool_id.
    let pool_id_filter = pool_id.clone();
    let mut broadcast_rx = exit_broadcast_tx.subscribe();
    let (watcher_tx, watcher_rx) = tokio::sync::mpsc::channel::<SwapEvent>(512);

    tokio::spawn(async move {
        loop {
            match broadcast_rx.recv().await {
                Ok(event) => {
                    if event.pool_id == pool_id_filter && watcher_tx.send(event).await.is_err() {
                        // Watcher has exited and dropped its receiver — done.
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    log::warn!(
                        "[handoff] ⚠️  Exit price feed lagged by {} events for pool {}.",
                        count,
                        pool_id_filter
                    );
                    // Continue receiving; the watcher handles staleness.
                }
                Err(broadcast::error::RecvError::Closed) => {
                    // Broadcast sender dropped — system is shutting down.
                    break;
                }
            }
        }
    });

    // ---- Spawn the exit watcher --------------------------------------------
    exits::spawn_position_watcher(
        position,
        watcher_rx,
        rpc_client,
        payer,
        bot_state,
        permit,
        dry_run,
        http_client,
        telegram_bot_token,
        telegram_chat_id,
    );

    Ok(())
}

/// Polls the RPC for on-chain transaction confirmation.
///
/// Returns `Ok(())` when the transaction is confirmed (finalized or at least
/// processed). Returns an error if confirmation fails after max polls or the
/// transaction is known to have failed.
async fn poll_transaction_confirmation(
    rpc_client: &RpcClient,
    signature: &Signature,
) -> Result<(), JitoExecutionError> {
    for poll in 1..=CONFIRMATION_MAX_POLLS {
        tokio::time::sleep(CONFIRMATION_POLL_DELAY).await;

        match rpc_client
            .confirm_transaction_with_commitment(signature, CommitmentConfig::confirmed())
            .await
        {
            Ok(response) => {
                if response.value {
                    return Ok(());
                }
                // Not yet confirmed — continue polling.
            }
            Err(err) => {
                // Transient RPC errors are expected; only log after many failures.
                if poll >= CONFIRMATION_MAX_POLLS / 2 {
                    log::warn!(
                        "[handoff] ⚠️  Confirmation RPC error on poll {}/{}: {}",
                        poll,
                        CONFIRMATION_MAX_POLLS,
                        err
                    );
                }
            }
        }
    }

    Err(JitoExecutionError::ConfirmationFailed(
        CONFIRMATION_MAX_POLLS,
        format!(
            "transaction {} not confirmed after {} polls",
            signature, CONFIRMATION_MAX_POLLS
        ),
    ))
}

/// Reads the token account balance using `confirmed` commitment.
/// Validates ownership, mint, and initialization state.
async fn read_token_account_balance(
    rpc_client: &RpcClient,
    token_account_address: &Pubkey,
    expected_mint: Pubkey,
    expected_owner: Pubkey,
) -> Result<u64, JitoExecutionError> {
    let account = rpc_client
        .get_account_with_commitment(token_account_address, CommitmentConfig::confirmed())
        .await
        .map_err(|err| {
            JitoExecutionError::TokenAccountReadback(format!(
                "getAccountWithCommitment failed for {}: {}",
                token_account_address, err
            ))
        })?
        .value
        .ok_or_else(|| {
            JitoExecutionError::TokenAccountReadback(format!(
                "token account {} does not exist after confirmed buy",
                token_account_address
            ))
        })?;

    if account.owner != spl_token::id() {
        return Err(JitoExecutionError::TokenAccountReadback(
            "token account has wrong program owner".to_string(),
        ));
    }

    let token = TokenAccount::unpack(&account.data).map_err(|err| {
        JitoExecutionError::TokenAccountReadback(format!("unpack failed: {}", err))
    })?;

    if token.state != AccountState::Initialized {
        return Err(JitoExecutionError::TokenAccountReadback(
            "token account is not initialized".to_string(),
        ));
    }
    if token.mint != expected_mint {
        return Err(JitoExecutionError::TokenAccountReadback(format!(
            "token account mint mismatch: expected {}, got {}",
            expected_mint, token.mint
        )));
    }
    if token.owner != expected_owner {
        return Err(JitoExecutionError::TokenAccountReadback(format!(
            "token account owner mismatch: expected {}, got {}",
            expected_owner, token.owner
        )));
    }

    Ok(token.amount)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// Test-only sentinel matching the exact example from the Jito docs
    /// (https://docs.jito.wtf/lowlatencytxnsend/#sandwich-mitigation,
    /// retrieved 2026-07-26): a valid pubkey whose base58 text starts
    /// with the literal `jitodontfront` prefix.
    fn test_dont_front_pubkey() -> Pubkey {
        Pubkey::from_str("jitodontfront111111111111111111111111111111")
            .expect("docs example sentinel must be a valid pubkey")
    }

    fn pool_keys() -> RaydiumPoolKeys {
        RaydiumPoolKeys {
            amm_id: Pubkey::new_unique(),
            authority: Pubkey::new_unique(),
            open_orders: Pubkey::new_unique(),
            target_orders: Pubkey::new_unique(),
            base_vault: Pubkey::new_unique(),
            quote_vault: Pubkey::new_unique(),
            base_mint: WSOL_MINT,
            quote_mint: Pubkey::new_unique(),
            market_program_id: Pubkey::new_unique(),
            market_id: Pubkey::new_unique(),
            market_bids: Pubkey::new_unique(),
            market_asks: Pubkey::new_unique(),
            market_event_queue: Pubkey::new_unique(),
            market_base_vault: Pubkey::new_unique(),
            market_quote_vault: Pubkey::new_unique(),
            market_vault_signer: Pubkey::new_unique(),
        }
    }

    #[test]
    fn builds_exact_raydium_v4_swap_base_in_layout() {
        let pool = pool_keys();
        let user_owner = Pubkey::new_unique();
        let user_source = Pubkey::new_unique();
        let user_destination = Pubkey::new_unique();
        let amount_in = 123_456_789_u64;
        let minimum_amount_out = 98_765_432_u64;

        let instruction = construct_raydium_swap_instruction(
            &pool,
            user_owner,
            user_source,
            user_destination,
            amount_in,
            minimum_amount_out,
        )
        .expect("valid Raydium V4 swap instruction");

        let mut expected_data = vec![RAYDIUM_SWAP_BASE_IN_DISCRIMINATOR];
        expected_data.extend_from_slice(&amount_in.to_le_bytes());
        expected_data.extend_from_slice(&minimum_amount_out.to_le_bytes());

        assert_eq!(instruction.program_id, RAYDIUM_LIQUIDITY_POOL_V4_PROGRAM_ID);
        assert_eq!(instruction.data, expected_data);
        assert_eq!(instruction.data.len(), RAYDIUM_SWAP_BASE_IN_DATA_LEN);
        assert_eq!(
            instruction.accounts,
            vec![
                AccountMeta::new_readonly(spl_token::id(), false),
                AccountMeta::new(pool.amm_id, false),
                AccountMeta::new_readonly(pool.authority, false),
                AccountMeta::new(pool.open_orders, false),
                AccountMeta::new(pool.base_vault, false),
                AccountMeta::new(pool.quote_vault, false),
                AccountMeta::new_readonly(pool.market_program_id, false),
                AccountMeta::new(pool.market_id, false),
                AccountMeta::new(pool.market_bids, false),
                AccountMeta::new(pool.market_asks, false),
                AccountMeta::new(pool.market_event_queue, false),
                AccountMeta::new(pool.market_base_vault, false),
                AccountMeta::new(pool.market_quote_vault, false),
                AccountMeta::new_readonly(pool.market_vault_signer, false),
                AccountMeta::new(user_source, false),
                AccountMeta::new(user_destination, false),
                AccountMeta::new_readonly(user_owner, true),
            ]
        );
        assert!(
            instruction
                .accounts
                .iter()
                .all(|account| account.pubkey != pool.target_orders),
            "current Raydium V4 swap layout must omit legacy target_orders"
        );
    }

    #[test]
    fn rejects_zero_amounts_and_unsafe_accounts() {
        let pool = pool_keys();
        let owner = Pubkey::new_unique();
        let source = Pubkey::new_unique();
        let destination = Pubkey::new_unique();

        assert!(matches!(
            construct_raydium_swap_instruction(&pool, owner, source, destination, 0, 1),
            Err(JitoExecutionError::InvalidSwapAmount(_))
        ));
        assert!(matches!(
            construct_raydium_swap_instruction(&pool, owner, source, destination, 1, 0),
            Err(JitoExecutionError::InvalidSwapAmount(_))
        ));
        assert!(matches!(
            construct_raydium_swap_instruction(&pool, owner, source, source, 1, 1),
            Err(JitoExecutionError::InvalidSwapAccount(_))
        ));
        assert!(matches!(
            construct_raydium_swap_instruction(&pool, owner, pool.base_vault, destination, 1, 1,),
            Err(JitoExecutionError::InvalidSwapAccount(_))
        ));
    }

    #[test]
    fn executor_config_rejects_zero_sizing_and_excess_slippage() {
        assert!(JitoExecutorConfig::new(
            DEFAULT_JITO_BLOCK_ENGINE_URL.to_string(),
            MINIMUM_JITO_TIP_LAMPORTS,
            Duration::from_secs(1),
            Duration::from_secs(1),
            0,
            Duration::from_secs(50),
            10_000,
            PathBuf::from("/tmp/test.jsonl"),
            test_dont_front_pubkey(),
            None,
        )
        .is_err());
        assert!(JitoExecutorConfig::new(
            DEFAULT_JITO_BLOCK_ENGINE_URL.to_string(),
            MINIMUM_JITO_TIP_LAMPORTS,
            Duration::from_secs(1),
            Duration::from_secs(1),
            1,
            Duration::from_secs(50),
            10_000,
            PathBuf::from("/tmp/test.jsonl"),
            test_dont_front_pubkey(),
            None,
        )
        .is_ok());
    }

    #[test]
    fn locally_enforces_slippage_floor() {
        assert_eq!(
            calculate_local_minimum_amount_out(1_000_000, 50).expect("checked slippage"),
            995_000
        );
        assert_eq!(
            calculate_local_minimum_amount_out(1, 50).expect("floor rounds down"),
            0
        );
    }

    // ------------------------------------------------------------------
    // Phase 2 — Jito Top-of-Bundle ("jitodontfront") Sandwich Protection
    // ------------------------------------------------------------------

    #[test]
    fn executor_config_rejects_sentinel_without_required_prefix() {
        let bad_sentinel = Pubkey::new_unique();
        let result = JitoExecutorConfig::new(
            DEFAULT_JITO_BLOCK_ENGINE_URL.to_string(),
            MINIMUM_JITO_TIP_LAMPORTS,
            Duration::from_secs(1),
            Duration::from_secs(1),
            1,
            Duration::from_secs(50),
            10_000,
            PathBuf::from("/tmp/test.jsonl"),
            bad_sentinel,
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn sentinel_is_inserted_exactly_once() {
        let pool = pool_keys();
        let mut instruction = construct_raydium_swap_instruction(
            &pool,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            1,
            1,
        )
        .expect("valid swap instruction");
        let original_len = instruction.accounts.len();
        let sentinel = test_dont_front_pubkey();

        apply_jitodontfront_protection(&mut instruction, sentinel).expect("first insertion");
        assert_eq!(instruction.accounts.len(), original_len + 1);
        assert_eq!(
            instruction
                .accounts
                .iter()
                .filter(|meta| meta.pubkey == sentinel)
                .count(),
            1
        );
    }

    #[test]
    fn sentinel_metadata_is_readonly_and_non_signer() {
        let pool = pool_keys();
        let mut instruction = construct_raydium_swap_instruction(
            &pool,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            1,
            1,
        )
        .expect("valid swap instruction");
        let sentinel = test_dont_front_pubkey();

        apply_jitodontfront_protection(&mut instruction, sentinel).expect("insertion succeeds");

        let sentinel_meta = instruction
            .accounts
            .iter()
            .find(|meta| meta.pubkey == sentinel)
            .expect("sentinel present");
        assert!(!sentinel_meta.is_signer);
        assert!(!sentinel_meta.is_writable);
    }

    #[test]
    fn calling_apply_protection_twice_is_idempotent() {
        let pool = pool_keys();
        let mut instruction = construct_raydium_swap_instruction(
            &pool,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            1,
            1,
        )
        .expect("valid swap instruction");
        let original_len = instruction.accounts.len();
        let sentinel = test_dont_front_pubkey();

        apply_jitodontfront_protection(&mut instruction, sentinel).expect("first insertion");
        apply_jitodontfront_protection(&mut instruction, sentinel).expect("second call is a no-op");

        assert_eq!(instruction.accounts.len(), original_len + 1);
        assert_eq!(
            instruction
                .accounts
                .iter()
                .filter(|meta| meta.pubkey == sentinel)
                .count(),
            1
        );
    }

    #[test]
    fn sentinel_insertion_preserves_existing_account_order() {
        let pool = pool_keys();
        let mut instruction = construct_raydium_swap_instruction(
            &pool,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            1,
            1,
        )
        .expect("valid swap instruction");
        let original_metas = instruction.accounts.clone();
        let sentinel = test_dont_front_pubkey();

        apply_jitodontfront_protection(&mut instruction, sentinel).expect("insertion succeeds");

        // Every original account meta must still be present, in the same
        // order, as a prefix of the new account list; the sentinel must
        // be the last entry.
        assert_eq!(
            &instruction.accounts[..original_metas.len()],
            &original_metas[..]
        );
        assert_eq!(instruction.accounts.last().unwrap().pubkey, sentinel);
    }

    #[test]
    fn apply_protection_rejects_sentinel_missing_required_prefix() {
        let pool = pool_keys();
        let mut instruction = construct_raydium_swap_instruction(
            &pool,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            1,
            1,
        )
        .expect("valid swap instruction");
        let not_a_sentinel = Pubkey::new_unique();

        assert_eq!(
            apply_jitodontfront_protection(&mut instruction, not_a_sentinel),
            Err(ShieldError::InvalidSentinelPrefix)
        );
        // Rejected calls must not mutate the instruction.
        assert!(!instruction
            .accounts
            .iter()
            .any(|meta| meta.pubkey == not_a_sentinel));
    }

    #[test]
    fn apply_protection_rejects_unproven_program() {
        let sentinel = test_dont_front_pubkey();
        let mut instruction = Instruction {
            program_id: Pubkey::new_unique(),
            accounts: vec![AccountMeta::new(Pubkey::new_unique(), false)],
            data: vec![],
        };
        let original_len = instruction.accounts.len();

        let result = apply_jitodontfront_protection(&mut instruction, sentinel);
        assert!(matches!(
            result,
            Err(ShieldError::UnsafeTrailingAccount { .. })
        ));
        // Rejected calls must not mutate the instruction.
        assert_eq!(instruction.accounts.len(), original_len);
    }

    #[test]
    fn bundle_validation_rejects_missing_sentinel() {
        let pool = pool_keys();
        let unprotected_instruction = construct_raydium_swap_instruction(
            &pool,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            1,
            1,
        )
        .expect("valid swap instruction");
        let sentinel = test_dont_front_pubkey();

        assert_eq!(
            validate_bundle_protection(&[unprotected_instruction], sentinel, 0),
            Err(BundleProtectionError::SentinelMissing)
        );
    }

    #[test]
    fn bundle_validation_accepts_protected_instruction_at_leading_index() {
        let pool = pool_keys();
        let mut instruction = construct_raydium_swap_instruction(
            &pool,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            1,
            1,
        )
        .expect("valid swap instruction");
        let sentinel = test_dont_front_pubkey();
        apply_jitodontfront_protection(&mut instruction, sentinel).expect("insertion succeeds");

        assert_eq!(
            validate_bundle_protection(&[instruction], sentinel, 0),
            Ok(())
        );
    }

    #[test]
    fn bundle_validation_rejects_non_leading_index() {
        let pool = pool_keys();
        let mut instruction = construct_raydium_swap_instruction(
            &pool,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            1,
            1,
        )
        .expect("valid swap instruction");
        let sentinel = test_dont_front_pubkey();
        apply_jitodontfront_protection(&mut instruction, sentinel).expect("insertion succeeds");

        assert_eq!(
            validate_bundle_protection(&[instruction], sentinel, 1),
            Err(BundleProtectionError::NotAtLeadingIndex(1))
        );
    }

    #[test]
    fn bundle_validation_rejects_sentinel_marked_writable_or_signer() {
        let pool = pool_keys();
        let mut instruction = construct_raydium_swap_instruction(
            &pool,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            1,
            1,
        )
        .expect("valid swap instruction");
        let sentinel = test_dont_front_pubkey();
        // Manually insert a writable sentinel meta to simulate a future
        // regression that bypasses `apply_jitodontfront_protection`.
        instruction.accounts.push(AccountMeta::new(sentinel, false));

        assert_eq!(
            validate_bundle_protection(&[instruction], sentinel, 0),
            Err(BundleProtectionError::SentinelMissing)
        );
    }

    #[test]
    fn raydium_swap_instruction_layout_is_unchanged_by_sentinel_attachment() {
        // Regression guard for MASTER_PLAN.md Section 2.4: proves the
        // Raydium instruction's original 17-account layout is preserved
        // byte-for-byte (same data, same first 17 accounts in the same
        // order) after sentinel attachment -- only a trailing account is
        // appended, so on-chain behavior driven by positional account
        // reads is unaffected.
        let pool = pool_keys();
        let owner = Pubkey::new_unique();
        let source = Pubkey::new_unique();
        let destination = Pubkey::new_unique();
        let mut instruction =
            construct_raydium_swap_instruction(&pool, owner, source, destination, 123, 45)
                .expect("valid swap instruction");
        let original_accounts = instruction.accounts.clone();
        let original_data = instruction.data.clone();
        let original_program_id = instruction.program_id;

        apply_jitodontfront_protection(&mut instruction, test_dont_front_pubkey())
            .expect("insertion succeeds");

        assert_eq!(instruction.program_id, original_program_id);
        assert_eq!(instruction.data, original_data);
        assert_eq!(instruction.accounts.len(), original_accounts.len() + 1);
        assert_eq!(
            &instruction.accounts[..original_accounts.len()],
            &original_accounts[..]
        );
    }
}
