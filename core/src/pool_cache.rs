use reqwest::{redirect::Policy, Client};
use serde::{de::DeserializeOwned, Deserialize};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    account::Account, address_lookup_table::AddressLookupTableAccount, program_pack::Pack, pubkey,
    pubkey::Pubkey,
};
use spl_token::state::{Account as TokenAccount, AccountState, Mint};
use std::{str::FromStr, time::Duration};
use thiserror::Error;

pub const RAYDIUM_LIQUIDITY_POOL_V4_PROGRAM_ID: Pubkey =
    pubkey!("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8");
pub const OPENBOOK_PROGRAM_ID: Pubkey = pubkey!("srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX");
pub const WSOL_MINT: Pubkey = pubkey!("So11111111111111111111111111111111111111112");

const RAYDIUM_POOL_LIST_URL: &str = "https://api-v3.raydium.io/pools/info/list-v2";
const RAYDIUM_POOL_KEYS_URL: &str = "https://api-v3.raydium.io/pools/key/ids";
const RAYDIUM_API_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_API_RESPONSE_BYTES: usize = 256 * 1024;
const AMM_INFO_LEN: usize = 752;
const AMM_AUTHORITY_SEED: &[u8] = b"amm authority";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaydiumPoolKeys {
    pub amm_id: Pubkey,
    pub authority: Pubkey,
    pub open_orders: Pubkey,
    pub target_orders: Pubkey,
    pub base_vault: Pubkey,
    pub quote_vault: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub market_program_id: Pubkey,
    pub market_id: Pubkey,
    pub market_bids: Pubkey,
    pub market_asks: Pubkey,
    pub market_event_queue: Pubkey,
    pub market_base_vault: Pubkey,
    pub market_quote_vault: Pubkey,
    pub market_vault_signer: Pubkey,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PoolKeyValidationError {
    #[error("Raydium pool key `{0}` must not be the default pubkey")]
    DefaultPubkey(&'static str),
}

#[derive(Debug, Error)]
pub enum PoolResolutionError {
    #[error("target mint is not a valid Solana pubkey: {0}")]
    InvalidTargetMint(String),

    #[error("target mint must differ from WSOL")]
    TargetIsWsol,

    #[error("failed to construct the Raydium API client: {0}")]
    Client(String),

    #[error("Raydium API request failed: {0}")]
    Http(String),

    #[error("Raydium API returned HTTP {0}")]
    HttpStatus(reqwest::StatusCode),

    #[error("Raydium API response exceeded {MAX_API_RESPONSE_BYTES} bytes")]
    ResponseTooLarge,

    #[error("Raydium API response was invalid: {0}")]
    InvalidResponse(String),

    #[error("Raydium API rejected the request: {0}")]
    ApiRejected(String),

    #[error("no liquid Raydium V4 WSOL pool was found for {0}")]
    PoolNotFound(Pubkey),

    #[error("Raydium V4 pool selection is ambiguous for {0}")]
    AmbiguousPool(Pubkey),

    #[error("Raydium API field `{field}` is not a valid pubkey: {reason}")]
    InvalidPubkey { field: &'static str, reason: String },

    #[error("Raydium API returned an unsupported account relationship: {0}")]
    ApiAccountMismatch(&'static str),

    #[error("Solana RPC pool verification failed: {0}")]
    Rpc(String),

    #[error("required on-chain account `{0}` does not exist")]
    MissingAccount(&'static str),

    #[error("on-chain account `{account}` has owner {actual}; expected {expected}")]
    InvalidAccountOwner {
        account: &'static str,
        actual: Pubkey,
        expected: Pubkey,
    },

    #[error("Raydium AMM state is invalid: {0}")]
    InvalidAmmState(&'static str),

    #[error("Raydium AMM state field `{0}` disagrees with the resolved pool keys")]
    AmmStateMismatch(&'static str),

    #[error("SPL token account `{0}` is invalid")]
    InvalidTokenAccount(&'static str),

    #[error("SPL mint account `{0}` is invalid")]
    InvalidMint(&'static str),

    #[error(transparent)]
    InvalidPoolKeys(#[from] PoolKeyValidationError),
}

pub type Result<T> = std::result::Result<T, PoolResolutionError>;

// ---------------------------------------------------------------------------
// Address Lookup Table support (AN-ALT-01)
// ---------------------------------------------------------------------------

/// Errors that can occur when fetching an on-chain Address Lookup Table.
#[derive(Debug, Error)]
pub enum AltFetchError {
    #[error("RPC error fetching ALT account {alt}: {detail}")]
    Rpc { alt: Pubkey, detail: String },

    #[error("ALT account {0} does not exist on-chain")]
    #[allow(dead_code)]
    NotFound(Pubkey),

    #[error("failed to deserialize ALT account {alt}: {detail}")]
    Deserialize { alt: Pubkey, detail: String },
}

/// Fetch and deserialize an Address Lookup Table from chain.
///
/// The `alt_address` is provided via configuration — ALT creation is a
/// one-time admin operation performed outside this daemon.
///
/// Returns an `AddressLookupTableAccount` ready to pass to
/// `v0::Message::try_compile` as the fourth argument.
pub async fn fetch_alt(
    rpc: &RpcClient,
    alt_address: Pubkey,
) -> std::result::Result<AddressLookupTableAccount, AltFetchError> {
    let account = rpc
        .get_account(&alt_address)
        .await
        .map_err(|e| AltFetchError::Rpc {
            alt: alt_address,
            detail: e.to_string(),
        })?;

    let table_state =
        solana_sdk::address_lookup_table::state::AddressLookupTable::deserialize(&account.data)
            .map_err(|e| AltFetchError::Deserialize {
                alt: alt_address,
                detail: e.to_string(),
            })?;

    Ok(AddressLookupTableAccount {
        key: alt_address,
        addresses: table_state.addresses.to_vec(),
    })
}

/// A resolved Raydium pool paired with its optional on-chain ALT.
///
/// `alt` is `None` when no ALT address is configured for this pool;
/// in that case the dispatcher falls back to a legacy `v0::Message`
/// compiled with an empty lookup-table slice.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct ResolvedPool {
    pub keys: RaydiumPoolKeys,
    pub alt: Option<AddressLookupTableAccount>,
}

impl RaydiumPoolKeys {
    pub fn validate(&self) -> std::result::Result<(), PoolKeyValidationError> {
        let keys = [
            ("amm_id", self.amm_id),
            ("authority", self.authority),
            ("open_orders", self.open_orders),
            ("target_orders", self.target_orders),
            ("base_vault", self.base_vault),
            ("quote_vault", self.quote_vault),
            ("base_mint", self.base_mint),
            ("quote_mint", self.quote_mint),
            ("market_program_id", self.market_program_id),
            ("market_id", self.market_id),
            ("market_bids", self.market_bids),
            ("market_asks", self.market_asks),
            ("market_event_queue", self.market_event_queue),
            ("market_base_vault", self.market_base_vault),
            ("market_quote_vault", self.market_quote_vault),
            ("market_vault_signer", self.market_vault_signer),
        ];

        for (name, key) in keys {
            if key == Pubkey::default() {
                return Err(PoolKeyValidationError::DefaultPubkey(name));
            }
        }

        Ok(())
    }
}

#[derive(Deserialize)]
struct ApiEnvelope<T> {
    success: bool,
    #[serde(default)]
    msg: String,
    data: Option<T>,
}

#[derive(Deserialize)]
struct PoolList {
    #[serde(default)]
    data: Vec<PoolSummary>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PoolSummary {
    id: String,
    program_id: String,
    mint_a: ApiMint,
    mint_b: ApiMint,
    tvl: f64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiMint {
    address: String,
    program_id: String,
}

#[derive(Deserialize)]
struct ApiVaults {
    #[serde(rename = "A")]
    base: String,
    #[serde(rename = "B")]
    quote: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiPoolKeys {
    id: String,
    program_id: String,
    authority: String,
    open_orders: String,
    target_orders: String,
    mint_a: ApiMint,
    mint_b: ApiMint,
    vault: ApiVaults,
    market_program_id: String,
    market_id: String,
    market_authority: String,
    market_base_vault: String,
    market_quote_vault: String,
    market_bids: String,
    market_asks: String,
    market_event_queue: String,
}

pub async fn resolve_pool_keys(
    rpc_client: &RpcClient,
    target_mint: &str,
) -> Result<RaydiumPoolKeys> {
    let target_mint = Pubkey::from_str(target_mint)
        .map_err(|error| PoolResolutionError::InvalidTargetMint(error.to_string()))?;
    if target_mint == WSOL_MINT {
        return Err(PoolResolutionError::TargetIsWsol);
    }

    let client = Client::builder()
        .https_only(true)
        .redirect(Policy::none())
        .timeout(RAYDIUM_API_TIMEOUT)
        .build()
        .map_err(|error| PoolResolutionError::Client(error.to_string()))?;

    let mut pair = [WSOL_MINT.to_string(), target_mint.to_string()];
    pair.sort();
    let list_query = [
        ("size", "20".to_string()),
        ("mint1", pair[0].clone()),
        ("mint2", pair[1].clone()),
        ("poolType", "Standard".to_string()),
        ("sortField", "liquidity".to_string()),
        ("sortType", "desc".to_string()),
    ];
    let pool_list: PoolList = get_api_data(&client, RAYDIUM_POOL_LIST_URL, &list_query).await?;

    let mut candidates: Vec<PoolSummary> = pool_list
        .data
        .into_iter()
        .filter(|pool| {
            pool.program_id == RAYDIUM_LIQUIDITY_POOL_V4_PROGRAM_ID.to_string()
                && pool.tvl.is_finite()
                && pool.tvl > 0.0
                && mint_pair_matches(&pool.mint_a.address, &pool.mint_b.address, target_mint)
        })
        .collect();
    candidates.sort_by(|left, right| {
        right
            .tvl
            .total_cmp(&left.tvl)
            .then_with(|| left.id.cmp(&right.id))
    });

    let selected = candidates
        .first()
        .ok_or(PoolResolutionError::PoolNotFound(target_mint))?;
    if candidates
        .get(1)
        .is_some_and(|candidate| candidate.tvl == selected.tvl)
    {
        return Err(PoolResolutionError::AmbiguousPool(target_mint));
    }

    let keys_query = [("ids", selected.id.clone())];
    let mut key_records: Vec<ApiPoolKeys> =
        get_api_data(&client, RAYDIUM_POOL_KEYS_URL, &keys_query).await?;
    if key_records.len() != 1 {
        return Err(PoolResolutionError::InvalidResponse(
            "pool-key lookup must return exactly one record".to_string(),
        ));
    }
    let record = key_records
        .pop()
        .ok_or_else(|| PoolResolutionError::InvalidResponse("missing pool keys".to_string()))?;

    let pool_keys = parse_api_pool_keys(record, &selected.id, target_mint)?;
    pool_keys.validate()?;
    verify_pool_keys_on_chain(rpc_client, &pool_keys).await?;
    Ok(pool_keys)
}

async fn get_api_data<T: DeserializeOwned>(
    client: &Client,
    url: &str,
    query: &[(&str, String)],
) -> Result<T> {
    let response = client
        .get(url)
        .query(query)
        .send()
        .await
        .map_err(|error| PoolResolutionError::Http(error.to_string()))?;
    let status = response.status();
    if !status.is_success() {
        return Err(PoolResolutionError::HttpStatus(status));
    }

    let bytes = response
        .bytes()
        .await
        .map_err(|error| PoolResolutionError::Http(error.to_string()))?;
    if bytes.len() > MAX_API_RESPONSE_BYTES {
        return Err(PoolResolutionError::ResponseTooLarge);
    }

    let envelope: ApiEnvelope<T> = serde_json::from_slice(&bytes)
        .map_err(|error| PoolResolutionError::InvalidResponse(error.to_string()))?;
    if !envelope.success {
        return Err(PoolResolutionError::ApiRejected(envelope.msg));
    }
    envelope.data.ok_or_else(|| {
        PoolResolutionError::InvalidResponse("successful response contained no data".to_string())
    })
}

fn mint_pair_matches(base: &str, quote: &str, target_mint: Pubkey) -> bool {
    (base == WSOL_MINT.to_string() && quote == target_mint.to_string())
        || (quote == WSOL_MINT.to_string() && base == target_mint.to_string())
}

fn parse_api_pool_keys(
    record: ApiPoolKeys,
    selected_id: &str,
    target_mint: Pubkey,
) -> Result<RaydiumPoolKeys> {
    if record.id != selected_id {
        return Err(PoolResolutionError::ApiAccountMismatch("pool id"));
    }
    if record.program_id != RAYDIUM_LIQUIDITY_POOL_V4_PROGRAM_ID.to_string() {
        return Err(PoolResolutionError::ApiAccountMismatch(
            "Raydium V4 program id",
        ));
    }
    if record.market_program_id != OPENBOOK_PROGRAM_ID.to_string() {
        return Err(PoolResolutionError::ApiAccountMismatch(
            "OpenBook program id",
        ));
    }
    if !mint_pair_matches(&record.mint_a.address, &record.mint_b.address, target_mint) {
        return Err(PoolResolutionError::ApiAccountMismatch("mint pair"));
    }
    if record.mint_a.program_id != spl_token::id().to_string()
        || record.mint_b.program_id != spl_token::id().to_string()
    {
        return Err(PoolResolutionError::ApiAccountMismatch(
            "legacy SPL Token program",
        ));
    }

    Ok(RaydiumPoolKeys {
        amm_id: parse_pubkey("id", &record.id)?,
        authority: parse_pubkey("authority", &record.authority)?,
        open_orders: parse_pubkey("openOrders", &record.open_orders)?,
        target_orders: parse_pubkey("targetOrders", &record.target_orders)?,
        base_vault: parse_pubkey("vault.A", &record.vault.base)?,
        quote_vault: parse_pubkey("vault.B", &record.vault.quote)?,
        base_mint: parse_pubkey("mintA.address", &record.mint_a.address)?,
        quote_mint: parse_pubkey("mintB.address", &record.mint_b.address)?,
        market_program_id: parse_pubkey("marketProgramId", &record.market_program_id)?,
        market_id: parse_pubkey("marketId", &record.market_id)?,
        market_bids: parse_pubkey("marketBids", &record.market_bids)?,
        market_asks: parse_pubkey("marketAsks", &record.market_asks)?,
        market_event_queue: parse_pubkey("marketEventQueue", &record.market_event_queue)?,
        market_base_vault: parse_pubkey("marketBaseVault", &record.market_base_vault)?,
        market_quote_vault: parse_pubkey("marketQuoteVault", &record.market_quote_vault)?,
        market_vault_signer: parse_pubkey("marketAuthority", &record.market_authority)?,
    })
}

fn parse_pubkey(field: &'static str, value: &str) -> Result<Pubkey> {
    Pubkey::from_str(value).map_err(|error| PoolResolutionError::InvalidPubkey {
        field,
        reason: error.to_string(),
    })
}

async fn verify_pool_keys_on_chain(rpc_client: &RpcClient, keys: &RaydiumPoolKeys) -> Result<()> {
    let addresses = [
        keys.amm_id,
        keys.open_orders,
        keys.target_orders,
        keys.base_vault,
        keys.quote_vault,
        keys.market_id,
        keys.market_bids,
        keys.market_asks,
        keys.market_event_queue,
        keys.market_base_vault,
        keys.market_quote_vault,
        keys.base_mint,
        keys.quote_mint,
    ];
    let accounts = rpc_client
        .get_multiple_accounts(&addresses)
        .await
        .map_err(|_| PoolResolutionError::Rpc("getMultipleAccounts failed".to_string()))?;

    let amm = required_account(&accounts, 0, "amm_id")?;
    require_owner("amm_id", amm, RAYDIUM_LIQUIDITY_POOL_V4_PROGRAM_ID)?;
    verify_amm_state(&amm.data, keys)?;

    require_owner(
        "open_orders",
        required_account(&accounts, 1, "open_orders")?,
        keys.market_program_id,
    )?;
    require_owner(
        "target_orders",
        required_account(&accounts, 2, "target_orders")?,
        RAYDIUM_LIQUIDITY_POOL_V4_PROGRAM_ID,
    )?;
    require_owner(
        "market_id",
        required_account(&accounts, 5, "market_id")?,
        keys.market_program_id,
    )?;
    for (index, name) in [
        (6, "market_bids"),
        (7, "market_asks"),
        (8, "market_event_queue"),
    ] {
        require_owner(
            name,
            required_account(&accounts, index, name)?,
            keys.market_program_id,
        )?;
    }

    verify_mint(required_account(&accounts, 11, "base_mint")?, "base_mint")?;
    verify_mint(required_account(&accounts, 12, "quote_mint")?, "quote_mint")?;
    verify_token_account(
        required_account(&accounts, 3, "base_vault")?,
        "base_vault",
        keys.base_mint,
        keys.authority,
    )?;
    verify_token_account(
        required_account(&accounts, 4, "quote_vault")?,
        "quote_vault",
        keys.quote_mint,
        keys.authority,
    )?;
    verify_token_account(
        required_account(&accounts, 9, "market_base_vault")?,
        "market_base_vault",
        keys.base_mint,
        keys.market_vault_signer,
    )?;
    verify_token_account(
        required_account(&accounts, 10, "market_quote_vault")?,
        "market_quote_vault",
        keys.quote_mint,
        keys.market_vault_signer,
    )?;
    Ok(())
}

fn required_account<'a>(
    accounts: &'a [Option<Account>],
    index: usize,
    name: &'static str,
) -> Result<&'a Account> {
    accounts
        .get(index)
        .and_then(Option::as_ref)
        .ok_or(PoolResolutionError::MissingAccount(name))
}

fn require_owner(name: &'static str, account: &Account, expected: Pubkey) -> Result<()> {
    if account.owner != expected {
        return Err(PoolResolutionError::InvalidAccountOwner {
            account: name,
            actual: account.owner,
            expected,
        });
    }
    Ok(())
}

fn verify_mint(account: &Account, name: &'static str) -> Result<()> {
    require_owner(name, account, spl_token::id())?;
    let mint = Mint::unpack(&account.data).map_err(|_| PoolResolutionError::InvalidMint(name))?;
    if !mint.is_initialized {
        return Err(PoolResolutionError::InvalidMint(name));
    }
    Ok(())
}

fn verify_token_account(
    account: &Account,
    name: &'static str,
    expected_mint: Pubkey,
    expected_owner: Pubkey,
) -> Result<()> {
    require_owner(name, account, spl_token::id())?;
    let token = TokenAccount::unpack(&account.data)
        .map_err(|_| PoolResolutionError::InvalidTokenAccount(name))?;
    if token.state != AccountState::Initialized
        || token.mint != expected_mint
        || token.owner != expected_owner
    {
        return Err(PoolResolutionError::InvalidTokenAccount(name));
    }
    Ok(())
}

fn verify_amm_state(data: &[u8], keys: &RaydiumPoolKeys) -> Result<()> {
    if data.len() != AMM_INFO_LEN {
        return Err(PoolResolutionError::InvalidAmmState(
            "unexpected AMM account length",
        ));
    }
    let status = read_u64(data, 0)?;
    if !matches!(status, 1 | 6) {
        return Err(PoolResolutionError::InvalidAmmState(
            "pool status does not permit swaps",
        ));
    }

    let nonce = u8::try_from(read_u64(data, 8)?)
        .map_err(|_| PoolResolutionError::InvalidAmmState("authority nonce exceeds u8"))?;
    let nonce_seed = [nonce];
    let authority = Pubkey::create_program_address(
        &[AMM_AUTHORITY_SEED, &nonce_seed],
        &RAYDIUM_LIQUIDITY_POOL_V4_PROGRAM_ID,
    )
    .map_err(|_| PoolResolutionError::InvalidAmmState("invalid authority PDA nonce"))?;
    if authority != keys.authority {
        return Err(PoolResolutionError::AmmStateMismatch("authority"));
    }

    for (name, offset, expected) in [
        ("base_vault", 336, keys.base_vault),
        ("quote_vault", 368, keys.quote_vault),
        ("base_mint", 400, keys.base_mint),
        ("quote_mint", 432, keys.quote_mint),
        ("open_orders", 496, keys.open_orders),
        ("market_id", 528, keys.market_id),
        ("market_program_id", 560, keys.market_program_id),
        ("target_orders", 592, keys.target_orders),
    ] {
        if read_pubkey(data, offset)? != expected {
            return Err(PoolResolutionError::AmmStateMismatch(name));
        }
    }
    Ok(())
}

fn read_u64(data: &[u8], offset: usize) -> Result<u64> {
    let bytes: [u8; 8] = data
        .get(offset..offset + 8)
        .and_then(|slice| slice.try_into().ok())
        .ok_or(PoolResolutionError::InvalidAmmState("truncated u64 field"))?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_pubkey(data: &[u8], offset: usize) -> Result<Pubkey> {
    let bytes: [u8; 32] = data
        .get(offset..offset + 32)
        .and_then(|slice| slice.try_into().ok())
        .ok_or(PoolResolutionError::InvalidAmmState(
            "truncated pubkey field",
        ))?;
    Ok(Pubkey::new_from_array(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

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
            market_program_id: OPENBOOK_PROGRAM_ID,
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
    fn rejects_default_pool_keys() {
        let mut keys = pool_keys();
        keys.amm_id = Pubkey::default();

        assert_eq!(
            keys.validate(),
            Err(PoolKeyValidationError::DefaultPubkey("amm_id"))
        );
    }

    #[test]
    fn verifies_exact_amm_v4_layout() {
        let nonce = 254_u8;
        let authority = Pubkey::create_program_address(
            &[AMM_AUTHORITY_SEED, &[nonce]],
            &RAYDIUM_LIQUIDITY_POOL_V4_PROGRAM_ID,
        )
        .expect("valid test PDA");
        let mut keys = pool_keys();
        keys.authority = authority;
        let mut data = vec![0_u8; AMM_INFO_LEN];
        data[0..8].copy_from_slice(&6_u64.to_le_bytes());
        data[8..16].copy_from_slice(&(nonce as u64).to_le_bytes());
        for (offset, key) in [
            (336, keys.base_vault),
            (368, keys.quote_vault),
            (400, keys.base_mint),
            (432, keys.quote_mint),
            (496, keys.open_orders),
            (528, keys.market_id),
            (560, keys.market_program_id),
            (592, keys.target_orders),
        ] {
            data[offset..offset + 32].copy_from_slice(key.as_ref());
        }

        verify_amm_state(&data, &keys).expect("valid AMM state");
        data[336] ^= 1;
        assert!(matches!(
            verify_amm_state(&data, &keys),
            Err(PoolResolutionError::AmmStateMismatch("base_vault"))
        ));
    }

    #[test]
    fn deserializes_official_pool_list_and_key_envelopes() {
        let keys = pool_keys();
        let list_fixture = format!(
            r#"{{
                "success":true,
                "data":{{
                    "count":1,
                    "hasNextPage":false,
                    "data":[{{
                        "id":"{}",
                        "programId":"{}",
                        "mintA":{{"address":"{}","programId":"{}"}},
                        "mintB":{{"address":"{}","programId":"{}"}},
                        "tvl":1234.5
                    }}]
                }}
            }}"#,
            keys.amm_id,
            RAYDIUM_LIQUIDITY_POOL_V4_PROGRAM_ID,
            keys.base_mint,
            spl_token::id(),
            keys.quote_mint,
            spl_token::id(),
        );
        let list: ApiEnvelope<PoolList> =
            serde_json::from_str(&list_fixture).expect("official pool-list shape");
        let summaries = list.data.expect("pool-list data").data;
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].id, keys.amm_id.to_string());

        let keys_fixture = format!(
            r#"{{
                "success":true,
                "data":[{{
                    "id":"{}",
                    "programId":"{}",
                    "authority":"{}",
                    "openOrders":"{}",
                    "targetOrders":"{}",
                    "mintA":{{"address":"{}","programId":"{}"}},
                    "mintB":{{"address":"{}","programId":"{}"}},
                    "vault":{{"A":"{}","B":"{}"}},
                    "marketProgramId":"{}",
                    "marketId":"{}",
                    "marketAuthority":"{}",
                    "marketBaseVault":"{}",
                    "marketQuoteVault":"{}",
                    "marketBids":"{}",
                    "marketAsks":"{}",
                    "marketEventQueue":"{}"
                }}]
            }}"#,
            keys.amm_id,
            RAYDIUM_LIQUIDITY_POOL_V4_PROGRAM_ID,
            keys.authority,
            keys.open_orders,
            keys.target_orders,
            keys.base_mint,
            spl_token::id(),
            keys.quote_mint,
            spl_token::id(),
            keys.base_vault,
            keys.quote_vault,
            OPENBOOK_PROGRAM_ID,
            keys.market_id,
            keys.market_vault_signer,
            keys.market_base_vault,
            keys.market_quote_vault,
            keys.market_bids,
            keys.market_asks,
            keys.market_event_queue,
        );
        let records: ApiEnvelope<Vec<ApiPoolKeys>> =
            serde_json::from_str(&keys_fixture).expect("official pool-key shape");
        let parsed = parse_api_pool_keys(
            records.data.expect("pool-key data").remove(0),
            &keys.amm_id.to_string(),
            keys.quote_mint,
        )
        .expect("valid API pool keys");
        assert_eq!(parsed, keys);
    }

    #[test]
    fn rejects_waiting_trade_without_time_validation() {
        let nonce = 254_u8;
        let authority = Pubkey::create_program_address(
            &[AMM_AUTHORITY_SEED, &[nonce]],
            &RAYDIUM_LIQUIDITY_POOL_V4_PROGRAM_ID,
        )
        .expect("valid test PDA");
        let mut keys = pool_keys();
        keys.authority = authority;
        let mut data = vec![0_u8; AMM_INFO_LEN];
        data[0..8].copy_from_slice(&7_u64.to_le_bytes());
        data[8..16].copy_from_slice(&(nonce as u64).to_le_bytes());

        assert!(matches!(
            verify_amm_state(&data, &keys),
            Err(PoolResolutionError::InvalidAmmState(_))
        ));
    }
}
