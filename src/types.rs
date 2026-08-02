use solana_sdk::signature::Signature;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SwapEvent {
    pub target_mint: String,
    pub pool_id: String,
    /// Target-token raw units. Ingestion normalizes this regardless of the
    /// pool's on-chain base/quote ordering.
    pub base_amount: u64,
    /// WSOL lamports. Ingestion normalizes this regardless of the pool's
    /// on-chain base/quote ordering.
    pub quote_amount: u64,
    pub timestamp_ms: u64,
    pub source_signature: Signature,
    pub source_slot: u64,
    pub outer_instruction_index: u8,
    pub inner_instruction_index: Option<u8>,
    pub stream_epoch: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WhaleSignal {
    pub target_mint: String,
    pub whale_wallet: String,
    pub trade_size_sol: f64,
    pub timestamp_ms: u64,
}


