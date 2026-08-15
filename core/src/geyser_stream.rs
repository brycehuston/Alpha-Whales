use bs58;
use log::{error, info};
use std::collections::HashMap;
use tokio::sync::mpsc;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestFilterTransactions,
};

const PUMP_FUN_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfX9MLnqiX+";

// Anchor discriminators for Pump.fun
// sha256("global:create")[..8] and sha256("global:create_v2")[..8]
const GLOBAL_CREATE: [u8; 8] = [24, 30, 200, 40, 5, 28, 7, 119];
const GLOBAL_CREATE_V2: [u8; 8] = [214, 144, 76, 236, 95, 139, 49, 180];

pub async fn run_geyser_stream(
    endpoint: String,
    x_token: Option<String>,
    mint_tx: mpsc::Sender<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut client = GeyserGrpcClient::build_from_shared(endpoint)?
        .x_token(x_token)?
        .connect()
        .await?;

    let mut transactions = HashMap::new();
    transactions.insert(
        "pump_fun_mints".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            signature: None,
            account_include: vec![PUMP_FUN_PROGRAM_ID.to_string()],
            account_exclude: vec![],
            account_required: vec![],
        },
    );

    let request = SubscribeRequest {
        transactions,
        ..Default::default()
    };

    let (mut subscribe_tx, mut stream) = client.subscribe().await?;
    subscribe_tx.send(request).await?;

    info!("Geyser stream subscribed to Pump.fun program ID: {}", PUMP_FUN_PROGRAM_ID);

    while let Some(message) = stream.message().await? {
        if let Some(update) = message.update_oneof {
            if let UpdateOneof::Transaction(tx) = update {
                if let Some(transaction) = tx.transaction {
                    if let Some(tx_inner) = transaction.transaction {
                        if let Some(message_inner) = tx_inner.message {
                            let account_keys = message_inner.account_keys;

                            for ix in message_inner.instructions {
                                let program_id_index = ix.program_id_index as usize;
                                if program_id_index < account_keys.len() {
                                    let program_id = bs58::encode(&account_keys[program_id_index]).into_string();

                                    if program_id == PUMP_FUN_PROGRAM_ID {
                                        if ix.data.len() >= 8 {
                                            let discriminator: [u8; 8] = ix.data[0..8].try_into().unwrap_or([0; 8]);
                                            
                                            if discriminator == GLOBAL_CREATE || discriminator == GLOBAL_CREATE_V2 {
                                                if !ix.accounts.is_empty() {
                                                    // For pump.fun create, the mint address is at index 0 of the instruction accounts
                                                    let mint_index = ix.accounts[0] as usize;
                                                    if mint_index < account_keys.len() {
                                                        let mint = bs58::encode(&account_keys[mint_index]).into_string();

                                                        if let Err(e) = mint_tx.send(mint.clone()).await {
                                                            error!("Failed to route mint {} to execution channel: {}", mint, e);
                                                        } else {
                                                            info!("Sniped Slot-0 Pump.fun Mint: {}", mint);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
